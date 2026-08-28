use crate::composer::{self, Composer, PastedImage, Submitted};
use borg_dictation::{DictationUpdate, DictationWorker};
use borg_ui::{
    ApprovalDecision, CodingProvider, FrontendCommand, PromptDelivery, ResponseLanguage,
    SessionView,
    local::{LocalSessionOption, LocalSessionUpdate, LocalSessionWorker},
    palette,
    timeline::{TimelineEntry, TimelineKind},
};
use gpui::{
    App, Application, Bounds, Context, Entity, Focusable, FontWeight, KeyBinding, ListAlignment,
    ListState, PathPromptOptions, SharedString, Window, WindowBounds, WindowOptions, div, list,
    prelude::*, px, rgb, size,
};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use uuid::Uuid;

gpui::actions!(borg_gui, [Interrupt, Escape, ToggleHelp]);

struct BorgGui {
    worker: Option<LocalSessionWorker>,
    view: Option<SessionView>,
    composer: Entity<Composer>,
    error: Option<String>,
    delivery: PromptDelivery,
    root_session_id: Option<Uuid>,
    timeline: Arc<Vec<Arc<TimelineEntry>>>,
    transcript_state: ListState,
    attachments: Vec<PathBuf>,
    sessions: Vec<LocalSessionOption>,
    sessions_open: bool,
    dictation: Option<DictationWorker>,
    dictation_status: SharedString,
    help_open: bool,
    expanded_entries: HashSet<String>,
    temporary_attachments: HashSet<PathBuf>,
}

impl BorgGui {
    fn new(
        worker: Option<LocalSessionWorker>,
        composer: Entity<Composer>,
        cx: &mut Context<Self>,
    ) -> Self {
        cx.subscribe(&composer, |this, _, event: &Submitted, cx| {
            if let Some((kind, payload)) = this.view.as_ref().and_then(|view| {
                Some((
                    view.state.pending_provider_interaction_kind.clone()?,
                    view.state.pending_provider_interaction_payload.clone()?,
                ))
            }) {
                if !this.attachments.is_empty() {
                    this.error = Some("Provider input responses cannot include attachments".into());
                } else {
                    match borg_ui::provider_interaction_response(&kind, &payload, &event.0) {
                        Ok(response) => {
                            this.send(FrontendCommand::RespondToProviderInteraction(response))
                        }
                        Err(error) => this.error = Some(error.to_string()),
                    }
                }
                cx.notify();
                return;
            }
            match event.0.trim() {
                "/copy" => {
                    if let Some(entry) = this
                        .timeline
                        .iter()
                        .rev()
                        .find(|entry| entry.kind == TimelineKind::Assistant)
                    {
                        cx.write_to_clipboard(gpui::ClipboardItem::new_string(entry.body.clone()));
                    } else {
                        this.error = Some("There is no assistant response to copy".into());
                    }
                    cx.notify();
                    return;
                }
                "/dictate" => {
                    match this.dictation.as_ref() {
                        Some(dictation) => {
                            if let Err(error) = dictation.toggle() {
                                this.error = Some(error.to_string());
                            }
                        }
                        None => this.error = Some("Dictation is unavailable".into()),
                    }
                    cx.notify();
                    return;
                }
                "/resume" => {
                    this.sessions_open = true;
                    cx.notify();
                    return;
                }
                "/help" | "/settings" => {
                    this.help_open = true;
                    cx.notify();
                    return;
                }
                "/goal" | "/goal view" | "/todo" | "/todos" | "/todo view" | "/todos view"
                | "/usage" | "/status" => {
                    this.help_open = true;
                    cx.notify();
                    return;
                }
                _ => {}
            }
            let provider = this
                .view
                .as_ref()
                .and_then(|view| view.state.configuration.as_ref())
                .map(|configuration| configuration.provider)
                .unwrap_or(CodingProvider::Codex);
            let todos = this
                .view
                .as_ref()
                .map(|view| view.state.todos.as_slice())
                .unwrap_or_default();
            match borg_ui::parse_submission(&event.0, provider, this.delivery, todos) {
                Ok(FrontendCommand::SubmitPrompt { text, delivery, .. }) => {
                    let attachments = std::mem::take(&mut this.attachments);
                    this.send(FrontendCommand::SubmitPrompt {
                        text,
                        attachments,
                        delivery,
                    });
                }
                Ok(FrontendCommand::ControlPeer {
                    target,
                    intent,
                    delivery,
                    ..
                }) => {
                    let attachments = std::mem::take(&mut this.attachments);
                    this.send(FrontendCommand::ControlPeer {
                        target,
                        intent,
                        attachments,
                        delivery,
                    });
                }
                Ok(command) => this.send(command),
                Err(error) => this.error = Some(error.to_string()),
            }
            cx.notify();
        })
        .detach();
        cx.subscribe(&composer, |_this, _, event: &PastedImage, cx| {
            let extension = match event.0.format {
                gpui::ImageFormat::Png => "png",
                gpui::ImageFormat::Jpeg => "jpg",
                gpui::ImageFormat::Webp => "webp",
                gpui::ImageFormat::Gif => "gif",
                gpui::ImageFormat::Svg => "svg",
                gpui::ImageFormat::Bmp => "bmp",
                gpui::ImageFormat::Tiff => "tiff",
            };
            let directory = std::env::temp_dir().join("borg-gui-attachments");
            let path = directory.join(format!("{}.{}", Uuid::new_v4(), extension));
            let bytes = event.0.bytes.clone();
            let write = cx.background_executor().spawn(async move {
                std::fs::create_dir_all(&directory)
                    .and_then(|_| std::fs::write(&path, bytes))
                    .map(|_| path)
            });
            cx.spawn(async move |this, cx| {
                let result = write.await;
                let _ = this.update(cx, |this, cx| {
                    match result {
                        Ok(path) => {
                            this.temporary_attachments.insert(path.clone());
                            this.attachments.push(path);
                        }
                        Err(error) => {
                            this.error = Some(format!("Could not save pasted image: {error}"))
                        }
                    }
                    cx.notify();
                });
            })
            .detach();
        })
        .detach();
        let updates = worker.as_ref().map(LocalSessionWorker::updates);
        let dictation = DictationWorker::start().ok();
        let dictation_updates = dictation.as_ref().map(DictationWorker::updates);
        let mut this = Self {
            worker,
            view: None,
            composer,
            error: None,
            delivery: PromptDelivery::Steer,
            root_session_id: None,
            timeline: Arc::new(Vec::new()),
            transcript_state: ListState::new(0, ListAlignment::Bottom, px(320.)),
            attachments: Vec::new(),
            sessions: Vec::new(),
            sessions_open: false,
            dictation,
            dictation_status: "dictate".into(),
            help_open: false,
            expanded_entries: HashSet::new(),
            temporary_attachments: HashSet::new(),
        };
        if let Some(updates) = updates {
            this.schedule_updates(updates, cx);
        }
        if let Some(updates) = dictation_updates {
            this.schedule_dictation(updates, cx);
        }
        this
    }

    fn schedule_updates(
        &mut self,
        updates: async_channel::Receiver<LocalSessionUpdate>,
        cx: &mut Context<Self>,
    ) {
        cx.spawn(async move |this, cx| {
            while let Ok(update) = updates.recv().await {
                if this
                    .update(cx, |this, cx| {
                        match update {
                            LocalSessionUpdate::Presentation(presentation) => {
                                let view = presentation.view;
                                this.root_session_id = Some(presentation.root_session_id);
                                if this
                                    .view
                                    .as_ref()
                                    .is_none_or(|current| current.session_id != view.session_id)
                                {
                                    this.transcript_state.reset(presentation.timeline.len());
                                    this.expanded_entries.clear();
                                } else {
                                    let common_prefix = this
                                        .timeline
                                        .iter()
                                        .zip(presentation.timeline.iter())
                                        .take_while(|(old, new)| Arc::ptr_eq(old, new))
                                        .count();
                                    this.transcript_state.splice(
                                        common_prefix..this.timeline.len(),
                                        presentation.timeline.len() - common_prefix,
                                    );
                                }
                                this.timeline = presentation.timeline;
                                let secret = view
                                    .state
                                    .pending_provider_interaction_payload
                                    .as_ref()
                                    .is_some_and(borg_ui::provider_interaction_contains_secret);
                                this.composer
                                    .update(cx, |composer, cx| composer.set_secret(secret, cx));
                                this.view = Some(view);
                            }
                            LocalSessionUpdate::Sessions(sessions) => this.sessions = sessions,
                            LocalSessionUpdate::RestoreComposer { text, attachments } => {
                                this.composer
                                    .update(cx, |composer, cx| composer.append_recalled(&text, cx));
                                this.attachments.extend(attachments);
                            }
                            LocalSessionUpdate::Error(error) => this.error = Some(error),
                        }
                        cx.notify();
                    })
                    .is_err()
                {
                    break;
                }
            }
        })
        .detach();
    }

    fn schedule_dictation(
        &mut self,
        updates: async_channel::Receiver<DictationUpdate>,
        cx: &mut Context<Self>,
    ) {
        cx.spawn(async move |this, cx| {
            while let Ok(update) = updates.recv().await {
                if this
                    .update(cx, |this, cx| {
                        match update {
                            DictationUpdate::Preparing => {
                                this.dictation_status = "preparing…".into()
                            }
                            DictationUpdate::Recording => {
                                this.dictation_status = "recording".into()
                            }
                            DictationUpdate::Transcribing => {
                                this.dictation_status = "transcribing…".into()
                            }
                            DictationUpdate::Transcript(text) => {
                                this.dictation_status = "dictate".into();
                                this.composer
                                    .update(cx, |composer, cx| composer.append_text(&text, cx));
                            }
                            DictationUpdate::Error(error) => {
                                this.dictation_status = "dictate".into();
                                this.error = Some(error);
                            }
                        }
                        cx.notify();
                    })
                    .is_err()
                {
                    break;
                }
            }
        })
        .detach();
    }

    fn send(&mut self, command: FrontendCommand) {
        if let Some(worker) = &self.worker
            && let Err(error) = worker.send(command)
        {
            self.error = Some(error.to_string());
        }
    }
    fn interrupt(&mut self, _: &gpui::ClickEvent, _: &mut Window, cx: &mut Context<Self>) {
        self.send(FrontendCommand::Interrupt);
        cx.notify();
    }
    fn interrupt_action(&mut self, _: &Interrupt, _: &mut Window, cx: &mut Context<Self>) {
        self.send(FrontendCommand::Interrupt);
        cx.notify();
    }
    fn escape_action(&mut self, _: &Escape, _: &mut Window, cx: &mut Context<Self>) {
        if self.help_open {
            self.help_open = false;
        } else if self.sessions_open {
            self.sessions_open = false;
        } else {
            self.send(FrontendCommand::Interrupt);
        }
        cx.notify();
    }
    fn toggle_help_action(&mut self, _: &ToggleHelp, _: &mut Window, cx: &mut Context<Self>) {
        self.help_open = !self.help_open;
        cx.notify();
    }
    fn toggle_help(&mut self, _: &gpui::ClickEvent, _: &mut Window, cx: &mut Context<Self>) {
        self.help_open = !self.help_open;
        cx.notify();
    }
    fn approve_once(&mut self, _: &gpui::ClickEvent, _: &mut Window, cx: &mut Context<Self>) {
        self.send(FrontendCommand::Approve(ApprovalDecision::AllowOnce));
        cx.notify();
    }
    fn deny(&mut self, _: &gpui::ClickEvent, _: &mut Window, cx: &mut Context<Self>) {
        self.send(FrontendCommand::Approve(ApprovalDecision::Deny));
        cx.notify();
    }
    fn approve_session(&mut self, _: &gpui::ClickEvent, _: &mut Window, cx: &mut Context<Self>) {
        self.send(FrontendCommand::Approve(ApprovalDecision::AllowSession));
        cx.notify();
    }
    fn cancel_provider_input(
        &mut self,
        _: &gpui::ClickEvent,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let Some(kind) = self
            .view
            .as_ref()
            .and_then(|view| view.state.pending_provider_interaction_kind.as_deref())
        {
            self.send(FrontendCommand::RespondToProviderInteraction(
                borg_ui::cancelled_provider_interaction_response(kind),
            ));
            cx.notify();
        }
    }
    fn toggle_delivery(&mut self, _: &gpui::ClickEvent, _: &mut Window, cx: &mut Context<Self>) {
        self.delivery = match self.delivery {
            PromptDelivery::Steer => PromptDelivery::Queue,
            PromptDelivery::Queue => PromptDelivery::Steer,
        };
        cx.notify();
    }
    fn edit_model(&mut self, _: &gpui::ClickEvent, window: &mut Window, cx: &mut Context<Self>) {
        self.composer
            .update(cx, |composer, cx| composer.set_text("/model ", cx));
        window.focus(&self.composer.focus_handle(cx));
    }
    fn cycle_effort(&mut self, _: &gpui::ClickEvent, _: &mut Window, cx: &mut Context<Self>) {
        const LEVELS: &[&str] = &["low", "medium", "high", "xhigh", "max", "ultra"];
        let current = self
            .view
            .as_ref()
            .and_then(|view| view.state.configuration.as_ref())
            .and_then(|configuration| configuration.effort.as_deref());
        let next = LEVELS
            .iter()
            .position(|level| Some(*level) == current)
            .map_or(LEVELS[0], |index| LEVELS[(index + 1) % LEVELS.len()]);
        self.send(FrontendCommand::SetEffort(next.into()));
        cx.notify();
    }
    fn cycle_permission(&mut self, _: &gpui::ClickEvent, _: &mut Window, cx: &mut Context<Self>) {
        let current = self
            .view
            .as_ref()
            .and_then(|view| view.state.configuration.as_ref())
            .map(|configuration| configuration.permission_mode)
            .unwrap_or(borg_ui::PermissionMode::FullAccess);
        let next = match current {
            borg_ui::PermissionMode::FullAccess => borg_ui::PermissionMode::Auto,
            borg_ui::PermissionMode::Auto => borg_ui::PermissionMode::Manual,
            borg_ui::PermissionMode::Manual => borg_ui::PermissionMode::FullAccess,
        };
        self.send(FrontendCommand::SetPermission(next));
        cx.notify();
    }
    fn toggle_fast(&mut self, _: &gpui::ClickEvent, _: &mut Window, cx: &mut Context<Self>) {
        let enabled = self
            .view
            .as_ref()
            .and_then(|view| view.state.configuration.as_ref())
            .is_some_and(|configuration| configuration.fast);
        self.send(FrontendCommand::SetFast(!enabled));
        cx.notify();
    }
    fn cycle_language(&mut self, _: &gpui::ClickEvent, _: &mut Window, cx: &mut Context<Self>) {
        let current = self
            .view
            .as_ref()
            .and_then(|view| view.state.configuration.as_ref())
            .map(|configuration| configuration.response_language)
            .unwrap_or_default();
        let index = ResponseLanguage::ALL
            .iter()
            .position(|language| *language == current)
            .unwrap_or(0);
        self.send(FrontendCommand::SetLanguage(
            ResponseLanguage::ALL[(index + 1) % ResponseLanguage::ALL.len()],
        ));
        cx.notify();
    }
    fn load_older(&mut self, _: &gpui::ClickEvent, _: &mut Window, cx: &mut Context<Self>) {
        self.send(FrontendCommand::LoadOlderHistory);
        cx.notify();
    }
    fn focus_root(&mut self, _: &gpui::ClickEvent, _: &mut Window, cx: &mut Context<Self>) {
        self.send(FrontendCommand::FocusAgent(None));
        cx.notify();
    }
    fn choose_attachments(&mut self, _: &gpui::ClickEvent, _: &mut Window, cx: &mut Context<Self>) {
        let receiver = cx.prompt_for_paths(PathPromptOptions {
            files: true,
            directories: false,
            multiple: true,
            prompt: Some("Attach to prompt".into()),
        });
        cx.spawn(async move |this, cx| {
            if let Ok(Ok(Some(paths))) = receiver.await {
                let _ = this.update(cx, |this, cx| {
                    this.attachments.extend(paths);
                    cx.notify();
                });
            }
        })
        .detach();
    }
    fn toggle_sessions(&mut self, _: &gpui::ClickEvent, _: &mut Window, cx: &mut Context<Self>) {
        self.sessions_open = !self.sessions_open;
        cx.notify();
    }
    fn new_session(&mut self, _: &gpui::ClickEvent, _: &mut Window, cx: &mut Context<Self>) {
        self.send(FrontendCommand::NewSession);
        self.sessions_open = false;
        cx.notify();
    }
    fn toggle_dictation(&mut self, _: &gpui::ClickEvent, _: &mut Window, cx: &mut Context<Self>) {
        match self.dictation.as_ref() {
            Some(dictation) => {
                if let Err(error) = dictation.toggle() {
                    self.error = Some(error.to_string());
                }
            }
            None => self.error = Some("Dictation is unavailable on this platform".into()),
        }
        cx.notify();
    }

    fn dismiss_error(&mut self, _: &gpui::ClickEvent, _: &mut Window, cx: &mut Context<Self>) {
        self.error = None;
        cx.notify();
    }

    fn status_segment(
        label: impl Into<SharedString>,
        value: impl Into<SharedString>,
        color: u32,
    ) -> impl IntoElement {
        div()
            .flex()
            .gap_1()
            .child(label.into())
            .child(div().text_color(rgb(color)).child(value.into()))
    }

    fn render_entry(
        entry: TimelineEntry,
        expanded: bool,
        view: Entity<BorgGui>,
    ) -> impl IntoElement {
        let color = match entry.kind {
            TimelineKind::User => palette::BLUE,
            TimelineKind::Assistant => palette::ORANGE,
            TimelineKind::Reasoning | TimelineKind::Tool => palette::TEXT_MUTED,
            TimelineKind::Subagent => palette::PINK,
            TimelineKind::Approval => palette::PEACH,
            TimelineKind::Error => palette::RED,
            TimelineKind::Status => palette::GREEN,
        };
        let compact = matches!(
            entry.kind,
            TimelineKind::Reasoning | TimelineKind::Tool | TimelineKind::Status
        );
        let time = entry.created_at.format("%H:%M").to_string();
        let indicator = if entry.kind == TimelineKind::Tool && !entry.body.is_empty() {
            if expanded { "▾" } else { "▸" }
        } else if entry.running {
            "◆"
        } else if entry.failed {
            "×"
        } else {
            ""
        };
        let copy_body = entry.body.clone();
        let body = if entry.kind == TimelineKind::Tool && !expanded {
            String::new()
        } else if entry.body.len() > 12_000 {
            let boundary = entry.body.floor_char_boundary(12_000);
            format!("{}\n… output truncated", &entry.body[..boundary])
        } else {
            entry.body
        };
        let header = div()
            .flex()
            .items_center()
            .gap_2()
            .text_sm()
            .child(
                div()
                    .text_color(rgb(color))
                    .font_weight(FontWeight::SEMIBOLD)
                    .child(entry.title),
            )
            .when_some(entry.detail, |row, detail| {
                row.child(div().text_color(rgb(palette::TEXT_MUTED)).child(detail))
            })
            .child(div().text_color(rgb(palette::TEXT_MUTED)).child(time))
            .child(div().text_color(rgb(color)).child(indicator));
        let entry_id = entry.id.clone();
        div()
            .id(SharedString::from(entry.id))
            .flex()
            .flex_col()
            .gap_1()
            .border_l_2()
            .border_color(rgb(color))
            .bg(rgb(if compact {
                palette::CANVAS
            } else {
                palette::SURFACE_RAISED
            }))
            .px_3()
            .py_2()
            .child(header)
            .when(entry.kind == TimelineKind::Tool, |card| {
                card.cursor_pointer()
                    .on_click(move |_, _, cx| {
                        let _ = view.update(cx, |this, cx| {
                            if !this.expanded_entries.remove(&entry_id) {
                                this.expanded_entries.insert(entry_id.clone());
                            }
                            cx.notify();
                        });
                    })
                    .on_mouse_up(gpui::MouseButton::Right, move |_, _, cx| {
                        cx.write_to_clipboard(gpui::ClipboardItem::new_string(copy_body.clone()));
                    })
            })
            .when(!body.is_empty(), |card| {
                card.child(
                    div()
                        .text_color(rgb(if compact {
                            palette::TEXT_MUTED
                        } else {
                            palette::TEXT
                        }))
                        .whitespace_normal()
                        .child(body),
                )
            })
    }
}

impl Drop for BorgGui {
    fn drop(&mut self) {
        if !self.temporary_attachments.is_empty() {
            let paths = std::mem::take(&mut self.temporary_attachments);
            let _ = std::thread::Builder::new()
                .name("borg-gui-cleanup".into())
                .spawn(move || {
                    for path in paths {
                        let _ = std::fs::remove_file(path);
                    }
                });
        }
    }
}

impl Render for BorgGui {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let cwd: SharedString = self
            .view
            .as_ref()
            .map(|v| v.cwd.display().to_string())
            .unwrap_or_else(|| "No local Borg session".into())
            .into();
        let timeline = Arc::clone(&self.timeline);
        let transcript_state = self.transcript_state.clone();
        let expanded_entries = self.expanded_entries.clone();
        let view_entity = cx.entity();
        let agents = self
            .view
            .as_ref()
            .map(|v| v.agents.clone())
            .unwrap_or_default();
        let todos = self
            .view
            .as_ref()
            .map(|view| view.state.todos.clone())
            .unwrap_or_default();
        let focused_child = self.view.as_ref().is_some_and(|view| {
            self.root_session_id
                .is_some_and(|root| root != view.session_id)
        });
        let configuration = self
            .view
            .as_ref()
            .and_then(|v| v.state.configuration.as_ref());
        let model: SharedString = configuration
            .and_then(|c| c.model.clone())
            .unwrap_or_else(|| "unconfigured".into())
            .into();
        let effort: SharedString = configuration
            .and_then(|c| c.effort.clone())
            .unwrap_or_else(|| "default".into())
            .into();
        let access: SharedString = configuration
            .map(|c| match c.permission_mode {
                borg_ui::PermissionMode::FullAccess => "full access",
                borg_ui::PermissionMode::Auto => "auto",
                borg_ui::PermissionMode::Manual => "manual",
            })
            .unwrap_or_else(|| "unknown".into())
            .into();
        let fast = configuration.is_some_and(|c| c.fast);
        let language = configuration
            .map(|configuration| configuration.response_language.code())
            .unwrap_or("auto");
        let context: SharedString = self
            .view
            .as_ref()
            .and_then(|v| {
                Some(format!(
                    "{}% context left",
                    100_u64.saturating_sub(
                        v.state.usage.context_tokens? * 100
                            / v.state.usage.context_window_tokens?.max(1)
                    )
                ))
            })
            .unwrap_or_else(|| "context —".into())
            .into();
        let status: SharedString = self
            .view
            .as_ref()
            .and_then(|v| v.state.status)
            .map(|status| format!("{status:?}").to_lowercase())
            .unwrap_or_else(|| "offline".into())
            .into();
        let goal = self
            .view
            .as_ref()
            .and_then(|v| v.goal.as_ref())
            .map(|g| g.objective.clone());
        let approval = self
            .view
            .as_ref()
            .is_some_and(|v| v.state.pending_approval_id.is_some());
        let provider_interaction = self.view.as_ref().and_then(|view| {
            let kind = view.state.pending_provider_interaction_kind.clone()?;
            let payload = view.state.pending_provider_interaction_payload.as_ref()?;
            let prompt = payload
                .get("questions")
                .and_then(serde_json::Value::as_array)
                .and_then(|questions| questions.first())
                .and_then(|question| question.get("prompt").or_else(|| question.get("header")))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("The provider needs input to continue")
                .to_string();
            Some((kind, prompt))
        });
        let delivery = match self.delivery {
            PromptDelivery::Steer => "steer",
            PromptDelivery::Queue => "queue",
        };
        let attachment_labels = self
            .attachments
            .iter()
            .enumerate()
            .map(|(index, path)| {
                let label = path
                    .file_name()
                    .map(|name| name.to_string_lossy().into_owned())
                    .unwrap_or_else(|| path.display().to_string());
                (index, label)
            })
            .collect::<Vec<_>>();
        let session_options = self.sessions.clone();
        let dictation_status = self.dictation_status.clone();
        let command_help = [
            ("/ask PROFILE TEXT", "consult a second model", "/ask "),
            ("/director TEXT", "message the director", "/director "),
            ("/claude TEXT", "consult the Claude peer", "/claude "),
            ("/gpt TEXT", "consult the GPT peer", "/gpt "),
            (
                "/peer claude|gpt",
                "control a durable peer directly",
                "/peer ",
            ),
            ("/queue TEXT", "send after the active turn", "/queue "),
            ("/steer TEXT", "redirect the active turn", "/steer "),
            (
                "/goal OBJECTIVE",
                "set or control the durable goal",
                "/goal ",
            ),
            ("/todo add TEXT", "update the durable plan", "/todo add "),
            ("/model MODEL", "switch model", "/model "),
            ("/effort LEVEL", "change reasoning effort", "/effort "),
            ("/permission MODE", "full, auto, or manual", "/permission "),
            ("/language NAME", "change response language", "/language "),
            ("/fast on|off", "toggle priority mode", "/fast "),
            ("/compact", "compact conversation context", "/compact"),
            ("/clear", "clear conversation context", "/clear"),
            ("/recall", "return queued input to the composer", "/recall"),
            ("/flush", "discard pending queued input", "/flush"),
            ("/copy", "copy the latest response", "/copy"),
            ("/dictate", "start or stop dictation", "/dictate"),
            ("/ext:ID:COMMAND", "run an extension command", "/ext:"),
            ("/resume", "open recent sessions", "/resume"),
            ("/interrupt", "interrupt the active turn", "/interrupt"),
            ("/quit", "stop this session", "/quit"),
        ];
        let palette_composer = self.composer.clone();
        div()
            .on_action(cx.listener(Self::interrupt_action))
            .on_action(cx.listener(Self::escape_action))
            .on_action(cx.listener(Self::toggle_help_action))
            .relative()
            .flex()
            .flex_col()
            .size_full()
            .bg(rgb(palette::CANVAS))
            .text_color(rgb(palette::TEXT))
            .font_family("Berkeley Mono")
            .child(
                div()
                    .flex()
                    .items_center()
                    .justify_between()
                    .h(px(42.))
                    .px_4()
                    .border_b_1()
                    .border_color(rgb(palette::BORDER))
                    .bg(rgb(palette::SURFACE))
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_2()
                            .child(div().text_color(rgb(palette::ORANGE)).child("▰"))
                            .child(div().font_weight(FontWeight::SEMIBOLD).child("BORG"))
                            .child(
                                div()
                                    .id("session-switcher")
                                    .cursor_pointer()
                                    .on_click(cx.listener(Self::toggle_sessions))
                                    .text_color(rgb(palette::TEXT_MUTED))
                                    .child("native session  ▾"),
                            ),
                    )
                    .child(
                        div()
                            .text_sm()
                            .text_color(rgb(palette::TEXT_MUTED))
                            .child(cwd),
                    ),
            )
            .when_some(goal, |root, goal| {
                root.child(
                    div()
                        .px_4()
                        .py_2()
                        .border_b_1()
                        .border_color(rgb(palette::BORDER))
                        .bg(rgb(palette::SURFACE_RAISED))
                        .text_sm()
                        .child(
                            div()
                                .flex()
                                .gap_2()
                                .child(div().text_color(rgb(palette::GREEN)).child("GOAL"))
                                .child(div().min_w_0().overflow_hidden().text_ellipsis().child(goal)),
                        ),
                )
            })
            .when(self.sessions_open, |root| {
                root.child(
                    div()
                        .id("session-menu")
                        .absolute()
                        .top(px(46.))
                        .left(px(12.))
                        .w(px(460.))
                        .max_h(px(520.))
                        .overflow_y_scroll()
                        .border_1()
                        .border_color(rgb(palette::BORDER))
                        .bg(rgb(palette::SURFACE))
                        .p_2()
                        .flex()
                        .flex_col()
                        .gap_1()
                        .child(
                            div()
                                .px_2()
                                .py_1()
                                .text_xs()
                                .text_color(rgb(palette::TEXT_MUTED))
                                .child("RECENT SESSIONS"),
                        )
                        .child(div().id("new-session").px_3().py_2().mb_1().cursor_pointer().text_color(rgb(palette::ORANGE)).hover(|style| style.bg(rgb(palette::SURFACE_RAISED))).on_click(cx.listener(Self::new_session)).child("+ New session"))
                        .children(session_options.into_iter().map(|session| {
                            let session_id = session.session_id;
                            let title = session.title.chars().take(72).collect::<String>();
                            let detail =
                                format!("{}  ·  {:?}", session.cwd.display(), session.status)
                                    .to_lowercase();
                            div()
                                .id(SharedString::from(format!("session-{session_id}")))
                                .px_3()
                                .py_2()
                                .cursor_pointer()
                                .hover(|style| style.bg(rgb(palette::SURFACE_RAISED)))
                                .on_click(cx.listener(move |this, _, _, cx| {
                                    this.send(FrontendCommand::OpenSession(session_id));
                                    this.sessions_open = false;
                                    cx.notify();
                                }))
                                .child(div().text_sm().child(title))
                                .child(
                                    div()
                                        .text_xs()
                                        .text_color(rgb(palette::TEXT_MUTED))
                                        .child(detail),
                                )
                        })),
                )
            })
            .child(
                div()
                    .flex_1()
                    .overflow_hidden()
                    .flex()
                    .child(
                        div()
                            .size_full()
                            .flex()
                            .flex_col()
                            .px_5()
                            .py_4()
                            .child(
                                div()
                                    .id("older")
                                    .mx_auto()
                                    .px_3()
                                    .py_1()
                                    .mb_2()
                                    .text_xs()
                                    .text_color(rgb(palette::TEXT_MUTED))
                                    .cursor_pointer()
                                    .hover(|s| s.bg(rgb(palette::SURFACE_RAISED)))
                                    .on_click(cx.listener(Self::load_older))
                                    .child("load earlier history"),
                            )
                            .child(
                                list(transcript_state, move |index, _, _| {
                                    let entry = timeline[index].as_ref().clone();
                                    let expanded = expanded_entries.contains(&entry.id);
                                    Self::render_entry(entry, expanded, view_entity.clone())
                                        .into_any_element()
                                })
                                .flex_1()
                                .gap_3(),
                            ),
                    )
                    .when(
                        !agents.is_empty() || !todos.is_empty() || focused_child,
                        |body| {
                            body.child(
                                div()
                                    .w(px(238.))
                                    .flex_none()
                                    .border_l_1()
                                    .border_color(rgb(palette::BORDER))
                                    .bg(rgb(palette::SURFACE))
                                    .p_3()
                                    .flex()
                                    .flex_col()
                                    .gap_2()
                                    .child(
                                        div()
                                            .flex()
                                            .items_center()
                                            .justify_between()
                                            .text_xs()
                                            .text_color(rgb(palette::TEXT_MUTED))
                                            .child("TEAM")
                                            .when(focused_child, |header| {
                                                header.child(
                                                    div()
                                                        .id("focus-root")
                                                        .cursor_pointer()
                                                        .text_color(rgb(palette::BLUE))
                                                        .on_click(cx.listener(Self::focus_root))
                                                        .child("back to root"),
                                                )
                                            }),
                                    )
                                    .children(agents.into_iter().map(|agent| {
                                        let session_id = agent.session_id;
                                        let status = format!("{:?}", agent.status).to_lowercase();
                                        div()
                                            .id(SharedString::from(format!("agent-{session_id}")))
                                            .border_1()
                                            .border_color(rgb(palette::BORDER))
                                            .bg(rgb(palette::CANVAS))
                                            .px_3()
                                            .py_2()
                                            .cursor_pointer()
                                            .hover(|s| s.border_color(rgb(palette::PINK)))
                                            .on_click(cx.listener(move |this, _, _, cx| {
                                                this.send(FrontendCommand::FocusAgent(Some(
                                                    session_id,
                                                )));
                                                cx.notify();
                                            }))
                                            .child(
                                                div()
                                                    .text_sm()
                                                    .text_color(rgb(palette::PINK))
                                                    .child(agent.task_name),
                                            )
                                            .child(
                                                div()
                                                    .text_xs()
                                                    .text_color(rgb(palette::TEXT_MUTED))
                                                    .child(status),
                                            )
                                    }))
                                    .when(!todos.is_empty(), |sidebar| {
                                        sidebar
                                            .child(
                                                div()
                                                    .mt_3()
                                                    .pt_3()
                                                    .border_t_1()
                                                    .border_color(rgb(palette::BORDER))
                                                    .text_xs()
                                                    .text_color(rgb(palette::TEXT_MUTED))
                                                    .child("PLAN"),
                                            )
                                            .children(todos.into_iter().map(|item| {
                                                let item_id = item.id;
                                                let next_status = match item.status {
                                                    borg_ui::PlanItemStatus::Pending => borg_ui::PlanItemStatus::InProgress,
                                                    borg_ui::PlanItemStatus::InProgress => borg_ui::PlanItemStatus::Completed,
                                                    borg_ui::PlanItemStatus::Completed => borg_ui::PlanItemStatus::Pending,
                                                };
                                                let (marker, color) = match item.status {
                                                    borg_ui::PlanItemStatus::Pending => {
                                                        ("○", palette::TEXT_MUTED)
                                                    }
                                                    borg_ui::PlanItemStatus::InProgress => {
                                                        ("◆", palette::PEACH)
                                                    }
                                                    borg_ui::PlanItemStatus::Completed => {
                                                        ("✓", palette::GREEN)
                                                    }
                                                };
                                                div()
                                                    .id(SharedString::from(format!("todo-{item_id}")))
                                                    .flex()
                                                    .gap_2()
                                                    .text_xs()
                                                    .cursor_pointer()
                                                    .hover(|style| style.bg(rgb(palette::SURFACE_RAISED)))
                                                    .on_click(cx.listener(move |this, _, _, cx| {
                                                        this.send(FrontendCommand::ApplyTodo(borg_ui::TodoAction::SetStatus { id: item_id, status: next_status }));
                                                        cx.notify();
                                                    }))
                                                    .child(
                                                        div().text_color(rgb(color)).child(marker),
                                                    )
                                                    .child(
                                                        div()
                                                            .min_w_0()
                                                            .flex_1()
                                                            .whitespace_normal()
                                                            .text_color(rgb(palette::TEXT))
                                                            .child(item.content),
                                                    )
                                            }))
                                    }),
                            )
                        },
                    ),
            )
            .when_some(self.error.clone(), |root, error| {
                root.child(
                    div()
                        .mx_4()
                        .mb_2()
                        .px_3()
                        .py_2()
                        .border_1()
                        .border_color(rgb(palette::RED))
                        .text_color(rgb(palette::RED))
                        .text_sm()
                        .flex()
                        .items_center()
                        .justify_between()
                        .gap_3()
                        .child(div().flex_1().child(error))
                        .child(
                            div()
                                .id("dismiss-error")
                                .cursor_pointer()
                                .text_color(rgb(palette::TEXT_MUTED))
                                .on_click(cx.listener(Self::dismiss_error))
                                .child("dismiss"),
                        ),
                )
            })
            .child(
                div()
                    .border_t_1()
                    .border_color(rgb(palette::BORDER))
                    .bg(rgb(palette::SURFACE))
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .justify_between()
                            .h(px(34.))
                            .px_4()
                            .text_sm()
                            .child(
                                div()
                                    .flex()
                                    .gap_3()
                                    .child(Self::status_segment("◆", status, palette::PEACH))
                                    .child("·")
                                    .child(div().id("model-setting").cursor_pointer().hover(|style| style.bg(rgb(palette::SURFACE_RAISED))).on_click(cx.listener(Self::edit_model)).child(Self::status_segment("model", model, palette::TEXT_MUTED)))
                                    .child("·")
                                    .child(div().id("effort-setting").cursor_pointer().hover(|style| style.bg(rgb(palette::SURFACE_RAISED))).on_click(cx.listener(Self::cycle_effort)).child(Self::status_segment("effort", effort, palette::TEXT_MUTED)))
                                    .child("·")
                                    .child(div().id("fast-setting").cursor_pointer().text_color(rgb(if fast { palette::PEACH } else { palette::TEXT_MUTED })).hover(|style| style.bg(rgb(palette::SURFACE_RAISED))).on_click(cx.listener(Self::toggle_fast)).child(if fast { "fast" } else { "standard" }))
                                    .child("·")
                                    .child(div().id("access-setting").cursor_pointer().hover(|style| style.bg(rgb(palette::SURFACE_RAISED))).on_click(cx.listener(Self::cycle_permission)).child(Self::status_segment("access", access, palette::PEACH)))
                                    .child("·")
                                    .child(div().id("language-setting").cursor_pointer().text_color(rgb(palette::TEXT_MUTED)).hover(|style| style.bg(rgb(palette::SURFACE_RAISED))).on_click(cx.listener(Self::cycle_language)).child(language))
                                    .child("·")
                                    .child(
                                        div().text_color(rgb(palette::TEXT_MUTED)).child(context),
                                    ),
                            )
                            .child(
                                div()
                                    .flex()
                                    .gap_2()
                                    .child(
                                        div()
                                            .id("delivery")
                                            .px_2()
                                            .rounded_sm()
                                            .text_color(rgb(palette::BLUE))
                                            .hover(|s| s.bg(rgb(palette::SURFACE_RAISED)))
                                            .cursor_pointer()
                                            .on_click(cx.listener(Self::toggle_delivery))
                                            .child(delivery),
                                    )
                                    .child(
                                        div()
                                            .id("interrupt")
                                            .px_2()
                                            .rounded_sm()
                                            .hover(|s| s.bg(rgb(palette::SURFACE_RAISED)))
                                            .cursor_pointer()
                                            .on_click(cx.listener(Self::interrupt))
                                            .child("stop"),
                                    ),
                            ),
                    )
                    .when(approval, |footer| {
                        footer.child(
                            div()
                                .mx_4()
                                .mb_2()
                                .flex()
                                .items_center()
                                .justify_between()
                                .border_1()
                                .border_color(rgb(palette::PEACH))
                                .bg(rgb(palette::SURFACE_RAISED))
                                .px_3()
                                .py_2()
                                .child(
                                    div()
                                        .text_color(rgb(palette::PEACH))
                                        .child("Approval required"),
                                )
                                .child(
                                    div()
                                        .flex()
                                        .gap_2()
                                        .child(
                                            div()
                                                .id("deny")
                                                .px_3()
                                                .py_1()
                                                .cursor_pointer()
                                                .on_click(cx.listener(Self::deny))
                                                .child("Deny"),
                                        )
                                        .child(
                                            div()
                                                .id("approve")
                                                .px_3()
                                                .py_1()
                                                .bg(rgb(palette::ORANGE))
                                                .text_color(rgb(palette::CANVAS))
                                                .cursor_pointer()
                                                .on_click(cx.listener(Self::approve_once))
                                                .child("Approve once"),
                                        )
                                        .child(
                                            div()
                                                .id("approve-session")
                                                .px_3()
                                                .py_1()
                                                .border_1()
                                                .border_color(rgb(palette::ORANGE))
                                                .text_color(rgb(palette::ORANGE))
                                                .cursor_pointer()
                                                .on_click(cx.listener(Self::approve_session))
                                                .child("Allow session"),
                                        ),
                                ),
                        )
                    })
                    .when_some(provider_interaction, |footer, (kind, prompt)| {
                        footer.child(
                            div()
                                .mx_4()
                                .mb_2()
                                .flex()
                                .items_center()
                                .justify_between()
                                .border_1()
                                .border_color(rgb(palette::BLUE))
                                .bg(rgb(palette::SURFACE_RAISED))
                                .px_3()
                                .py_2()
                                .child(
                                    div()
                                        .flex()
                                        .flex_col()
                                        .gap_1()
                                        .child(
                                            div()
                                                .text_color(rgb(palette::BLUE))
                                                .child("Provider input"),
                                        )
                                        .child(div().text_sm().child(prompt))
                                        .child(
                                            div()
                                                .text_xs()
                                                .text_color(rgb(palette::TEXT_MUTED))
                                                .child(kind),
                                        ),
                                )
                                .child(
                                    div()
                                        .id("cancel-provider-input")
                                        .px_3()
                                        .py_1()
                                        .cursor_pointer()
                                        .on_click(cx.listener(Self::cancel_provider_input))
                                        .child("Cancel"),
                                ),
                        )
                    })
                    .child(div().mx_4().when(!attachment_labels.is_empty(), |row| {
                        row.flex()
                            .gap_2()
                            .pb_2()
                            .children(attachment_labels.into_iter().map(|(index, label)| {
                                div()
                                    .id(SharedString::from(format!("attachment-{index}")))
                                    .px_2()
                                    .py_1()
                                    .rounded_sm()
                                    .bg(rgb(palette::SURFACE_RAISED))
                                    .text_xs()
                                    .text_color(rgb(palette::BLUE))
                                    .flex()
                                    .items_center()
                                    .gap_2()
                                    .child(label)
                                    .child(
                                        div()
                                            .id(SharedString::from(format!(
                                                "remove-attachment-{index}"
                                            )))
                                            .cursor_pointer()
                                            .text_color(rgb(palette::TEXT_MUTED))
                                            .on_click(cx.listener(move |this, _, _, cx| {
                                                if index < this.attachments.len() {
                                                    this.attachments.remove(index);
                                                    cx.notify();
                                                }
                                            }))
                                            .child("×"),
                                    )
                            }))
                    }))
                    .child(
                        div()
                            .mx_4()
                            .mb_2()
                            .min_h(px(48.))
                            .rounded_sm()
                            .border_1()
                            .border_color(rgb(palette::BORDER))
                            .bg(rgb(palette::CANVAS))
                            .px_3()
                            .py_3()
                            .flex()
                            .items_center()
                            .gap_2()
                            .child(
                                div()
                                    .id("attach")
                                    .px_2()
                                    .text_color(rgb(palette::BLUE))
                                    .cursor_pointer()
                                    .on_click(cx.listener(Self::choose_attachments))
                                    .child("+"),
                            )
                            .child(
                                div()
                                    .id("dictation")
                                    .px_2()
                                    .text_xs()
                                    .text_color(rgb(if self.dictation_status == "recording" {
                                        palette::RED
                                    } else {
                                        palette::TEXT_MUTED
                                    }))
                                    .cursor_pointer()
                                    .on_click(cx.listener(Self::toggle_dictation))
                                    .child(dictation_status),
                            )
                            .child(div().flex_1().child(self.composer.clone())),
                    )
                    .child(
                        div()
                            .flex()
                            .justify_between()
                            .px_4()
                            .pb_3()
                            .text_xs()
                            .text_color(rgb(palette::TEXT_MUTED))
                        .child(div().id("command-help").cursor_pointer().on_click(cx.listener(Self::toggle_help)).child("send  enter  ·  newline  shift-enter  ·  commands  ctrl-shift-p"))
                            .child(
                                self.view
                                    .as_ref()
                                    .map(|v| v.session_id.to_string())
                                    .unwrap_or_default(),
                            ),
                    ),
            )
            .when(self.help_open, |root| {
                root.child(
                    div().absolute().inset_0().bg(gpui::rgba(0x00000088)).flex().items_center().justify_center()
                        .child(div().id("help-panel").w(px(620.)).max_h(px(620.)).overflow_y_scroll().border_1().border_color(rgb(palette::BORDER)).bg(rgb(palette::SURFACE)).p_5().flex().flex_col().gap_2()
                            .child(div().flex().items_center().justify_between().mb_2().child(div().font_weight(FontWeight::SEMIBOLD).text_color(rgb(palette::ORANGE)).child("COMMANDS")).child(div().id("close-help").cursor_pointer().text_color(rgb(palette::TEXT_MUTED)).on_click(cx.listener(Self::toggle_help)).child("esc / close")))
                            .children(command_help.into_iter().enumerate().map(|(index, (command, detail, insertion))| {
                                let composer = palette_composer.clone();
                                div().id(SharedString::from(format!("command-{index}"))).flex().gap_4().px_2().py_1().text_sm().cursor_pointer().hover(|style| style.bg(rgb(palette::SURFACE_RAISED))).on_click(cx.listener(move |this, _, window, cx| {
                                    composer.update(cx, |composer, cx| composer.set_text(insertion, cx));
                                    this.help_open = false;
                                    window.focus(&composer.focus_handle(cx));
                                    cx.notify();
                                })).child(div().w(px(190.)).text_color(rgb(palette::BLUE)).child(command)).child(div().text_color(rgb(palette::TEXT_MUTED)).child(detail))
                            }))
                    )
                )
            })
    }
}

fn requested_session() -> Option<Uuid> {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--session" {
            return args.next().and_then(|value| value.parse().ok());
        }
    }
    None
}

pub fn run() {
    let worker = LocalSessionWorker::start(requested_session()).unwrap_or_else(|error| {
        eprintln!("borg-gui: {error:#}");
        None
    });
    Application::new().run(move |cx: &mut App| {
        cx.bind_keys([
            KeyBinding::new("backspace", composer::Backspace, Some("Composer")),
            KeyBinding::new("delete", composer::Delete, Some("Composer")),
            KeyBinding::new("left", composer::Left, Some("Composer")),
            KeyBinding::new("right", composer::Right, Some("Composer")),
            KeyBinding::new("shift-left", composer::SelectLeft, Some("Composer")),
            KeyBinding::new("shift-right", composer::SelectRight, Some("Composer")),
            KeyBinding::new("ctrl-a", composer::SelectAll, Some("Composer")),
            KeyBinding::new("ctrl-v", composer::Paste, Some("Composer")),
            KeyBinding::new("ctrl-c", composer::Copy, Some("Composer")),
            KeyBinding::new("ctrl-x", composer::Cut, Some("Composer")),
            KeyBinding::new("home", composer::Home, Some("Composer")),
            KeyBinding::new("end", composer::End, Some("Composer")),
            KeyBinding::new("shift-enter", composer::Newline, Some("Composer")),
            KeyBinding::new("alt-enter", composer::Newline, Some("Composer")),
            KeyBinding::new("enter", composer::Submit, Some("Composer")),
            KeyBinding::new("escape", Escape, Some("Composer")),
            KeyBinding::new("ctrl-shift-p", ToggleHelp, Some("Composer")),
            KeyBinding::new("cmd-shift-p", ToggleHelp, Some("Composer")),
            KeyBinding::new("cmd-a", composer::SelectAll, Some("Composer")),
            KeyBinding::new("cmd-v", composer::Paste, Some("Composer")),
            KeyBinding::new("cmd-c", composer::Copy, Some("Composer")),
            KeyBinding::new("cmd-x", composer::Cut, Some("Composer")),
        ]);
        let bounds = Bounds::centered(None, size(px(1120.), px(760.)), cx);
        let window = cx
            .open_window(
                WindowOptions {
                    window_bounds: Some(WindowBounds::Windowed(bounds)),
                    window_min_size: Some(size(px(720.), px(520.))),
                    titlebar: Some(gpui::TitlebarOptions {
                        title: Some("Borg".into()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                |_, cx| {
                    let composer = cx.new(Composer::new);
                    cx.new(|cx| BorgGui::new(worker, composer, cx))
                },
            )
            .expect("failed to open Borg window");
        window
            .update(cx, |view, window, cx| {
                window.focus(&view.composer.focus_handle(cx));
            })
            .ok();
        cx.activate(true);
    });
}
