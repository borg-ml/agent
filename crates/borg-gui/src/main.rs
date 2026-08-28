mod composer;

use borg_ui::{
    ApprovalDecision, CodingProvider, FrontendCommand, PromptDelivery, SessionView,
    local::{LocalSessionOption, LocalSessionUpdate, LocalSessionWorker},
    palette,
    timeline::{TimelineEntry, TimelineKind},
};
use composer::{Composer, Submitted};
use gpui::{
    App, Application, Bounds, Context, Entity, Focusable, FontWeight, KeyBinding, ListAlignment,
    ListState, PathPromptOptions, SharedString, Window, WindowBounds, WindowOptions, div, list,
    prelude::*, px, rgb, size,
};
use std::path::PathBuf;
use std::sync::Arc;
use uuid::Uuid;

gpui::actions!(borg_gui, [Interrupt]);

struct BorgGui {
    worker: Option<LocalSessionWorker>,
    view: Option<SessionView>,
    composer: Entity<Composer>,
    error: Option<String>,
    delivery: PromptDelivery,
    root_session_id: Option<Uuid>,
    timeline: Arc<Vec<TimelineEntry>>,
    transcript_state: ListState,
    attachments: Vec<PathBuf>,
    sessions: Vec<LocalSessionOption>,
    sessions_open: bool,
}

impl BorgGui {
    fn new(
        worker: Option<LocalSessionWorker>,
        composer: Entity<Composer>,
        cx: &mut Context<Self>,
    ) -> Self {
        cx.subscribe(&composer, |this, _, event: &Submitted, cx| {
            let provider = this
                .view
                .as_ref()
                .and_then(|view| view.state.configuration.as_ref())
                .map(|configuration| configuration.provider)
                .unwrap_or(CodingProvider::Codex);
            match borg_ui::parse_submission(&event.0, provider, this.delivery) {
                Ok(FrontendCommand::SubmitPrompt { text, delivery, .. }) => {
                    let attachments = std::mem::take(&mut this.attachments);
                    this.send(FrontendCommand::SubmitPrompt {
                        text,
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
        let updates = worker.as_ref().map(LocalSessionWorker::updates);
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
        };
        if let Some(updates) = updates {
            this.schedule_updates(updates, cx);
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
                                this.root_session_id.get_or_insert(view.session_id);
                                this.transcript_state.reset(presentation.timeline.len());
                                this.timeline = presentation.timeline;
                                this.view = Some(view);
                                this.error = None;
                            }
                            LocalSessionUpdate::Sessions(sessions) => this.sessions = sessions,
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
    fn toggle_delivery(&mut self, _: &gpui::ClickEvent, _: &mut Window, cx: &mut Context<Self>) {
        self.delivery = match self.delivery {
            PromptDelivery::Steer => PromptDelivery::Queue,
            PromptDelivery::Queue => PromptDelivery::Steer,
        };
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

    fn render_entry(entry: TimelineEntry) -> impl IntoElement {
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
        let indicator = if entry.running {
            "◆"
        } else if entry.failed {
            "×"
        } else {
            ""
        };
        let body = if entry.body.len() > 12_000 {
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
        div()
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
        let agents = self
            .view
            .as_ref()
            .map(|v| v.agents.clone())
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
            .map(|c| format!("{:?}", c.permission_mode).to_lowercase())
            .unwrap_or_else(|| "unknown".into())
            .into();
        let fast = configuration.is_some_and(|c| c.fast);
        let context: SharedString = self
            .view
            .as_ref()
            .and_then(|v| {
                Some(format!(
                    "{}% context",
                    v.state.usage.context_tokens? * 100
                        / v.state.usage.context_window_tokens?.max(1)
                ))
            })
            .unwrap_or_else(|| "context —".into())
            .into();
        let status: SharedString = self
            .view
            .as_ref()
            .map(|v| format!("{:?}", v.state.status).to_lowercase())
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
        let delivery = match self.delivery {
            PromptDelivery::Steer => "steer",
            PromptDelivery::Queue => "queue",
        };
        let attachment_labels = self
            .attachments
            .iter()
            .map(|path| {
                path.file_name()
                    .map(|name| name.to_string_lossy().into_owned())
                    .unwrap_or_else(|| path.display().to_string())
            })
            .collect::<Vec<_>>();
        let session_options = self.sessions.clone();
        div()
            .on_action(cx.listener(Self::interrupt_action))
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
                                .child(goal),
                        ),
                )
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
                                    Self::render_entry(timeline[index].clone()).into_any_element()
                                })
                                .flex_1()
                                .gap_3(),
                            ),
                    )
                    .when(!agents.is_empty() || focused_child, |body| {
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
                                })),
                        )
                    }),
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
                        .child(error),
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
                                    .child(Self::status_segment(
                                        "model",
                                        model,
                                        palette::TEXT_MUTED,
                                    ))
                                    .child("·")
                                    .child(Self::status_segment(
                                        "effort",
                                        effort,
                                        palette::TEXT_MUTED,
                                    ))
                                    .when(fast, |row| {
                                        row.child("·").child(
                                            div().text_color(rgb(palette::PEACH)).child("fast"),
                                        )
                                    })
                                    .child("·")
                                    .child(Self::status_segment("access", access, palette::PEACH))
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
                    .child(div().mx_4().when(!attachment_labels.is_empty(), |row| {
                        row.flex()
                            .gap_2()
                            .pb_2()
                            .children(attachment_labels.into_iter().map(|label| {
                                div()
                                    .px_2()
                                    .py_1()
                                    .rounded_sm()
                                    .bg(rgb(palette::SURFACE_RAISED))
                                    .text_xs()
                                    .text_color(rgb(palette::BLUE))
                                    .child(label)
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
                            .child("send  enter  ·  commands  /  ·  click steer/queue")
                            .child(
                                self.view
                                    .as_ref()
                                    .map(|v| v.session_id.to_string())
                                    .unwrap_or_default(),
                            ),
                    ),
            )
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

fn main() {
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
            KeyBinding::new("enter", composer::Submit, Some("Composer")),
            KeyBinding::new("escape", Interrupt, Some("Composer")),
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
