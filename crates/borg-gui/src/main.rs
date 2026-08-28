mod composer;

use borg_ui::{
    FrontendCommand, SessionView,
    local::{LocalSessionUpdate, LocalSessionWorker},
    palette,
    timeline::{TimelineEntry, TimelineKind, project_timeline},
};
use composer::{Composer, Submitted};
use gpui::{
    App, Application, Bounds, Context, Entity, Focusable, FontWeight, KeyBinding, SharedString,
    Timer, Window, WindowBounds, WindowOptions, div, prelude::*, px, rgb, size,
};
use std::time::Duration;
use uuid::Uuid;

struct BorgGui {
    worker: Option<LocalSessionWorker>,
    view: Option<SessionView>,
    composer: Entity<Composer>,
    error: Option<String>,
}

impl BorgGui {
    fn new(
        worker: Option<LocalSessionWorker>,
        composer: Entity<Composer>,
        cx: &mut Context<Self>,
    ) -> Self {
        cx.subscribe(&composer, |this, _, event: &Submitted, cx| {
            this.send(FrontendCommand::SubmitPrompt {
                text: event.0.clone(),
                attachments: Vec::new(),
                delivery: borg_remote::PromptDelivery::Steer,
            });
            cx.notify();
        })
        .detach();
        let mut this = Self {
            worker,
            view: None,
            composer,
            error: None,
        };
        this.schedule_poll(cx);
        this
    }

    fn schedule_poll(&mut self, cx: &mut Context<Self>) {
        cx.spawn(async move |this, cx| {
            loop {
                Timer::after(Duration::from_millis(100)).await;
                if this
                    .update(cx, |this, cx| {
                        let mut changed = false;
                        if let Some(worker) = &this.worker {
                            while let Some(update) = worker.try_recv() {
                                match update {
                                    LocalSessionUpdate::View(view) => {
                                        this.view = Some(view);
                                        this.error = None;
                                    }
                                    LocalSessionUpdate::Error(error) => this.error = Some(error),
                                }
                                changed = true;
                            }
                        }
                        if changed {
                            cx.notify();
                        }
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
    fn approve_once(&mut self, _: &gpui::ClickEvent, _: &mut Window, cx: &mut Context<Self>) {
        self.send(FrontendCommand::Approve(
            borg_remote::ApprovalDecision::AllowOnce,
        ));
        cx.notify();
    }
    fn deny(&mut self, _: &gpui::ClickEvent, _: &mut Window, cx: &mut Context<Self>) {
        self.send(FrontendCommand::Approve(
            borg_remote::ApprovalDecision::Deny,
        ));
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
        let entries = self
            .view
            .as_ref()
            .map(|v| project_timeline(&v.history))
            .unwrap_or_default();
        let configuration = self
            .view
            .as_ref()
            .and_then(|v| v.state.configuration.as_ref());
        let model: SharedString = configuration
            .and_then(|c| c.model.clone())
            .unwrap_or_else(|| "unconfigured".into())
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
        div()
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
                                    .text_color(rgb(palette::TEXT_MUTED))
                                    .child("native session"),
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
            })
            .child(
                div().flex_1().overflow_hidden().child(
                    div()
                        .id("transcript")
                        .size_full()
                        .overflow_y_scroll()
                        .px_5()
                        .py_4()
                        .flex()
                        .flex_col()
                        .gap_3()
                        .children(entries.into_iter().map(Self::render_entry)),
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
                                    )),
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
                                        ),
                                ),
                        )
                    })
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
                            .child(self.composer.clone()),
                    )
                    .child(
                        div()
                            .flex()
                            .justify_between()
                            .px_4()
                            .pb_3()
                            .text_xs()
                            .text_color(rgb(palette::TEXT_MUTED))
                            .child("send  enter  ·  interrupt  esc")
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
