use borg_ui::palette;
use gpui::{
    App, Application, Bounds, Context, SharedString, Window, WindowBounds, WindowOptions, div,
    prelude::*, px, rgb, size,
};

struct BorgGui {
    cwd: SharedString,
}

impl BorgGui {
    fn status_segment(label: &'static str, value: &'static str, color: u32) -> impl IntoElement {
        div()
            .flex()
            .gap_1()
            .child(label)
            .child(div().text_color(rgb(color)).child(value))
    }

    fn activity_row(time: &'static str, action: &'static str, detail: &'static str) -> impl IntoElement {
        div()
            .flex()
            .gap_2()
            .text_sm()
            .child(div().w(px(42.)).text_color(rgb(palette::TEXT_MUTED)).child(time))
            .child(div().w(px(84.)).text_color(rgb(palette::TEXT)).child(action))
            .child(div().text_color(rgb(palette::TEXT_MUTED)).child(detail))
    }
}

impl Render for BorgGui {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
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
                            .child(div().font_weight(gpui::FontWeight::SEMIBOLD).child("BORG"))
                            .child(div().text_color(rgb(palette::TEXT_MUTED)).child("native session")),
                    )
                    .child(div().text_sm().text_color(rgb(palette::TEXT_MUTED)).child(self.cwd.clone())),
            )
            .child(
                div()
                    .flex_1()
                    .overflow_hidden()
                    .child(
                        div()
                            .id("transcript")
                            .size_full()
                            .overflow_y_scroll()
                            .px_5()
                            .py_4()
                            .flex()
                            .flex_col()
                            .gap_4()
                            .child(
                                div()
                                    .border_l_2()
                                    .border_color(rgb(palette::BLUE))
                                    .bg(rgb(palette::SURFACE_RAISED))
                                    .px_4()
                                    .py_3()
                                    .child(
                                        div()
                                            .flex()
                                            .gap_2()
                                            .text_sm()
                                            .child(div().text_color(rgb(palette::BLUE)).child("you"))
                                            .child(div().text_color(rgb(palette::TEXT_MUTED)).child("now")),
                                    )
                                    .child("Build the native Borg interface without losing the terminal's density."),
                            )
                            .child(
                                div()
                                    .flex()
                                    .flex_col()
                                    .gap_1()
                                    .border_l_1()
                                    .border_color(rgb(palette::BORDER))
                                    .pl_3()
                                    .child(Self::activity_row("12:34", "Thinking", "mapping shared session state"))
                                    .child(Self::activity_row("12:34", "Read", "goal and plan"))
                                    .child(Self::activity_row("12:35", "Edited", "native GUI shell")),
                            )
                            .child(
                                div()
                                    .border_l_2()
                                    .border_color(rgb(palette::ORANGE))
                                    .bg(rgb(palette::SURFACE_RAISED))
                                    .px_4()
                                    .py_3()
                                    .child(
                                        div()
                                            .flex()
                                            .gap_2()
                                            .text_sm()
                                            .child(div().text_color(rgb(palette::ORANGE)).child("borg"))
                                            .child(div().text_color(rgb(palette::TEXT_MUTED)).child("gpui · xhigh")),
                                    )
                                    .child("The GUI is a native view over the same durable session—not a second implementation of Borg."),
                            ),
                    ),
            )
            .child(
                div()
                    .border_t_1()
                    .border_color(rgb(palette::BORDER))
                    .bg(rgb(palette::SURFACE))
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_3()
                            .h(px(34.))
                            .px_4()
                            .text_sm()
                            .child(Self::status_segment("◆", "running", palette::PEACH))
                            .child("·")
                            .child(Self::status_segment("goal", "active", palette::GREEN))
                            .child("·")
                            .child(Self::status_segment("model", "gpt-5.6-sol", palette::TEXT_MUTED))
                            .child("·")
                            .child(Self::status_segment("access", "full", palette::PEACH)),
                    )
                    .child(
                        div()
                            .mx_4()
                            .mb_2()
                            .min_h(px(56.))
                            .rounded_sm()
                            .border_1()
                            .border_color(rgb(palette::BORDER))
                            .bg(rgb(palette::CANVAS))
                            .px_3()
                            .py_2()
                            .text_color(rgb(palette::TEXT_MUTED))
                            .child("› Type a follow-up to redirect the current turn…"),
                    )
                    .child(
                        div()
                            .flex()
                            .justify_between()
                            .px_4()
                            .pb_3()
                            .text_xs()
                            .text_color(rgb(palette::TEXT_MUTED))
                            .child("send  enter  ·  commands  /  ·  palette  ctrl-shift-p")
                            .child("10 to-dos  ·  main  ·  clean"),
                    ),
            )
    }
}

fn main() {
    Application::new().run(|cx: &mut App| {
        let bounds = Bounds::centered(None, size(px(1120.), px(760.)), cx);
        cx.open_window(
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
                let cwd = std::env::current_dir()
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|_| "Borg workspace".into());
                cx.new(|_| BorgGui { cwd: cwd.into() })
            },
        )
        .expect("failed to open Borg window");
        cx.activate(true);
    });
}
