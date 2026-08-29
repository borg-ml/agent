use std::ops::Range;

use pulldown_cmark::{Event, Options, Parser, Tag, TagEnd};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RichStyle {
    pub strong: bool,
    pub emphasis: bool,
    pub code: bool,
    pub heading: bool,
    pub link: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RichSpan {
    pub range: Range<usize>,
    pub style: RichStyle,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RichText {
    pub text: String,
    pub spans: Vec<RichSpan>,
}

pub fn project_markdown(source: &str) -> RichText {
    let mut output = RichText::default();
    let mut style = RichStyle::default();
    let mut list_depth = 0_usize;
    let append = |text: &str, style: RichStyle, output: &mut RichText| {
        let start = output.text.len();
        output.text.push_str(text);
        if output.text.len() > start && style != RichStyle::default() {
            output.spans.push(RichSpan {
                range: start..output.text.len(),
                style,
            });
        }
    };
    let newline = |output: &mut RichText| {
        if !output.text.is_empty() && !output.text.ends_with('\n') {
            output.text.push('\n');
        }
    };

    for event in Parser::new_ext(source, Options::all()) {
        match event {
            Event::Start(Tag::Strong) => style.strong = true,
            Event::End(TagEnd::Strong) => style.strong = false,
            Event::Start(Tag::Emphasis) => style.emphasis = true,
            Event::End(TagEnd::Emphasis) => style.emphasis = false,
            Event::Start(Tag::Heading { .. }) => {
                newline(&mut output);
                style.heading = true;
                style.strong = true;
            }
            Event::End(TagEnd::Heading(_)) => {
                style.heading = false;
                style.strong = false;
                newline(&mut output);
            }
            Event::Start(Tag::Link { .. }) => style.link = true,
            Event::End(TagEnd::Link) => style.link = false,
            Event::Start(Tag::CodeBlock(_)) => {
                newline(&mut output);
                style.code = true;
            }
            Event::End(TagEnd::CodeBlock) => {
                style.code = false;
                newline(&mut output);
            }
            Event::Start(Tag::List(_)) => {
                list_depth += 1;
                newline(&mut output);
            }
            Event::End(TagEnd::List(_)) => {
                list_depth = list_depth.saturating_sub(1);
                newline(&mut output);
            }
            Event::Start(Tag::Item) => {
                newline(&mut output);
                output
                    .text
                    .push_str(&"  ".repeat(list_depth.saturating_sub(1)));
                output.text.push_str("• ");
            }
            Event::End(TagEnd::Item | TagEnd::Paragraph) => newline(&mut output),
            Event::Text(text) => append(&text, style, &mut output),
            Event::Code(text) => {
                let mut code_style = style;
                code_style.code = true;
                append(&text, code_style, &mut output);
            }
            Event::SoftBreak => output.text.push(' '),
            Event::HardBreak => newline(&mut output),
            Event::Rule => {
                newline(&mut output);
                output.text.push_str("────────");
                newline(&mut output);
            }
            Event::TaskListMarker(done) => {
                output.text.push_str(if done { "[✓] " } else { "[ ] " });
            }
            Event::InlineMath(text) => append(&format!("${text}$"), style, &mut output),
            Event::DisplayMath(text) => {
                newline(&mut output);
                append(&format!("${text}$"), style, &mut output);
                newline(&mut output);
            }
            Event::Html(text) | Event::InlineHtml(text) => append(&text, style, &mut output),
            Event::FootnoteReference(_) => {}
            Event::Start(_) | Event::End(_) => {}
        }
    }
    while output.text.ends_with('\n') {
        output.text.pop();
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_projection_keeps_markdown_semantics_without_source_delimiters() {
        let rich =
            project_markdown("# Result\n\nUse **cargo check** and `cargo test`.\n\n- fast\n- safe");

        assert_eq!(
            rich.text,
            "Result\nUse cargo check and cargo test.\n• fast\n• safe"
        );
        assert!(rich.spans.iter().any(|span| span.style.heading));
        assert!(rich.spans.iter().any(|span| span.style.strong));
        assert!(rich.spans.iter().any(|span| span.style.code));
    }

    #[test]
    fn native_projection_preserves_literal_angle_bracket_text() {
        assert_eq!(
            project_markdown("Borg Agent <ver> starts").text,
            "Borg Agent <ver> starts"
        );
    }
}
