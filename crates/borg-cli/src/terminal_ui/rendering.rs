use std::sync::OnceLock;

use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use syntect::easy::HighlightLines;
use syntect::highlighting::{Color as SyntectColor, Theme, ThemeSet};
use syntect::parsing::SyntaxSet;
use unicode_width::UnicodeWidthStr;

const SPLIT_DIFF_MIN_WIDTH: usize = 160;
const CODE_GUTTER_WIDTH: usize = 5;
const DIFF_NUMBER_WIDTH: usize = 4;
const DIFF_ADDED_BG: Color = Color::Rgb(25, 57, 39);
const DIFF_REMOVED_BG: Color = Color::Rgb(67, 31, 34);

pub(super) fn code_block_lines(language: &str, source: &str, width: usize) -> Vec<Line<'static>> {
    let language = language
        .split_ascii_whitespace()
        .next()
        .unwrap_or_default()
        .trim_matches(|character: char| matches!(character, '{' | '}' | '.'))
        .to_ascii_lowercase();
    let (language, source_language) = language
        .split_once(':')
        .map_or((language.as_str(), None), |(kind, source)| {
            (kind, Some(source))
        });
    match language {
        "diff" | "patch" | "udiff" => diff_lines(source, width, source_language),
        "diagnostic" | "diagnostics" | "lsp" => diagnostic_lines(source, width),
        "reasoning" => reasoning_lines(source, width),
        "command" => plain_lines(source, width),
        "subagent" => colored_plain_lines(source, width, super::SUBAGENT_PINK),
        _ => syntax_lines(language, source, width),
    }
}

pub(super) fn tool_body_lines(
    language: &str,
    source: &str,
    width: usize,
    prefix: &str,
) -> Vec<Line<'static>> {
    let prefix_width = UnicodeWidthStr::width(prefix);
    code_block_lines(language, source, width.saturating_sub(prefix_width).max(1))
        .into_iter()
        .map(|mut line| {
            line.spans.insert(
                0,
                Span::styled(prefix.to_string(), Style::default().fg(Color::DarkGray)),
            );
            line
        })
        .collect()
}

fn reasoning_lines(source: &str, width: usize) -> Vec<Line<'static>> {
    let items = reasoning_items(source);
    if items.is_empty() {
        return vec![Line::default()];
    }
    items
        .into_iter()
        .enumerate()
        .flat_map(|(item_index, item)| {
            let mut lines = super::wrap_display(&item, width.saturating_sub(2).max(1))
                .into_iter()
                .enumerate()
                .map(|(line_index, line)| {
                    Line::from(vec![
                        Span::styled(
                            if line_index == 0 { "• " } else { "  " },
                            Style::default().fg(super::BORG_ORANGE_HOVER),
                        ),
                        Span::styled(
                            line,
                            Style::default()
                                .fg(Color::Gray)
                                .add_modifier(Modifier::ITALIC),
                        ),
                    ])
                })
                .collect::<Vec<_>>();
            if item_index > 0 {
                lines.insert(0, Line::default());
            }
            lines
        })
        .collect()
}

fn reasoning_items(source: &str) -> Vec<String> {
    let trimmed = source.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }
    if let Some(thoughts) = bold_reasoning_thoughts(trimmed) {
        return thoughts.into_iter().map(str::to_string).collect();
    }
    let cleaned = source.replace("**", "");
    cleaned
        .lines()
        .filter_map(|raw_line| {
            let line = raw_line.trim();
            if line.is_empty() {
                return None;
            }
            let line = line
                .strip_prefix("• ")
                .or_else(|| line.strip_prefix("- "))
                .or_else(|| line.strip_prefix("* "))
                .unwrap_or(line);
            Some(line.to_string())
        })
        .collect()
}

fn bold_reasoning_thoughts(mut source: &str) -> Option<Vec<&str>> {
    let mut thoughts = Vec::new();
    loop {
        source = source.trim_start();
        let body = source.strip_prefix("**")?;
        if let Some(end) = body.find("**") {
            let thought = body[..end].trim();
            if thought.is_empty() {
                return None;
            }
            thoughts.push(thought);
            source = &body[end + 2..];
            if source.trim().is_empty() {
                return Some(thoughts);
            }
        } else {
            let thought = body.trim();
            if thought.is_empty() {
                return None;
            }
            thoughts.push(thought);
            return Some(thoughts);
        }
    }
}

fn plain_lines(source: &str, width: usize) -> Vec<Line<'static>> {
    colored_plain_lines(source, width, Color::White)
}

fn colored_plain_lines(source: &str, width: usize, color: Color) -> Vec<Line<'static>> {
    let lines = source.lines().collect::<Vec<_>>();
    if lines.is_empty() {
        return vec![Line::default()];
    }
    lines
        .into_iter()
        .map(|line| {
            Line::from(Span::styled(
                truncate_cells(line, width),
                Style::default().fg(color),
            ))
        })
        .collect()
}

fn syntax_lines(language: &str, source: &str, width: usize) -> Vec<Line<'static>> {
    let (syntaxes, theme) = syntax_assets();
    let syntax = syntaxes
        .find_syntax_by_token(language)
        .or_else(|| syntaxes.find_syntax_by_extension(language))
        .unwrap_or_else(|| syntaxes.find_syntax_plain_text());
    let mut highlighter = HighlightLines::new(syntax, theme);
    let digits = source.lines().count().max(1).to_string().len();
    let gutter_width = (digits + 3).min(CODE_GUTTER_WIDTH + digits);
    let content_width = width.saturating_sub(gutter_width).max(1);
    let mut output = Vec::new();

    for (index, raw) in source.lines().enumerate() {
        let line = format!("{raw}\n");
        let highlighted = highlighter
            .highlight_line(&line, syntaxes)
            .unwrap_or_else(|_| vec![(syntect::highlighting::Style::default(), line.as_str())]);
        let mut spans = vec![Span::styled(
            format!("{:>digits$} │ ", index + 1),
            Style::default().fg(Color::DarkGray),
        )];
        for (style, text) in highlighted {
            let text = text.trim_end_matches('\n');
            if text.is_empty() {
                continue;
            }
            spans.push(Span::styled(
                text.to_string(),
                Style::default().fg(terminal_color(style.foreground)),
            ));
        }
        // Code is intentionally clipped rather than softly wrapped: line
        // structure, diagnostics, and diff alignment remain trustworthy.
        output.push(clip_line(spans, content_width, gutter_width));
    }
    if source.is_empty() {
        output.push(Line::from(Span::styled(
            "  · │ ",
            Style::default().fg(Color::DarkGray),
        )));
    }
    output
}

fn syntax_assets() -> (&'static SyntaxSet, &'static Theme) {
    static SYNTAXES: OnceLock<SyntaxSet> = OnceLock::new();
    static THEMES: OnceLock<ThemeSet> = OnceLock::new();
    let syntaxes = SYNTAXES.get_or_init(SyntaxSet::load_defaults_newlines);
    let themes = THEMES.get_or_init(ThemeSet::load_defaults);
    (syntaxes, &themes.themes["base16-ocean.dark"])
}

fn diff_lines(source: &str, width: usize, source_language: Option<&str>) -> Vec<Line<'static>> {
    if width >= SPLIT_DIFF_MIN_WIDTH
        && diff_has_balanced_changes(source)
        && source.lines().any(|line| hunk_starts(line).is_some())
        && !source.lines().any(is_apply_patch_control_line)
    {
        split_diff_lines(source, width)
    } else {
        unified_diff_lines(source, width, source_language)
    }
}

fn diff_has_balanced_changes(source: &str) -> bool {
    let (mut additions, mut deletions) = (0usize, 0usize);
    for line in source.lines() {
        if line.starts_with("+++") || line.starts_with("---") {
            continue;
        }
        if line.starts_with('+') {
            additions += 1;
        } else if line.starts_with('-') {
            deletions += 1;
        }
    }
    let larger = additions.max(deletions);
    let smaller = additions.min(deletions);
    additions > 0 && deletions > 0 && smaller.saturating_mul(2) >= larger
}

fn unified_diff_lines(
    source: &str,
    width: usize,
    source_language: Option<&str>,
) -> Vec<Line<'static>> {
    let mut output = Vec::new();
    let (mut old_line, mut new_line) = (None, None);
    let (syntaxes, theme) = syntax_assets();
    let mut highlighter = source_language
        .and_then(|language| {
            syntaxes
                .find_syntax_by_token(language)
                .or_else(|| syntaxes.find_syntax_by_extension(language))
        })
        .map(|syntax| HighlightLines::new(syntax, theme));
    let show_line_numbers = source.lines().any(|line| hunk_starts(line).is_some());
    for raw in source.lines() {
        if let Some(path) = diff_file_path(raw) {
            highlighter = path
                .rsplit_once('.')
                .and_then(|(_, extension)| syntaxes.find_syntax_by_extension(extension))
                .map(|syntax| HighlightLines::new(syntax, theme));
            continue;
        }
        if is_apply_patch_control_line(raw) {
            continue;
        }
        if let Some((old, new)) = hunk_starts(raw) {
            old_line = Some(old);
            new_line = Some(new);
            continue;
        }
        if raw.starts_with("---") || raw.starts_with("+++") {
            output.push(Line::from(Span::styled(
                pad_cells(raw, width),
                Style::default().fg(Color::DarkGray),
            )));
            continue;
        }
        let (old_number, new_number, marker, text, background) =
            if let Some(text) = raw.strip_prefix('-') {
                let number = old_line;
                old_line = old_line.map(|line| line + 1);
                (number, None, '−', text, Some(DIFF_REMOVED_BG))
            } else if let Some(text) = raw.strip_prefix('+') {
                let number = new_line;
                new_line = new_line.map(|line| line + 1);
                (None, number, '+', text, Some(DIFF_ADDED_BG))
            } else {
                let numbers = (old_line, new_line);
                old_line = old_line.map(|line| line + 1);
                new_line = new_line.map(|line| line + 1);
                (
                    numbers.0,
                    numbers.1,
                    ' ',
                    raw.strip_prefix(' ').unwrap_or(raw),
                    None,
                )
            };
        output.push(highlighted_diff_line(
            old_number,
            new_number,
            marker,
            text,
            background,
            highlighter.as_mut(),
            syntaxes,
            width,
            show_line_numbers,
        ));
    }
    output
}

#[allow(clippy::too_many_arguments)]
fn highlighted_diff_line(
    old_number: Option<usize>,
    new_number: Option<usize>,
    marker: char,
    text: &str,
    background: Option<Color>,
    highlighter: Option<&mut HighlightLines<'static>>,
    syntaxes: &SyntaxSet,
    width: usize,
    show_line_numbers: bool,
) -> Line<'static> {
    let prefix = if show_line_numbers {
        format!(
            "{} {} │ {marker} ",
            diff_number(old_number),
            diff_number(new_number)
        )
    } else {
        format!("{marker} ")
    };
    let marker_color = match marker {
        '+' => Color::LightGreen,
        '−' => Color::LightRed,
        _ => Color::DarkGray,
    };
    let base_style = background
        .map(|color| Style::default().bg(color))
        .unwrap_or_default();
    let mut spans = vec![Span::styled(
        prefix.clone(),
        base_style.fg(marker_color).add_modifier(Modifier::BOLD),
    )];
    let mut remaining = width.saturating_sub(UnicodeWidthStr::width(prefix.as_str()));
    let line = format!("{text}\n");
    let highlighted =
        highlighter.and_then(|highlighter| highlighter.highlight_line(&line, syntaxes).ok());
    if let Some(highlighted) = highlighted {
        for (style, content) in highlighted {
            let content = content.trim_end_matches('\n');
            if content.is_empty() || remaining == 0 {
                continue;
            }
            let content = truncate_cells(content, remaining);
            remaining = remaining.saturating_sub(UnicodeWidthStr::width(content.as_str()));
            spans.push(Span::styled(
                content,
                base_style.fg(terminal_color(style.foreground)),
            ));
        }
    } else {
        let content = truncate_cells(text, remaining);
        remaining = remaining.saturating_sub(UnicodeWidthStr::width(content.as_str()));
        spans.push(Span::styled(content, base_style.fg(Color::Gray)));
    }
    if remaining > 0 {
        spans.push(Span::styled(" ".repeat(remaining), base_style));
    }
    Line::from(spans)
}

fn diff_file_path(line: &str) -> Option<&str> {
    [
        "*** Update File: ",
        "*** Add File: ",
        "*** Delete File: ",
        "+++ b/",
    ]
    .into_iter()
    .find_map(|prefix| line.strip_prefix(prefix))
    .filter(|path| *path != "/dev/null")
}

fn is_apply_patch_control_line(line: &str) -> bool {
    matches!(
        line,
        "*** Begin Patch" | "*** End Patch" | "@@" | "*** End of File"
    ) || line.starts_with("*** Update File: ")
        || line.starts_with("*** Add File: ")
        || line.starts_with("*** Delete File: ")
        || line.starts_with("*** Move to: ")
}

fn split_diff_lines(source: &str, width: usize) -> Vec<Line<'static>> {
    let pane = width.saturating_sub(3) / 2;
    let mut output = Vec::new();
    let mut pending_removed: Vec<(Option<usize>, &str)> = Vec::new();
    let (mut old_line, mut new_line) = (None, None);

    for raw in source.lines() {
        if raw.starts_with("---") || raw.starts_with("+++") || diff_file_path(raw).is_some() {
            continue;
        }
        if let Some((old, new)) = hunk_starts(raw) {
            for (number, before) in pending_removed.drain(..) {
                output.push(split_diff_row(number, before, None, "", pane));
            }
            old_line = Some(old);
            new_line = Some(new);
            continue;
        }
        if raw.starts_with('-') {
            pending_removed.push((old_line, raw.strip_prefix('-').unwrap_or(raw)));
            old_line = old_line.map(|line| line + 1);
            continue;
        }
        if raw.starts_with('+') {
            let (before_number, before) = pending_removed.first().copied().unwrap_or((None, ""));
            if !pending_removed.is_empty() {
                pending_removed.remove(0);
            }
            output.push(split_diff_row(
                before_number,
                before,
                new_line,
                raw.strip_prefix('+').unwrap_or(raw),
                pane,
            ));
            new_line = new_line.map(|line| line + 1);
            continue;
        }
        for (number, before) in pending_removed.drain(..) {
            output.push(split_diff_row(number, before, None, "", pane));
        }
        let text = raw.strip_prefix(' ').unwrap_or(raw);
        output.push(split_context_row(old_line, new_line, text, pane));
        old_line = old_line.map(|line| line + 1);
        new_line = new_line.map(|line| line + 1);
    }
    for (number, before) in pending_removed {
        output.push(split_diff_row(number, before, None, "", pane));
    }
    output
}

fn split_diff_row(
    before_number: Option<usize>,
    before: &str,
    after_number: Option<usize>,
    after: &str,
    pane: usize,
) -> Line<'static> {
    Line::from(vec![
        Span::styled(
            pad_cells(
                &format!("{} │ − {before}", diff_number(before_number)),
                pane,
            ),
            before_number.map_or_else(
                || Style::default().fg(Color::DarkGray),
                |_| Style::default().fg(Color::White).bg(DIFF_REMOVED_BG),
            ),
        ),
        Span::styled(" │ ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            pad_cells(&format!("{} │ + {after}", diff_number(after_number)), pane),
            after_number.map_or_else(
                || Style::default().fg(Color::DarkGray),
                |_| Style::default().fg(Color::White).bg(DIFF_ADDED_BG),
            ),
        ),
    ])
}

fn split_context_row(
    old_number: Option<usize>,
    new_number: Option<usize>,
    text: &str,
    pane: usize,
) -> Line<'static> {
    let before = pad_cells(&format!("{} │   {text}", diff_number(old_number)), pane);
    let after = pad_cells(&format!("{} │   {text}", diff_number(new_number)), pane);
    Line::from(vec![
        Span::styled(before, Style::default().fg(Color::Gray)),
        Span::styled(" │ ", Style::default().fg(Color::DarkGray)),
        Span::styled(after, Style::default().fg(Color::Gray)),
    ])
}

fn diagnostic_lines(source: &str, width: usize) -> Vec<Line<'static>> {
    let mut output = Vec::new();
    let rendered = parsed_diagnostics(source)
        .unwrap_or_else(|| source.lines().map(str::to_string).collect::<Vec<String>>());
    for raw in &rendered {
        let lowercase = raw.to_ascii_lowercase();
        let (glyph, color) = if lowercase.contains("error") {
            ("×", Color::LightRed)
        } else if lowercase.contains("warning") || lowercase.contains("warn") {
            ("▲", Color::Yellow)
        } else if lowercase.contains("hint") {
            ("·", Color::LightCyan)
        } else {
            ("●", Color::Blue)
        };
        output.push(Line::from(vec![
            Span::styled(
                format!(" {glyph} "),
                Style::default().fg(color).add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                truncate_cells(raw, width.saturating_sub(3)),
                Style::default().fg(color),
            ),
        ]));
    }
    output
}

fn parsed_diagnostics(source: &str) -> Option<Vec<String>> {
    let value: serde_json::Value = serde_json::from_str(source).ok()?;
    let items = value
        .get("items")
        .and_then(serde_json::Value::as_array)
        .or_else(|| value.as_array())?;
    if items.is_empty() {
        return Some(vec!["✓ no diagnostics".to_string()]);
    }
    Some(
        items
            .iter()
            .map(|item| {
                let severity = match item.get("severity").and_then(serde_json::Value::as_u64) {
                    Some(1) => "error",
                    Some(2) => "warning",
                    Some(3) => "info",
                    Some(4) => "hint",
                    _ => "diagnostic",
                };
                let line = item
                    .pointer("/range/start/line")
                    .and_then(serde_json::Value::as_u64)
                    .map(|line| line + 1)
                    .unwrap_or(0);
                let character = item
                    .pointer("/range/start/character")
                    .and_then(serde_json::Value::as_u64)
                    .map(|character| character + 1)
                    .unwrap_or(0);
                let message = item
                    .get("message")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("language-server diagnostic");
                format!(
                    "{severity} {line}:{character}  {}",
                    message.replace('\n', " ")
                )
            })
            .collect(),
    )
}

fn hunk_starts(line: &str) -> Option<(usize, usize)> {
    let mut fields = line.split_ascii_whitespace();
    (fields.next()? == "@@").then_some(())?;
    let old = fields
        .next()?
        .strip_prefix('-')?
        .split(',')
        .next()?
        .parse()
        .ok()?;
    let new = fields
        .next()?
        .strip_prefix('+')?
        .split(',')
        .next()?
        .parse()
        .ok()?;
    Some((old, new))
}

fn diff_number(number: Option<usize>) -> String {
    number.map_or_else(
        || " ".repeat(DIFF_NUMBER_WIDTH),
        |number| format!("{number:>DIFF_NUMBER_WIDTH$}"),
    )
}

fn terminal_color(color: SyntectColor) -> Color {
    Color::Rgb(color.r, color.g, color.b)
}

fn clip_line(
    mut spans: Vec<Span<'static>>,
    content_width: usize,
    _gutter_width: usize,
) -> Line<'static> {
    let mut remaining = content_width;
    for span in spans.iter_mut().skip(1) {
        let width = UnicodeWidthStr::width(span.content.as_ref());
        if width > remaining {
            span.content = truncate_cells(span.content.as_ref(), remaining).into();
            remaining = 0;
        } else {
            remaining -= width;
        }
    }
    Line::from(spans)
}

fn truncate_cells(value: &str, width: usize) -> String {
    if width == 0 {
        return String::new();
    }
    if UnicodeWidthStr::width(value) <= width {
        return value.to_string();
    }
    let keep = width.saturating_sub(1);
    let mut output = String::new();
    let mut cells = 0;
    for character in value.chars() {
        let character_width = unicode_width::UnicodeWidthChar::width(character).unwrap_or(0);
        if cells + character_width > keep {
            break;
        }
        cells += character_width;
        output.push(character);
    }
    output.push('…');
    output
}

fn pad_cells(value: &str, width: usize) -> String {
    let mut value = truncate_cells(value, width);
    value.push_str(&" ".repeat(width.saturating_sub(UnicodeWidthStr::width(value.as_str()))));
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diff_layout_tracks_the_available_terminal_width() {
        let diff = "@@ -1 +1 @@\n-old\n+new";
        let narrow = diff_lines(diff, 120, None);
        let wide = diff_lines(diff, 180, None);
        assert_eq!(narrow[0].to_string().matches('│').count(), 1);
        assert_eq!(wide[0].to_string().matches('│').count(), 3);
    }

    #[test]
    fn largely_one_sided_diff_forces_unified_layout() {
        let additions = "@@ -1,1 +1,5 @@\n old\n+one\n+two\n+three\n+four";
        let deletions = "@@ -1,5 +1,1 @@\n old\n-one\n-two\n-three\n-four";
        assert_eq!(
            diff_lines(additions, 180, None)[0]
                .to_string()
                .matches('│')
                .count(),
            1
        );
        assert_eq!(
            diff_lines(deletions, 180, None)[0]
                .to_string()
                .matches('│')
                .count(),
            1
        );
    }

    #[test]
    fn mixed_but_unbalanced_diff_uses_unified_layout() {
        let diff = "@@ -1 +1,3 @@\n-old\n+new one\n+new two\n+new three";
        assert_eq!(
            diff_lines(diff, 180, None)[0]
                .to_string()
                .matches('│')
                .count(),
            1
        );
    }

    #[test]
    fn removals_only_diff_uses_unified_layout() {
        let diff = "--- a/src/main.rs\n+++ /dev/null\n@@ -1,2 +0,0 @@\n-one\n-two";
        let lines = diff_lines(diff, 180, None);
        assert!(
            lines
                .iter()
                .all(|line| line.to_string().matches('│').count() <= 1),
            "a removals-only diff has no meaningful after pane: {lines:?}"
        );
        assert!(lines.iter().any(|line| line.to_string().contains("− one")));
    }

    #[test]
    fn additions_only_patch_uses_unified_layout() {
        let diff = "*** Begin Patch\n*** Add File: src/main.rs\n+fn main() {\n+    println!(\"hi\");\n+}\n*** End Patch";
        let lines = diff_lines(diff, 180, Some("rs"));

        assert!(
            lines.iter().all(|line| !line.to_string().contains(" │ − ")),
            "an additions-only patch has no meaningful before pane: {lines:?}"
        );
        assert!(lines.iter().all(|line| !line.to_string().contains("***")));
        assert_eq!(lines.len(), 3);
    }

    #[test]
    fn syntax_renderer_preserves_code_line_structure() {
        let lines = syntax_lines("rust", "fn main() {\n    println!(\"hi\");\n}", 80);
        assert_eq!(lines.len(), 3);
        assert!(lines[0].to_string().contains("fn main"));
    }

    #[test]
    fn command_renderer_omits_source_line_numbers() {
        let lines = code_block_lines("command", "just cli\ncargo check", 80);
        assert_eq!(lines[0].to_string(), "just cli");
        assert_eq!(lines[1].to_string(), "cargo check");
    }

    #[test]
    fn subagent_renderer_uses_the_shared_identity_colour() {
        let lines = code_block_lines("subagent", "TEAM · 1 subagent", 80);

        assert_eq!(
            lines[0].spans[0].style.fg,
            Some(super::super::SUBAGENT_PINK)
        );
    }

    #[test]
    fn reasoning_renderer_separates_codex_bold_summary_segments() {
        let lines = tool_body_lines(
            "reasoning",
            "**Inspecting blank lines****Restoring card padding**",
            80,
            "  │ ",
        );

        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0].to_string(), "  │ • Inspecting blank lines");
        assert_eq!(lines[1].to_string(), "  │ ");
        assert_eq!(lines[2].to_string(), "  │ • Restoring card padding");
        assert!(lines.iter().all(|line| !line.to_string().contains("**")));
    }

    #[test]
    fn reasoning_renderer_preserves_explicit_lines_and_wraps_each_item() {
        let lines = tool_body_lines(
            "reasoning",
            "First sentence\nsecond sentence that continues the same thought.",
            42,
            "  │ ",
        );

        assert_eq!(lines[0].to_string(), "  │ • First sentence");
        assert_eq!(lines[1].to_string(), "  │ ");
        assert!(lines[2].to_string().starts_with("  │ • second sentence"));
        assert!(lines[3].to_string().starts_with("  │   "));
        assert!(lines[3..].iter().all(|line| {
            line.to_string().contains("• ") == (line.to_string() == lines[2].to_string())
        }));
    }

    #[test]
    fn expanded_tool_bodies_share_a_containment_gutter() {
        for (language, source) in [
            ("command", "cargo check"),
            ("json", "{\"path\":\"src/lib.rs\"}"),
            ("lsp", "warning 1:1  unused"),
            ("diff:rs", "@@ -1 +1 @@\n-old\n+new"),
        ] {
            let lines = tool_body_lines(language, source, 80, "  │ ");
            assert!(!lines.is_empty());
            assert!(
                lines
                    .iter()
                    .all(|line| line.to_string().starts_with("  │ "))
            );
        }
    }

    #[test]
    fn code_diff_combines_syntax_foregrounds_with_change_backgrounds() {
        let diff = "*** Update File: src/main.rs\n@@ -1 +1 @@\n-fn old() {}\n+fn main() {}";
        let lines = diff_lines(diff, 100, None);
        let added = lines
            .iter()
            .find(|line| line.to_string().contains("fn main"))
            .expect("added Rust line");
        assert!(
            added
                .spans
                .iter()
                .any(|span| matches!(span.style.fg, Some(Color::Rgb(_, _, _)))),
            "syntax highlighting should retain an RGB foreground"
        );
        assert!(
            added
                .spans
                .iter()
                .any(|span| span.style.bg == Some(DIFF_ADDED_BG)),
            "added code should retain the diff background"
        );
    }

    #[test]
    fn path_annotated_diff_highlights_rust_without_file_headers() {
        let diff = "@@ -1 +1 @@\n-fn old() {}\n+fn main() {}";
        let lines = code_block_lines("diff:rs", diff, 100);
        let added = lines
            .iter()
            .find(|line| line.to_string().contains("fn main"))
            .expect("added Rust line");
        assert!(
            added
                .spans
                .iter()
                .any(|span| matches!(span.style.fg, Some(Color::Rgb(_, _, _)))),
            "path-derived Rust syntax should be highlighted"
        );
    }

    #[test]
    fn wide_numbered_diff_uses_split_layout_with_a_language_hint() {
        let diff = "@@ -1 +1 @@\n-fn old() {}\n+fn main() {}";
        let lines = code_block_lines("diff:rs", diff, 180);

        assert!(
            lines
                .iter()
                .any(|line| line.to_string().matches('│').count() == 3),
            "wide numbered diff should use before/after panes: {lines:?}"
        );
    }
}
