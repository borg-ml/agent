use std::collections::VecDeque;
use std::sync::OnceLock;

use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use syntect::easy::HighlightLines;
use syntect::highlighting::{Color as SyntectColor, Theme, ThemeSet};
use syntect::parsing::{SyntaxDefinition, SyntaxReference, SyntaxSet};
use unicode_width::UnicodeWidthStr;

const SPLIT_DIFF_MIN_WIDTH: usize = 160;
const CODE_GUTTER_WIDTH: usize = 5;
const DIFF_NUMBER_WIDTH: usize = 4;
const DIFF_ADDED_BG: Color = Color::Rgb(25, 57, 39);
const DIFF_REMOVED_BG: Color = Color::Rgb(67, 31, 34);
const TOML_SYNTAX: &str = r#"%YAML 1.2
---
name: TOML
file_extensions: [toml, tml]
scope: source.toml
contexts:
  main:
    - match: '#.*$'
      scope: comment.line.number-sign.toml
    - match: '^\s*(\[+)([^\]]+)(\]+)'
      captures:
        1: punctuation.section.brackets.begin.toml
        2: entity.name.section.toml
        3: punctuation.section.brackets.end.toml
    - match: '^\s*([A-Za-z0-9_.-]+)\s*(=)'
      captures:
        1: meta.mapping.key.toml
        2: punctuation.separator.key-value.toml
    - match: '"'
      scope: punctuation.definition.string.begin.toml
      push: double-quoted-string
    - match: "'"
      scope: punctuation.definition.string.begin.toml
      push: single-quoted-string
    - match: '\b(?:true|false)\b'
      scope: constant.language.boolean.toml
    - match: '[-+]?\b(?:0x[0-9A-Fa-f_]+|0o[0-7_]+|0b[01_]+|[0-9][0-9_.eE+-]*)\b'
      scope: constant.numeric.toml
  double-quoted-string:
    - meta_scope: string.quoted.double.toml
    - match: '\\.'
      scope: constant.character.escape.toml
    - match: '"'
      scope: punctuation.definition.string.end.toml
      pop: true
  single-quoted-string:
    - meta_scope: string.quoted.single.toml
    - match: "'"
      scope: punctuation.definition.string.end.toml
      pop: true
"#;

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
        .flat_map(|item| {
            super::wrap_display(&item, width.max(1))
                .into_iter()
                .map(|line| {
                    Line::from(Span::styled(
                        line,
                        Style::default()
                            .fg(Color::Gray)
                            .add_modifier(Modifier::ITALIC),
                    ))
                })
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
    source
        .lines()
        .flat_map(|line| super::wrap_display(line, width.max(1)))
        .map(|line| Line::from(Span::styled(line, Style::default().fg(Color::White))))
        .collect()
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
    let syntax = syntax_for_language(syntaxes, language)
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
    let syntaxes = SYNTAXES.get_or_init(|| {
        let mut builder = SyntaxSet::load_defaults_newlines().into_builder();
        builder.add(
            SyntaxDefinition::load_from_str(TOML_SYNTAX, true, Some("TOML"))
                .expect("embedded TOML syntax definition must parse"),
        );
        builder.build()
    });
    let themes = THEMES.get_or_init(ThemeSet::load_defaults);
    (syntaxes, &themes.themes["base16-ocean.dark"])
}

fn syntax_for_language<'a>(syntaxes: &'a SyntaxSet, language: &str) -> Option<&'a SyntaxReference> {
    syntaxes
        .find_syntax_by_token(language)
        .or_else(|| syntaxes.find_syntax_by_extension(language))
        .or_else(|| match language {
            // Syntect's compact default catalog omits TypeScript. JavaScript
            // still highlights the shared syntax instead of dropping these
            // very common diffs to plain text.
            "ts" | "tsx" | "mts" | "cts" | "typescript" => syntaxes
                .find_syntax_by_extension("js")
                .or_else(|| syntaxes.find_syntax_by_token("javascript")),
            _ => None,
        })
}

fn diff_lines(source: &str, width: usize, source_language: Option<&str>) -> Vec<Line<'static>> {
    if width >= SPLIT_DIFF_MIN_WIDTH
        && diff_has_balanced_changes(source)
        && source.lines().any(|line| hunk_starts(line).is_some())
        && !source.lines().any(is_apply_patch_control_line)
    {
        split_diff_lines(source, width, source_language)
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
    let number_width = diff_number_width(source);
    let (syntaxes, theme) = syntax_assets();
    let mut highlighter = source_language
        .and_then(|language| syntax_for_language(syntaxes, language))
        .map(|syntax| HighlightLines::new(syntax, theme));
    let show_line_numbers = source.lines().any(|line| hunk_starts(line).is_some());
    for raw in source.lines() {
        if let Some(path) = diff_file_path(raw) {
            highlighter = path
                .rsplit_once('.')
                .and_then(|(_, extension)| syntax_for_language(syntaxes, extension))
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
            number_width,
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
    number_width: usize,
) -> Line<'static> {
    let prefix = if show_line_numbers {
        format!(
            "{} {} │ {marker} ",
            diff_number(old_number, number_width),
            diff_number(new_number, number_width)
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
    let remaining = width.saturating_sub(UnicodeWidthStr::width(prefix.as_str()));
    spans.extend(highlighted_source_spans(
        text,
        highlighter,
        syntaxes,
        base_style,
        Color::Gray,
        remaining,
    ));
    Line::from(spans)
}

fn highlighted_source_spans(
    text: &str,
    highlighter: Option<&mut HighlightLines<'static>>,
    syntaxes: &SyntaxSet,
    base_style: Style,
    fallback_color: Color,
    width: usize,
) -> Vec<Span<'static>> {
    let mut spans = Vec::new();
    let mut remaining = width;
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
        spans.push(Span::styled(content, base_style.fg(fallback_color)));
    }
    if remaining > 0 {
        spans.push(Span::styled(" ".repeat(remaining), base_style));
    }
    spans
}

fn diff_file_path(line: &str) -> Option<&str> {
    let path = [
        "*** Update File: ",
        "*** Add File: ",
        "*** Delete File: ",
        "+++ b/",
    ]
    .into_iter()
    .find_map(|prefix| line.strip_prefix(prefix))
    .or_else(|| line.strip_prefix("+++ "))?;
    (path != "/dev/null").then_some(path)
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

fn split_diff_lines(
    source: &str,
    width: usize,
    source_language: Option<&str>,
) -> Vec<Line<'static>> {
    let pane = width.saturating_sub(3) / 2;
    let number_width = diff_number_width(source);
    let mut output = Vec::new();
    let mut pending_removed: VecDeque<(Option<usize>, &str)> = VecDeque::new();
    let (mut old_line, mut new_line) = (None, None);
    let mut syntax = SplitDiffSyntax::new(source_language);

    for raw in source.lines() {
        if raw.starts_with("---") || raw.starts_with("+++") || diff_file_path(raw).is_some() {
            continue;
        }
        if let Some((old, new)) = hunk_starts(raw) {
            for (number, before) in pending_removed.drain(..) {
                output.push(split_diff_row(
                    number,
                    before,
                    None,
                    "",
                    pane,
                    number_width,
                    &mut syntax,
                ));
            }
            old_line = Some(old);
            new_line = Some(new);
            continue;
        }
        if raw.starts_with('-') {
            pending_removed.push_back((old_line, raw.strip_prefix('-').unwrap_or(raw)));
            old_line = old_line.map(|line| line + 1);
            continue;
        }
        if raw.starts_with('+') {
            let (before_number, before) = pending_removed.pop_front().unwrap_or((None, ""));
            output.push(split_diff_row(
                before_number,
                before,
                new_line,
                raw.strip_prefix('+').unwrap_or(raw),
                pane,
                number_width,
                &mut syntax,
            ));
            new_line = new_line.map(|line| line + 1);
            continue;
        }
        for (number, before) in pending_removed.drain(..) {
            output.push(split_diff_row(
                number,
                before,
                None,
                "",
                pane,
                number_width,
                &mut syntax,
            ));
        }
        let text = raw.strip_prefix(' ').unwrap_or(raw);
        output.push(split_context_row(
            old_line,
            new_line,
            text,
            pane,
            number_width,
            &mut syntax,
        ));
        old_line = old_line.map(|line| line + 1);
        new_line = new_line.map(|line| line + 1);
    }
    for (number, before) in pending_removed {
        output.push(split_diff_row(
            number,
            before,
            None,
            "",
            pane,
            number_width,
            &mut syntax,
        ));
    }
    output
}

struct SplitDiffSyntax {
    syntaxes: &'static SyntaxSet,
    before: Option<HighlightLines<'static>>,
    after: Option<HighlightLines<'static>>,
}

impl SplitDiffSyntax {
    fn new(source_language: Option<&str>) -> Self {
        let (syntaxes, theme) = syntax_assets();
        let syntax = source_language.and_then(|language| syntax_for_language(syntaxes, language));
        Self {
            syntaxes,
            before: syntax.map(|syntax| HighlightLines::new(syntax, theme)),
            after: syntax.map(|syntax| HighlightLines::new(syntax, theme)),
        }
    }
}

fn split_diff_row(
    before_number: Option<usize>,
    before: &str,
    after_number: Option<usize>,
    after: &str,
    pane: usize,
    number_width: usize,
    syntax: &mut SplitDiffSyntax,
) -> Line<'static> {
    let mut spans = split_diff_pane(
        before_number,
        '−',
        before,
        pane,
        number_width,
        DIFF_REMOVED_BG,
        syntax.before.as_mut(),
        syntax.syntaxes,
    );
    spans.push(Span::styled(" │ ", Style::default().fg(Color::DarkGray)));
    spans.extend(split_diff_pane(
        after_number,
        '+',
        after,
        pane,
        number_width,
        DIFF_ADDED_BG,
        syntax.after.as_mut(),
        syntax.syntaxes,
    ));
    Line::from(spans)
}

#[allow(clippy::too_many_arguments)]
fn split_diff_pane(
    number: Option<usize>,
    marker: char,
    text: &str,
    width: usize,
    number_width: usize,
    background: Color,
    highlighter: Option<&mut HighlightLines<'static>>,
    syntaxes: &SyntaxSet,
) -> Vec<Span<'static>> {
    let prefix = format!("{} │ {marker} ", diff_number(number, number_width));
    let content_width = width.saturating_sub(UnicodeWidthStr::width(prefix.as_str()));
    let Some(_) = number else {
        return vec![Span::styled(
            pad_cells(&prefix, width),
            Style::default().fg(Color::DarkGray),
        )];
    };
    let base_style = Style::default().bg(background);
    let marker_color = if marker == '+' {
        Color::LightGreen
    } else {
        Color::LightRed
    };
    let mut spans = vec![Span::styled(
        prefix,
        base_style.fg(marker_color).add_modifier(Modifier::BOLD),
    )];
    spans.extend(highlighted_source_spans(
        text,
        highlighter,
        syntaxes,
        base_style,
        Color::White,
        content_width,
    ));
    spans
}

fn split_context_row(
    old_number: Option<usize>,
    new_number: Option<usize>,
    text: &str,
    pane: usize,
    number_width: usize,
    syntax: &mut SplitDiffSyntax,
) -> Line<'static> {
    let mut spans = split_context_pane(
        old_number,
        text,
        pane,
        number_width,
        syntax.before.as_mut(),
        syntax.syntaxes,
    );
    spans.push(Span::styled(" │ ", Style::default().fg(Color::DarkGray)));
    spans.extend(split_context_pane(
        new_number,
        text,
        pane,
        number_width,
        syntax.after.as_mut(),
        syntax.syntaxes,
    ));
    Line::from(spans)
}

fn split_context_pane(
    number: Option<usize>,
    text: &str,
    width: usize,
    number_width: usize,
    highlighter: Option<&mut HighlightLines<'static>>,
    syntaxes: &SyntaxSet,
) -> Vec<Span<'static>> {
    let prefix = format!("{} │   ", diff_number(number, number_width));
    let content_width = width.saturating_sub(UnicodeWidthStr::width(prefix.as_str()));
    let mut spans = vec![Span::styled(prefix, Style::default().fg(Color::DarkGray))];
    spans.extend(highlighted_source_spans(
        text,
        highlighter,
        syntaxes,
        Style::default(),
        Color::Gray,
        content_width,
    ));
    spans
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

fn diff_number_width(source: &str) -> usize {
    let (mut old_line, mut new_line) = (None, None);
    let mut largest = 0;
    for raw in source.lines() {
        if let Some((old, new)) = hunk_starts(raw) {
            old_line = Some(old);
            new_line = Some(new);
            continue;
        }
        if raw.starts_with("---")
            || raw.starts_with("+++")
            || diff_file_path(raw).is_some()
            || is_apply_patch_control_line(raw)
        {
            continue;
        }
        if let Some(line) = old_line.filter(|_| !raw.starts_with('+')) {
            largest = largest.max(line);
            old_line = Some(line + 1);
        }
        if let Some(line) = new_line.filter(|_| !raw.starts_with('-')) {
            largest = largest.max(line);
            new_line = Some(line + 1);
        }
    }
    largest.to_string().len().max(DIFF_NUMBER_WIDTH)
}

fn diff_number(number: Option<usize>, width: usize) -> String {
    number.map_or_else(|| " ".repeat(width), |number| format!("{number:>width$}"))
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
    use std::fmt::Write as _;
    use std::time::Instant;

    #[test]
    fn diff_layout_tracks_the_available_terminal_width() {
        let diff = "@@ -1 +1 @@\n-old\n+new";
        let narrow = diff_lines(diff, 120, None);
        let wide = diff_lines(diff, 180, None);
        assert_eq!(narrow[0].to_string().matches('│').count(), 1);
        assert_eq!(wide[0].to_string().matches('│').count(), 3);
    }

    #[test]
    fn five_digit_diff_numbers_keep_one_aligned_gutter() {
        let diff = "@@ -5294 +5292 @@\n-old\n+new\n@@ -11701 +11694 @@\n context";
        let lines = diff_lines(diff, 120, None);
        let gutters = lines
            .iter()
            .map(|line| line.to_string().find('│').expect("number gutter"))
            .collect::<Vec<_>>();

        assert!(
            gutters.iter().all(|gutter| *gutter == gutters[0]),
            "{lines:?}"
        );
        assert!(
            lines
                .iter()
                .any(|line| line.to_string().starts_with(" 5294"))
        );
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

        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0].to_string(), "  │ Inspecting blank lines");
        assert_eq!(lines[1].to_string(), "  │ Restoring card padding");
        assert!(lines.iter().all(|line| !line.to_string().contains("**")));
        assert!(lines.iter().all(|line| !line.to_string().contains('•')));
    }

    #[test]
    fn reasoning_renderer_preserves_explicit_lines_and_wraps_each_item() {
        let lines = tool_body_lines(
            "reasoning",
            "First sentence\nsecond sentence that continues the same thought.",
            42,
            "  │ ",
        );

        assert_eq!(lines[0].to_string(), "  │ First sentence");
        assert!(lines[1].to_string().starts_with("  │ second sentence"));
        assert!(lines[2..].iter().all(|line| {
            !line.to_string().contains("• ") && !line.to_string().trim().is_empty()
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
    fn multi_file_diff_switches_syntax_for_yaml_and_toml_hunks() {
        let diff = "*** Update File: config/app.yaml\n@@ -1 +1 @@\n-enabled: false\n+enabled: true\n*** Update File: Cargo.toml\n@@ -1 +1 @@\n-name = \"old\"\n+name = \"borg\"";
        let lines = code_block_lines("diff", diff, 100);

        for content in ["enabled: true", "name = \"borg\""] {
            let added = lines
                .iter()
                .find(|line| line.to_string().contains(content))
                .unwrap_or_else(|| panic!("missing highlighted line: {content}"));
            assert!(
                added
                    .spans
                    .iter()
                    .any(|span| matches!(span.style.fg, Some(Color::Rgb(_, _, _)))),
                "{content} should retain a syntax foreground: {added:?}"
            );
            assert!(
                added
                    .spans
                    .iter()
                    .any(|span| span.style.bg == Some(DIFF_ADDED_BG)),
                "{content} should retain the addition background: {added:?}"
            );
        }
    }

    #[test]
    fn default_syntax_catalog_covers_common_diff_filetypes() {
        let (syntaxes, _) = syntax_assets();
        for extension in [
            "yaml", "yml", "toml", "rs", "py", "js", "ts", "tsx", "go", "java", "c", "cpp", "cs",
            "rb", "php", "sh",
        ] {
            assert!(
                syntax_for_language(syntaxes, extension).is_some(),
                "missing syntax grammar for .{extension}"
            );
        }
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
        let changed = lines
            .iter()
            .find(|line| line.to_string().contains("fn main"))
            .expect("split changed row");
        assert!(
            changed.spans.iter().any(|span| {
                span.style.bg == Some(DIFF_ADDED_BG)
                    && matches!(span.style.fg, Some(Color::Rgb(_, _, _)))
            }),
            "split diff should retain syntax and change colors: {changed:?}"
        );
    }

    #[test]
    #[ignore = "explicit large split-diff rendering performance gate"]
    fn large_replacement_diff_profile() {
        const CHANGED_LINES: usize = 50_000;
        let mut diff = format!("@@ -1,{CHANGED_LINES} +1,{CHANGED_LINES} @@\n");
        for index in 0..CHANGED_LINES {
            writeln!(diff, "-before line {index}").expect("write removal");
        }
        for index in 0..CHANGED_LINES {
            writeln!(diff, "+after line {index}").expect("write addition");
        }

        let started = Instant::now();
        let lines = diff_lines(&diff, 180, None);
        let elapsed = started.elapsed();
        eprintln!("50k-line split replacement diff: {elapsed:?}");

        assert_eq!(lines.len(), CHANGED_LINES);
        assert!(
            elapsed < std::time::Duration::from_millis(100),
            "large split-diff rendering exceeded 100 ms: {elapsed:?}"
        );
    }
}
