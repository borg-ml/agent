use super::*;

pub(super) fn markdown_lines(
    markdown: &str,
    width: usize,
    text_color: Option<Color>,
) -> Vec<Line<'static>> {
    let markdown = escape_currency_dollars(markdown);
    let mut lines = Vec::new();
    let mut current = Vec::new();
    let base_style = text_color.map_or_else(Style::default, |color| Style::default().fg(color));
    let mut styles = vec![base_style];
    let mut code_block: Option<String> = None;
    let mut code = String::new();
    let mut table: Option<MarkdownTable> = None;
    let mut quote_depth = 0usize;
    let mut list_indices = Vec::new();
    for event in Parser::new_ext(&markdown, Options::all()) {
        match event {
            MarkdownEvent::Start(Tag::Table(alignments)) => {
                flush_markdown_line(&mut lines, &mut current, width, quote_depth);
                table = Some(MarkdownTable::new(alignments));
            }
            MarkdownEvent::Start(Tag::TableHead | Tag::TableRow) => {
                if let Some(table) = table.as_mut() {
                    table.start_row();
                }
            }
            MarkdownEvent::Start(Tag::TableCell) => {
                if let Some(table) = table.as_mut() {
                    table.start_cell();
                }
            }
            MarkdownEvent::Start(Tag::Heading { level, .. }) => {
                flush_markdown_line(&mut lines, &mut current, width, quote_depth);
                push_markdown_style(&mut styles, markdown_heading_style(level));
            }
            MarkdownEvent::End(TagEnd::Heading(_)) => {
                flush_markdown_line(&mut lines, &mut current, width, quote_depth);
                pop_markdown_style(&mut styles);
            }
            MarkdownEvent::Start(Tag::CodeBlock(kind)) => {
                flush_markdown_line(&mut lines, &mut current, width, quote_depth);
                code_block = Some(match kind {
                    CodeBlockKind::Fenced(language) => language.to_string(),
                    CodeBlockKind::Indented => String::new(),
                });
                code.clear();
            }
            MarkdownEvent::End(TagEnd::CodeBlock) => {
                if quote_depth > 0 {
                    lines.extend(quoted_lines(
                        &code,
                        width,
                        markdown_style(&styles),
                        quote_depth,
                    ));
                } else {
                    lines.extend(rendering::code_block_lines(
                        code_block.as_deref().unwrap_or_default(),
                        &code,
                        width,
                    ));
                }
                code_block = None;
                code.clear();
            }
            MarkdownEvent::Start(Tag::BlockQuote(_)) => {
                flush_markdown_line(&mut lines, &mut current, width, quote_depth);
                quote_depth += 1;
                push_markdown_style(
                    &mut styles,
                    Style::default()
                        .fg(Color::Gray)
                        .add_modifier(Modifier::ITALIC),
                );
            }
            MarkdownEvent::End(TagEnd::BlockQuote(_)) => {
                flush_markdown_line(&mut lines, &mut current, width, quote_depth);
                pop_markdown_style(&mut styles);
                quote_depth = quote_depth.saturating_sub(1);
            }
            MarkdownEvent::Start(Tag::List(start)) => {
                if !current.is_empty() {
                    flush_markdown_line(&mut lines, &mut current, width, quote_depth);
                }
                list_indices.push(start);
            }
            MarkdownEvent::End(TagEnd::List(_)) => {
                list_indices.pop();
            }
            MarkdownEvent::Start(Tag::Strong) => {
                push_markdown_style(&mut styles, Style::default().add_modifier(Modifier::BOLD));
            }
            MarkdownEvent::End(TagEnd::Strong) => pop_markdown_style(&mut styles),
            MarkdownEvent::Start(Tag::Emphasis) => {
                push_markdown_style(&mut styles, Style::default().add_modifier(Modifier::ITALIC));
            }
            MarkdownEvent::End(TagEnd::Emphasis) => pop_markdown_style(&mut styles),
            MarkdownEvent::Start(Tag::Strikethrough) => {
                push_markdown_style(
                    &mut styles,
                    Style::default().add_modifier(Modifier::CROSSED_OUT),
                );
            }
            MarkdownEvent::End(TagEnd::Strikethrough) => pop_markdown_style(&mut styles),
            MarkdownEvent::Start(Tag::Link { .. }) => {
                push_markdown_style(
                    &mut styles,
                    Style::default()
                        .fg(Color::LightBlue)
                        .add_modifier(Modifier::UNDERLINED),
                );
            }
            MarkdownEvent::End(TagEnd::Link) => pop_markdown_style(&mut styles),
            MarkdownEvent::End(TagEnd::TableCell) => {
                if let Some(table) = table.as_mut() {
                    table.finish_cell();
                }
            }
            MarkdownEvent::End(TagEnd::TableHead | TagEnd::TableRow) => {
                if let Some(table) = table.as_mut() {
                    table.finish_row();
                }
            }
            MarkdownEvent::End(TagEnd::Table) => {
                if let Some(table) = table.take() {
                    lines.extend(table.render(width, base_style));
                }
            }
            MarkdownEvent::Text(text) => {
                if let Some(table) = table.as_mut() {
                    table.push_text(&text);
                } else if code_block.is_some() {
                    code.push_str(&text);
                } else {
                    current.push(Span::styled(text.into_string(), markdown_style(&styles)));
                }
            }
            MarkdownEvent::Code(text) => {
                if let Some(table) = table.as_mut() {
                    table.push_text(&text);
                } else {
                    current.push(Span::styled(
                        text.into_string(),
                        markdown_style(&styles)
                            .fg(Color::LightCyan)
                            .add_modifier(Modifier::BOLD),
                    ));
                }
            }
            MarkdownEvent::InlineMath(source) => {
                if let Some(table) = table.as_mut() {
                    if currency_math_source(&source) {
                        table.push_text(&format!("${source}$"));
                    } else {
                        table.push_text(&terminal_math_lines(&source).join(" "));
                    }
                } else if currency_math_source(&source) {
                    // pulldown-cmark treats a currency expression such as
                    // `$0.1667/hour (~$4/day)` as math because the second
                    // dollar sign closes the first pair. Keep it literal.
                    current.push(Span::styled(format!("${source}$"), markdown_style(&styles)));
                } else {
                    let math = terminal_math_lines(&source);
                    if let [line] = math.as_slice() {
                        current.push(Span::styled(
                            line.clone(),
                            markdown_style(&styles).fg(Color::LightCyan),
                        ));
                    } else {
                        flush_markdown_line(&mut lines, &mut current, width, quote_depth);
                        push_terminal_math_lines(
                            &mut lines,
                            &mut current,
                            math,
                            width,
                            quote_depth,
                            markdown_style(&styles),
                        );
                    }
                }
            }
            MarkdownEvent::DisplayMath(source) => {
                let math = terminal_math_lines(&source);
                if let Some(table) = table.as_mut() {
                    table.push_text(&math.join(" "));
                } else {
                    flush_markdown_line(&mut lines, &mut current, width, quote_depth);
                    push_terminal_math_lines(
                        &mut lines,
                        &mut current,
                        math,
                        width,
                        quote_depth,
                        markdown_style(&styles),
                    );
                }
            }
            MarkdownEvent::SoftBreak => {
                if let Some(table) = table.as_mut() {
                    table.push_text(" ");
                } else {
                    current.push(Span::styled(" ", markdown_style(&styles)));
                }
            }
            MarkdownEvent::HardBreak => {
                flush_markdown_line(&mut lines, &mut current, width, quote_depth);
            }
            MarkdownEvent::Start(Tag::Item) => {
                if !current.is_empty() {
                    flush_markdown_line(&mut lines, &mut current, width, quote_depth);
                }
                let depth = list_indices.len().saturating_sub(1);
                if depth > 0 {
                    current.push(Span::raw("  ".repeat(depth)));
                }
                let marker = match list_indices.last_mut() {
                    Some(Some(index)) => {
                        let marker = format!("{index}. ");
                        *index += 1;
                        marker
                    }
                    _ => "• ".to_string(),
                };
                current.push(Span::styled(
                    marker,
                    Style::default()
                        .fg(BORG_ORANGE_HOVER)
                        .add_modifier(Modifier::BOLD),
                ));
            }
            MarkdownEvent::Rule => {
                flush_markdown_line(&mut lines, &mut current, width, quote_depth);
                lines.push(Line::from(Span::styled(
                    "─".repeat(width.min(48)),
                    Style::default().fg(Color::DarkGray),
                )));
            }
            MarkdownEvent::End(TagEnd::Item | TagEnd::Paragraph) => {
                flush_markdown_line(&mut lines, &mut current, width, quote_depth);
            }
            _ => {}
        }
    }
    flush_markdown_line(&mut lines, &mut current, width, quote_depth);
    lines
}

/// Return the readable text represented by Markdown without copying its
/// presentation syntax. In particular, fenced code contributes only its
/// source, not the opening language marker or closing fence.
pub(super) fn markdown_plain_text(markdown: &str) -> String {
    let markdown = escape_currency_dollars(markdown);
    let mut output = String::new();
    let mut list_indices = Vec::new();

    for event in Parser::new_ext(&markdown, Options::all()) {
        match event {
            MarkdownEvent::Start(Tag::CodeBlock(_)) => append_line_break(&mut output),
            MarkdownEvent::End(TagEnd::CodeBlock) => append_line_break(&mut output),
            MarkdownEvent::Start(Tag::List(start)) => list_indices.push(start),
            MarkdownEvent::End(TagEnd::List(_)) => {
                list_indices.pop();
                append_line_break(&mut output);
            }
            MarkdownEvent::Start(Tag::Item) => {
                append_line_break(&mut output);
                let depth = list_indices.len().saturating_sub(1);
                if depth > 0 {
                    output.push_str(&"  ".repeat(depth));
                }
                match list_indices.last_mut() {
                    Some(Some(index)) => {
                        output.push_str(&format!("{index}. "));
                        *index += 1;
                    }
                    _ => output.push_str("• "),
                }
            }
            MarkdownEvent::End(TagEnd::Item | TagEnd::Paragraph | TagEnd::Heading(_)) => {
                append_line_break(&mut output);
            }
            MarkdownEvent::Start(Tag::TableRow) => append_line_break(&mut output),
            MarkdownEvent::End(TagEnd::TableRow | TagEnd::Table) => {
                append_line_break(&mut output);
            }
            MarkdownEvent::End(TagEnd::TableCell) => output.push('\t'),
            MarkdownEvent::Text(text) | MarkdownEvent::Code(text) => output.push_str(&text),
            MarkdownEvent::InlineMath(source) | MarkdownEvent::DisplayMath(source) => {
                output.push_str(&source)
            }
            MarkdownEvent::SoftBreak => output.push(' '),
            MarkdownEvent::HardBreak | MarkdownEvent::Rule => append_line_break(&mut output),
            MarkdownEvent::TaskListMarker(checked) => {
                output.push_str(if checked { "[x] " } else { "[ ] " });
            }
            MarkdownEvent::Html(html) => output.push_str(&html),
            _ => {}
        }
    }

    let mut lines = output.split('\n').collect::<Vec<_>>();
    while lines.first().is_some_and(|line| line.trim().is_empty()) {
        lines.remove(0);
    }
    while lines.last().is_some_and(|line| line.trim().is_empty()) {
        lines.pop();
    }
    lines.join("\n")
}

fn append_line_break(output: &mut String) {
    if !output.is_empty() && !output.ends_with('\n') {
        output.push('\n');
    }
}

pub(super) fn markdown_link_ranges(markdown: &str, lines: &[Line<'_>]) -> Vec<LinkRowRange> {
    let markdown = escape_currency_dollars(markdown);
    let mut targets = Vec::new();
    let mut active: Option<(Option<String>, String)> = None;
    for event in Parser::new_ext(&markdown, Options::all()) {
        match event {
            MarkdownEvent::Start(Tag::Link { dest_url, .. }) => {
                active = Some((safe_http_url(&dest_url), String::new()));
            }
            MarkdownEvent::Text(text) | MarkdownEvent::Code(text) => {
                if let Some((_, label)) = active.as_mut() {
                    label.push_str(&text);
                }
            }
            MarkdownEvent::SoftBreak | MarkdownEvent::HardBreak => {
                if let Some((_, label)) = active.as_mut() {
                    label.push(' ');
                }
            }
            MarkdownEvent::End(TagEnd::Link) => {
                if let Some((url, label)) = active.take() {
                    let key: String = label
                        .chars()
                        .filter(|character| !character.is_whitespace())
                        .collect();
                    if !key.is_empty() {
                        targets.push((url, key));
                    }
                }
            }
            _ => {}
        }
    }
    let mut links = Vec::new();
    let mut target_index = 0usize;
    let mut consumed = String::new();
    for (row, line) in lines.iter().enumerate() {
        let mut column = 0usize;
        for span in &line.spans {
            let width = span.width();
            if target_index < targets.len()
                && span.style.fg == Some(Color::LightBlue)
                && span.style.add_modifier.contains(Modifier::UNDERLINED)
            {
                if let Some(url) = &targets[target_index].0 {
                    links.push(LinkRowRange {
                        row,
                        start: column,
                        end: column + width,
                        url: url.clone(),
                    });
                }
                consumed.extend(
                    span.content
                        .chars()
                        .filter(|character| !character.is_whitespace()),
                );
                if consumed == targets[target_index].1 {
                    consumed.clear();
                    target_index += 1;
                }
            }
            column += width;
        }
    }
    links
}

fn safe_http_url(value: &str) -> Option<String> {
    let parsed = url::Url::parse(value).ok()?;
    matches!(parsed.scheme(), "http" | "https").then(|| parsed.to_string())
}

pub(super) fn open_http_link(url: &str) -> Result<()> {
    let url = safe_http_url(url).context("only HTTP(S) links can be opened")?;
    let mut command = if cfg!(target_os = "macos") {
        std::process::Command::new("open")
    } else if cfg!(target_os = "windows") {
        let mut command = std::process::Command::new("cmd");
        command.args(["/C", "start", ""]);
        command
    } else {
        std::process::Command::new("xdg-open")
    };
    command
        .arg(url)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .context("failed to start the system browser")?;
    Ok(())
}

fn terminal_math_lines(source: &str) -> Vec<String> {
    term_maths::render(source)
        .to_string()
        .lines()
        .map(|line| line.trim_end().to_string())
        .collect()
}

fn escape_currency_dollars(markdown: &str) -> String {
    let mut escaped = String::with_capacity(markdown.len());
    let mut index = 0;
    while index < markdown.len() {
        let character = markdown[index..]
            .chars()
            .next()
            .expect("index must remain on a character boundary");
        if character == '\\' {
            let next = index + character.len_utf8();
            escaped.push(character);
            if let Some(next_character) = markdown[next..].chars().next() {
                escaped.push(next_character);
                index = next + next_character.len_utf8();
            } else {
                index = next;
            }
            continue;
        }
        if character == '`' {
            let run = markdown[index..]
                .chars()
                .take_while(|character| *character == '`')
                .count();
            let content_start = index + run;
            let end = find_code_span_end(markdown, content_start, run).unwrap_or(markdown.len());
            escaped.push_str(&markdown[index..end]);
            index = end;
            continue;
        }
        if character == '$'
            && markdown[index + character.len_utf8()..]
                .chars()
                .next()
                .is_some_and(|next| next.is_ascii_digit())
        {
            let content_start = index + character.len_utf8();
            let close = find_unescaped_dollar(markdown, content_start);
            let is_currency = close
                .is_some_and(|close| currency_math_source(&markdown[content_start..close]))
                || close.is_none() && currency_math_source(&markdown[content_start..]);
            if is_currency {
                escaped.push('\\');
                escaped.push('$');
                index = content_start;
                continue;
            }
        }
        escaped.push(character);
        index += character.len_utf8();
    }
    escaped
}

fn find_code_span_end(markdown: &str, mut index: usize, delimiter_len: usize) -> Option<usize> {
    while index < markdown.len() {
        let character = markdown[index..]
            .chars()
            .next()
            .expect("index must remain on a character boundary");
        if character != '`' {
            index += character.len_utf8();
            continue;
        }
        let run = markdown[index..]
            .chars()
            .take_while(|character| *character == '`')
            .count();
        if run == delimiter_len {
            return Some(index + run);
        }
        index += run;
    }
    None
}

fn find_unescaped_dollar(markdown: &str, mut index: usize) -> Option<usize> {
    while index < markdown.len() {
        let character = markdown[index..]
            .chars()
            .next()
            .expect("index must remain on a character boundary");
        if character == '\\' {
            index += character.len_utf8();
            if let Some(next) = markdown[index..].chars().next() {
                index += next.len_utf8();
            }
            continue;
        }
        if character == '$' {
            return Some(index);
        }
        index += character.len_utf8();
    }
    None
}

fn currency_math_source(source: &str) -> bool {
    let lower = source.to_ascii_lowercase();
    let starts_numeric = source
        .chars()
        .next()
        .is_some_and(|character| character.is_ascii_digit());
    if !starts_numeric {
        return false;
    }
    if ["/hour", "/day", "/month", " per hour", " per day"]
        .iter()
        .any(|suffix| lower.contains(suffix))
    {
        return true;
    }
    if source.contains('\\') {
        return false;
    }
    if source.contains(['–', '—']) || source.contains("**") {
        return true;
    }
    let has_word = source.split_whitespace().any(|word| {
        word.chars()
            .filter(|character| character.is_ascii_alphabetic())
            .count()
            >= 2
    });
    let has_uppercase_suffix = source
        .chars()
        .find(|character| !(character.is_ascii_digit() || matches!(character, '.' | ',')))
        .is_some_and(|character| character.is_ascii_uppercase());
    has_word || has_uppercase_suffix
}

fn push_terminal_math_lines(
    output: &mut Vec<Line<'static>>,
    current: &mut Vec<Span<'static>>,
    math: Vec<String>,
    width: usize,
    quote_depth: usize,
    style: Style,
) {
    for line in math {
        current.push(Span::styled(line, style.fg(Color::LightCyan)));
        flush_markdown_line(output, current, width, quote_depth);
    }
}

fn markdown_heading_style(level: HeadingLevel) -> Style {
    let style = Style::default().fg(BORG_ORANGE_HOVER);
    match level {
        HeadingLevel::H1 => style.add_modifier(Modifier::BOLD | Modifier::UNDERLINED),
        HeadingLevel::H2 => style.add_modifier(Modifier::BOLD),
        HeadingLevel::H3 => style.add_modifier(Modifier::BOLD | Modifier::ITALIC),
        HeadingLevel::H4 | HeadingLevel::H5 | HeadingLevel::H6 => {
            style.add_modifier(Modifier::ITALIC)
        }
    }
}

fn markdown_style(styles: &[Style]) -> Style {
    styles.last().copied().unwrap_or_default()
}

fn push_markdown_style(styles: &mut Vec<Style>, overlay: Style) {
    styles.push(markdown_style(styles).patch(overlay));
}

fn pop_markdown_style(styles: &mut Vec<Style>) {
    if styles.len() > 1 {
        styles.pop();
    }
}

struct MarkdownTable {
    alignments: Vec<MarkdownAlignment>,
    rows: Vec<Vec<String>>,
    row: Vec<String>,
    cell: String,
}

impl MarkdownTable {
    fn new(alignments: Vec<MarkdownAlignment>) -> Self {
        Self {
            alignments,
            rows: Vec::new(),
            row: Vec::new(),
            cell: String::new(),
        }
    }

    fn start_row(&mut self) {
        self.row.clear();
    }

    fn start_cell(&mut self) {
        self.cell.clear();
    }

    fn push_text(&mut self, text: &str) {
        self.cell.push_str(text);
    }

    fn finish_cell(&mut self) {
        self.row.push(self.cell.trim().to_string());
        self.cell.clear();
    }

    fn finish_row(&mut self) {
        if !self.row.is_empty() {
            self.rows.push(std::mem::take(&mut self.row));
        }
    }

    fn render(self, width: usize, base_style: Style) -> Vec<Line<'static>> {
        let columns = self.rows.iter().map(Vec::len).max().unwrap_or(0);
        if columns == 0 {
            return Vec::new();
        }
        let minimum_table_width = columns.saturating_mul(4).saturating_add(1);
        if width < minimum_table_width {
            return render_stacked_table(&self.rows, width, base_style);
        }

        let available = width.saturating_sub(columns.saturating_mul(3).saturating_add(1));
        let mut column_widths = (0..columns)
            .map(|column| {
                self.rows
                    .iter()
                    .filter_map(|row| row.get(column))
                    .map(|cell| UnicodeWidthStr::width(cell.as_str()))
                    .max()
                    .unwrap_or(1)
                    .max(1)
            })
            .collect::<Vec<_>>();
        while column_widths.iter().sum::<usize>() > available {
            let Some((widest, _)) = column_widths
                .iter()
                .enumerate()
                .filter(|(_, width)| **width > 1)
                .max_by_key(|(_, width)| **width)
            else {
                break;
            };
            column_widths[widest] -= 1;
        }

        let mut output = Vec::new();
        for (row_index, row) in self.rows.iter().enumerate() {
            output.push(table_row_line(
                row,
                &column_widths,
                &self.alignments,
                if row_index == 0 {
                    base_style.add_modifier(Modifier::BOLD)
                } else {
                    base_style
                },
            ));
            if row_index == 0 {
                output.push(table_rule_line(&column_widths));
            }
        }
        output
    }
}

fn table_row_line(
    row: &[String],
    widths: &[usize],
    alignments: &[MarkdownAlignment],
    style: Style,
) -> Line<'static> {
    let mut spans = vec![Span::styled("│", Style::default().fg(Color::DarkGray))];
    for (column, width) in widths.iter().copied().enumerate() {
        let cell = row.get(column).map(String::as_str).unwrap_or_default();
        let cell = truncate_table_cell(cell, width);
        let cell_width = UnicodeWidthStr::width(cell.as_str());
        let padding = width.saturating_sub(cell_width);
        let (left, right) = match alignments
            .get(column)
            .copied()
            .unwrap_or(MarkdownAlignment::None)
        {
            MarkdownAlignment::Right => (padding, 0),
            MarkdownAlignment::Center => (padding / 2, padding - padding / 2),
            MarkdownAlignment::None | MarkdownAlignment::Left => (0, padding),
        };
        spans.push(Span::styled(
            format!(" {}{}{} ", " ".repeat(left), cell, " ".repeat(right)),
            style,
        ));
        spans.push(Span::styled("│", Style::default().fg(Color::DarkGray)));
    }
    Line::from(spans)
}

fn table_rule_line(widths: &[usize]) -> Line<'static> {
    let mut rule = String::from("├");
    for (index, width) in widths.iter().copied().enumerate() {
        rule.push_str(&"─".repeat(width + 2));
        rule.push(if index + 1 == widths.len() {
            '┤'
        } else {
            '┼'
        });
    }
    Line::from(Span::styled(rule, Style::default().fg(Color::DarkGray)))
}

fn render_stacked_table(rows: &[Vec<String>], width: usize, style: Style) -> Vec<Line<'static>> {
    let Some(headers) = rows.first() else {
        return Vec::new();
    };
    let mut output = Vec::new();
    for (row_index, row) in rows.iter().skip(1).enumerate() {
        if row_index > 0 {
            output.push(Line::default());
        }
        for (column, value) in row.iter().enumerate() {
            let header = headers
                .get(column)
                .filter(|header| !header.is_empty())
                .map(String::as_str)
                .unwrap_or("Value");
            let prefix = format!("{header}: ");
            let prefix_width = UnicodeWidthStr::width(prefix.as_str());
            let value_width = UnicodeWidthStr::width(value.as_str());
            if prefix_width.saturating_add(value_width) > width {
                output.push(Line::from(Span::styled(
                    truncate_table_cell(prefix.trim_end(), width),
                    style.add_modifier(Modifier::BOLD),
                )));
                output.push(Line::from(vec![
                    Span::raw("  "),
                    Span::styled(truncate_table_cell(value, width.saturating_sub(2)), style),
                ]));
            } else {
                output.push(Line::from(vec![
                    Span::styled(prefix, style.add_modifier(Modifier::BOLD)),
                    Span::styled(truncate_table_cell(value, width - prefix_width), style),
                ]));
            }
        }
    }
    output
}

pub(super) fn truncate_table_cell(value: &str, width: usize) -> String {
    if UnicodeWidthStr::width(value) <= width {
        return value.to_string();
    }
    if width == 0 {
        return String::new();
    }
    let keep = width.saturating_sub(1);
    let mut output = String::new();
    let mut used = 0usize;
    for grapheme in value.graphemes(true) {
        let grapheme_width = UnicodeWidthStr::width(grapheme);
        if used.saturating_add(grapheme_width) > keep {
            break;
        }
        output.push_str(grapheme);
        used += grapheme_width;
    }
    output.push('…');
    output
}

fn flush_markdown_line(
    output: &mut Vec<Line<'static>>,
    current: &mut Vec<Span<'static>>,
    width: usize,
    quote_depth: usize,
) {
    if quote_depth > 0 {
        output.extend(quoted_markdown_lines(current, width, quote_depth));
    } else {
        output.extend(wrap_markdown_spans(current, width));
    }
    current.clear();
}

fn wrap_markdown_spans(spans: &[Span<'static>], width: usize) -> Vec<Line<'static>> {
    let source = spans
        .iter()
        .map(|span| span.content.as_ref())
        .collect::<String>();
    display_ranges(&source, width, false)
        .into_iter()
        .map(|(start, end)| {
            let mut line = Vec::new();
            let mut span_start = 0usize;
            for span in spans {
                let span_end = span_start.saturating_add(span.content.len());
                let overlap_start = start.max(span_start);
                let overlap_end = end.min(span_end);
                if overlap_start < overlap_end {
                    line.push(Span::styled(
                        span.content[overlap_start - span_start..overlap_end - span_start]
                            .to_string(),
                        span.style,
                    ));
                }
                span_start = span_end;
                if span_start >= end {
                    break;
                }
            }
            Line::from(line)
        })
        .collect()
}

fn quoted_markdown_lines(
    spans: &[Span<'static>],
    width: usize,
    depth: usize,
) -> Vec<Line<'static>> {
    let gutter = "│ ".repeat(depth);
    let content_width = width
        .saturating_sub(UnicodeWidthStr::width(gutter.as_str()))
        .max(1);
    wrap_markdown_spans(spans, content_width)
        .into_iter()
        .map(|mut line| {
            line.spans.insert(
                0,
                Span::styled(gutter.clone(), Style::default().fg(BORG_ORANGE_HOVER)),
            );
            line
        })
        .collect()
}

fn quoted_lines(source: &str, width: usize, style: Style, depth: usize) -> Vec<Line<'static>> {
    let gutter = "│ ".repeat(depth);
    let content_width = width
        .saturating_sub(UnicodeWidthStr::width(gutter.as_str()))
        .max(1);
    source
        .lines()
        .flat_map(|line| wrap_display(line, content_width))
        .map(|line| {
            Line::from(vec![
                Span::styled(gutter.clone(), Style::default().fg(Color::Gray)),
                Span::styled(line, style),
            ])
        })
        .collect()
}
