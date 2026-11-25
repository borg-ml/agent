//! The full-screen sign-in view: a QR code and link for a browser on another
//! device, ChatGPT's device code, and the field Claude and Gemini codes are
//! pasted back into.

use crate::settings::SignIn;
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::widgets::Clear;

const MUTED: Color = Color::Rgb(140, 140, 148);
const ACCENT: Color = Color::Rgb(255, 155, 104);
const FIELD: Color = Color::Rgb(36, 36, 40);
const MAX_WIDTH: u16 = 100;

/// `url` as QR rows of half-block characters, two modules per cell. Dark
/// modules are the foreground, so draw it dark on light to scan on any theme.
/// The margin is one module rather than the standard four: OAuth links are
/// long, and scanners read a code on a lit screen with the narrower margin.
pub fn qr_lines(url: &str) -> Option<Vec<String>> {
    use qrcode::render::unicode::Dense1x2;
    let code =
        qrcode::QrCode::with_error_correction_level(url.as_bytes(), qrcode::EcLevel::L).ok()?;
    let image = code.render::<Dense1x2>().quiet_zone(false).build();
    let wide = image.lines().next()?.chars().count() + 2;
    let blank = " ".repeat(wide);
    let mut rows = vec![blank.clone()];
    rows.extend(image.lines().map(|row| format!(" {row} ")));
    rows.push(blank);
    Some(rows)
}

/// `text` cut into rows of at most `width` characters; links have no spaces
/// to wrap at.
fn chunks(text: &str, width: usize) -> Vec<String> {
    let chars: Vec<char> = text.chars().collect();
    chars
        .chunks(width.max(1))
        .map(|chunk| chunk.iter().collect())
        .collect()
}

fn words(text: &str, width: usize) -> Vec<String> {
    let mut rows = Vec::new();
    let mut row = String::new();
    for word in text.split_whitespace() {
        if !row.is_empty() && row.chars().count() + 1 + word.chars().count() > width {
            rows.push(std::mem::take(&mut row));
        }
        if !row.is_empty() {
            row.push(' ');
        }
        row.push_str(word);
    }
    if !row.is_empty() {
        rows.push(row);
    }
    rows
}

pub(crate) fn render(frame: &mut Frame<'_>, provider: &str, sign_in: &SignIn) {
    let full = frame.area();
    frame.render_widget(Clear, full);
    let width = full.width.min(MAX_WIDTH).saturating_sub(4).max(1);
    let area = Rect {
        x: full.x + (full.width.saturating_sub(width)) / 2,
        y: full.y,
        width,
        height: full.height,
    };
    let url = sign_in.url.as_deref().unwrap_or_default();
    let text_width = area.width as usize;
    let mut rows: Vec<(String, Style)> = Vec::new();
    if let Some(code) = &sign_in.user_code {
        rows.push((String::new(), Style::default()));
        rows.push((
            format!("Enter code  {code}"),
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ));
    }
    rows.push((String::new(), Style::default()));
    rows.push((
        "Scan with your phone, or open:".to_string(),
        Style::default().fg(MUTED),
    ));
    for row in chunks(url, text_width) {
        rows.push((
            row,
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::UNDERLINED),
        ));
    }
    let status = sign_in.status.replace(url, "the link above");
    rows.push((String::new(), Style::default()));
    for row in words(&status, text_width) {
        rows.push((row, Style::default().fg(Color::Gray)));
    }
    if sign_in.accepts_code {
        rows.push((String::new(), Style::default()));
        rows.push((
            "Paste the code, or the address the browser landed on, then press Enter:".to_string(),
            Style::default().fg(MUTED),
        ));
        let field = format!("› {}▏", sign_in.pasted);
        let shown: String = field
            .chars()
            .rev()
            .take(text_width)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        rows.push((shown, Style::default().fg(Color::White).bg(FIELD)));
    }
    let footer = if sign_in.accepts_code {
        "Enter submit · Ctrl+Y copy link · Ctrl+O open in browser · Esc hide"
    } else {
        "Ctrl+Y copy link · Ctrl+O open in browser · Esc hide"
    };
    // Title, blank, rows, blank, footer.
    let chrome = rows.len() as u16 + 4;
    let qr = qr_lines(url).filter(|qr| {
        let wide = qr.first().map_or(0, |row| row.chars().count()) as u16;
        wide <= area.width && qr.len() as u16 + chrome <= area.height
    });
    let buffer = frame.buffer_mut();
    let mut y = area.y + 1;
    let right = area.right();
    let mut put = |y: u16, x: u16, text: &str, style: Style| {
        if y < full.bottom() && x < right {
            buffer.set_stringn(x, y, text, (right - x) as usize, style);
        }
    };
    put(
        y,
        area.x,
        &format!("Sign in to {provider}"),
        Style::default().fg(ACCENT).add_modifier(Modifier::BOLD),
    );
    y += 2;
    match &qr {
        Some(qr) => {
            let wide = qr.first().map_or(0, |row| row.chars().count()) as u16;
            let x = area.x + (area.width - wide) / 2;
            for row in qr {
                put(
                    y,
                    x,
                    row,
                    Style::default().fg(Color::Black).bg(Color::White),
                );
                y += 1;
            }
        }
        None => {
            put(
                y,
                area.x,
                "Enlarge the terminal to show a QR code.",
                Style::default().fg(MUTED),
            );
            y += 1;
        }
    }
    for (row, style) in &rows {
        put(y, area.x, row, *style);
        y += 1;
    }
    put(
        full.bottom().saturating_sub(1).max(y + 1),
        area.x,
        footer,
        Style::default().fg(MUTED),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    // Claude's real sign-in link must fit an 80-column terminal, or the view
    // falls back to text on exactly the SSH screens that need the QR.
    #[test]
    fn claude_link_fits_an_80_column_terminal() {
        // Shaped like the paste-only link: real client, redirect and scopes,
        // with a 43-character PKCE challenge and state.
        let url = format!(
            "https://claude.com/cai/oauth/authorize?code=true&client_id=9d1c250a-e61b-44d9-88ed-5944d1962f5e&response_type=code&redirect_uri=https%3A%2F%2Fplatform.claude.com%2Foauth%2Fcode%2Fcallback&scope=user%3Aprofile+user%3Ainference+user%3Asessions%3Aclaude_code+user%3Amcp_servers+user%3Afile_upload+user%3Aplugins&code_challenge={0}&code_challenge_method=S256&state={0}",
            "A".repeat(43)
        );
        let qr = qr_lines(&url).unwrap();
        assert!(
            qr[0].chars().count() <= 80,
            "{} for {} bytes",
            qr[0].chars().count(),
            url.len()
        );
    }
}
