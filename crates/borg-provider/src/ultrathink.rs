//! Claude Code's `ultrathink` keyword. Typed as a word in a prompt, it runs
//! that turn at xhigh effort on the Claude models that offer it.

use std::ops::Range;

/// What the model is told when a turn's prompt asks for it, in Claude Code's
/// words.
pub const REMINDER: &str = "The user included the keyword \"ultrathink\", requesting deeper reasoning on this turn. Reason as thoroughly as the task warrants.";

/// Whether typing the keyword can take effect on this Claude model: the
/// models Claude Code raises (Opus 5.5, Sonnet 5.5, Haiku 5.5, Fable 5.1).
pub fn available(model: &str) -> bool {
    crate::CLAUDE_SELECTABLE_MODELS
        .iter()
        .any(|(id, _)| *id == model)
}

/// The effort a turn that asks for it runs at: xhigh, unless the session is
/// already at xhigh or max, where the turn stays as it is.
pub fn effort(current: Option<&str>) -> &str {
    match current.unwrap_or(crate::claude_default_effort()) {
        effort @ ("xhigh" | "max") => effort,
        _ => "xhigh",
    }
}

/// Byte ranges of the keyword in `text`, by Claude Code's rule: a whole word
/// in any case, not in a slash command, not inside closed quotes or brackets,
/// and not part of something that looks like a path or file name.
pub fn ranges(text: &str) -> Vec<Range<usize>> {
    const WORD: &str = "ultrathink";
    if text.starts_with('/') || !text.to_ascii_lowercase().contains(WORD) {
        return Vec::new();
    }
    let chars: Vec<(usize, char)> = text.char_indices().collect();
    let at = |index: usize| chars.get(index).map(|(_, character)| *character);
    let wordlike = |character: Option<char>| {
        character.is_some_and(|character| character.is_alphanumeric() || character == '_')
    };
    let closer = |opener: char| match opener {
        '<' => '>',
        '{' => '}',
        '[' => ']',
        '(' => ')',
        other => other,
    };
    // Quoted and bracketed spans, as character index ranges. An opener that
    // never closes quotes nothing.
    let mut quoted: Vec<Range<usize>> = Vec::new();
    let mut open: Option<(char, usize)> = None;
    for (index, &(_, character)) in chars.iter().enumerate() {
        if let Some((opener, start)) = open {
            if opener == '[' && character == '[' {
                open = Some((opener, index));
                continue;
            }
            if character != closer(opener) || (opener == '\'' && wordlike(at(index + 1))) {
                continue;
            }
            quoted.push(start..index + 1);
            open = None;
        } else if (character == '<'
            && at(index + 1).is_some_and(|next| next.is_ascii_alphabetic() || next == '/'))
            || (character == '\'' && !wordlike(index.checked_sub(1).and_then(at)))
            || matches!(character, '`' | '"' | '{' | '[' | '(')
        {
            open = Some((character, index));
        }
    }
    let ascii_word = |character: Option<char>| {
        character.is_some_and(|character| character.is_ascii_alphanumeric() || character == '_')
    };
    let mut found = Vec::new();
    for start in 0..chars.len() {
        let end = start + WORD.len();
        if end > chars.len()
            || !chars[start..end]
                .iter()
                .map(|(_, character)| character.to_ascii_lowercase())
                .eq(WORD.chars())
        {
            continue;
        }
        let (before, after) = (start.checked_sub(1).and_then(at), at(end));
        if ascii_word(before) || ascii_word(after) {
            continue;
        }
        if quoted.iter().any(|span| span.contains(&start))
            || matches!(before, Some('/' | '\\' | '-'))
            || matches!(after, Some('/' | '\\' | '-' | '?'))
            || (after == Some('.') && wordlike(at(end + 1)))
        {
            continue;
        }
        let byte_end = chars.get(end).map_or(text.len(), |(offset, _)| *offset);
        found.push(chars[start].0..byte_end);
    }
    found
}

#[cfg(test)]
mod tests {
    use super::*;

    fn words(text: &str) -> Vec<&str> {
        ranges(text).into_iter().map(|range| &text[range]).collect()
    }

    #[test]
    fn the_keyword_counts_only_where_claude_code_counts_it() {
        assert_eq!(words("ultrathink about this"), ["ultrathink"]);
        assert_eq!(words("please UltraThink, then fix it."), ["UltraThink"]);
        assert_eq!(words("ultrathink."), ["ultrathink"]);
        for ignored in [
            "/model ultrathink",
            "ultrathinking",
            "`ultrathink`",
            "\"ultrathink\"",
            "see (ultrathink) notes",
            "docs/ultrathink.md",
            "ultrathink/notes",
            "pre-ultrathink",
            "ultrathink?",
            "ultrathink.md",
        ] {
            assert!(ranges(ignored).is_empty(), "{ignored}");
        }
        // An apostrophe in a word quotes nothing, and an unclosed quote is text.
        assert_eq!(words("don't ultrathink"), ["ultrathink"]);
        assert_eq!(words("\"ultrathink it"), ["ultrathink"]);
    }

    #[test]
    fn the_turn_rises_to_xhigh_but_never_falls() {
        assert_eq!(effort(None), "xhigh");
        assert_eq!(effort(Some("low")), "xhigh");
        assert_eq!(effort(Some("xhigh")), "xhigh");
        assert_eq!(effort(Some("max")), "max");
        assert!(available("claude-opus-5-5"));
        assert!(!available("claude-opus-4-8"));
    }
}
