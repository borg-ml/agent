//! Finished Markdown block delivery policy shared by all projections.
/// How much of a streaming reply is finished Markdown, as a byte offset.
///
/// Paragraph streaming shows a reply a block at a time: a paragraph once a
/// blank line ends it, a list one whole item at a time, a code block once its
/// own fence closes. A heading or bold-only title waits for the content under
/// it, so it never sits alone above nothing.
///
/// Fence state is tracked by marker and indent rather than by "inside a
/// backtick block": a fence nested in a list item is still a fence, only a
/// closing fence of the same marker at no deeper an indent ends the block, and
/// a longer run of the same marker closes it. Everything before the returned
/// offset can no longer change shape as more text arrives, which is what makes
/// it safe to paint and stop re-rendering.
pub fn paragraph_release(text: &str) -> usize {
    // An opening fence may sit at any indentation, since fences inside list
    // items are indented past the marker. A closing fence may be indented at
    // most three spaces more than its opener. Deeper lines are block content.
    fn fence_marker(line: &str) -> Option<(usize, char, usize)> {
        let indent = line.len() - line.trim_start_matches(' ').len();
        let body = &line[indent..];
        let marker = body.chars().next()?;
        if marker != '`' && marker != '~' {
            return None;
        }
        let run = body.chars().take_while(|byte| *byte == marker).count();
        // A backtick run is only a fence when it opens or closes an info
        // string; CommonMark forbids backticks inside a backtick fence's info.
        Some((indent, marker, run))
    }
    let mut release = 0;
    let mut open_fence: Option<(usize, char, usize)> = None;
    // A title whose content has not arrived holds everything from its start.
    let mut held_title: Option<usize> = None;
    let mut offset = 0;
    fn advance(release: &mut usize, candidate: usize, held: Option<usize>) {
        *release = (*release).max(held.map_or(candidate, |title| candidate.min(title)));
    }
    for line in text.split_inclusive('\n') {
        let start = offset;
        let trimmed = line.trim_end_matches(['\n', '\r']);
        let unterminated = !line.ends_with('\n');
        if unterminated {
            // A partial line is never itself released, but a new item's marker
            // already proves the item before it finished, which is what lets
            // tight lists land one whole item at a time.
            if open_fence.is_none() && start > 0 && is_list_item(trimmed) && held_title.is_none() {
                release = release.max(start);
            }
            break;
        }
        offset += line.len();
        match open_fence {
            Some((fence_indent, marker, run)) => {
                if let Some((indent, line_marker, line_run)) = fence_marker(trimmed)
                    && line_marker == marker
                    && line_run >= run
                    && indent <= fence_indent + 3
                    // CommonMark: a closing fence carries no info string.
                    && trimmed.len() == indent + line_run
                {
                    open_fence = None;
                    held_title = None;
                    advance(&mut release, offset, None);
                }
            }
            None => match fence_marker(trimmed) {
                Some(opened) => {
                    open_fence = Some(opened);
                    advance(&mut release, start, held_title);
                }
                None => {
                    if trimmed.trim().is_empty() {
                        advance(&mut release, offset, held_title);
                    } else if is_list_item(trimmed) {
                        // A whole item, including its continuation lines, is one
                        // block: a wrapped bullet never lands half-rendered.
                        advance(&mut release, start, held_title);
                        if held_title.is_some() {
                            advance(&mut release, offset, held_title);
                        }
                        held_title = None;
                    } else if is_title(trimmed) {
                        advance(&mut release, start, held_title);
                        held_title.get_or_insert(start);
                    } else {
                        held_title = None;
                    }
                }
            },
        }
    }
    release
}

fn is_title(line: &str) -> bool {
    let hashes = line.bytes().take_while(|byte| *byte == b'#').count();
    (1..=6).contains(&hashes) && line[hashes..].starts_with(' ')
        || line.len() > 4
            && line.starts_with("**")
            && line.ends_with("**")
            && !line[2..line.len() - 2].contains("**")
}

fn is_list_item(line: &str) -> bool {
    let item = line.trim_start();
    if let Some(rest) = item.strip_prefix(['-', '*', '+']) {
        return rest.starts_with([' ', '\t']);
    }
    let digits = item.bytes().take_while(u8::is_ascii_digit).count();
    digits > 0
        && item[digits..].starts_with(['.', ')'])
        && item[digits + 1..].starts_with([' ', '\t'])
}

#[cfg(test)]
mod paragraph_release_tests {
    use super::paragraph_release;

    fn shown(text: &str) -> &str {
        &text[..paragraph_release(text)]
    }

    #[test]
    fn a_paragraph_appears_once_a_blank_line_ends_it() {
        assert_eq!(shown("First para"), "");
        assert_eq!(shown("First para\n\nSecond"), "First para\n\n");
    }

    #[test]
    fn a_code_block_waits_for_its_closing_fence() {
        assert_eq!(shown("Intro\n\n```rust\nfn a() {}\n\nfn b"), "Intro\n\n");
        assert_eq!(
            shown("Intro\n\n```rust\nfn a() {}\n```\nmore"),
            "Intro\n\n```rust\nfn a() {}\n```\n"
        );
    }

    #[test]
    fn list_items_stream_one_at_a_time() {
        assert_eq!(
            shown("## Steps\n\n- one\n- two\n- thr"),
            "## Steps\n\n- one\n- two\n"
        );
    }

    #[test]
    fn a_fence_nested_in_a_list_item_is_still_a_fence() {
        // The inner ```rust is content, not the outer ~~~ block's closer, so
        // the whole block releases only once the real ~~~ arrives.
        assert_eq!(
            shown("Steps\n\n~~~\n```rust\nfn a() {}\n```\n~~~\nafter"),
            "Steps\n\n~~~\n```rust\nfn a() {}\n```\n~~~\n"
        );
    }

    #[test]
    fn a_different_fence_marker_does_not_close_the_block() {
        // The inner ~~~ is content, so the block runs to its own ``` closer
        // and releases as one piece instead of splitting at the ~~~.
        assert_eq!(
            shown("```\n~~~\nstill inside\n```\nout"),
            "```\n~~~\nstill inside\n```\n"
        );
    }

    #[test]
    fn a_longer_run_of_the_same_marker_closes_the_block() {
        assert_eq!(shown("```\nbody\n````\nafter"), "```\nbody\n````\n");
    }

    #[test]
    fn a_closing_fence_may_sit_up_to_three_spaces_deeper() {
        assert_eq!(shown("```\nbody\n   ```\nafter"), "```\nbody\n   ```\n");
    }

    #[test]
    fn a_bullet_and_its_continuation_lines_land_as_one_item() {
        assert_eq!(
            shown("## Steps\n\n- first item that\n  wraps onto a second line\n- next"),
            "## Steps\n\n- first item that\n  wraps onto a second line\n"
        );
    }

    #[test]
    fn a_numbered_list_streams_one_whole_item_at_a_time() {
        assert_eq!(shown("1. alpha\n2. beta\n3. ga"), "1. alpha\n2. beta\n");
    }

    #[test]
    fn a_bare_partial_marker_holds_the_item_before_it() {
        // A marker with no text after it yet is not a finished item, and a
        // one-item tight list has no other boundary, so nothing shows yet.
        assert_eq!(shown("- item one\n-"), "");
    }

    #[test]
    fn a_partially_written_item_releases_only_the_item_before_it() {
        assert_eq!(shown("- item one\n- item tw"), "- item one\n");
    }

    #[test]
    fn a_title_waits_for_the_content_under_it() {
        assert_eq!(shown("Done.\n\n## Risks\n\n"), "Done.\n\n");
        assert_eq!(
            shown("Done.\n\n**Risks**\n\nFirst risk.\n\n"),
            "Done.\n\n**Risks**\n\nFirst risk.\n\n"
        );
    }
}
