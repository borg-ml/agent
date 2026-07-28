use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use ignore::WalkBuilder;
use regex::{Regex, RegexBuilder};
use serde::Serialize;

const MAX_READ_LINES: usize = 20_000;
const MAX_READ_BYTES: usize = 2 * 1024 * 1024;
const MAX_SEARCH_MATCHES: usize = 2_000;
const MAX_SEARCH_FILE_BYTES: u64 = 4 * 1024 * 1024;
const MAX_SEARCH_LINE_BYTES: usize = 64 * 1024;

#[derive(Debug, Serialize)]
pub(crate) struct ReadTextRange {
    pub path: PathBuf,
    pub text: String,
    pub start_line: usize,
    pub end_line: usize,
    pub next_line: Option<usize>,
    pub total_bytes: u64,
    pub returned_bytes: usize,
    pub truncated: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct SearchResult {
    pub matches: Vec<SearchMatch>,
    pub offset: usize,
    pub next_offset: Option<usize>,
    pub files_searched: usize,
    pub files_skipped: usize,
    pub truncated: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct SearchMatch {
    pub path: PathBuf,
    pub line: usize,
    pub column: usize,
    pub text: String,
}

pub(crate) async fn read_text_range(
    root: PathBuf,
    path: PathBuf,
    offset_line: usize,
    limit_lines: usize,
    max_bytes: usize,
) -> Result<ReadTextRange> {
    tokio::task::spawn_blocking(move || {
        read_text_range_blocking(
            &root,
            &path,
            offset_line.max(1),
            limit_lines.clamp(1, MAX_READ_LINES),
            max_bytes.clamp(1, MAX_READ_BYTES),
        )
    })
    .await
    .context("ranged file reader stopped")?
}

fn read_text_range_blocking(
    root: &Path,
    relative: &Path,
    offset_line: usize,
    limit_lines: usize,
    max_bytes: usize,
) -> Result<ReadTextRange> {
    let target = crate::filesystem::resolve_existing_workspace_path(root, relative)?;
    let metadata = target.metadata()?;
    if !metadata.is_file() {
        bail!("path must be a regular file");
    }
    let file = File::open(&target)?;
    let mut lines = BufReader::new(file).lines();
    for _ in 1..offset_line {
        if lines.next().transpose()?.is_none() {
            break;
        }
    }

    let mut text = String::new();
    let mut returned_lines = 0_usize;
    let mut has_more = false;
    while returned_lines < limit_lines {
        let Some(line) = lines.next().transpose()? else {
            break;
        };
        let addition = line.len().saturating_add(1);
        if text.len().saturating_add(addition) > max_bytes {
            has_more = true;
            break;
        }
        text.push_str(&line);
        text.push('\n');
        returned_lines += 1;
    }
    if returned_lines == limit_lines && lines.next().transpose()?.is_some() {
        has_more = true;
    }
    let end_line = offset_line.saturating_add(returned_lines.saturating_sub(1));
    Ok(ReadTextRange {
        path: relative.to_path_buf(),
        returned_bytes: text.len(),
        text,
        start_line: offset_line,
        end_line,
        next_line: has_more.then_some(offset_line.saturating_add(returned_lines)),
        total_bytes: metadata.len(),
        truncated: has_more,
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn search_text(
    root: PathBuf,
    relative: PathBuf,
    pattern: String,
    literal: bool,
    case_sensitive: bool,
    offset: usize,
    limit: usize,
) -> Result<SearchResult> {
    tokio::task::spawn_blocking(move || {
        search_text_blocking(
            &root,
            &relative,
            &pattern,
            literal,
            case_sensitive,
            offset,
            limit.clamp(1, MAX_SEARCH_MATCHES),
        )
    })
    .await
    .context("workspace search worker stopped")?
}

#[allow(clippy::too_many_arguments)]
fn search_text_blocking(
    root: &Path,
    relative: &Path,
    pattern: &str,
    literal: bool,
    case_sensitive: bool,
    offset: usize,
    limit: usize,
) -> Result<SearchResult> {
    let search_root = crate::filesystem::resolve_existing_workspace_path(root, relative)?;
    let expression = if literal {
        regex::escape(pattern)
    } else {
        pattern.to_string()
    };
    let regex = RegexBuilder::new(&expression)
        .case_insensitive(!case_sensitive)
        .build()
        .with_context(|| format!("invalid search pattern `{pattern}`"))?;
    let mut matches = Vec::new();
    let mut matched_before = 0_usize;
    let mut files_searched = 0_usize;
    let mut files_skipped = 0_usize;
    let mut truncated = false;

    let mut builder = WalkBuilder::new(&search_root);
    builder
        .hidden(false)
        .follow_links(false)
        .git_ignore(true)
        .git_global(true)
        .git_exclude(true)
        .max_filesize(Some(MAX_SEARCH_FILE_BYTES));
    for entry in builder.build() {
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => {
                files_skipped += 1;
                continue;
            }
        };
        let Some(file_type) = entry.file_type() else {
            continue;
        };
        if !file_type.is_file() {
            continue;
        }
        let file = match File::open(entry.path()) {
            Ok(file) => file,
            Err(_) => {
                files_skipped += 1;
                continue;
            }
        };
        files_searched += 1;
        let relative_path = entry
            .path()
            .strip_prefix(root)
            .unwrap_or(entry.path())
            .to_path_buf();
        for (line_index, line) in BufReader::new(file).lines().enumerate() {
            let line = match line {
                Ok(line) if line.len() <= MAX_SEARCH_LINE_BYTES => line,
                Ok(_) | Err(_) => {
                    files_skipped += 1;
                    break;
                }
            };
            collect_line_matches(
                &regex,
                &line,
                &relative_path,
                line_index + 1,
                offset,
                limit,
                &mut matched_before,
                &mut matches,
                &mut truncated,
            );
            if truncated {
                break;
            }
        }
        if truncated {
            break;
        }
    }
    Ok(SearchResult {
        next_offset: truncated.then_some(offset.saturating_add(matches.len())),
        matches,
        offset,
        files_searched,
        files_skipped,
        truncated,
    })
}

#[allow(clippy::too_many_arguments)]
fn collect_line_matches(
    regex: &Regex,
    line: &str,
    path: &Path,
    line_number: usize,
    offset: usize,
    limit: usize,
    matched_before: &mut usize,
    matches: &mut Vec<SearchMatch>,
    truncated: &mut bool,
) {
    for found in regex.find_iter(line) {
        if *matched_before < offset {
            *matched_before += 1;
            continue;
        }
        if matches.len() == limit {
            *truncated = true;
            return;
        }
        matches.push(SearchMatch {
            path: path.to_path_buf(),
            line: line_number,
            column: line[..found.start()].chars().count() + 1,
            text: line.to_string(),
        });
        *matched_before += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn ranged_reads_continue_without_loading_the_whole_file() {
        let root = tempfile::tempdir().expect("workspace");
        std::fs::write(root.path().join("large.txt"), "one\ntwo\nthree\nfour\n").expect("write");
        let range = read_text_range(
            root.path().to_path_buf(),
            PathBuf::from("large.txt"),
            2,
            2,
            1024,
        )
        .await
        .expect("read range");
        assert_eq!(range.text, "two\nthree\n");
        assert_eq!(range.next_line, Some(4));
    }

    #[tokio::test]
    async fn search_is_bounded_and_has_a_stable_cursor() {
        let root = tempfile::tempdir().expect("workspace");
        std::fs::write(root.path().join("matches.txt"), "hit\nhit\nhit\n").expect("write");
        let result = search_text(
            root.path().to_path_buf(),
            PathBuf::from("."),
            "hit".to_string(),
            true,
            true,
            1,
            1,
        )
        .await
        .expect("search");
        assert_eq!(result.matches[0].line, 2);
        assert_eq!(result.next_offset, Some(2));
    }
}
