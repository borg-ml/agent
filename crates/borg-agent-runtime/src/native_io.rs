use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use ignore::WalkBuilder;
use regex::RegexBuilder;
use serde::Serialize;

const MAX_READ_LINES: usize = 20_000;
const MAX_READ_BYTES: usize = 2 * 1024 * 1024;
const MAX_SEARCH_MATCHES: usize = 2_000;
/// Matches gathered before ranking. Larger than one page so the ranking has
/// something to rank; bounded so a hot pattern in a huge tree stays cheap.
const MAX_SEARCH_CANDIDATES: usize = 5_000;
const MAX_SEARCH_MATCHES_PER_FILE: usize = 200;
const GENERATED_PATH_COMPONENTS: &[&str] = &[
    "target",
    "node_modules",
    "dist",
    "build",
    "out",
    ".git",
    "vendor",
    "__pycache__",
    ".venv",
    "venv",
    "coverage",
    ".next",
    ".cache",
];
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

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SearchMatch {
    pub path: PathBuf,
    pub line: usize,
    pub column: usize,
    pub text: String,
    /// File-level rank that ordered this page: filename hits and shallow,
    /// hand-written source rank above deep, generated, or vendored paths.
    pub score: i32,
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
    let filename_needle = filename_needle(pattern, literal);
    // The walk root is canonical (macOS resolves /var to /private/var), so
    // strip the canonical workspace root or matches come back absolute.
    let canonical_root = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    let mut files = Vec::new();
    let mut candidates = 0_usize;
    let mut files_searched = 0_usize;
    let mut files_skipped = 0_usize;
    let mut pool_truncated = false;

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
            .strip_prefix(&canonical_root)
            .or_else(|_| entry.path().strip_prefix(root))
            .unwrap_or(entry.path())
            .to_path_buf();
        let mut matches = Vec::new();
        let mut file_truncated = false;
        for (line_index, line) in BufReader::new(file).lines().enumerate() {
            let line = match line {
                Ok(line) if line.len() <= MAX_SEARCH_LINE_BYTES => line,
                Ok(_) | Err(_) => {
                    files_skipped += 1;
                    break;
                }
            };
            for found in regex.find_iter(&line) {
                if matches.len() == MAX_SEARCH_MATCHES_PER_FILE
                    || candidates == MAX_SEARCH_CANDIDATES
                {
                    file_truncated = true;
                    break;
                }
                matches.push(SearchMatch {
                    path: relative_path.clone(),
                    line: line_index + 1,
                    column: line[..found.start()].chars().count() + 1,
                    text: line.clone(),
                    score: 0,
                });
                candidates += 1;
            }
            if file_truncated {
                break;
            }
        }
        if !matches.is_empty() {
            let score =
                search_file_score(&relative_path, matches.len(), filename_needle.as_deref());
            for found in &mut matches {
                found.score = score;
            }
            files.push((score, relative_path, matches));
        }
        if candidates == MAX_SEARCH_CANDIDATES {
            pool_truncated = true;
            break;
        }
    }
    files.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));
    let ranked = files
        .into_iter()
        .flat_map(|(_, _, matches)| matches)
        .collect::<Vec<_>>();
    let page = ranked
        .iter()
        .skip(offset)
        .take(limit)
        .cloned()
        .collect::<Vec<_>>();
    let truncated = pool_truncated || ranked.len() > offset.saturating_add(page.len());
    Ok(SearchResult {
        next_offset: truncated.then_some(offset.saturating_add(page.len())),
        matches: page,
        offset,
        files_searched,
        files_skipped,
        truncated,
    })
}

/// The pattern as a plain filename fragment, when it is simple enough to be
/// one (a regex like `fn\s+\w+` names no file).
fn filename_needle(pattern: &str, literal: bool) -> Option<String> {
    let trimmed = pattern.trim();
    let simple = literal
        || trimmed
            .chars()
            .all(|character| character.is_alphanumeric() || matches!(character, '_' | '-' | '.'));
    (simple && !trimmed.is_empty()).then(|| trimmed.to_ascii_lowercase())
}

/// Rank a file's matches. The signals are the ones a person uses when they
/// scan grep output: a file named after the thing, close to the root, in
/// hand-written source, with several hits, beats a single hit deep inside a
/// build directory.
fn search_file_score(path: &Path, match_count: usize, filename_needle: Option<&str>) -> i32 {
    let components = path
        .components()
        .filter_map(|component| component.as_os_str().to_str())
        .collect::<Vec<_>>();
    let file_name = components.last().copied().unwrap_or_default();
    let lower_name = file_name.to_ascii_lowercase();
    let mut score = 0_i32;
    score -= i32::try_from(components.len().saturating_sub(1))
        .unwrap_or(i32::MAX)
        .min(8);
    if components
        .iter()
        .any(|component| GENERATED_PATH_COMPONENTS.contains(component))
        || lower_name.ends_with(".min.js")
        || lower_name.ends_with(".min.css")
        || lower_name.ends_with(".map")
        || lower_name.ends_with(".lock")
        || lower_name.ends_with("-lock.json")
        || lower_name.ends_with(".lock.json")
    {
        score -= 25;
    }
    if components[..components.len().saturating_sub(1)]
        .iter()
        .any(|component| {
            matches!(
                *component,
                "test" | "tests" | "__tests__" | "spec" | "fixtures"
            )
        })
        || lower_name.contains("_test.")
        || lower_name.contains(".test.")
        || lower_name.contains(".spec.")
        || lower_name.starts_with("test_")
    {
        score -= 3;
    }
    if let Some(needle) = filename_needle
        && lower_name.contains(needle)
    {
        score += 15;
    }
    score += i32::try_from(match_count.min(10)).unwrap_or(10) * 2;
    score
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

    #[tokio::test]
    async fn search_ranks_named_shallow_source_above_deep_generated_paths() {
        let root = tempfile::tempdir().expect("workspace");
        let write = |relative: &str, body: &str| {
            let path = root.path().join(relative);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, body).unwrap();
        };
        write(
            "node_modules/lib/index.js",
            "widget\nwidget\nwidget\nwidget\nwidget\n",
        );
        write("deep/a/b/c/d.rs", "widget\n");
        write("src/lib.rs", "widget\n");
        write("src/widget.rs", "fn widget() {}\n");
        write("tests/widget_test.rs", "widget\n");
        let result = search_text(
            root.path().to_path_buf(),
            PathBuf::from("."),
            "widget".to_string(),
            true,
            false,
            0,
            50,
        )
        .await
        .expect("search");
        let order = result
            .matches
            .iter()
            .map(|found| found.path.to_string_lossy().replace('\\', "/"))
            .collect::<Vec<_>>();
        // A file named after the query outranks a plain hit even under tests/.
        assert_eq!(order[0], "src/widget.rs", "{order:?}");
        assert_eq!(order[1], "tests/widget_test.rs", "{order:?}");
        assert_eq!(order[2], "src/lib.rs", "{order:?}");
        assert_eq!(order[3], "deep/a/b/c/d.rs", "{order:?}");
        assert!(
            order[4..]
                .iter()
                .all(|path| path == "node_modules/lib/index.js"),
            "{order:?}"
        );
        assert!(result.matches[0].score > result.matches[4].score);
        assert!(!result.truncated);

        // Pagination walks the ranked order, not the filesystem order.
        let page = search_text(
            root.path().to_path_buf(),
            PathBuf::from("."),
            "widget".to_string(),
            true,
            false,
            1,
            2,
        )
        .await
        .expect("page");
        assert_eq!(page.matches[0].path, PathBuf::from("tests/widget_test.rs"));
        assert_eq!(page.matches[1].path, PathBuf::from("src/lib.rs"));
        assert_eq!(page.next_offset, Some(3));
        assert!(page.truncated);
    }

    #[test]
    fn regex_patterns_only_name_files_when_they_are_plain_fragments() {
        assert_eq!(filename_needle("Widget", false).as_deref(), Some("widget"));
        assert_eq!(filename_needle("fn\\s+\\w+", false), None);
        assert_eq!(
            filename_needle("fn\\s+\\w+", true).as_deref(),
            Some("fn\\s+\\w+")
        );
        assert_eq!(filename_needle("  ", true), None);
    }
}
