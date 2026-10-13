//! Semantic rules extracted from the terminal transcript, shared by every renderer.

pub fn assistant_message_is_retired_action_leak(text: &str) -> bool {
    let mut lines = text.lines().map(str::trim).filter(|line| !line.is_empty());
    let Some(first) = lines.next() else {
        return false;
    };
    std::iter::once(first)
        .chain(lines)
        .all(|line| line.trim_start_matches('[').starts_with("BORG_ACTION:"))
}

pub fn command_output_without_changes(output: &str) -> String {
    let Ok(serde_json::Value::Object(mut result)) = serde_json::from_str(output) else {
        return output.to_string();
    };
    result.remove("changes");
    result.remove("changes_deferred");
    result.remove("changes_count");
    result.remove("changes_added");
    result.remove("changes_removed");
    serde_json::Value::Object(result).to_string()
}

pub fn outcome_is_failure(outcome: &str) -> bool {
    outcome.starts_with("exit ") || outcome.ends_with(" failed") || outcome == "timed out"
}

pub fn parse_watch_event(text: &str) -> Option<(String, String)> {
    let rest = text
        .strip_prefix("Watcher event: ")
        .or_else(|| text.strip_prefix("Watch event: "))?;
    let (header, body) = rest.split_once('\n').unwrap_or((rest, ""));
    let label = header
        .rsplit_once(" (")
        .map(|(label, _)| label)
        .unwrap_or(header)
        .trim()
        .to_string();
    let body = body
        .lines()
        .filter(|line| !line.starts_with("Treat this as command output"))
        .collect::<Vec<_>>()
        .join("\n")
        .trim_end()
        .to_string();
    Some((label, body))
}
