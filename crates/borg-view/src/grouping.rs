//! Transcript grouping authority, extracted from terminal semantics.
use crate::entry::{OperationKind, ToolRunWindow, TranscriptEntry};
use borg_agent_runtime::{EventActor, MessageStatus};
use std::borrow::Cow;
const TOOL_RUN_BOX_THRESHOLD: usize = 1;
pub fn tool_lifecycle_label(name: &str, complete: bool) -> Cow<'_, str> {
    if name == "Reasoning" {
        return Cow::Borrowed(if complete { "Reasoned" } else { "Reasoning" });
    }
    if name == "Wait for provider" {
        return Cow::Borrowed(if complete {
            "Tool preparation ended"
        } else {
            "Awaiting tool-call arguments…"
        });
    }
    if name == "Generate image" {
        return Cow::Borrowed(if complete {
            "Generated image"
        } else {
            "Generating image…"
        });
    }
    if name == "Generate" || name.starts_with("Generate ") {
        let label = name.strip_prefix("Generate ").unwrap_or("");
        let state = if complete {
            "Stopped generating tool call"
        } else {
            "Generating tool call"
        };
        return Cow::Owned(format!(
            "{state}{}{}{}",
            if label.is_empty() { "" } else { " · " },
            label,
            if complete { "" } else { "…" }
        ));
    }
    if name == "Git add" {
        return Cow::Borrowed(if complete {
            "Updated Git index"
        } else {
            "Updating Git index…"
        });
    }
    if let Some(rest) = name.strip_prefix("Wait on ") {
        let form = if complete {
            "Finished waiting for "
        } else {
            "Waiting for "
        };
        return Cow::Owned(format!("{form}{rest}{}", if complete { "" } else { "…" }));
    }
    let (verb, rest) = name.split_once(' ').unwrap_or((name, ""));
    let forms = match verb {
        "Run" => Some(("Running", "Ran")),
        "Prepare" => Some(("Preparing", "Prepared")),
        "Consult" => Some(("Consulting", "Consulted")),
        "Inspect" => Some(("Inspecting", "Inspected")),
        "Read" => Some(("Reading", "Read")),
        "Get" => Some(("Retrieving", "Retrieved")),
        "Set" => Some(("Setting", "Set")),
        "Save" => Some(("Saving", "Saved")),
        "Record" => Some(("Recording", "Recorded")),
        "Publish" => Some(("Publishing", "Published")),
        "Assign" => Some(("Assigning", "Assigned")),
        "Claim" => Some(("Claiming", "Claimed")),
        "Resume" => Some(("Resuming", "Resumed")),
        "Cancel" => Some(("Cancelling", "Cancelled")),
        "Acknowledge" => Some(("Acknowledging", "Acknowledged")),
        "Reload" => Some(("Reloading", "Reloaded")),
        "Configure" => Some(("Configuring", "Configured")),
        "Broadcast" => Some(("Broadcasting", "Broadcast")),
        "Write" => Some(("Writing", "Wrote")),
        "Delegate" => Some(("Delegating", "Delegated")),
        "Edit" => Some(("Editing", "Edited")),
        "Update" => Some(("Updating", "Updated")),
        "Refine" => Some(("Refining", "Refined")),
        "Roll" => Some(("Rolling", "Rolled")),
        "Search" => Some(("Searching", "Searched")),
        "List" => Some(("Listing", "Listed")),
        "Check" => Some(("Checking", "Checked")),
        "Build" => Some(("Building", "Built")),
        "Rebuild" => Some(("Rebuilding", "Rebuilt")),
        "Compile" => Some(("Compiling", "Compiled")),
        "Test" => Some(("Testing", "Tested")),
        "Verify" => Some(("Verifying", "Verified")),
        "Validate" => Some(("Validating", "Validated")),
        "Audit" => Some(("Auditing", "Audited")),
        "Analyze" => Some(("Analyzing", "Analyzed")),
        "Diagnose" => Some(("Diagnosing", "Diagnosed")),
        "Measure" => Some(("Measuring", "Measured")),
        "Capture" => Some(("Capturing", "Captured")),
        "Apply" => Some(("Applying", "Applied")),
        "Execute" => Some(("Executing", "Executed")),
        "Launch" => Some(("Launching", "Launched")),
        "Install" => Some(("Installing", "Installed")),
        "Fix" => Some(("Fixing", "Fixed")),
        "Restore" => Some(("Restoring", "Restored")),
        "Clean" => Some(("Cleaning", "Cleaned")),
        "Clear" => Some(("Clearing", "Cleared")),
        "Find" => Some(("Finding", "Found")),
        "Go" => Some(("Going", "Went")),
        "View" => Some(("Viewing", "Viewed")),
        "Create" => Some(("Creating", "Created")),
        "Delete" => Some(("Deleting", "Deleted")),
        "Wait" if !rest.is_empty() && !rest.starts_with("for ") => {
            Some(("Waiting for", "Finished waiting for"))
        }
        "Wait" => Some(("Waiting", "Finished waiting")),
        "Send" => Some(("Sending", "Sent")),
        "Follow" => Some(("Following", "Followed")),
        "Message" => Some(("Sending message to", "Sent message to")),
        "Use" => Some(("Using", "Used")),
        "Spawn" => Some(("Spawning", "Spawned")),
        "Interrupt" => Some(("Interrupting", "Interrupted")),
        "Stop" => Some(("Stopping", "Stopped")),
        "Compare" => Some(("Comparing", "Compared")),
        "Review" => Some(("Reviewing", "Reviewed")),
        "Show" => Some(("Showing", "Shown")),
        "Add" => Some(("Adding", "Added")),
        "Remove" => Some(("Removing", "Removed")),
        "Prune" => Some(("Pruning", "Pruned")),
        "Lock" => Some(("Locking", "Locked")),
        "Unlock" => Some(("Unlocking", "Unlocked")),
        "Switch" => Some(("Switching", "Switched")),
        "Commit" => Some(("Committing", "Committed")),
        "Fetch" => Some(("Fetching", "Fetched")),
        "Pull" => Some(("Pulling", "Pulled")),
        "Push" => Some(("Pushing", "Pushed")),
        "Merge" => Some(("Merging", "Merged")),
        "Rebase" => Some(("Rebasing", "Rebased")),
        _ => None,
    };
    if let Some((running, completed)) = forms {
        let form = if complete { completed } else { running };
        return Cow::Owned(format!(
            "{form}{}{}{}",
            if rest.is_empty() { "" } else { " " },
            rest,
            if complete { "" } else { "…" }
        ));
    }
    let phrase = match name {
        "Git status" | "Git diff" | "Git log" | "Git branch" | "Git tags" | "Git remotes" => {
            Some(if complete { "Inspected" } else { "Inspecting" })
        }
        "Repository info" => Some(if complete { "Inspected" } else { "Inspecting" }),
        "Language servers" | "Workspace diagnostics" => {
            Some(if complete { "Checked" } else { "Checking" })
        }
        _ => None,
    };
    phrase.map_or_else(
        || {
            if complete {
                Cow::Borrowed(name)
            } else {
                Cow::Owned(format!("{name}…"))
            }
        },
        |form| Cow::Owned(format!("{form} {name}{}", if complete { "" } else { "…" })),
    )
}
pub fn is_reasoning_entry(entry: &TranscriptEntry) -> bool {
    entry
        .operation()
        .is_some_and(|operation| operation.kind == OperationKind::Reasoning)
}

/// Whether two operations are the same sort of work, so a run of them can
/// fold into one counted row: tool calls by source and verb, lifecycle rows
/// by title.
pub fn same_action_type(left: &TranscriptEntry, right: &TranscriptEntry) -> bool {
    let (Some(left), Some(right)) = (left.operation(), right.operation()) else {
        return false;
    };
    left.kind == right.kind
        && if left.kind.is_call() {
            left.source == right.source
                && tool_lifecycle_label(&left.label, true)
                    == tool_lifecycle_label(&right.label, true)
        } else {
            left.label == right.label
        }
}

/// Empty in-progress assistant rows are ordering anchors for a reply that
/// follows a preparing tool, but they render nothing. They must not break a
/// run of actions into separate groups.
fn is_empty_assistant_anchor(entry: &TranscriptEntry) -> bool {
    matches!(
        entry,
        TranscriptEntry::Message {
            actor: EventActor::Assistant,
            text,
            attachments,
            status: MessageStatus::InProgress,
            ..
        } if text.trim().is_empty() && attachments.is_empty()
    )
}

/// A run is every maximal sequence of consecutive operations, ignoring empty
/// assistant anchors that render nothing.
pub fn tool_run_windows(entries: &[TranscriptEntry]) -> Vec<Option<ToolRunWindow>> {
    let mut windows = vec![None; entries.len()];
    let mut index = 0;
    while index < entries.len() {
        if !entries[index].is_operation() {
            index += 1;
            continue;
        }
        let start = index;
        let mut end = index;
        let mut total = 0;
        while index < entries.len() {
            if entries[index].is_operation() {
                total += 1;
                index += 1;
                end = index;
            } else if is_empty_assistant_anchor(&entries[index]) {
                index += 1;
            } else {
                break;
            }
        }
        if total > TOOL_RUN_BOX_THRESHOLD {
            windows[start..end].fill(Some(ToolRunWindow { start, end, total }));
        }
    }
    windows
}

pub fn action_run_windows(
    entries: &[TranscriptEntry],
    outer: &[Option<ToolRunWindow>],
) -> Vec<Option<ToolRunWindow>> {
    let mut windows = vec![None; entries.len()];
    let mut index = 0;
    while index < entries.len() {
        let Some(parent) = outer[index] else {
            index += 1;
            continue;
        };
        if !entries[index].is_operation() {
            index += 1;
            continue;
        }
        let start = index;
        let first = &entries[start];
        index += 1;
        let mut end = index;
        let mut total = 1;
        while index < parent.end {
            let entry = &entries[index];
            if is_empty_assistant_anchor(entry) {
                index += 1;
            } else if entry.is_operation() && same_action_type(first, entry) {
                total += 1;
                index += 1;
                end = index;
            } else {
                break;
            }
        }
        if total > 1 {
            let window = ToolRunWindow { start, end, total };
            windows[start..end].fill(Some(window));
        }
    }
    windows
}
