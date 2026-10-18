use std::collections::HashMap;

use borg_agent_runtime::{
    EventActor, MessageStatus, SessionEvent, SessionEventKind, SessionPayloadKind,
};
use uuid::Uuid;

use crate::core::TranscriptCore;
use crate::entry::TranscriptEntry;
use crate::grouping::is_reasoning_entry;
use crate::wire::*;

/// Host projection of the actual terminal semantic state. Rendering never runs here.
pub struct ViewReducer {
    pub core: TranscriptCore,
    pub(crate) cursor: Cursor,
    pub(crate) session_id: String,
    pub(crate) row_revisions: HashMap<String, u64>,
    pub(crate) text_offsets: HashMap<String, usize>,
    pub(crate) history_positions: HashMap<String, String>,
    pub(crate) group_membership: HashMap<String, String>,
    pub(crate) group_totals: HashMap<String, usize>,
}

impl ViewReducer {
    pub fn new(session_id: Uuid, epoch: String) -> Self {
        let mut reducer = Self {
            core: TranscriptCore::default(),
            cursor: Cursor {
                epoch,
                revision: "0".into(),
            },
            session_id: session_id.to_string(),
            row_revisions: HashMap::new(),
            text_offsets: HashMap::new(),
            history_positions: HashMap::new(),
            group_membership: HashMap::new(),
            group_totals: HashMap::new(),
        };
        reducer.sync_meta();
        reducer
    }

    pub fn turn_usage(&self, message_id: &Uuid) -> Option<&Usage> {
        self.core.usage_by_turn.get(message_id)
    }

    pub fn apply_child_meta(
        &mut self,
        child_id: Uuid,
        child_cursor: Cursor,
        child_meta: &Meta,
    ) -> anyhow::Result<Vec<ViewOp>> {
        let child = self
            .core
            .subagent_snapshots
            .get(&child_id)
            .ok_or_else(|| anyhow::anyhow!("unknown child ownership"))?;
        anyhow::ensure!(
            child.parent_session_id.to_string() == self.session_id,
            "child belongs to another parent scope"
        );
        if !self
            .core
            .apply_child_meta(child_id, child_cursor, child_meta)?
        {
            return Ok(Vec::new());
        }
        let revision = self
            .cursor
            .revision
            .parse::<u64>()?
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("view revision exhausted"))?;
        self.cursor.revision = revision.to_string();
        self.sync_meta();
        let mut ops = Vec::new();
        // Settled child rows can leave the active window while their metadata
        // remains authoritative; do not resurrect them at the transcript tail.
        if let Some(index) = self.core.subagent_entries.get(&child_id).copied()
            && self.core.order.dirty.borrow_mut().remove(&index)
        {
            let id = self.semantic_row_id(index).expect("child row identity");
            self.row_revisions.insert(id.clone(), revision);
            self.history_positions
                .entry(id)
                .or_insert_with(|| format!("child:{child_id}"));
            if let Some(row) = self.row(index) {
                ops.push(ViewOp::RowUpsert { row });
            }
        }
        ops.push(ViewOp::MetaPatch {
            meta: self.core.meta.clone(),
        });
        self.evict_settled_rows();
        Ok(ops)
    }

    pub fn cursor(&self) -> &Cursor {
        &self.cursor
    }

    pub fn apply(&mut self, event: SessionEvent) -> Vec<ViewOp> {
        let revision = self
            .cursor
            .revision
            .parse::<u64>()
            .expect("validated authority revision")
            .checked_add(1)
            .expect("view revision exhausted");
        self.cursor.revision = revision.to_string();
        self.core.apply(&event);
        let mut ops = Vec::new();
        for (event_id, ordinal) in self.core.order.removed.drain(..) {
            let id = format!("{event_id}:{ordinal}");
            self.row_revisions.remove(&id);
            self.text_offsets.remove(&id);
            ops.push(ViewOp::RowRemove { id });
        }
        let mut dirty = self
            .core
            .order
            .dirty
            .borrow_mut()
            .drain()
            .collect::<Vec<_>>();
        dirty.sort_unstable();
        for index in dirty {
            let Some((_event_id, ordinal)) = self.core.order.identities.get(index).copied() else {
                continue;
            };
            let id = self.semantic_row_id(index).expect("row identity");
            self.history_positions
                .entry(id.clone())
                .or_insert_with(|| format!("{}:{ordinal}", event.sequence));
            let Some(entry) = self.core.order.get(index) else {
                continue;
            };
            let (field, text) = entry_text(entry);
            let offset = self.text_offsets.get(&id).copied();
            let old_revision = self.row_revisions.get(&id).copied();
            let next = revision;
            self.row_revisions.insert(id.clone(), next);
            let append_target = match &event.kind {
                SessionEventKind::MessageDelta { message_id, .. } => {
                    self.core.messages.get(message_id).copied() == Some(index)
                }
                SessionEventKind::ReasoningTextDelta { .. } => {
                    self.core.active_reasoning == Some(index)
                }
                SessionEventKind::ReasoningBlockDelta { reasoning_id, .. } => {
                    self.core.reasoning_blocks.get(reasoning_id).copied() == Some(index)
                }
                _ => false,
            };
            if append_target
                && let (Some(offset), Some(old_revision), Some(field), Some(text)) =
                    (offset, old_revision, field, text)
                && offset <= text.len()
                && text.is_char_boundary(offset)
            {
                ops.push(ViewOp::RowAppend {
                    id: id.clone(),
                    expected_revision: old_revision.to_string(),
                    revision: next.to_string(),
                    offset,
                    field,
                    text: text[offset..].to_string(),
                });
                self.text_offsets.insert(id, text.len());
                continue;
            }
            if let Some(row) = self.row(index) {
                if let Some(text) = text {
                    self.text_offsets.insert(id, text.len());
                }
                ops.push(ViewOp::RowUpsert { row });
            }
        }
        ops.extend(self.group_ops());
        self.sync_meta();
        ops.push(ViewOp::MetaPatch {
            meta: self.core.meta.clone(),
        });
        self.evict_settled_rows();
        ops
    }

    pub fn snapshot(&self) -> Snapshot {
        let mut rows = (0..self.core.order.len())
            .rev()
            .filter_map(|i| self.row(i))
            .take(SNAPSHOT_ROWS)
            .collect::<Vec<_>>();
        rows.reverse();
        for row in &mut rows {
            let original = match &mut row.body {
                RowBody::Message { text, .. } | RowBody::Reasoning { text, .. } => Some(text),
                _ => None,
            };
            if let Some(text) = original
                && text.len() > 4096
            {
                if let Some(state) = row.text_state.as_mut() {
                    state.asset = Some(
                        crate::assets::TextAssetAddress {
                            row_id: row.id.clone(),
                            revision: row.revision.clone(),
                            field: state.field.clone(),
                            bytes: state.bytes.clone(),
                        }
                        .asset_ref(),
                    );
                }
                *text = preview(text);
            }
        }
        let mut groups: HashMap<String, Vec<String>> = HashMap::new();
        for row in &rows {
            if let Some(group) = self.group_membership.get(&row.id) {
                groups
                    .entry(group.clone())
                    .or_default()
                    .push(row.id.clone());
            }
        }
        for (id, members) in groups {
            if members.len() < 2 {
                continue;
            }
            let Some(first) = rows.iter().find(|row| row.id == members[0]).cloned() else {
                continue;
            };
            rows.push(Row {
                id: id.clone(),
                revision: self
                    .row_revisions
                    .get(&id)
                    .copied()
                    .unwrap_or(1)
                    .to_string(),
                kind: RowKind::ActionGroup,
                timestamp: first.timestamp,
                source_session_id: self.session_id.clone(),
                history_before: first.history_before,
                after_id: first.after_id,
                text_state: None,
                body: RowBody::ActionGroup {
                    label: format!(
                        "{} actions",
                        self.group_totals.get(&id).copied().unwrap_or(members.len())
                    ),
                    row_ids: members,
                    actions: Vec::new(),
                    status: Lifecycle::Complete,
                },
            });
        }
        while rows.len() > SNAPSHOT_ROWS {
            rows.remove(0);
        }
        let present = rows
            .iter()
            .map(|row| row.id.clone())
            .collect::<std::collections::HashSet<_>>();
        for row in &mut rows {
            if let RowBody::ActionGroup { row_ids, .. } = &mut row.body {
                row_ids.retain(|id| present.contains(id));
            }
        }
        let mut view = Snapshot {
            cursor: self.cursor.clone(),
            history_anchor: rows.first().map(|row| row.history_before.clone()),
            rows,
            meta: self.core.meta.clone(),
        };
        while serde_json::to_vec(&view).expect("serializable view").len() > SNAPSHOT_BYTES
            && !view.rows.is_empty()
        {
            view.rows.remove(0);
            view.history_anchor = view.rows.first().map(|row| row.history_before.clone());
        }
        let present = view
            .rows
            .iter()
            .map(|row| row.id.clone())
            .collect::<std::collections::HashSet<_>>();
        for row in &mut view.rows {
            if let RowBody::ActionGroup { row_ids, .. } = &mut row.body {
                row_ids.retain(|id| present.contains(id));
            }
        }
        view
    }

    fn semantic_row_id(&self, index: usize) -> Option<String> {
        self.core
            .order
            .identities
            .get(index)
            .map(|(id, ordinal)| format!("{id}:{ordinal}"))
    }

    fn previous_row_id(&self, index: usize) -> Option<String> {
        self.semantic_row_id(index.checked_sub(1)?)
    }

    fn row(&self, index: usize) -> Option<Row> {
        let (event_id, _ordinal) = *self.core.order.identities.get(index)?;
        let id = self.semantic_row_id(index)?;
        let entry = self.core.order.get(index)?;
        if let Some((message_id, sender_id, sender_name)) =
            self.core.team_report_sources.get(&event_id)
            && let TranscriptEntry::Action { body, time, .. } = entry
        {
            return Some(Row {
                id: id.clone(),
                revision: self
                    .row_revisions
                    .get(&id)
                    .copied()
                    .unwrap_or(1)
                    .to_string(),
                kind: RowKind::TeamReport,
                timestamp: time.clone(),
                source_session_id: self.session_id.clone(),
                history_before: self.history_positions.get(&id).cloned().unwrap_or_default(),
                after_id: self.previous_row_id(index),
                text_state: None,
                body: RowBody::TeamReport {
                    sender_id: sender_id.to_string(),
                    sender_name: sender_name.clone(),
                    message_id: message_id.to_string(),
                    text: preview(body.as_deref().unwrap_or("")),
                },
            });
        }
        let (kind, timestamp, body) = match entry {
            TranscriptEntry::Message {
                actor,
                text,
                attachments,
                model,
                effort,
                time,
                status,
                execution,
                redirected,
                ..
            } => {
                if *actor == EventActor::System
                    || *status == MessageStatus::Queued
                    || (*actor == EventActor::Assistant && text.trim().is_empty())
                {
                    return None;
                }
                (
                    RowKind::Message,
                    time.clone(),
                    RowBody::Message {
                        actor: if *actor == EventActor::User {
                            Actor::User
                        } else {
                            Actor::Assistant
                        },
                        text: text.clone(),
                        status: execution.lifecycle(),
                        attachments: attachments
                            .iter()
                            .map(|(number, _)| AssetRef {
                                id: format!("attachment:{id}:{number}"),
                                media_type: "image/*".into(),
                                bytes: "0".into(),
                            })
                            .collect(),
                        model: model.clone(),
                        effort: effort.clone(),
                        interrupted: execution.is_interrupted(),
                        redirected: *redirected,
                    },
                )
            }
            TranscriptEntry::Tool {
                source_name,
                name,
                detail,
                code_view,
                output_view,
                payload_refs,
                time,
                execution,
                outcome,
                cwd,
                ..
            } => {
                let status = execution.lifecycle();
                if let Some((language, patch)) = code_view
                    .as_ref()
                    .filter(|(language, _)| borg_agent_runtime::is_diff_language(language))
                {
                    (
                        RowKind::Diff,
                        time.clone(),
                        RowBody::Diff {
                            patch: preview(patch),
                            status,
                            path: detail.clone(),
                            language: language.clone(),
                            before: None,
                            after: None,
                            added: patch
                                .lines()
                                .filter(|line| line.starts_with('+') && !line.starts_with("+++"))
                                .count(),
                            removed: patch
                                .lines()
                                .filter(|line| line.starts_with('-') && !line.starts_with("---"))
                                .count(),
                        },
                    )
                } else if is_reasoning_entry(entry) {
                    (
                        RowKind::Reasoning,
                        time.clone(),
                        RowBody::Reasoning {
                            text: (code_view
                                .as_ref()
                                .map(|(_, text)| text.as_str())
                                .unwrap_or(""))
                            .to_string(),
                            status,
                        },
                    )
                } else {
                    (
                        RowKind::Tool,
                        time.clone(),
                        RowBody::Tool {
                            source_name: source_name.clone(),
                            name: name.clone(),
                            detail: preview(detail),
                            input: code_view.as_ref().map(|(_, text)| preview(text)),
                            output_preview: output_view.as_ref().map(|(_, text)| preview(text)),
                            assets: payload_refs
                                .iter()
                                .filter(|p| {
                                    matches!(
                                        p.kind,
                                        SessionPayloadKind::ToolInput
                                            | SessionPayloadKind::ToolOutput
                                            | SessionPayloadKind::ToolResultInput
                                    )
                                })
                                .map(|p| AssetRef {
                                    id: p.id.to_string(),
                                    media_type: "text/plain".into(),
                                    bytes: p.byte_len.to_string(),
                                })
                                .collect(),
                            status,
                            started_at: execution.started_at().to_rfc3339(),
                            completed_at: execution.completed_at().map(|t| t.to_rfc3339()),
                            outcome: outcome.clone(),
                            cwd: cwd.clone(),
                            parent_id: None,
                        },
                    )
                }
            }
            TranscriptEntry::Action {
                label,
                detail,
                time,
                execution,
                ..
            } => (
                RowKind::ActionGroup,
                time.clone(),
                RowBody::ActionGroup {
                    label: format!("{label} · {detail}"),
                    row_ids: Vec::new(),
                    actions: vec![action_row(entry)],
                    status: execution.lifecycle(),
                },
            ),
            TranscriptEntry::Activity { text, time } => (
                RowKind::ActionGroup,
                time.clone(),
                RowBody::ActionGroup {
                    label: preview(text),
                    row_ids: Vec::new(),
                    actions: vec![action_row(entry)],
                    status: Lifecycle::Complete,
                },
            ),
            TranscriptEntry::Info { title, text, time } => (
                RowKind::ActionGroup,
                time.clone(),
                RowBody::ActionGroup {
                    label: format!("{title} · {}", preview(text)),
                    row_ids: Vec::new(),
                    actions: vec![action_row(entry)],
                    status: Lifecycle::Complete,
                },
            ),
            TranscriptEntry::Compaction {
                summary,
                time,
                execution,
                ..
            } => (
                RowKind::ActionGroup,
                time.clone(),
                RowBody::ActionGroup {
                    label: preview(summary),
                    row_ids: Vec::new(),
                    actions: vec![action_row(entry)],
                    status: execution.lifecycle(),
                },
            ),
            TranscriptEntry::Goal { time, .. } | TranscriptEntry::Plan { time, .. } => (
                RowKind::ActionGroup,
                time.clone(),
                RowBody::ActionGroup {
                    label: String::new(),
                    row_ids: Vec::new(),
                    actions: vec![action_row(entry)],
                    status: Lifecycle::Complete,
                },
            ),
        };
        Some(Row {
            id: id.clone(),
            revision: self
                .row_revisions
                .get(&id)
                .copied()
                .unwrap_or(1)
                .to_string(),
            kind,
            timestamp,
            source_session_id: self.session_id.clone(),
            history_before: self.history_positions.get(&id).cloned().unwrap_or_default(),
            after_id: self.previous_row_id(index),
            text_state: entry_text(entry).1.map(|text| RowTextState {
                field: entry_text(entry).0.expect("text field"),
                bytes: text.len().to_string(),
                asset: if matches!(entry, TranscriptEntry::Tool { .. }) && text.len() > 4096 {
                    Some(
                        crate::assets::TextAssetAddress {
                            row_id: id.clone(),
                            revision: self
                                .row_revisions
                                .get(&id)
                                .copied()
                                .unwrap_or(1)
                                .to_string(),
                            field: entry_text(entry).0.expect("text field"),
                            bytes: text.len().to_string(),
                        }
                        .asset_ref(),
                    )
                } else {
                    None
                },
            }),
            body,
        })
    }

    fn group_ops(&mut self) -> Vec<ViewOp> {
        let windows = crate::grouping::tool_run_windows(&self.core.order);
        let mut ops = Vec::new();
        for (index, window) in windows.iter().enumerate() {
            let Some(window) = window else { continue };
            if index != window.start {
                continue;
            }
            let members = (window.start..window.end)
                .filter_map(|index| self.semantic_row_id(index))
                .collect::<Vec<_>>();
            let id = members
                .iter()
                .find_map(|id| self.group_membership.get(id))
                .cloned()
                .unwrap_or_else(|| format!("group:{}", members[0]));
            for member in &members {
                if self.group_membership.get(member) != Some(&id) {
                    *self.group_totals.entry(id.clone()).or_default() += 1;
                }
                self.group_membership.insert(member.clone(), id.clone());
            }
            let revision = self
                .cursor
                .revision
                .parse::<u64>()
                .expect("authority revision");
            self.row_revisions.insert(id.clone(), revision);
            let Some(first) = self.row(window.start) else {
                continue;
            };
            let status = if self.core.order[window.start..window.end].iter().any(|row| {
                matches!(
                    row,
                    TranscriptEntry::Tool { execution, .. } if !execution.is_terminal()
                )
            }) {
                Lifecycle::Running
            } else {
                Lifecycle::Complete
            };
            ops.push(ViewOp::RowUpsert {
                row: Row {
                    id: id.clone(),
                    revision: revision.to_string(),
                    kind: RowKind::ActionGroup,
                    timestamp: first.timestamp,
                    source_session_id: self.session_id.clone(),
                    history_before: first.history_before,
                    after_id: first.after_id,
                    text_state: None,
                    body: RowBody::ActionGroup {
                        label: format!(
                            "{} actions",
                            self.group_totals.get(&id).copied().unwrap_or(window.total)
                        ),
                        row_ids: members,
                        actions: Vec::new(),
                        status,
                    },
                },
            });
        }
        ops
    }

    fn evict_settled_rows(&mut self) {
        while self.core.order.len() > 128 {
            let Some(index) = self.core.order.iter().position(|entry| match entry {
                TranscriptEntry::Tool { execution, .. } => execution.is_terminal(),
                TranscriptEntry::Message { execution, .. } => execution.is_terminal(),
                TranscriptEntry::Action { execution, .. }
                | TranscriptEntry::Compaction { execution, .. } => execution.is_terminal(),
                _ => true,
            }) else {
                break;
            };
            let (event_id, _ordinal) = self.core.order.identities[index];
            let id = self.semantic_row_id(index).expect("row identity");
            self.row_revisions.remove(&id);
            self.text_offsets.remove(&id);
            self.history_positions.remove(&id);
            self.group_membership.remove(&id);
            self.core.team_report_sources.remove(&event_id);
            self.core.messages.retain(|_, i| *i != index);
            self.core.tools.retain(|_, i| *i != index);
            self.core.subagent_entries.retain(|_, i| *i != index);
            self.core.team_broadcast_entries.retain(|_, i| *i != index);
            self.core.order.remove(index);
            self.core.reindex_after_removal(index);
        }
        self.group_totals
            .retain(|id, _| self.group_membership.values().any(|group| group == id));
        self.core.prune_tool_inspection();
        self.core.order.removed.clear();
        self.core.order.dirty.get_mut().clear();
        self.core.order.invalidations.get_mut().clear();
    }

    fn sync_meta(&mut self) {
        self.core.meta.configuration = self.core.config.as_ref().map(|config| ViewConfiguration {
            cwd: config.cwd.to_string_lossy().into_owned(),
            provider: config.provider,
            model: config.model.clone(),
            effort: config.effort.clone(),
            fast: config.fast,
            ultrafast: config.ultrafast,
            permission_mode: config.permission_mode,
            response_language: config.response_language,
        });
        self.core.meta.waiting_on_watchers = self.core.waiting_on_watchers;
        self.core.meta.goal = self.core.goal.as_ref().map(ViewGoal::from);
        self.core.meta.plan = self.core.todos.clone();
        let usage = &self.core.session_usage;
        self.core.meta.usage = Usage {
            input_tokens: usage.input_tokens.to_string(),
            output_tokens: usage.output_tokens.to_string(),
            cached_tokens: usage.cached_input_tokens.to_string(),
            cost_microusd: usage.cost_microusd.map(|n| n.to_string()),
            cache_creation_tokens: Some(usage.cache_creation_input_tokens.to_string()),
            total_tokens: Some(usage.total_tokens.to_string()),
            context_tokens: usage.context_tokens.map(|n| n.to_string()),
            context_window_tokens: self.core.context_window_tokens.map(|n| n.to_string()),
            cost_basis: (!usage.cost_basis.is_empty()).then(|| usage.cost_basis.clone()),
            ..Usage::default()
        };
        self.core.meta.context = Context {
            tokens: self.core.context_tokens.map(|n| n.to_string()),
            window_tokens: self.core.context_window_tokens.map(|n| n.to_string()),
            remaining_percent: self
                .core
                .context_known
                .then_some(self.core.context_remaining_percent),
        };
        self.core.meta.watches = self
            .core
            .watches
            .iter()
            .map(|watch| Watch {
                id: watch.watch_id.to_string(),
                started_at: watch.started_at.to_rfc3339(),
                last_event_at: watch.last_event_at.map(|at| at.to_rfc3339()),
                event_count: watch.event_count.to_string(),
                label: watch.label.clone(),
                status: if watch.running {
                    Lifecycle::Running
                } else {
                    Lifecycle::Complete
                },
                source_session_id: self.session_id.clone(),
            })
            .collect();
        self.core.meta.children = self
            .core
            .subagent_snapshots
            .keys()
            .filter_map(|id| self.core.subagent_summary(*id))
            .collect();
    }
}
fn entry_text(entry: &TranscriptEntry) -> (Option<TextField>, Option<&str>) {
    match entry {
        TranscriptEntry::Message { text, .. } => (Some(TextField::Message), Some(text)),
        TranscriptEntry::Tool {
            code_view: Some((language, text)),
            ..
        } if language == "reasoning" => (Some(TextField::Reasoning), Some(text)),
        TranscriptEntry::Tool {
            output_view: Some((_, text)),
            ..
        } => (Some(TextField::ToolOutput), Some(text)),
        TranscriptEntry::Tool {
            code_view: Some((_, text)),
            ..
        } => (Some(TextField::ToolInput), Some(text)),
        _ => (None, None),
    }
}
fn preview(text: &str) -> String {
    let mut end = text.len().min(4096);
    while !text.is_char_boundary(end) {
        end -= 1
    }
    text[..end].to_string()
}
fn action_row(entry: &TranscriptEntry) -> ActionRow {
    match entry {
        TranscriptEntry::Action {
            source_session_id,
            kind,
            label,
            detail,
            body,
            execution,
            ..
        } => ActionRow::Lifecycle {
            target_session_id: source_session_id.map(|id| id.to_string()),
            kind: match kind {
                crate::entry::TranscriptActionKind::Agent => ActionKind::Agent,
                crate::entry::TranscriptActionKind::Watch => ActionKind::Watch,
                crate::entry::TranscriptActionKind::Approval => ActionKind::Approval,
                crate::entry::TranscriptActionKind::ProviderInteraction => {
                    ActionKind::ProviderInteraction
                }
                crate::entry::TranscriptActionKind::Error => ActionKind::Error,
            },
            label: label.clone(),
            detail: preview(detail),
            body: body.as_ref().map(|text| preview(text)),
            status: execution.lifecycle(),
        },
        TranscriptEntry::Plan {
            items, previous, ..
        } => ActionRow::Plan {
            items: items.clone(),
            previous: previous.clone(),
        },
        TranscriptEntry::Goal { goal, .. } => ActionRow::Goal {
            goal: ViewGoal::from(goal),
        },
        TranscriptEntry::Info { title, text, .. } => ActionRow::Information {
            title: title.clone(),
            text: preview(text),
        },
        TranscriptEntry::Compaction {
            summary,
            sequence,
            execution,
            ..
        } => ActionRow::Compaction {
            summary: preview(summary),
            sequence: sequence.to_string(),
            complete: execution.is_terminal(),
        },
        TranscriptEntry::Activity { text, .. } => ActionRow::Activity {
            text: preview(text),
        },
        _ => unreachable!("typed action projection"),
    }
}

pub(crate) fn child_summary(
    child: &borg_agent_runtime::SubagentSnapshot,
    approvals: &[Approval],
) -> ChildSummary {
    ChildSummary {
        source_session_id: child.session_id.to_string(),
        parent_session_id: child.parent_session_id.to_string(),
        name: child.task_name.clone(),
        status: match child.lifecycle() {
            borg_agent_runtime::SubagentLifecycle::Starting
            | borg_agent_runtime::SubagentLifecycle::Running => Lifecycle::Running,
            borg_agent_runtime::SubagentLifecycle::WaitingOnWatchers
            | borg_agent_runtime::SubagentLifecycle::WaitingForApproval
            | borg_agent_runtime::SubagentLifecycle::Dormant
            | borg_agent_runtime::SubagentLifecycle::DormantInterrupted => Lifecycle::Waiting,
            borg_agent_runtime::SubagentLifecycle::Ready => Lifecycle::Complete,
            borg_agent_runtime::SubagentLifecycle::Stopped => Lifecycle::Stopped,
            borg_agent_runtime::SubagentLifecycle::Failed => Lifecycle::Failed,
        },
        last_activity: child
            .waiting_on_watchers()
            .or_else(|| child.detail.clone())
            .map(|s| preview(&s)),
        updated_at: child.updated_at.to_rfc3339(),
        approvals: approvals
            .iter()
            .filter(|a| a.source_session_id == child.session_id.to_string())
            .cloned()
            .collect(),
        usage: Usage {
            input_tokens: child.usage.input_tokens.to_string(),
            output_tokens: child.usage.output_tokens.to_string(),
            cached_tokens: child.usage.cached_input_tokens.to_string(),
            cost_microusd: child.usage.cost_microusd.map(|n| n.to_string()),
            cache_creation_tokens: Some(child.usage.cache_creation_input_tokens.to_string()),
            total_tokens: Some(child.usage.total_tokens.to_string()),
            context_tokens: child.usage.context_tokens.map(|n| n.to_string()),
            cost_basis: (!child.usage.cost_basis.is_empty())
                .then(|| child.usage.cost_basis.clone()),
            ..Usage::default()
        },
    }
}
