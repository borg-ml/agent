//! Durable instruction and tool-declaration replay.
//!
//! The native harness assembles the leading `System` message and the tool
//! catalog from scratch every turn. Three inputs genuinely vary
//! mid-conversation: the skills appendix, which also contributes a tool; MCP
//! startup failures, which append a warning and withdraw that server's tools;
//! and tools surfaced on demand by `ToolSearch`. Rebuilding silently means the
//! replayed head stops matching the head the model was shown, and nothing
//! durable records that the declarations moved.
//!
//! So: an immutable base per context generation, typed ordered deltas against
//! it, and a fold to current state plus at most [`MAX_REPLAYED_DELTAS`]
//! positional markers. The bound is the point. Without it a long session
//! carries one marker per turn, which is a log rather than a replay contract.
//!
//! What this record is NOT: a replayable copy of historical tool definitions.
//! [`ToolDecl`] keeps a digest instead of the input schema, so an earlier
//! tool's schema cannot be reconstructed from it. That is sufficient only
//! because declarations are never sent to a provider as tool definitions --
//! the live catalog is. This answers "what was declared, and when did it
//! change", not "what exactly did that schema look like".
//!
//! The same property is the security one: carrying no schema and no dispatcher
//! handle, a replayed declaration cannot authorize a call. Admission stays
//! with the live dispatcher.
//!
//! Transport, measured from the encoders rather than assumed: a
//! chat-completions route keeps `System` in conversation position; the Codex
//! Responses encoder collects every `System` into one `instructions` field
//! regardless of position; subscription lanes take `system_prompt` as a scalar
//! field. No lane accepts a tool-declaration delta, because `tools` is one
//! flat array per request everywhere. Lanes that cannot carry a change in
//! position use [`DeclarationTransport::Collapsed`] and make no
//! cache-preservation claim.

use std::collections::BTreeMap;

use anyhow::Result;
use borg_provider::provider::ModelToolDefinition;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;

use crate::{CodingProvider, SessionEventKind};

pub(crate) const DECLARATION_BASE_EVENT: &str = "native_declaration_base";
pub(crate) const DECLARATION_DELTA_EVENT: &str = "native_declaration_delta";

/// How many declaration changes keep a positional marker in the replayed
/// conversation. Older changes are folded into the base instead, so replay
/// stays bounded no matter how long the session runs.
pub(crate) const MAX_REPLAYED_DELTAS: usize = 8;

/// The parts of the leading instructions that can change mid-conversation.
///
/// Exactly the three that vary in tree. A slot that never changes belongs in
/// the immutable base, not here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum InstructionSlot {
    /// Skills discovered under the session roots.
    Skills,
    /// External MCP servers that failed to start this turn.
    McpUnavailable,
    /// Caller-supplied system prompt appendix.
    Appendix,
}

/// One tool as the model was told about it.
///
/// The input schema is reduced to a digest, which is lossy on purpose: the
/// journal would otherwise carry a copy of every tool's full JSON schema on
/// every change. The consequence is explicit -- this cannot reproduce a
/// historical tool definition, only detect that one differs. Nothing needs to:
/// a provider is always sent the live catalog, never a reconstruction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ToolDecl {
    pub name: String,
    pub description: String,
    pub schema_digest: String,
}

impl ToolDecl {
    pub(crate) fn from_definition(definition: &ModelToolDefinition) -> Self {
        Self {
            name: definition.name.clone(),
            description: definition.description.clone(),
            schema_digest: digest_of(&definition.input_schema),
        }
    }
}

/// `serde_json` serializes maps in insertion order for `Value::Object`, which
/// is `BTreeMap`-backed here, so the digest is stable across processes for an
/// unchanged schema.
fn digest_of(schema: &serde_json::Value) -> String {
    let mut hasher = Sha256::new();
    hasher.update(schema.to_string().as_bytes());
    format!("{:x}", hasher.finalize())[..32].to_string()
}

/// The effective instruction slots and tool catalog at one point in a context
/// generation. The first snapshot of a generation is the immutable base.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Declarations {
    #[serde(default)]
    pub instructions: BTreeMap<InstructionSlot, String>,
    #[serde(default)]
    pub tools: BTreeMap<String, ToolDecl>,
}

impl Declarations {
    pub(crate) fn capture(
        slots: impl IntoIterator<Item = (InstructionSlot, String)>,
        tools: &[ModelToolDefinition],
    ) -> Self {
        Self {
            instructions: slots
                .into_iter()
                .filter(|(_, text)| !text.is_empty())
                .collect(),
            tools: tools
                .iter()
                .map(|definition| (definition.name.clone(), ToolDecl::from_definition(definition)))
                .collect(),
        }
    }

    /// What to journal for this turn: the base when the context generation has
    /// none yet, a delta when something moved, nothing when it did not.
    ///
    /// Taking `Option` rather than requiring the caller to branch keeps the
    /// harness hook to one call, and keeps the "first turn of a generation"
    /// rule in one place instead of at every call site.
    pub(crate) fn change_against(
        &self,
        previous: Option<&Self>,
    ) -> Option<DeclarationChange> {
        match previous {
            None => Some(DeclarationChange::Base(self.clone())),
            Some(previous) => self.diff(previous).map(DeclarationChange::Delta),
        }
    }

    /// The change that takes `previous` to `self`, or `None` when nothing moved.
    fn diff(&self, previous: &Self) -> Option<DeclarationDelta> {
        let mut instructions = BTreeMap::new();
        for (slot, text) in &self.instructions {
            if previous.instructions.get(slot) != Some(text) {
                instructions.insert(*slot, Some(text.clone()));
            }
        }
        for slot in previous.instructions.keys() {
            if !self.instructions.contains_key(slot) {
                instructions.insert(*slot, None);
            }
        }
        let tools_added = self
            .tools
            .values()
            .filter(|tool| previous.tools.get(&tool.name) != Some(*tool))
            .cloned()
            .collect::<Vec<_>>();
        let tools_removed = previous
            .tools
            .keys()
            .filter(|name| !self.tools.contains_key(*name))
            .cloned()
            .collect::<Vec<_>>();
        let delta = DeclarationDelta {
            instructions,
            tools_added,
            tools_removed,
        };
        (!delta.is_empty()).then_some(delta)
    }

    pub(crate) fn apply(&mut self, delta: &DeclarationDelta) {
        for (slot, text) in &delta.instructions {
            match text {
                Some(text) => {
                    self.instructions.insert(*slot, text.clone());
                }
                None => {
                    self.instructions.remove(slot);
                }
            }
        }
        for name in &delta.tools_removed {
            self.tools.remove(name);
        }
        for tool in &delta.tools_added {
            self.tools.insert(tool.name.clone(), tool.clone());
        }
    }
}

/// One turn's change to the declarations.
///
/// A single struct rather than one event per dimension because the changes
/// that actually occur are correlated: a skill appearing rewrites an
/// instruction slot *and* adds a tool in the same turn, and splitting that
/// into two events would let replay observe a state the model never saw.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DeclarationDelta {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub instructions: BTreeMap<InstructionSlot, Option<String>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools_added: Vec<ToolDecl>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools_removed: Vec<String>,
}

impl DeclarationDelta {
    pub(crate) fn is_empty(&self) -> bool {
        self.instructions.is_empty() && self.tools_added.is_empty() && self.tools_removed.is_empty()
    }

    /// What the model is told when this change keeps a positional marker.
    ///
    /// Names only: the effective declarations are already in the head, so the
    /// marker exists to place the change in time, not to restate it.
    pub(crate) fn marker_text(&self) -> String {
        let mut parts = Vec::new();
        if !self.tools_added.is_empty() {
            let names = self
                .tools_added
                .iter()
                .map(|tool| tool.name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            parts.push(format!("tools available: {names}"));
        }
        if !self.tools_removed.is_empty() {
            parts.push(format!(
                "tools withdrawn: {}",
                self.tools_removed.join(", ")
            ));
        }
        for (slot, text) in &self.instructions {
            let slot = match slot {
                InstructionSlot::Skills => "skills",
                InstructionSlot::McpUnavailable => "mcp availability",
                InstructionSlot::Appendix => "instructions",
            };
            parts.push(match text {
                Some(_) => format!("{slot} updated"),
                None => format!("{slot} cleared"),
            });
        }
        format!("## Declaration change\n{}", parts.join("; "))
    }
}

/// What one turn contributes to the durable declaration record.
///
/// Each arm is journaled under its own event kind and serialized from the
/// value it holds, so this enum itself never reaches the wire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DeclarationChange {
    /// First turn of a context generation: the immutable base itself.
    Base(Declarations),
    /// A later turn: only what moved since the previous effective state.
    Delta(DeclarationDelta),
}

/// How a lane carries a declaration change.
///
/// Chosen by the caller, which knows its own route; this module does not guess
/// a transport it cannot observe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeclarationTransport {
    /// The change rides in conversation position and the request prefix is
    /// preserved.
    InPlace,
    /// The lane rewrites the head. Correct, but no cache-preservation claim.
    Collapsed,
}

impl DeclarationTransport {
    pub(crate) fn prefix_preserved(self) -> bool {
        matches!(self, Self::InPlace)
    }
}

/// The bounded result of replaying a base and its recorded changes.
pub(crate) struct ReplayPlan<'a> {
    /// Declarations in force now, for lanes that must collapse to current.
    pub effective: Declarations,
    /// The most recent changes, oldest first, capped at [`MAX_REPLAYED_DELTAS`].
    pub markers: &'a [DeclarationDelta],
}

/// Fold a base and its ordered changes into current state plus a bounded tail
/// of positional markers.
pub(crate) fn plan_replay<'a>(
    base: &Declarations,
    deltas: &'a [DeclarationDelta],
) -> ReplayPlan<'a> {
    let mut effective = base.clone();
    for delta in deltas {
        effective.apply(delta);
    }
    let markers = &deltas[deltas.len().saturating_sub(MAX_REPLAYED_DELTAS)..];
    ReplayPlan {
        effective,
        markers,
    }
}

/// Journal one turn's contribution, base or delta, under the matching kind.
pub(crate) async fn record_declaration_change(
    events: &mpsc::Sender<SessionEventKind>,
    provider: CodingProvider,
    change: &DeclarationChange,
) -> Result<()> {
    match change {
        DeclarationChange::Base(base) => record_declaration_base(events, provider, base).await,
        DeclarationChange::Delta(delta) => record_declaration_delta(events, provider, delta).await,
    }
}

pub(crate) async fn record_declaration_base(
    events: &mpsc::Sender<SessionEventKind>,
    provider: CodingProvider,
    base: &Declarations,
) -> Result<()> {
    record(events, provider, DECLARATION_BASE_EVENT, base).await
}

pub(crate) async fn record_declaration_delta(
    events: &mpsc::Sender<SessionEventKind>,
    provider: CodingProvider,
    delta: &DeclarationDelta,
) -> Result<()> {
    record(events, provider, DECLARATION_DELTA_EVENT, delta).await
}

async fn record<T: Serialize>(
    events: &mpsc::Sender<SessionEventKind>,
    provider: CodingProvider,
    kind: &str,
    value: &T,
) -> Result<()> {
    let payload = serde_json::to_value(value)?;
    events
        .send(SessionEventKind::ProviderEvent {
            provider,
            kind: kind.to_string(),
            payload,
        })
        .await
        .map_err(|_| anyhow::anyhow!("session actor stopped while recording declarations"))
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

    fn tool(name: &str, description: &str) -> ModelToolDefinition {
        ModelToolDefinition::new(name, description, json!({"type": "object"})).unwrap()
    }

    fn skills(text: &str) -> [(InstructionSlot, String); 1] {
        [(InstructionSlot::Skills, text.to_string())]
    }

    /// The durability rule the whole contract rests on: a session rebuilt from
    /// its base and recorded changes has to arrive at the same declarations a
    /// session that never restarted would hold. If these diverge, a resumed or
    /// forked turn tells the model something the live turn never did, which is
    /// exactly the silent drift the base exists to prevent.
    #[test]
    fn folding_recorded_changes_lands_where_a_fresh_capture_does() {
        let base = Declarations::capture(skills("no skills"), &[tool("exec", "run")]);

        // A skill appears: it rewrites an instruction slot and adds a tool in
        // the same turn, which is why one delta carries both.
        let with_skill = Declarations::capture(
            skills("skill: deploy"),
            &[tool("exec", "run"), tool("Skill", "invoke a skill")],
        );
        // Then an MCP server withdraws its tool.
        let after_mcp = Declarations::capture(
            [
                (InstructionSlot::Skills, "skill: deploy".to_string()),
                (InstructionSlot::McpUnavailable, "linear".to_string()),
            ],
            &[tool("exec", "run"), tool("Skill", "invoke a skill")],
        );

        let deltas = vec![
            with_skill.diff(&base).expect("the skill changed something"),
            after_mcp
                .diff(&with_skill)
                .expect("the server outage changed something"),
        ];

        assert_eq!(plan_replay(&base, &deltas).effective, after_mcp);
    }

    /// Without a bound this is a log, not a replay contract: one marker per
    /// turn means the request grows for as long as the session runs. A session
    /// that changes its declarations far more often than the bound must still
    /// replay a bounded number of markers, and must still end at the correct
    /// effective state.
    #[test]
    fn a_long_run_of_changes_still_replays_a_bounded_number_of_markers() {
        let base = Declarations::capture(skills("start"), &[]);
        let mut previous = base.clone();
        let mut deltas = Vec::new();
        for turn in 0..MAX_REPLAYED_DELTAS * 5 {
            let current = Declarations::capture(skills(&format!("skill set {turn}")), &[]);
            deltas.push(current.diff(&previous).expect("each turn changed the slot"));
            previous = current;
        }

        let plan = plan_replay(&base, &deltas);
        assert_eq!(plan.markers.len(), MAX_REPLAYED_DELTAS);
        assert_eq!(plan.effective, previous);
    }
}
