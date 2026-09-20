//! Durable instruction and tool-declaration replay.
//!
//! The native harness assembles the leading `System` message and the tool
//! catalog from scratch on every turn. Most of those inputs are fixed for the
//! life of a context generation, but three genuinely vary mid-conversation:
//! the skills appendix (`native_context::prompt_appendix`, which also adds a
//! tool), MCP startup failures (which both append a warning and withdraw that
//! server's tools), and tools surfaced on demand by `ToolSearch`.
//!
//! Rebuilding silently is wrong in two ways. The replayed request head stops
//! matching the head the model was actually shown earlier in the same
//! conversation, so a later turn rewrites its prefix instead of extending it;
//! and nothing durable records that the declarations ever changed, so a
//! resumed or forked session cannot reconstruct what the model knew.
//!
//! The contract here is an immutable base plus typed, ordered, bounded
//! changes:
//!
//! * The first turn of a context generation records a [`Declarations`] base.
//!   Later turns replay that base verbatim rather than rebuilding it.
//! * Each later turn records only a [`DeclarationDelta`] against the previous
//!   effective state.
//! * Replay folds base + deltas into the effective state and re-emits at most
//!   [`MAX_REPLAYED_DELTAS`] positional markers. The bound is what makes this
//!   a replay contract rather than an unbounded log: without it a long session
//!   carries one marker per turn and the request grows without limit.
//!
//! Declarations are presentation only. [`ToolDecl`] holds a digest instead of
//! the input schema and carries no dispatcher handle, so a replayed
//! declaration is structurally incapable of authorizing a call. Admission
//! stays with the live dispatcher, and a replayed declaration for a tool that
//! no longer exists still fails admission.
//!
//! # Provider transport
//!
//! Measured from the encoders, not assumed:
//!
//! * `NativeRoute::ChatCompletions` serializes `ModelMessage` directly, and
//!   the enum is `#[serde(tag = "role")]`, so a `System` message keeps its
//!   conversation position. Instruction changes ride in place.
//! * `NativeRoute::CodexAccount` targets the Responses API, whose encoder
//!   collects every `System` into one `instructions` field regardless of
//!   position, so an instruction change necessarily rewrites the head.
//! * Subscription CLI lanes take `system_prompt` as a scalar request field and
//!   have no in-conversation representation at all.
//!
//! No lane in tree accepts a tool-declaration delta; `tools` is one flat array
//! per request everywhere. Lanes that cannot carry a change in position use
//! [`DeclarationTransport::Collapsed`], which rewrites the head and makes no
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
/// The input schema is reduced to a digest: the journal would otherwise carry
/// a copy of every tool's full JSON schema on every change, and replay only
/// needs to know *that* a declaration differs, not to re-derive it.
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

    /// The change that takes `previous` to `self`, or `None` when nothing moved.
    pub(crate) fn diff(&self, previous: &Self) -> Option<DeclarationDelta> {
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

    fn apply(&mut self, delta: &DeclarationDelta) {
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
