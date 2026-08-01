//! Native Claude protocol: binary resolution and control-channel framing.
//!
//! Replaces the Node sidecar (`packages/borg-claude-sdk`) by speaking the
//! `claude` binary's stream-json + control_request protocol directly.
//!
//! The message half of the stream is unchanged and still parsed by
//! [`super::claude_stream::ClaudeStreamState`]; this module adds the control
//! half the sidecar used to broker.
//!
//! Wire protocol reference: `docs/claude-native-protocol.md`.

// The framing and resolution layer lands before the runner that drives it.
// Remove once `run_claude_native_chat_stream` consumes these.
#![allow(dead_code)]

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// `sdkCompat.harnessSchema` values this implementation understands. The
/// bundled manifest declares the schema the binary speaks; refusing an unknown
/// value fails loudly at spawn time instead of misparsing frames mid-turn.
const SUPPORTED_HARNESS_SCHEMAS: &[u64] = &[1];

/// Base argv shared by every invocation. `--verbose` is mandatory: the CLI
/// rejects `--output-format stream-json` without it.
pub(super) const BASE_ARGS: &[&str] = &[
    "--output-format",
    "stream-json",
    "--verbose",
    "--input-format",
    "stream-json",
];

/// Routes permission decisions to us as inbound `can_use_tool` control
/// requests rather than to a named permission-prompt tool.
pub(super) const STDIO_PERMISSION_ARGS: &[&str] = &["--permission-prompt-tool", "stdio"];

// ---------------------------------------------------------------------------
// Frames
// ---------------------------------------------------------------------------

/// One line of the child's stdout.
///
/// The stream is a multiplexed demux, not a pure message stream: control
/// traffic is interleaved with SDK messages. Unrecognized frames must be
/// ignored rather than treated as errors — Anthropic adds frame types without
/// bumping the protocol.
#[derive(Debug)]
pub(super) enum Frame {
    /// Reply to a request we sent, correlated by `request_id`.
    ControlResponse {
        request_id: String,
        result: ControlOutcome,
    },
    /// The CLI asking *us* something (permissions, elicitation, hooks).
    /// Every one must be answered or the turn hangs.
    ControlRequest { request_id: String, request: Value },
    /// Withdraws an in-flight inbound request; drop it without replying.
    ControlCancel { request_id: String },
    /// An SDK message for `ClaudeStreamState`.
    Message(Value),
    /// Keep-alives, transcript mirrors, and future frame types.
    Ignored,
}

#[derive(Debug)]
pub(super) enum ControlOutcome {
    Success(Value),
    Error(String),
}

impl Frame {
    pub(super) fn parse(line: &str) -> Result<Self> {
        let value: Value = serde_json::from_str(line)
            .with_context(|| format!("failed to parse claude frame: {}", truncate(line, 200)))?;
        Ok(Self::from_value(value))
    }

    fn from_value(value: Value) -> Self {
        match value
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default()
        {
            "control_response" => {
                let response = match value.get("response") {
                    Some(response) => response,
                    None => return Frame::Ignored,
                };
                let request_id = response
                    .get("request_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                if request_id.is_empty() {
                    return Frame::Ignored;
                }
                // Success payloads may carry prompt-redelivery fields
                // (`pending_permission_requests`, `pending_user_dialog_requests`).
                // The SDK strips and ignores them; so do we.
                let result = match response.get("subtype").and_then(Value::as_str) {
                    Some("error") => ControlOutcome::Error(
                        response
                            .get("error")
                            .and_then(Value::as_str)
                            .unwrap_or("claude control request failed")
                            .to_string(),
                    ),
                    _ => ControlOutcome::Success(
                        response.get("response").cloned().unwrap_or(Value::Null),
                    ),
                };
                Frame::ControlResponse { request_id, result }
            }
            "control_request" => {
                let request_id = value
                    .get("request_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                let request = value.get("request").cloned().unwrap_or(Value::Null);
                if request_id.is_empty() {
                    return Frame::Ignored;
                }
                Frame::ControlRequest {
                    request_id,
                    request,
                }
            }
            "control_cancel_request" => {
                let request_id = value
                    .get("request_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                if request_id.is_empty() {
                    return Frame::Ignored;
                }
                Frame::ControlCancel { request_id }
            }
            "keep_alive" | "transcript_mirror" => Frame::Ignored,
            "" => Frame::Ignored,
            _ => Frame::Message(value),
        }
    }
}

/// A `control_request` we send to the CLI. `request_id` is opaque and
/// caller-generated; the CLI echoes it back for correlation.
#[derive(Debug, Serialize)]
pub(super) struct OutboundControlRequest<'a> {
    #[serde(rename = "type")]
    pub(super) frame_type: &'static str,
    pub(super) request_id: &'a str,
    pub(super) request: Value,
}

impl<'a> OutboundControlRequest<'a> {
    pub(super) fn new(request_id: &'a str, request: Value) -> Self {
        Self {
            frame_type: "control_request",
            request_id,
            request,
        }
    }
}

/// Our reply to an inbound `control_request`.
pub(super) fn control_response_success(request_id: &str, payload: Value) -> Value {
    serde_json::json!({
        "type": "control_response",
        "response": {
            "subtype": "success",
            "request_id": request_id,
            "response": payload,
        }
    })
}

pub(super) fn control_response_error(request_id: &str, error: &str) -> Value {
    serde_json::json!({
        "type": "control_response",
        "response": {
            "subtype": "error",
            "request_id": request_id,
            "error": error,
        }
    })
}

// ---------------------------------------------------------------------------
// Binary resolution
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub(super) struct ClaudeBinary {
    pub(super) path: PathBuf,
    /// From the sibling `manifest.json` when the binary came from the npm
    /// platform package. `None` for a system `claude` on PATH, which ships no
    /// manifest — we accept it and rely on the `system/init` capability list.
    pub(super) harness_schema: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SdkManifest {
    #[serde(default)]
    version: Option<String>,
    #[serde(default)]
    sdk_compat: Option<SdkCompat>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SdkCompat {
    #[serde(default)]
    harness_schema: Option<u64>,
}

/// Locate the `claude` binary, most-explicit source first.
///
/// Unlike the sidecar's `provider.js` lookup this does not search the repo
/// checkout: the binary is never built from source, it is either installed or
/// vendored.
pub(super) fn resolve_claude_binary() -> Result<ClaudeBinary> {
    let mut attempted: Vec<String> = Vec::new();

    // 1. Explicit override wins, and is an error if it does not exist —
    //    silently falling through would hide a typo'd deployment config.
    if let Some(raw) = std::env::var_os("BORG_CLAUDE_BIN") {
        let path = PathBuf::from(raw);
        if !path.exists() {
            return Err(anyhow!(
                "BORG_CLAUDE_BIN points at {} which does not exist",
                path.display()
            ));
        }
        return describe(path);
    }

    // 2. Vendored alongside the Borg install.
    let vendored =
        borg_home().map(|home| home.join("providers/claude").join(platform_binary_name()));
    if let Some(candidate) = vendored {
        if candidate.exists() {
            return describe(candidate);
        }
        attempted.push(candidate.display().to_string());
    }

    // 3. The npm platform package, when running from a checkout.
    for root in npm_platform_package_roots() {
        let candidate = root.join(platform_binary_name());
        if candidate.exists() {
            return describe(candidate);
        }
        attempted.push(candidate.display().to_string());
    }

    // 4. A system-wide `claude` on PATH.
    if let Some(found) = which_on_path(platform_binary_name()) {
        return describe(found);
    }
    attempted.push(format!("{} on PATH", platform_binary_name()));

    Err(anyhow!(
        "the claude binary was not found; set BORG_CLAUDE_BIN or install Claude Code. Looked in:\n  {}",
        attempted.join("\n  ")
    ))
}

fn describe(path: PathBuf) -> Result<ClaudeBinary> {
    let path = path
        .canonicalize()
        .with_context(|| format!("failed to canonicalize {}", path.display()))?;
    let harness_schema = read_harness_schema(&path)?;
    Ok(ClaudeBinary {
        path,
        harness_schema,
    })
}

/// Read `sdkCompat.harnessSchema` from a manifest next to the binary, if one
/// exists, and reject a schema this build does not understand.
fn read_harness_schema(binary: &Path) -> Result<Option<u64>> {
    let Some(manifest_path) = manifest_beside(binary) else {
        return Ok(None);
    };
    let raw = match std::fs::read_to_string(&manifest_path) {
        Ok(raw) => raw,
        Err(_) => return Ok(None),
    };
    // A manifest we cannot parse is not fatal — it is metadata, and the binary
    // may still be fine. Only an explicitly unsupported schema stops us.
    let Ok(manifest) = serde_json::from_str::<SdkManifest>(&raw) else {
        return Ok(None);
    };
    let Some(schema) = manifest.sdk_compat.and_then(|compat| compat.harness_schema) else {
        return Ok(None);
    };
    if !SUPPORTED_HARNESS_SCHEMAS.contains(&schema) {
        return Err(anyhow!(
            "claude binary at {} declares harnessSchema {schema}, but this Borg build supports {:?}. \
             Upgrade Borg or pin an older Claude Code.",
            binary.display(),
            SUPPORTED_HARNESS_SCHEMAS,
        ));
    }
    tracing::debug!(
        schema,
        version = manifest.version.as_deref().unwrap_or("unknown"),
        "resolved claude binary"
    );
    Ok(Some(schema))
}

fn manifest_beside(binary: &Path) -> Option<PathBuf> {
    let dir = binary.parent()?;
    // The platform package holds the binary; the manifest lives in the wrapper
    // package next to it.
    let candidates = [
        dir.join("manifest.json"),
        dir.join("../claude-agent-sdk/manifest.json"),
    ];
    candidates.into_iter().find(|path| path.exists())
}

fn borg_home() -> Option<PathBuf> {
    std::env::var_os("BORG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".borg")))
}

fn platform_binary_name() -> &'static str {
    if cfg!(windows) {
        "claude.exe"
    } else {
        "claude"
    }
}

/// `@anthropic-ai/claude-agent-sdk-<platform>` — the optionalDependency that
/// carries the prebuilt binary.
fn npm_platform_package_roots() -> Vec<PathBuf> {
    let Some(package) = npm_platform_package_name() else {
        return Vec::new();
    };
    let relative = PathBuf::from("node_modules/@anthropic-ai").join(package);

    let mut roots = Vec::new();
    roots.push(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../packages/borg-claude-sdk")
            .join(&relative),
    );
    if let Ok(cwd) = std::env::current_dir() {
        roots.push(cwd.join("packages/borg-claude-sdk").join(&relative));
    }
    roots
}

fn npm_platform_package_name() -> Option<&'static str> {
    let os = match std::env::consts::OS {
        "linux" => "linux",
        "macos" => "darwin",
        "windows" => "win32",
        _ => return None,
    };
    let arch = match std::env::consts::ARCH {
        "x86_64" => "x64",
        "aarch64" => "arm64",
        _ => return None,
    };
    Some(match (os, arch) {
        ("linux", "x64") => "claude-agent-sdk-linux-x64",
        ("linux", "arm64") => "claude-agent-sdk-linux-arm64",
        ("darwin", "x64") => "claude-agent-sdk-darwin-x64",
        ("darwin", "arm64") => "claude-agent-sdk-darwin-arm64",
        ("win32", "x64") => "claude-agent-sdk-win32-x64",
        ("win32", "arm64") => "claude-agent-sdk-win32-arm64",
        _ => return None,
    })
}

fn which_on_path(name: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path)
        .map(|dir| dir.join(name))
        .find(|candidate| candidate.is_file())
}

fn truncate(text: &str, max: usize) -> &str {
    if text.len() <= max {
        return text;
    }
    match text.char_indices().nth(max) {
        Some((idx, _)) => &text[..idx],
        None => text,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_control_response_success_and_strips_redelivery_fields() {
        let frame = Frame::parse(
            &json!({
                "type": "control_response",
                "response": {
                    "subtype": "success",
                    "request_id": "req-1",
                    "response": {"still_queued": []},
                    "pending_permission_requests": [{"noise": true}]
                }
            })
            .to_string(),
        )
        .unwrap();
        match frame {
            Frame::ControlResponse {
                request_id,
                result: ControlOutcome::Success(payload),
            } => {
                assert_eq!(request_id, "req-1");
                assert_eq!(payload, json!({"still_queued": []}));
            }
            other => panic!("expected success response, got {other:?}"),
        }
    }

    #[test]
    fn parses_control_response_error() {
        let frame = Frame::parse(
            &json!({
                "type": "control_response",
                "response": {"subtype": "error", "request_id": "req-2", "error": "boom"}
            })
            .to_string(),
        )
        .unwrap();
        match frame {
            Frame::ControlResponse {
                result: ControlOutcome::Error(error),
                ..
            } => assert_eq!(error, "boom"),
            other => panic!("expected error response, got {other:?}"),
        }
    }

    #[test]
    fn parses_inbound_can_use_tool_request() {
        let frame = Frame::parse(
            &json!({
                "type": "control_request",
                "request_id": "req-3",
                "request": {
                    "subtype": "can_use_tool",
                    "tool_name": "Write",
                    "input": {"file_path": "/tmp/x"},
                    "tool_use_id": "toolu_1"
                }
            })
            .to_string(),
        )
        .unwrap();
        match frame {
            Frame::ControlRequest {
                request_id,
                request,
            } => {
                assert_eq!(request_id, "req-3");
                assert_eq!(request["subtype"], "can_use_tool");
                assert_eq!(request["tool_name"], "Write");
            }
            other => panic!("expected control request, got {other:?}"),
        }
    }

    #[test]
    fn routes_sdk_messages_and_ignores_transport_noise() {
        assert!(matches!(
            Frame::parse(&json!({"type": "assistant"}).to_string()).unwrap(),
            Frame::Message(_)
        ));
        assert!(matches!(
            Frame::parse(&json!({"type": "keep_alive"}).to_string()).unwrap(),
            Frame::Ignored
        ));
        // Unknown frame types must not be fatal.
        assert!(matches!(
            Frame::parse(&json!({"type": "transcript_mirror"}).to_string()).unwrap(),
            Frame::Ignored
        ));
    }

    #[test]
    fn native_results_only_reject_explicitly_mismatched_input_ids() {
        let (input_id, message) = user_message_with_id("hello");
        assert_eq!(message["uuid"], input_id);
        let input_ids = HashSet::from([input_id.clone()]);

        assert!(message_belongs_to_turn(
            &json!({
                "type": "result",
                "subtype": "success",
                "user_message_uuid": input_id,
            }),
            &input_ids,
        ));
        assert!(!message_belongs_to_turn(
            &json!({
                "type": "result",
                "subtype": "success",
                "user_message_uuid": "queued-task-notification",
            }),
            &input_ids,
        ));
        assert!(message_belongs_to_turn(
            &json!({"type": "result", "subtype": "success", "result": ""}),
            &input_ids,
        ));
    }

    #[test]
    fn native_turn_waits_for_background_follow_up_result() {
        let input_ids = HashSet::from(["prompt-id".to_string()]);
        let mut boundary = TurnMessageBoundary::default();

        assert_eq!(
            boundary.classify(
                &json!({
                    "type": "system",
                    "subtype": "background_tasks_changed",
                    "tasks": [{"task_id": "task-1"}],
                }),
                &input_ids,
            ),
            TurnMessageAction::Forward,
        );
        assert_eq!(
            boundary.classify(
                &json!({
                    "type": "result",
                    "subtype": "success",
                    "user_message_uuid": "prompt-id",
                }),
                &input_ids,
            ),
            TurnMessageAction::Suppress,
        );
        assert_eq!(
            boundary.classify(
                &json!({
                    "type": "system",
                    "subtype": "background_tasks_changed",
                    "tasks": [],
                }),
                &input_ids,
            ),
            TurnMessageAction::Forward,
        );
        assert_eq!(
            boundary.classify(
                &json!({
                    "type": "result",
                    "subtype": "success",
                    "user_message_uuid": "task-notification-id",
                }),
                &input_ids,
            ),
            TurnMessageAction::Terminal,
        );

        let mut fast_boundary = TurnMessageBoundary::default();
        assert_eq!(
            fast_boundary.classify(
                &json!({
                    "type": "system",
                    "subtype": "background_tasks_changed",
                    "tasks": [{"task_id": "quick"}],
                }),
                &input_ids,
            ),
            TurnMessageAction::Forward,
        );
        assert_eq!(
            fast_boundary.classify(
                &json!({
                    "type": "system",
                    "subtype": "background_tasks_changed",
                    "tasks": [],
                }),
                &input_ids,
            ),
            TurnMessageAction::Forward,
        );
        assert_eq!(
            fast_boundary.classify(
                &json!({
                    "type": "result",
                    "subtype": "success",
                    "user_message_uuid": "prompt-id",
                }),
                &input_ids,
            ),
            TurnMessageAction::Suppress,
        );
        assert_eq!(
            fast_boundary.classify(
                &json!({
                    "type": "result",
                    "subtype": "success",
                    "result": "done",
                }),
                &input_ids,
            ),
            TurnMessageAction::Terminal,
        );
    }

    #[test]
    fn cancel_frames_carry_the_request_id() {
        match Frame::parse(
            &json!({"type": "control_cancel_request", "request_id": "req-4"}).to_string(),
        )
        .unwrap()
        {
            Frame::ControlCancel { request_id } => assert_eq!(request_id, "req-4"),
            other => panic!("expected cancel, got {other:?}"),
        }
    }

    #[test]
    fn response_helpers_match_the_wire_shape() {
        assert_eq!(
            control_response_success("req-5", json!({"behavior": "allow"})),
            json!({
                "type": "control_response",
                "response": {
                    "subtype": "success",
                    "request_id": "req-5",
                    "response": {"behavior": "allow"}
                }
            })
        );
        assert_eq!(
            control_response_error("req-6", "nope")["response"]["subtype"],
            "error"
        );
    }

    #[test]
    fn platform_package_name_is_known_for_this_target() {
        // Guards against a silent PATH-only fallback on tier-1 targets.
        if matches!(std::env::consts::ARCH, "x86_64" | "aarch64")
            && matches!(std::env::consts::OS, "linux" | "macos" | "windows")
        {
            assert!(npm_platform_package_name().is_some());
        }
    }
}

#[cfg(test)]
mod live_tests {
    use super::*;

    /// Resolution must find a real binary and accept its declared schema.
    /// Ignored by default: depends on a local Claude Code install.
    #[test]
    #[ignore = "requires a local claude binary"]
    fn resolves_a_real_binary() {
        let resolved = resolve_claude_binary().expect("claude binary should resolve");
        assert!(resolved.path.is_file(), "{:?} is not a file", resolved.path);
        eprintln!(
            "resolved {} (harnessSchema {:?})",
            resolved.path.display(),
            resolved.harness_schema
        );
    }

    #[test]
    #[ignore = "requires a local claude binary"]
    fn rejects_a_missing_explicit_override() {
        // Safety: single-threaded ignored test, no concurrent env readers.
        unsafe { std::env::set_var("BORG_CLAUDE_BIN", "/nonexistent/claude") };
        let error = resolve_claude_binary().unwrap_err().to_string();
        unsafe { std::env::remove_var("BORG_CLAUDE_BIN") };
        assert!(error.contains("does not exist"), "{error}");
    }
}

// ---------------------------------------------------------------------------
// Control channel
// ---------------------------------------------------------------------------

use std::collections::HashMap;

use super::{ChatApprovalDecision, ChatStreamEvent};

/// Translates between Borg's `ChatStreamEvent` / `ChatStreamControl` vocabulary
/// and the CLI's control frames.
///
/// Deliberately I/O-free: every method returns the frames to write rather than
/// writing them, so the protocol logic is unit-testable without a subprocess.
/// The runner owns stdin and the actual byte pushing.
#[derive(Debug, Default)]
pub(super) struct ControlChannel {
    /// Inbound `can_use_tool` requests awaiting a user decision. Holds the
    /// `permission_suggestions` needed to build `updatedPermissions` on a
    /// session-scoped approval, and the original `input` so an allow can echo
    /// it back as `updatedInput`.
    pending_approvals: HashMap<String, PendingApproval>,
    /// Inbound elicitations awaiting a response.
    pending_interactions: HashSet<String>,
    /// Outbound requests awaiting a `control_response`, by `request_id`.
    pending_requests: HashMap<String, OutboundKind>,
}

#[derive(Debug, Clone)]
struct PendingApproval {
    tool_use_id: Option<String>,
    input: Value,
    suggestions: Option<Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OutboundKind {
    Initialize,
    Interrupt,
    StopTask,
    ContextUsage,
}

/// What the runner should do with an inbound frame.
#[derive(Debug)]
pub(super) enum Inbound {
    /// Surface to the caller; no immediate reply.
    Event(ChatStreamEvent),
    /// Reply immediately with this frame.
    Reply(Value),
    /// A reply to something we sent.
    Response {
        kind: OutboundKind,
        result: ControlOutcome,
    },
    Nothing,
}

impl ControlChannel {
    pub(super) fn new() -> Self {
        Self::default()
    }

    /// Register an outbound request so its response can be correlated.
    pub(super) fn begin_request(&mut self, request_id: &str, kind: OutboundKind) {
        self.pending_requests.insert(request_id.to_string(), kind);
    }

    pub(super) fn handle_frame(&mut self, frame: Frame) -> Inbound {
        match frame {
            Frame::ControlRequest {
                request_id,
                request,
            } => self.handle_inbound_request(request_id, &request),
            Frame::ControlResponse { request_id, result } => {
                match self.pending_requests.remove(&request_id) {
                    Some(kind) => Inbound::Response { kind, result },
                    // A response we never asked for, or a duplicate. The SDK
                    // parks these; we have no use for one.
                    None => Inbound::Nothing,
                }
            }
            Frame::ControlCancel { request_id } => {
                // Withdrawn: drop the pending entry and send nothing. Replying
                // to a cancelled request is a protocol error.
                self.pending_approvals.remove(&request_id);
                self.pending_interactions.remove(&request_id);
                Inbound::Nothing
            }
            Frame::Message(_) | Frame::Ignored => Inbound::Nothing,
        }
    }

    fn handle_inbound_request(&mut self, request_id: String, request: &Value) -> Inbound {
        let subtype = request
            .get("subtype")
            .and_then(Value::as_str)
            .unwrap_or_default();
        match subtype {
            "can_use_tool" => {
                let tool_name = request
                    .get("tool_name")
                    .and_then(Value::as_str)
                    .unwrap_or("a tool");
                let input = request.get("input").cloned().unwrap_or(Value::Null);
                self.pending_approvals.insert(
                    request_id.clone(),
                    PendingApproval {
                        tool_use_id: request
                            .get("tool_use_id")
                            .and_then(Value::as_str)
                            .map(str::to_string),
                        input: input.clone(),
                        suggestions: request.get("permission_suggestions").cloned(),
                    },
                );
                // Title/detail fall back in the same order the sidecar used, so
                // the approval UI is unchanged by the port.
                let title = first_str(request, &["title", "display_name"])
                    .unwrap_or_else(|| format!("Use {tool_name}"));
                let detail = first_str(request, &["description", "decision_reason"])
                    .unwrap_or_else(|| format!("Claude requested permission to use {tool_name}."));
                let command = (tool_name == "Bash")
                    .then(|| input.get("command").and_then(Value::as_str))
                    .flatten()
                    .map(str::to_string);
                Inbound::Event(ChatStreamEvent::ApprovalRequested {
                    approval_id: request_id,
                    title,
                    detail,
                    command,
                })
            }
            "elicitation" => {
                let server = request
                    .get("mcp_server_name")
                    .and_then(Value::as_str)
                    .unwrap_or("An MCP server");
                // elicitation_id is optional on the wire, but we must reply on
                // the request_id, so that is what we key and surface.
                self.pending_interactions.insert(request_id.clone());
                let title = first_str(request, &["title", "display_name"])
                    .unwrap_or_else(|| format!("{server} requests input"));
                let detail = first_str(request, &["description", "message"])
                    .unwrap_or_else(|| "Claude needs additional input.".to_string());
                Inbound::Event(ChatStreamEvent::ProviderInteractionRequested {
                    interaction_id: request_id,
                    kind: "mcp_elicitation".to_string(),
                    title,
                    detail,
                    payload: request.clone(),
                })
            }
            // Every inbound request must be answered or the turn hangs. We do
            // not implement hooks or SDK MCP servers, so decline explicitly
            // rather than leaving the CLI waiting.
            other => Inbound::Reply(control_response_error(
                &request_id,
                &format!("borg does not implement control request '{other}'"),
            )),
        }
    }

    /// Build the reply for a user's approval decision. Returns `None` if the
    /// approval is unknown (already cancelled, or a duplicate decision).
    pub(super) fn approval_reply(
        &mut self,
        approval_id: &str,
        decision: ChatApprovalDecision,
    ) -> Option<Value> {
        let pending = self.pending_approvals.remove(approval_id)?;
        let payload = match decision {
            ChatApprovalDecision::Reject => serde_json::json!({
                "behavior": "deny",
                "message": "User denied this action.",
            }),
            ChatApprovalDecision::ApproveOnce | ChatApprovalDecision::ApproveSession => {
                let mut allow = serde_json::json!({
                    "behavior": "allow",
                    // Echo the input back. The sidecar never set this, so
                    // "approve with edits" was inexpressible; threading it
                    // through keeps that door open.
                    "updatedInput": pending.input,
                });
                if decision == ChatApprovalDecision::ApproveSession
                    && let Some(suggestions) = pending.suggestions
                {
                    allow["updatedPermissions"] = suggestions;
                }
                allow
            }
        };
        let mut payload = payload;
        if let Some(tool_use_id) = pending.tool_use_id {
            payload["toolUseID"] = Value::String(tool_use_id);
        }
        Some(control_response_success(approval_id, payload))
    }

    /// Build the reply for an elicitation response.
    pub(super) fn interaction_reply(
        &mut self,
        interaction_id: &str,
        response: Value,
    ) -> Option<Value> {
        if !self.pending_interactions.remove(interaction_id) {
            return None;
        }
        Some(control_response_success(interaction_id, response))
    }

    /// Deny everything still outstanding. Called when the child is going away,
    /// so a UI waiting on an approval does not hang forever.
    pub(super) fn drain_pending(&mut self) -> Vec<Value> {
        let mut frames = Vec::new();
        for approval_id in self.pending_approvals.keys().cloned().collect::<Vec<_>>() {
            self.pending_approvals.remove(&approval_id);
            frames.push(control_response_success(
                &approval_id,
                serde_json::json!({
                    "behavior": "deny",
                    "message": "Claude session ended before permission was decided.",
                }),
            ));
        }
        for interaction_id in self
            .pending_interactions
            .iter()
            .cloned()
            .collect::<Vec<_>>()
        {
            self.pending_interactions.remove(&interaction_id);
            frames.push(control_response_success(
                &interaction_id,
                serde_json::json!({"action": "cancel"}),
            ));
        }
        frames
    }

    pub(super) fn has_pending_approval(&self, approval_id: &str) -> bool {
        self.pending_approvals.contains_key(approval_id)
    }
}

fn first_str(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        value
            .get(*key)
            .and_then(Value::as_str)
            .filter(|text| !text.trim().is_empty())
            .map(str::to_string)
    })
}

/// A user message line on stdin — the same envelope the sidecar built.
pub(super) fn user_message(text: &str) -> Value {
    user_message_with_id(text).1
}

fn user_message_with_id(text: &str) -> (String, Value) {
    let id = uuid::Uuid::new_v4().to_string();
    let message = serde_json::json!({
        "type": "user",
        "message": {"role": "user", "content": [{"type": "text", "text": text}]},
        "parent_tool_use_id": Value::Null,
        "session_id": "",
        "uuid": id,
    });
    (id, message)
}

fn message_belongs_to_turn(value: &Value, input_ids: &HashSet<String>) -> bool {
    if value.get("type").and_then(Value::as_str) != Some("result") {
        return true;
    }
    value
        .get("user_message_uuid")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .is_none_or(|id| input_ids.contains(id))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TurnMessageAction {
    Forward,
    Suppress,
    Terminal,
}

#[derive(Debug, Default)]
struct TurnMessageBoundary {
    live_background_tasks: HashSet<String>,
    observed_background_work: bool,
    awaiting_post_background_result: bool,
}

impl TurnMessageBoundary {
    fn background_task_ids(&self) -> Vec<String> {
        self.live_background_tasks.iter().cloned().collect()
    }

    fn classify(&mut self, value: &Value, input_ids: &HashSet<String>) -> TurnMessageAction {
        if value.get("type").and_then(Value::as_str) == Some("system")
            && value.get("subtype").and_then(Value::as_str) == Some("background_tasks_changed")
        {
            self.live_background_tasks.clear();
            if let Some(tasks) = value.get("tasks").and_then(Value::as_array) {
                self.live_background_tasks
                    .extend(tasks.iter().filter_map(|task| {
                        task.get("task_id")
                            .and_then(Value::as_str)
                            .map(str::to_string)
                    }));
            }
            if !self.live_background_tasks.is_empty() {
                self.observed_background_work = true;
            }
            return TurnMessageAction::Forward;
        }

        if value.get("type").and_then(Value::as_str) != Some("result") {
            return TurnMessageAction::Forward;
        }

        if self.awaiting_post_background_result && self.live_background_tasks.is_empty() {
            // Result correlation is optional. After withholding a foreground
            // result, the next result at an empty live-task level is the
            // post-notification terminal even when the SDK omits its UUID.
            self.awaiting_post_background_result = false;
            return TurnMessageAction::Terminal;
        }

        if !message_belongs_to_turn(value, input_ids) {
            // A completion notification is an internal user message with its
            // own UUID. Its follow-up result is valid only when this Borg turn
            // previously withheld a result while background work was live.
            return TurnMessageAction::Suppress;
        }

        if self.observed_background_work {
            self.awaiting_post_background_result = true;
            return TurnMessageAction::Suppress;
        }

        self.awaiting_post_background_result = false;
        TurnMessageAction::Terminal
    }
}

/// `initialize` handshake payload. `systemPrompt` is an array on the wire.
pub(super) fn initialize_request(system_prompt: Option<&str>) -> Value {
    let mut request = serde_json::json!({"subtype": "initialize"});
    if let Some(prompt) = system_prompt.filter(|text| !text.trim().is_empty()) {
        request["systemPrompt"] = Value::Array(vec![Value::String(prompt.to_string())]);
    }
    request
}

#[cfg(test)]
mod control_tests {
    use super::*;
    use serde_json::json;

    fn can_use_tool(request_id: &str, tool: &str, input: Value) -> Frame {
        Frame::parse(
            &json!({
                "type": "control_request",
                "request_id": request_id,
                "request": {
                    "subtype": "can_use_tool",
                    "tool_name": tool,
                    "input": input,
                    "tool_use_id": "toolu_x",
                    // The real binary sends these; a fixture without them
                    // silently exercises only the fallback path.
                    "display_name": tool,
                    "description": "Claude wants to run this",
                    "permission_suggestions": [{"type": "addRules", "rules": ["Bash(ls:*)"]}]
                }
            })
            .to_string(),
        )
        .unwrap()
    }

    #[test]
    fn can_use_tool_becomes_an_approval_request_with_bash_command() {
        let mut channel = ControlChannel::new();
        let inbound = channel.handle_frame(can_use_tool(
            "req-1",
            "Bash",
            json!({"command": "rm -rf /tmp/x"}),
        ));
        match inbound {
            Inbound::Event(ChatStreamEvent::ApprovalRequested {
                approval_id,
                command,
                title,
                ..
            }) => {
                assert_eq!(approval_id, "req-1");
                // The approval id is the control request_id — that is what we
                // must reply on.
                assert_eq!(command.as_deref(), Some("rm -rf /tmp/x"));
                assert_eq!(title, "Bash", "display_name wins when present");
            }
            other => panic!("expected approval request, got {other:?}"),
        }
        assert!(channel.has_pending_approval("req-1"));
    }

    #[test]
    fn command_is_only_extracted_for_bash() {
        let mut channel = ControlChannel::new();
        let inbound = channel.handle_frame(can_use_tool(
            "req-2",
            "Write",
            json!({"command": "not-bash"}),
        ));
        match inbound {
            Inbound::Event(ChatStreamEvent::ApprovalRequested { command, .. }) => {
                assert!(
                    command.is_none(),
                    "non-Bash tools must not surface a command"
                );
            }
            other => panic!("expected approval request, got {other:?}"),
        }
    }

    #[test]
    fn approve_once_allows_without_session_permissions() {
        let mut channel = ControlChannel::new();
        channel.handle_frame(can_use_tool(
            "req-3",
            "Write",
            json!({"file_path": "/tmp/a"}),
        ));
        let reply = channel
            .approval_reply("req-3", ChatApprovalDecision::ApproveOnce)
            .expect("pending approval should exist");
        let payload = &reply["response"]["response"];
        assert_eq!(payload["behavior"], "allow");
        assert_eq!(payload["updatedInput"], json!({"file_path": "/tmp/a"}));
        assert_eq!(payload["toolUseID"], "toolu_x");
        assert!(payload.get("updatedPermissions").is_none());
    }

    #[test]
    fn approve_session_carries_the_permission_suggestions() {
        let mut channel = ControlChannel::new();
        channel.handle_frame(can_use_tool("req-4", "Bash", json!({"command": "ls"})));
        let reply = channel
            .approval_reply("req-4", ChatApprovalDecision::ApproveSession)
            .unwrap();
        assert_eq!(
            reply["response"]["response"]["updatedPermissions"],
            json!([{"type": "addRules", "rules": ["Bash(ls:*)"]}])
        );
    }

    #[test]
    fn reject_denies() {
        let mut channel = ControlChannel::new();
        channel.handle_frame(can_use_tool("req-5", "Bash", json!({"command": "ls"})));
        let reply = channel
            .approval_reply("req-5", ChatApprovalDecision::Reject)
            .unwrap();
        assert_eq!(reply["response"]["response"]["behavior"], "deny");
    }

    #[test]
    fn a_decision_is_only_answered_once() {
        let mut channel = ControlChannel::new();
        channel.handle_frame(can_use_tool("req-6", "Bash", json!({"command": "ls"})));
        assert!(
            channel
                .approval_reply("req-6", ChatApprovalDecision::ApproveOnce)
                .is_some()
        );
        // A duplicate decision must not produce a second control_response;
        // the CLI treats an unmatched reply as a protocol error.
        assert!(
            channel
                .approval_reply("req-6", ChatApprovalDecision::ApproveOnce)
                .is_none()
        );
    }

    #[test]
    fn cancel_withdraws_a_pending_approval_without_replying() {
        let mut channel = ControlChannel::new();
        channel.handle_frame(can_use_tool("req-7", "Bash", json!({"command": "ls"})));
        let inbound = channel.handle_frame(
            Frame::parse(
                &json!({"type": "control_cancel_request", "request_id": "req-7"}).to_string(),
            )
            .unwrap(),
        );
        assert!(matches!(inbound, Inbound::Nothing));
        assert!(!channel.has_pending_approval("req-7"));
        assert!(
            channel
                .approval_reply("req-7", ChatApprovalDecision::ApproveOnce)
                .is_none(),
            "a cancelled request must never be answered"
        );
    }

    #[test]
    fn elicitation_becomes_a_provider_interaction() {
        let mut channel = ControlChannel::new();
        let inbound = channel.handle_frame(
            Frame::parse(
                &json!({
                    "type": "control_request",
                    "request_id": "req-8",
                    "request": {
                        "subtype": "elicitation",
                        "mcp_server_name": "github",
                        "message": "Authorize?"
                    }
                })
                .to_string(),
            )
            .unwrap(),
        );
        match inbound {
            Inbound::Event(ChatStreamEvent::ProviderInteractionRequested {
                interaction_id,
                kind,
                title,
                detail,
                ..
            }) => {
                assert_eq!(interaction_id, "req-8");
                assert_eq!(kind, "mcp_elicitation");
                assert_eq!(title, "github requests input");
                assert_eq!(detail, "Authorize?");
            }
            other => panic!("expected interaction, got {other:?}"),
        }
        let reply = channel
            .interaction_reply("req-8", json!({"action": "accept"}))
            .unwrap();
        assert_eq!(reply["response"]["response"]["action"], "accept");
    }

    #[test]
    fn unimplemented_subtypes_are_declined_not_ignored() {
        let mut channel = ControlChannel::new();
        let inbound = channel.handle_frame(
            Frame::parse(
                &json!({
                    "type": "control_request",
                    "request_id": "req-9",
                    "request": {"subtype": "hook_callback", "callback_id": "hook_1"}
                })
                .to_string(),
            )
            .unwrap(),
        );
        // Silence would hang the turn — the CLI blocks until answered.
        match inbound {
            Inbound::Reply(reply) => {
                assert_eq!(reply["response"]["subtype"], "error");
                assert_eq!(reply["response"]["request_id"], "req-9");
            }
            other => panic!("expected an error reply, got {other:?}"),
        }
    }

    #[test]
    fn outbound_responses_are_correlated_by_request_id() {
        let mut channel = ControlChannel::new();
        channel.begin_request("out-1", OutboundKind::Interrupt);
        let inbound = channel.handle_frame(
            Frame::parse(
                &json!({
                    "type": "control_response",
                    "response": {
                        "subtype": "success",
                        "request_id": "out-1",
                        "response": {"still_queued": ["a"]}
                    }
                })
                .to_string(),
            )
            .unwrap(),
        );
        match inbound {
            Inbound::Response {
                kind: OutboundKind::Interrupt,
                result: ControlOutcome::Success(payload),
            } => assert_eq!(payload["still_queued"][0], "a"),
            other => panic!("expected correlated response, got {other:?}"),
        }
        // Unsolicited or duplicate responses are dropped.
        assert!(matches!(
            channel.handle_frame(
                Frame::parse(
                    &json!({
                        "type": "control_response",
                        "response": {"subtype": "success", "request_id": "out-1", "response": {}}
                    })
                    .to_string()
                )
                .unwrap()
            ),
            Inbound::Nothing
        ));
    }

    #[test]
    fn shutdown_answers_everything_still_outstanding() {
        let mut channel = ControlChannel::new();
        channel.handle_frame(can_use_tool("req-10", "Bash", json!({"command": "ls"})));
        channel.handle_frame(
            Frame::parse(
                &json!({
                    "type": "control_request",
                    "request_id": "req-11",
                    "request": {"subtype": "elicitation", "mcp_server_name": "x", "message": "?"}
                })
                .to_string(),
            )
            .unwrap(),
        );
        let frames = channel.drain_pending();
        assert_eq!(frames.len(), 2, "a hung UI is worse than a denial");
        let behaviors: Vec<_> = frames
            .iter()
            .map(|frame| frame["response"]["response"].clone())
            .collect();
        assert!(
            behaviors
                .iter()
                .any(|payload| payload["behavior"] == "deny")
        );
        assert!(
            behaviors
                .iter()
                .any(|payload| payload["action"] == "cancel")
        );
        assert!(channel.drain_pending().is_empty());
    }

    #[test]
    fn initialize_wraps_the_system_prompt_in_an_array() {
        assert_eq!(
            initialize_request(Some("be terse")),
            json!({"subtype": "initialize", "systemPrompt": ["be terse"]})
        );
        // An empty prompt must not send an empty array — the CLI would treat
        // it as an override of the default system prompt.
        assert_eq!(
            initialize_request(Some("  ")),
            json!({"subtype": "initialize"})
        );
        assert_eq!(initialize_request(None), json!({"subtype": "initialize"}));
    }
}

/// End-to-end checks that the control channel drives the real `claude` binary.
///
/// Ignored by default: requires a local Claude Code install with credentials,
/// makes a real API call, and costs money. Run with
/// `cargo test -p borg-provider claude_native::live_control -- --ignored --nocapture`.
#[cfg(test)]
mod live_control {
    use super::*;
    use std::process::Stdio;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::process::Command;

    async fn write_frame(stdin: &mut tokio::process::ChildStdin, value: &Value) {
        stdin
            .write_all(&serde_json::to_vec(value).unwrap())
            .await
            .unwrap();
        stdin.write_all(b"\n").await.unwrap();
        stdin.flush().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "spawns the real claude binary and makes a paid API call"]
    async fn approves_a_gated_tool_through_the_control_channel() {
        let binary = resolve_claude_binary().expect("claude binary");
        let workspace = tempfile::tempdir().unwrap();
        let target = workspace.path().join("native-ok.txt");

        let mut command = Command::new(&binary.path);
        command
            .args(BASE_ARGS)
            .args(STDIO_PERMISSION_ARGS)
            .args(["--permission-mode", "default"])
            .current_dir(workspace.path())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command.spawn().expect("spawn claude");
        let mut stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();

        let mut channel = ControlChannel::new();

        let init_id = "borg-init-live";
        channel.begin_request(init_id, OutboundKind::Initialize);
        write_frame(
            &mut stdin,
            &serde_json::to_value(OutboundControlRequest::new(
                init_id,
                initialize_request(Some("You are a terse test harness.")),
            ))
            .unwrap(),
        )
        .await;

        write_frame(
            &mut stdin,
            &user_message(&format!(
                "Use the Write tool to create the file {} containing exactly: native-ok",
                target.display()
            )),
        )
        .await;

        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        let mut saw_init = false;
        let mut approved_tool: Option<String> = None;
        let mut result_subtype: Option<String> = None;

        loop {
            line.clear();
            let read = tokio::time::timeout(
                std::time::Duration::from_secs(180),
                reader.read_line(&mut line),
            )
            .await
            .expect("claude went quiet")
            .expect("read stdout");
            if read == 0 {
                break;
            }
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let frame = Frame::parse(trimmed).expect("frame parses");
            if let Frame::Message(value) = &frame
                && value.get("type").and_then(Value::as_str) == Some("result")
            {
                result_subtype = value
                    .get("subtype")
                    .and_then(Value::as_str)
                    .map(str::to_string);
                break;
            }
            match channel.handle_frame(frame) {
                Inbound::Response {
                    kind: OutboundKind::Initialize,
                    result: ControlOutcome::Success(payload),
                } => {
                    // The handshake response carries the session's model and
                    // account; a bare `{}` would mean we mis-sent it.
                    assert!(payload.get("models").is_some(), "init payload: {payload}");
                    saw_init = true;
                }
                Inbound::Event(ChatStreamEvent::ApprovalRequested {
                    approval_id, title, ..
                }) => {
                    approved_tool = Some(title);
                    let reply = channel
                        .approval_reply(&approval_id, ChatApprovalDecision::ApproveOnce)
                        .expect("pending approval");
                    write_frame(&mut stdin, &reply).await;
                }
                Inbound::Reply(reply) => write_frame(&mut stdin, &reply).await,
                _ => {}
            }
        }

        let _ = child.kill().await;

        assert!(saw_init, "initialize handshake was never acknowledged");
        // The real binary supplies `display_name`, so the title comes from the
        // first rung of the fallback chain, not the synthesized "Use <tool>".
        assert_eq!(
            approved_tool.as_deref(),
            Some("Write"),
            "the gated Write tool never produced a can_use_tool request"
        );
        assert_eq!(result_subtype.as_deref(), Some("success"));
        let written = std::fs::read_to_string(&target).expect("tool actually ran");
        assert!(written.contains("native-ok"), "wrote: {written}");
    }
}

#[cfg(test)]
mod fallback_tests {
    use super::*;
    use serde_json::json;

    /// Verified against the live binary: `display_name` and `description` are
    /// normally present. These cover the degraded frames, which is what the
    /// synthesized wording exists for.
    #[test]
    fn bare_can_use_tool_falls_back_to_synthesized_wording() {
        let mut channel = ControlChannel::new();
        let inbound = channel.handle_frame(
            Frame::parse(
                &json!({
                    "type": "control_request",
                    "request_id": "req-bare",
                    "request": {
                        "subtype": "can_use_tool",
                        "tool_name": "Edit",
                        "input": {}
                    }
                })
                .to_string(),
            )
            .unwrap(),
        );
        match inbound {
            Inbound::Event(ChatStreamEvent::ApprovalRequested { title, detail, .. }) => {
                assert_eq!(title, "Use Edit");
                assert_eq!(detail, "Claude requested permission to use Edit.");
            }
            other => panic!("expected approval, got {other:?}"),
        }
    }

    #[test]
    fn blank_display_fields_do_not_win_over_the_fallback() {
        let mut channel = ControlChannel::new();
        let inbound = channel.handle_frame(
            Frame::parse(
                &json!({
                    "type": "control_request",
                    "request_id": "req-blank",
                    "request": {
                        "subtype": "can_use_tool",
                        "tool_name": "Edit",
                        "input": {},
                        "display_name": "   ",
                        "description": ""
                    }
                })
                .to_string(),
            )
            .unwrap(),
        );
        match inbound {
            Inbound::Event(ChatStreamEvent::ApprovalRequested { title, detail, .. }) => {
                assert_eq!(title, "Use Edit");
                assert_eq!(detail, "Claude requested permission to use Edit.");
            }
            other => panic!("expected approval, got {other:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Runner
// ---------------------------------------------------------------------------

use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;

use super::claude_stream::ClaudeStreamState;
use super::{
    ChatStreamControl, ChatStreamRequest, LocalAgentPermission, ProviderAuthProvider,
    ProviderCallUsage, classify_claude_provider_event, elapsed_millis_u64, is_nonempty_session_id,
    prepare_git_credential_env, prepare_request_mcp, read_mcp_servers_from_config, restore_bundle,
    summarize_claude_provider_event,
};

/// True when the native path should be used instead of the Node sidecar.
pub(super) fn native_enabled() -> bool {
    matches!(
        std::env::var("BORG_CLAUDE_NATIVE").as_deref(),
        Ok("1") | Ok("true")
    )
}

/// Mirrors the sidecar's attachment handling: local paths are appended to the
/// prompt as a list for the model's own tools to read.
fn prompt_text(prompt: &str, attachments: &[PathBuf]) -> String {
    if attachments.is_empty() {
        return prompt.to_string();
    }
    let list = attachments
        .iter()
        .map(|path| format!("- {}", path.display()))
        .collect::<Vec<_>>()
        .join("\n");
    format!("{prompt}\n\nAttached files:\n{list}")
}

pub(super) async fn run(
    req: ChatStreamRequest,
    tx: tokio::sync::mpsc::Sender<super::ChatStreamEvent>,
    mut controls: Option<tokio::sync::mpsc::Receiver<ChatStreamControl>>,
    local_auth: bool,
    permission: LocalAgentPermission,
) -> Result<()> {
    let started_at = std::time::Instant::now();
    let binary = resolve_claude_binary()?;
    let provider_home = tempfile::tempdir().context("failed to create Claude provider home")?;
    let workspace_dir = req
        .working_directory
        .clone()
        .unwrap_or_else(|| provider_home.path().to_path_buf());
    std::fs::create_dir_all(&workspace_dir).with_context(|| {
        format!(
            "failed to create Claude workspace directory {}",
            workspace_dir.display()
        )
    })?;

    if let Some(auth) = req.provider_auth.as_ref()
        && auth.provider == ProviderAuthProvider::Claude
    {
        restore_bundle(
            ProviderAuthProvider::Claude,
            &auth.bundle,
            provider_home.path(),
        )
        .context("failed to restore Claude provider auth bundle")?;
    }

    let mcp_setup = prepare_request_mcp(provider_home.path(), &req, local_auth)?;
    let mcp_servers = read_mcp_servers_from_config(mcp_setup.claude_config_path.as_deref())
        .context("failed to load Claude MCP server config")?;
    let git_env = prepare_git_credential_env(provider_home.path(), &req.git_credentials)
        .context("failed to prepare git credential helper")?;

    let mut command = Command::new(&binary.path);
    command.args(BASE_ARGS);
    // Always route permissions to us. The permission mode still decides
    // whether the CLI asks at all; this only says who it asks.
    command.args(STDIO_PERMISSION_ARGS);
    command.args([
        "--permission-mode",
        match permission {
            LocalAgentPermission::FullAccess => "bypassPermissions",
            LocalAgentPermission::Auto => "acceptEdits",
            LocalAgentPermission::Manual => "default",
        },
    ]);
    if matches!(permission, LocalAgentPermission::FullAccess) {
        command.arg("--allow-dangerously-skip-permissions");
    }
    command.arg("--include-partial-messages");

    let model = req
        .model
        .clone()
        .or_else(|| {
            (!local_auth)
                .then(|| super::default_model_for_backend("claude"))
                .flatten()
        })
        .filter(|model| !model.trim().is_empty());
    if let Some(model) = model {
        command.args(["--model", &model]);
    }
    let effort = req.effort.clone().unwrap_or_else(|| "medium".to_string());
    command.args(["--effort", &effort]);

    if let Some(schema) = req.output_schema.as_ref() {
        command.args(["--json-schema", &serde_json::to_string(schema)?]);
    }
    if let Some(servers) = mcp_servers {
        // Inline rather than a config path: the CLI accepts the same JSON the
        // sidecar handed the SDK.
        command.args([
            "--mcp-config",
            &serde_json::to_string(&serde_json::json!({"mcpServers": servers}))?,
        ]);
    }
    if !mcp_setup.allowed_tools.is_empty() {
        command.args(["--allowedTools", &mcp_setup.allowed_tools]);
    }
    if let Some(resume_id) = req.session_id.as_deref()
        && is_nonempty_session_id(resume_id)
    {
        // `--resume=<id>`, not a separate argv entry: the CLI parses this flag
        // in its `=` form only.
        command.arg(format!("--resume={resume_id}"));
    }
    if req.persist_session == Some(false) {
        command.arg("--no-session-persistence");
    }

    command
        .current_dir(&workspace_dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // Without this a dropped future leaves the child (and its MCP servers)
        // running and billing.
        .kill_on_drop(true);
    if !local_auth {
        command.env("HOME", provider_home.path());
    }
    super::apply_git_credential_env(&mut command, &git_env);
    // Matches the SDK's own entrypoint tagging; see docs/claude-native-protocol.md.
    command.env("CLAUDE_CODE_ENTRYPOINT", "sdk-rs");
    if std::env::var("ENABLE_TOOL_SEARCH").is_err() {
        command.env("ENABLE_TOOL_SEARCH", "auto:5");
    }
    if std::env::var_os("ANTHROPIC_API_KEY").is_none()
        && let Some(key) =
            crate::credentials::stored_api_key(crate::credentials::ApiKeyCredential::Anthropic)
    {
        command.env("ANTHROPIC_API_KEY", key);
    }
    super::apply_claude_channel_env(&mut command, req.provider_channel)?;
    crate::subprocess::isolate_async_process_from_terminal(&mut command);

    let mut child = command
        .spawn()
        .with_context(|| format!("failed to spawn {}", binary.path.display()))?;
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| anyhow!("claude stdin pipe missing"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("claude stdout pipe missing"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| anyhow!("claude stderr pipe missing"))?;

    let stderr_buf = std::sync::Arc::new(tokio::sync::Mutex::new(String::new()));
    {
        let stderr_buf = stderr_buf.clone();
        tokio::spawn(async move {
            let mut reader = BufReader::new(stderr);
            let mut buf = String::new();
            loop {
                buf.clear();
                match reader.read_line(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => stderr_buf.lock().await.push_str(&buf),
                }
            }
        });
    }

    let mut channel = ControlChannel::new();
    let mut state = ClaudeStreamState::default();

    // Handshake first, then the turn's prompt.
    let init_id = format!("borg-init-{}", uuid::Uuid::new_v4());
    channel.begin_request(&init_id, OutboundKind::Initialize);
    write_line(
        &mut stdin,
        &serde_json::to_value(OutboundControlRequest::new(
            &init_id,
            initialize_request(Some(&req.system_prompt)),
        ))?,
    )
    .await?;
    let (initial_input_id, initial_message) =
        user_message_with_id(&prompt_text(&req.prompt, &req.attachments));
    let mut input_ids = HashSet::from([initial_input_id]);
    write_line(&mut stdin, &initial_message).await?;

    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    let mut stream_ended = false;
    let mut turn_boundary = TurnMessageBoundary::default();

    while !stream_ended {
        line.clear();
        tokio::select! {
            // Biased so pending output is drained before new control input is
            // acted on; an approval decision is meaningless before the request
            // that prompted it has been read.
            biased;
            read = reader.read_line(&mut line) => {
                let read = read.context("failed reading claude stdout")?;
                if read == 0 {
                    break;
                }
                let trimmed = line.trim_end_matches(['\r', '\n']);
                if trimmed.is_empty() {
                    continue;
                }
                let frame = Frame::parse(trimmed)?;
                match frame {
                    Frame::Message(value) => {
                        let action = turn_boundary.classify(&value, &input_ids);
                        if action == TurnMessageAction::Suppress {
                            continue;
                        }
                        let telemetry = classify_claude_provider_event(&value);
                        let _ = tx
                            .send(super::ChatStreamEvent::ProviderEvent {
                                kind: format!(
                                    "claude.{}",
                                    value.get("type").and_then(Value::as_str).unwrap_or("message")
                                ),
                                payload: summarize_claude_provider_event(&value),
                                raw_payload: Some(value.clone()),
                                stream_channel: telemetry.stream_channel,
                                content_text: telemetry.content_text,
                                provider_item_id: telemetry.provider_item_id,
                                tool_use_id: telemetry.tool_use_id,
                                tool_name: telemetry.tool_name,
                            })
                            .await;
                        // Unchanged from the sidecar path: the message half of
                        // the protocol was already implemented here.
                        if state.handle_message(&value, &tx).await? {
                            stream_ended = true;
                        }
                        // The sidecar exited after each turn, closing stdout and
                        // ending the read loop for us. The CLI does not: in
                        // streaming-input mode it stays alive waiting for the
                        // next user message, so `result` is the only signal that
                        // this turn is over. Without this the read blocks forever.
                        if action == TurnMessageAction::Terminal {
                            stream_ended = true;
                        }
                    }
                    other => match channel.handle_frame(other) {
                        Inbound::Event(event) => {
                            if tx.send(event).await.is_err() {
                                stream_ended = true;
                            }
                        }
                        Inbound::Reply(reply) => write_line(&mut stdin, &reply).await?,
                        Inbound::Response { kind, result } => {
                            handle_control_response(kind, result, &tx).await;
                        }
                        Inbound::Nothing => {}
                    },
                }
            }
            control = super::receive_claude_control(&mut controls) => {
                match control {
                    Some(control) => {
                        if !apply_control(
                            control,
                            &mut channel,
                            &mut stdin,
                            &mut input_ids,
                            &turn_boundary.background_task_ids(),
                        )
                        .await?
                        {
                            stream_ended = true;
                        }
                    }
                    // The control channel closing is not a reason to stop
                    // reading; the turn still has output to deliver.
                    None => controls = None,
                }
            }
        }
    }

    // Never leave a UI blocked on a request the child will no longer answer.
    for frame in channel.drain_pending() {
        let _ = write_line(&mut stdin, &frame).await;
    }
    let _ = stdin.shutdown().await;

    let status = child.wait().await.context("failed waiting for claude")?;
    if state.emitted_failure {
        return Ok(());
    }
    if !status.success() {
        let stderr_text = stderr_buf.lock().await.clone();
        let trimmed = stderr_text.trim();
        let suffix = if trimmed.is_empty() {
            String::new()
        } else {
            format!(": {trimmed}")
        };
        let _ = tx
            .send(super::ChatStreamEvent::Failed {
                error: format!(
                    "claude exited with status {}{}",
                    status
                        .code()
                        .map(|code| code.to_string())
                        .unwrap_or_else(|| "?".into()),
                    suffix
                ),
            })
            .await;
        return Ok(());
    }

    let final_text = state
        .final_text
        .take()
        .unwrap_or_else(|| state.delta_accumulator.clone());
    let usage = Some(state.final_usage.unwrap_or_else(|| ProviderCallUsage {
        duration_ms: elapsed_millis_u64(started_at),
        ..ProviderCallUsage::default()
    }));
    let session_id = state.session_id.take();
    let _ = tx
        .send(super::ChatStreamEvent::Done {
            final_text,
            usage,
            session_id,
        })
        .await;
    Ok(())
}

async fn handle_control_response(
    kind: OutboundKind,
    result: ControlOutcome,
    tx: &tokio::sync::mpsc::Sender<super::ChatStreamEvent>,
) {
    match (kind, result) {
        (OutboundKind::ContextUsage, ControlOutcome::Success(payload)) => {
            // Same shape the sidecar synthesized so downstream consumers of
            // `claude.context_usage` are unaffected.
            let _ = tx
                .send(super::ChatStreamEvent::ProviderEvent {
                    kind: "claude.context_usage".to_string(),
                    payload: serde_json::json!({
                        "type": "borg_context_usage",
                        "total_tokens": payload.get("totalTokens"),
                        "context_window_tokens": payload.get("maxTokens"),
                        "raw_context_window_tokens": payload.get("rawMaxTokens"),
                        "model": payload.get("model"),
                        "categories": payload.get("categories"),
                    }),
                    raw_payload: Some(payload),
                    stream_channel: Some("usage".to_string()),
                    content_text: None,
                    provider_item_id: None,
                    tool_use_id: None,
                    tool_name: None,
                })
                .await;
        }
        (kind, ControlOutcome::Error(error)) => {
            // A failed control request is not fatal to the turn; the model is
            // still producing output.
            tracing::warn!(?kind, %error, "claude control request failed");
        }
        _ => {}
    }
}

/// Returns `false` when the stream should stop.
async fn apply_control(
    control: ChatStreamControl,
    channel: &mut ControlChannel,
    stdin: &mut tokio::process::ChildStdin,
    input_ids: &mut HashSet<String>,
    background_task_ids: &[String],
) -> Result<bool> {
    match control {
        ChatStreamControl::Steer {
            text,
            attachments,
            ack,
            ..
        } => {
            let (input_id, message) = user_message_with_id(&prompt_text(&text, &attachments));
            let result = write_line(stdin, &message).await;
            if result.is_ok() {
                input_ids.insert(input_id);
            }
            let reply = match &result {
                Ok(()) => Ok(()),
                Err(error) => Err(format!("{error:#}")),
            };
            let _ = ack.send(reply);
            result?;
        }
        ChatStreamControl::Approval {
            approval_id,
            decision,
        } => {
            // `None` means already cancelled or already decided. Writing a
            // second response for one request is a protocol error.
            if let Some(reply) = channel.approval_reply(&approval_id, decision) {
                write_line(stdin, &reply).await?;
            }
        }
        ChatStreamControl::ProviderInteractionResponse {
            interaction_id,
            response,
        } => {
            if let Some(reply) = channel.interaction_reply(&interaction_id, response) {
                write_line(stdin, &reply).await?;
            }
        }
        ChatStreamControl::Interrupt => {
            for task_id in background_task_ids {
                let request_id = format!("borg-stop-task-{}", uuid::Uuid::new_v4());
                channel.begin_request(&request_id, OutboundKind::StopTask);
                write_line(
                    stdin,
                    &serde_json::to_value(OutboundControlRequest::new(
                        &request_id,
                        serde_json::json!({"subtype": "stop_task", "task_id": task_id}),
                    ))?,
                )
                .await?;
            }
            let request_id = format!("borg-interrupt-{}", uuid::Uuid::new_v4());
            channel.begin_request(&request_id, OutboundKind::Interrupt);
            write_line(
                stdin,
                &serde_json::to_value(OutboundControlRequest::new(
                    &request_id,
                    // Stop means stop: also drop anything queued behind this
                    // turn rather than letting it run after the interrupt.
                    serde_json::json!({"subtype": "interrupt", "cancel_queued": true}),
                ))?,
            )
            .await?;
        }
    }
    Ok(true)
}

async fn write_line(stdin: &mut tokio::process::ChildStdin, value: &Value) -> Result<()> {
    stdin.write_all(&serde_json::to_vec(value)?).await?;
    stdin.write_all(b"\n").await?;
    stdin.flush().await?;
    Ok(())
}

/// End-to-end checks through the public provider entrypoint with the native
/// path selected. Ignored by default: real binary, real API calls.
#[cfg(test)]
mod live_runner {
    use super::*;
    use crate::ProviderChannel;
    use crate::provider::chat_stream::{
        ChatStreamEvent, LocalAgentPermission, run_claude_local_chat_stream,
    };

    fn request(prompt: &str, workspace: &Path) -> ChatStreamRequest {
        ChatStreamRequest {
            prompt: prompt.to_string(),
            attachments: Vec::new(),
            model: None,
            effort: Some("low".to_string()),
            fast: false,
            system_prompt: "You are a terse test harness.".to_string(),
            output_schema: None,
            mcp_owner_id: None,
            mcp_allowed_scopes: Vec::new(),
            mcp_user_id: None,
            mcp_external_servers: Vec::new(),
            mcp_api_token: None,
            provider_auth: None,
            git_credentials: Vec::new(),
            working_directory: Some(workspace.to_path_buf()),
            session_id: None,
            provider_channel: ProviderChannel::Direct,
            persist_session: Some(false),
            web_search_allowed: false,
            resume_unavailable_prompt: None,
        }
    }

    #[tokio::test]
    #[ignore = "spawns the real claude binary and makes a paid API call"]
    async fn native_runner_completes_a_turn_and_reports_a_session() {
        // Safety: `--test-threads=1` for the ignored set; no concurrent readers.
        unsafe { std::env::set_var("BORG_CLAUDE_NATIVE", "1") };
        assert!(native_enabled(), "flag must select the native path");

        let workspace = tempfile::tempdir().unwrap();
        let mut rx = run_claude_local_chat_stream(
            request("Reply with exactly: native-runner-ok", workspace.path()),
            None,
            // FullAccess so this exercises the bypass argv branch too.
            LocalAgentPermission::FullAccess,
        );

        let mut done: Option<(String, Option<String>)> = None;
        let mut failure: Option<String> = None;
        let mut saw_provider_event = false;
        while let Some(event) = tokio::time::timeout(std::time::Duration::from_secs(180), rx.recv())
            .await
            .expect("native runner went quiet")
        {
            match event {
                ChatStreamEvent::ProviderEvent { .. } => saw_provider_event = true,
                ChatStreamEvent::Done {
                    final_text,
                    session_id,
                    usage,
                } => {
                    assert!(usage.is_some(), "usage must be reported");
                    done = Some((final_text, session_id));
                    break;
                }
                ChatStreamEvent::Failed { error } => {
                    failure = Some(error);
                    break;
                }
                _ => {}
            }
        }
        unsafe { std::env::remove_var("BORG_CLAUDE_NATIVE") };

        assert!(failure.is_none(), "native run failed: {failure:?}");
        let (final_text, session_id) = done.expect("stream ended without Done");
        assert!(
            final_text.contains("native-runner-ok"),
            "unexpected final text: {final_text}"
        );
        // Resume depends on capturing the CLI's session id; without it the
        // next turn silently re-hydrates the whole conversation.
        let session_id = session_id.expect("session_id must be captured for resume");
        assert!(!session_id.trim().is_empty());
        assert!(saw_provider_event, "provider telemetry was not forwarded");
    }

    #[tokio::test]
    #[ignore = "spawns the real claude binary and makes a paid API call"]
    async fn native_runner_enforces_structured_output() {
        unsafe { std::env::set_var("BORG_CLAUDE_NATIVE", "1") };
        let workspace = tempfile::tempdir().unwrap();
        let mut req = request("Return the capital of France.", workspace.path());
        req.output_schema = Some(serde_json::json!({
            "type": "object",
            "properties": {"capital": {"type": "string"}},
            "required": ["capital"],
            "additionalProperties": false
        }));

        let mut rx = run_claude_local_chat_stream(req, None, LocalAgentPermission::FullAccess);
        let mut final_text = None;
        while let Some(event) = tokio::time::timeout(std::time::Duration::from_secs(180), rx.recv())
            .await
            .expect("native runner went quiet")
        {
            match event {
                ChatStreamEvent::Done {
                    final_text: text, ..
                } => {
                    final_text = Some(text);
                    break;
                }
                ChatStreamEvent::Failed { error } => panic!("structured run failed: {error}"),
                _ => {}
            }
        }
        unsafe { std::env::remove_var("BORG_CLAUDE_NATIVE") };

        let final_text = final_text.expect("stream ended without Done");
        // `structured_output` is authoritative — the free-text `result` field
        // is only a transcript, so Done must carry parsed JSON.
        let parsed: Value = serde_json::from_str(&final_text)
            .unwrap_or_else(|error| panic!("final text was not JSON ({error}): {final_text}"));
        assert!(
            parsed.get("capital").and_then(Value::as_str).is_some(),
            "schema not enforced: {parsed}"
        );
    }
}
