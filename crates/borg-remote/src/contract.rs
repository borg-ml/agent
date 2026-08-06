use std::path::PathBuf;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use ts_rs::TS;
use uuid::Uuid;

pub const REMOTE_PROTOCOL_VERSION: u16 = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum CodingProvider {
    Codex,
    Claude,
    OpenRouter,
    OpenAiCompatible,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum ResponseLanguage {
    #[default]
    #[serde(rename = "auto")]
    #[ts(rename = "auto")]
    Auto,
    #[serde(rename = "en")]
    #[ts(rename = "en")]
    English,
    #[serde(rename = "zh-Hans")]
    #[ts(rename = "zh-Hans")]
    SimplifiedChinese,
    #[serde(rename = "zh-Hant")]
    #[ts(rename = "zh-Hant")]
    TraditionalChinese,
    #[serde(rename = "es")]
    #[ts(rename = "es")]
    Spanish,
    #[serde(rename = "pt-BR")]
    #[ts(rename = "pt-BR")]
    PortugueseBrazil,
    #[serde(rename = "fr")]
    #[ts(rename = "fr")]
    French,
    #[serde(rename = "de")]
    #[ts(rename = "de")]
    German,
    #[serde(rename = "ja")]
    #[ts(rename = "ja")]
    Japanese,
    #[serde(rename = "ko")]
    #[ts(rename = "ko")]
    Korean,
    #[serde(rename = "ru")]
    #[ts(rename = "ru")]
    Russian,
    #[serde(rename = "ar")]
    #[ts(rename = "ar")]
    Arabic,
}

impl ResponseLanguage {
    pub const ALL: [Self; 12] = [
        Self::Auto,
        Self::English,
        Self::SimplifiedChinese,
        Self::TraditionalChinese,
        Self::Spanish,
        Self::PortugueseBrazil,
        Self::French,
        Self::German,
        Self::Japanese,
        Self::Korean,
        Self::Russian,
        Self::Arabic,
    ];

    pub const fn code(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::English => "en",
            Self::SimplifiedChinese => "zh-Hans",
            Self::TraditionalChinese => "zh-Hant",
            Self::Spanish => "es",
            Self::PortugueseBrazil => "pt-BR",
            Self::French => "fr",
            Self::German => "de",
            Self::Japanese => "ja",
            Self::Korean => "ko",
            Self::Russian => "ru",
            Self::Arabic => "ar",
        }
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::Auto => "Auto",
            Self::English => "English",
            Self::SimplifiedChinese => "Simplified Chinese",
            Self::TraditionalChinese => "Traditional Chinese",
            Self::Spanish => "Spanish",
            Self::PortugueseBrazil => "Portuguese (Brazil)",
            Self::French => "French",
            Self::German => "German",
            Self::Japanese => "Japanese",
            Self::Korean => "Korean",
            Self::Russian => "Russian",
            Self::Arabic => "Arabic",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        let normalized = value
            .trim()
            .to_lowercase()
            .replace(['_', '-', '(', ')'], " ");
        match normalized
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .as_str()
        {
            "auto" => Some(Self::Auto),
            "en" | "english" => Some(Self::English),
            "zh hans" | "simplified chinese" | "chinese simplified" => {
                Some(Self::SimplifiedChinese)
            }
            "zh hant" | "traditional chinese" | "chinese traditional" => {
                Some(Self::TraditionalChinese)
            }
            "es" | "spanish" | "español" => Some(Self::Spanish),
            "pt br" | "portuguese" | "portuguese brazil" | "brazilian portuguese" => {
                Some(Self::PortugueseBrazil)
            }
            "fr" | "french" | "français" => Some(Self::French),
            "de" | "german" | "deutsch" => Some(Self::German),
            "ja" | "japanese" | "日本語" => Some(Self::Japanese),
            "ko" | "korean" | "한국어" => Some(Self::Korean),
            "ru" | "russian" | "русский" => Some(Self::Russian),
            "ar" | "arabic" | "العربية" => Some(Self::Arabic),
            _ => None,
        }
    }

    pub const fn instruction(self) -> Option<&'static str> {
        match self {
            Self::Auto => None,
            Self::English => Some("Write assistant responses and drafted content in English."),
            Self::SimplifiedChinese => {
                Some("Write assistant responses and drafted content in Simplified Chinese.")
            }
            Self::TraditionalChinese => {
                Some("Write assistant responses and drafted content in Traditional Chinese.")
            }
            Self::Spanish => Some("Write assistant responses and drafted content in Spanish."),
            Self::PortugueseBrazil => {
                Some("Write assistant responses and drafted content in Brazilian Portuguese.")
            }
            Self::French => Some("Write assistant responses and drafted content in French."),
            Self::German => Some("Write assistant responses and drafted content in German."),
            Self::Japanese => Some("Write assistant responses and drafted content in Japanese."),
            Self::Korean => Some("Write assistant responses and drafted content in Korean."),
            Self::Russian => Some("Write assistant responses and drafted content in Russian."),
            Self::Arabic => Some("Write assistant responses and drafted content in Arabic."),
        }
    }
}

impl CodingProvider {
    pub const fn catalog_backend(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::Claude => "claude",
            Self::OpenRouter => "openrouter",
            Self::OpenAiCompatible => "openai-compatible",
        }
    }

    pub fn model_catalog(self) -> Option<borg_provider::ProviderModelCatalog> {
        borg_provider::model_catalog_for_backend(self.catalog_backend())
    }

    pub const fn label(self) -> &'static str {
        match self {
            Self::Codex => "Codex",
            Self::Claude => "Claude",
            Self::OpenRouter => "OpenRouter",
            Self::OpenAiCompatible => "OpenAI-compatible",
        }
    }

    /// Every provider that publishes a fixed model catalog, in picker order.
    pub const CATALOG_PROVIDERS: [Self; 2] = [Self::Codex, Self::Claude];

    /// The provider whose catalog lists `model`, if any. Model ids are unique
    /// across catalogs, so this is what lets the model picker offer models
    /// from providers other than the session's current one.
    pub fn for_model(model: &str) -> Option<Self> {
        let model = model.trim();
        Self::CATALOG_PROVIDERS
            .into_iter()
            .find(|provider| {
                provider.model_catalog().is_some_and(|catalog| {
                    catalog.selectable_models.iter().any(|(id, _)| *id == model)
                })
            })
            .or_else(|| {
                (model == borg_provider::openrouter_product_model()
                    || borg_provider::openrouter_model_entries()
                        .iter()
                        .any(|entry| entry.id == model))
                .then_some(Self::OpenRouter)
            })
    }

    pub fn executable(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::Claude => "claude",
            Self::OpenRouter | Self::OpenAiCompatible => "borg",
        }
    }

    pub fn supports_fast(self) -> bool {
        matches!(self, Self::Codex | Self::Claude)
    }

    pub fn uses_native_harness(self) -> bool {
        matches!(self, Self::OpenRouter | Self::OpenAiCompatible)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum PermissionMode {
    FullAccess,
    Auto,
    Manual,
}

impl<'de> Deserialize<'de> for PermissionMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "snake_case")]
        enum StoredPermissionMode {
            FullAccess,
            Auto,
            Manual,
            ReadOnly,
            WorkspaceWrite,
        }

        Ok(match StoredPermissionMode::deserialize(deserializer)? {
            StoredPermissionMode::FullAccess => Self::FullAccess,
            StoredPermissionMode::Auto => Self::Auto,
            StoredPermissionMode::Manual
            | StoredPermissionMode::ReadOnly
            | StoredPermissionMode::WorkspaceWrite => Self::Manual,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum SessionStatus {
    Starting,
    Ready,
    Running,
    WaitingForApproval,
    Completed,
    Failed,
    Stopped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum HostStatus {
    Online,
    Offline,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ProviderCapability {
    pub provider: CodingProvider,
    pub installed: bool,
    pub version: Option<String>,
    /// True when at least one credential or endpoint which can authorize this
    /// lane is present. This is deliberately a boolean: no secret or token is
    /// ever sent to a model or persisted in the session journal.
    pub authenticated: bool,
    pub auth_detail: Option<String>,
    /// The authenticated/configured mechanisms currently usable on the host.
    /// Older remote payloads omitted this field, so an empty list means that
    /// the mechanism was not classified by that older host.
    #[serde(default)]
    pub auth_methods: Vec<ProviderAuthMethod>,
    /// Whether the host can admit a child on this provider right now. This is
    /// stronger than `authenticated`: a provider CLI or endpoint may still be
    /// missing even when credentials exist.
    #[serde(default)]
    pub can_spawn: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ProviderAuthMethod {
    Subscription,
    ApiKey,
    Endpoint,
}

impl ProviderAuthMethod {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Subscription => "subscription",
            Self::ApiKey => "API key",
            Self::Endpoint => "endpoint",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct HostCapabilities {
    pub protocol_version: u16,
    pub providers: Vec<ProviderCapability>,
    pub roots: Vec<PathBuf>,
    pub can_launch: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_attachment: Option<WorkspaceAttachmentCapabilities>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct WorkspaceAttachmentCapabilities {
    #[serde(default)]
    pub presence_leases: bool,
    #[serde(default)]
    pub approval_provenance: bool,
    #[serde(default)]
    pub reconnect_sync_cursors: bool,
    /// The host enforces the launch attachment's participant command grants.
    #[serde(default)]
    pub participant_scoped_command_authority: bool,
}

/// Commands a workspace participant may authorize for an attached session.
/// The relay remains the transport authority; this limits which session actions
/// the enrolled host will accept for the attached participant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ParticipantCommandKind {
    Prompt,
    RecallQueuedPrompt,
    Configure,
    Approve,
    RespondToProviderInteraction,
    Goal,
    Todo,
    Subagent,
    Interrupt,
    Compact,
    ClearContext,
    Stop,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ParticipantCommandAuthority {
    pub participant_id: Uuid,
    #[serde(default)]
    pub allowed: Vec<ParticipantCommandKind>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct RemoteHostIdentity {
    pub host_id: Uuid,
    pub hostname: String,
    pub platform: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct RemotePresenceLease {
    pub lease_id: Uuid,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct RemoteApprovalProvenance {
    pub approval_id: String,
    pub approved_by_participant_id: Uuid,
    pub approved_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct RemoteReconnectSyncCursors {
    pub command_cursor: u64,
    pub event_cursor: u64,
    pub live_revision: u64,
}

/// Optional workspace attachment carried with a launch command. It is metadata
/// for the existing session journal, never a second message/event log.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct WorkspaceAttachment {
    pub workspace_id: Option<Uuid>,
    pub participant_id: Option<Uuid>,
    /// Optional restriction for commands sent after this launch. Missing
    /// preserves legacy host behavior.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command_authority: Option<ParticipantCommandAuthority>,
    pub host_identity: Option<RemoteHostIdentity>,
    pub host_capabilities: Option<WorkspaceAttachmentCapabilities>,
    pub presence_lease: Option<RemotePresenceLease>,
    pub approval_provenance: Option<RemoteApprovalProvenance>,
    pub reconnect_sync_cursors: Option<RemoteReconnectSyncCursors>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct RemoteHost {
    pub id: Uuid,
    pub name: String,
    pub status: HostStatus,
    pub platform: String,
    pub hostname: String,
    pub capabilities: HostCapabilities,
    pub last_seen_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<RemoteHostIdentity>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct LaunchSession {
    pub request_id: Uuid,
    pub cwd: PathBuf,
    pub provider: CodingProvider,
    pub model: Option<String>,
    pub effort: Option<String>,
    #[serde(default)]
    pub fast: Option<bool>,
    #[serde(default)]
    pub response_language: ResponseLanguage,
    pub permission_mode: PermissionMode,
    pub name: Option<String>,
    pub initial_prompt: Option<String>,
    /// Provider-neutral runtime feature gates. Missing fields preserve legacy behavior.
    #[serde(default)]
    pub capabilities: SessionCapabilities,
    /// Maximum concurrently live child agents for this team. Missing values
    /// use Borg's current default and keep older launch payloads compatible.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subagent_concurrency_limit: Option<u32>,
    /// Canonical paths from active, trusted extension manifests for local
    /// execution. Enrolled hosts discard this controller hint and derive their
    /// own active catalog, so a serialized path never grants host access.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extension_skill_roots: Vec<PathBuf>,
    /// Optional autonomous-team policy. Absent preserves ordinary manual
    /// subagent behavior.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(type = "unknown")]
    pub team_policy: Option<crate::TeamPolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct SessionCapabilities {
    pub multiplayer: bool,
    pub subagents: bool,
    pub autonomous_team: bool,
    pub shared_work: bool,
    pub presence: bool,
    pub cloud_sync: bool,
    pub web_relay: bool,
    pub telemetry: bool,
    /// Host-local provider authentication and admission state. This is safe
    /// model metadata, never a credential, and is refreshed at session launch
    /// by local and enrolled hosts.
    #[serde(default)]
    pub provider_capabilities: Vec<ProviderCapability>,
}

impl Default for SessionCapabilities {
    fn default() -> Self {
        Self {
            multiplayer: true,
            subagents: true,
            autonomous_team: true,
            shared_work: true,
            presence: true,
            cloud_sync: true,
            web_relay: true,
            telemetry: false,
            provider_capabilities: Vec::new(),
        }
    }
}

/// Render the secret-free provider admission snapshot that is appended to a
/// model's system instructions. Keep the order stable so a status refresh does
/// not perturb unrelated prompt bytes and provider caches.
pub fn provider_capabilities_prompt(capabilities: &[ProviderCapability]) -> String {
    if capabilities.is_empty() {
        return "Provider/subagent admission status is unavailable in this session. Do not assume a provider is usable; call `get_provider_capabilities` before spawning a child.".to_string();
    }

    let mut prompt = String::from(
        "Host-local provider/subagent admission status (safe metadata; never request or expose credentials):\n",
    );
    for provider in [
        CodingProvider::Codex,
        CodingProvider::Claude,
        CodingProvider::OpenRouter,
        CodingProvider::OpenAiCompatible,
    ] {
        let Some(capability) = capabilities.iter().find(|item| item.provider == provider) else {
            prompt.push_str(&format!(
                "- {}: status unknown; do not spawn until checked\n",
                provider.label()
            ));
            continue;
        };
        let methods = capability
            .auth_methods
            .iter()
            .map(|method| method.label())
            .collect::<Vec<_>>();
        let mechanism = if methods.is_empty() {
            if capability.authenticated {
                "credentials available (mechanism not classified)".to_string()
            } else {
                "no authenticated mechanism detected".to_string()
            }
        } else {
            format!("authenticated via {}", methods.join(" + "))
        };
        let status = if capability.can_spawn {
            "READY"
        } else if capability.authenticated {
            "NOT READY (credential present, but the provider executable/endpoint is unavailable)"
        } else {
            "NOT READY (not authenticated/configured)"
        };
        prompt.push_str(&format!("- {}: {status}; {mechanism}.\n", provider.label()));
    }
    prompt.push_str(
        "Only use `spawn_agent` for a provider marked READY; it performs the same admission check and returns a clear remediation error otherwise. Use `get_provider_capabilities` for the complete structured snapshot.",
    );
    prompt
}

/// Provider-neutral runtime capability names exposed to hosts and clients.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum SessionCapability {
    Multiplayer,
    Subagents,
    AutonomousTeam,
    SharedWork,
    Presence,
    CloudSync,
    WebRelay,
    Telemetry,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct InactiveCapability {
    pub capability: SessionCapability,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct EffectiveCapabilities {
    pub active: Vec<SessionCapability>,
    pub inactive: Vec<InactiveCapability>,
}

impl SessionCapabilities {
    pub fn effective(&self) -> EffectiveCapabilities {
        let states = [
            (SessionCapability::Multiplayer, self.multiplayer, None),
            (SessionCapability::Subagents, self.subagents, None),
            (
                SessionCapability::AutonomousTeam,
                self.autonomous_team,
                (!self.subagents).then_some("requires subagents"),
            ),
            (
                SessionCapability::SharedWork,
                self.shared_work,
                (!self.multiplayer).then_some("requires multiplayer"),
            ),
            (
                SessionCapability::Presence,
                self.presence,
                (!self.multiplayer).then_some("requires multiplayer"),
            ),
            (SessionCapability::CloudSync, self.cloud_sync, None),
            (
                SessionCapability::WebRelay,
                self.web_relay,
                (!self.cloud_sync).then_some("requires cloud_sync"),
            ),
            (SessionCapability::Telemetry, self.telemetry, None),
        ];
        let mut active = Vec::new();
        let mut inactive = Vec::new();
        for (capability, configured, dependency) in states {
            if configured && dependency.is_none() {
                active.push(capability);
            } else {
                inactive.push(InactiveCapability {
                    capability,
                    reason: if configured {
                        dependency
                            .unwrap_or("disabled by configuration")
                            .to_string()
                    } else {
                        "disabled by configuration".to_string()
                    },
                });
            }
        }
        EffectiveCapabilities { active, inactive }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct RemoteSession {
    pub id: Uuid,
    pub host_id: Uuid,
    pub workspace_id: Option<Uuid>,
    pub name: String,
    #[serde(default)]
    pub latest_response: Option<String>,
    pub cwd: PathBuf,
    pub provider: CodingProvider,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub fast: bool,
    pub permission_mode: PermissionMode,
    pub provider_session_id: Option<String>,
    pub status: SessionStatus,
    pub last_event_sequence: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
#[ts(export)]
pub enum HostCommand {
    Launch {
        session_id: Uuid,
        request: Box<LaunchSession>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        attachment: Option<WorkspaceAttachment>,
    },
    Prompt {
        session_id: Uuid,
        message_id: Uuid,
        text: String,
        attachments: Vec<PathBuf>,
        output_schema: Option<Value>,
        delivery: PromptDelivery,
    },
    RecallQueuedPrompt {
        session_id: Uuid,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message_id: Option<Uuid>,
    },
    Configure {
        session_id: Uuid,
        action: SessionConfigAction,
    },
    Approve {
        session_id: Uuid,
        approval_id: String,
        decision: ApprovalDecision,
    },
    RespondToProviderInteraction {
        session_id: Uuid,
        interaction_id: String,
        response: Value,
    },
    Goal {
        session_id: Uuid,
        action: GoalAction,
    },
    Todo {
        session_id: Uuid,
        action: TodoAction,
    },
    Subagent {
        session_id: Uuid,
        action: SubagentAction,
    },
    Interrupt {
        session_id: Uuid,
    },
    Compact {
        session_id: Uuid,
    },
    ClearContext {
        session_id: Uuid,
    },
    Stop {
        session_id: Uuid,
    },
    WorkspaceFilesystem {
        request: WorkspaceFilesystemRequest,
    },
    CancelWorkspaceFilesystem {
        request_id: Uuid,
    },
    WorkspaceCommand {
        request: WorkspaceCommandRequest,
    },
    CancelWorkspaceCommand {
        request_id: Uuid,
    },
}

impl HostCommand {
    pub fn session_id(&self) -> Option<Uuid> {
        match self {
            Self::Launch { session_id, .. }
            | Self::Prompt { session_id, .. }
            | Self::RecallQueuedPrompt { session_id, .. }
            | Self::Configure { session_id, .. }
            | Self::Approve { session_id, .. }
            | Self::RespondToProviderInteraction { session_id, .. }
            | Self::Goal { session_id, .. }
            | Self::Todo { session_id, .. }
            | Self::Subagent { session_id, .. }
            | Self::Interrupt { session_id }
            | Self::Compact { session_id }
            | Self::ClearContext { session_id }
            | Self::Stop { session_id } => Some(*session_id),
            Self::WorkspaceFilesystem { .. }
            | Self::CancelWorkspaceFilesystem { .. }
            | Self::WorkspaceCommand { .. }
            | Self::CancelWorkspaceCommand { .. } => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
#[ts(export)]
pub enum SessionConfigAction {
    SetModel {
        model: String,
    },
    /// Repoints the live session at another provider without relaunching it.
    /// The turn loop reads `launch.provider` fresh each turn, so the switch
    /// takes effect on the next prompt; the caller-side handler drops the
    /// provider session id, which belongs to the provider being left.
    SetProvider {
        provider: CodingProvider,
        model: Option<String>,
    },
    SetEffort {
        effort: String,
    },
    SetPermissionMode {
        permission_mode: PermissionMode,
    },
    SetFast {
        enabled: bool,
    },
    SetResponseLanguage {
        language: ResponseLanguage,
    },
}

/// Typed control plane for child sessions owned by one parent session actor.
///
/// The parent coordinator remains the topology authority. Remote transports
/// only enqueue these actions and observe the resulting durable event.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
#[ts(export)]
pub enum SubagentAction {
    /// Ensure one deterministic, provider-pinned child exists. This is used
    /// by client-side persistent lanes such as `/claude` and `/gpt`; unlike
    /// `spawn_agent`, it creates an idle child without spending a turn on an
    /// initial task message.
    Ensure {
        request_id: Uuid,
        task_name: String,
        provider: CodingProvider,
        model: Option<String>,
        effort: Option<String>,
    },
    List {
        request_id: Uuid,
        path_prefix: Option<String>,
    },
    Message {
        request_id: Uuid,
        target: String,
        message: String,
        delivery: PromptDelivery,
    },
    /// Send human-authored input directly to a child session.
    ///
    /// Unlike `Message`, this is not wrapped as an agent-to-agent team
    /// message. It is the remote/TUI equivalent of typing into that child's
    /// own composer.
    Prompt {
        request_id: Uuid,
        target: String,
        message_id: Uuid,
        text: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        attachments: Vec<PathBuf>,
        delivery: PromptDelivery,
    },
    RecallPrompt {
        request_id: Uuid,
        target: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message_id: Option<Uuid>,
    },
    /// Clear a child session's provider context while preserving its durable
    /// identity, journal, and lane routing.
    ClearContext {
        request_id: Uuid,
        target: String,
    },
    Interrupt {
        request_id: Uuid,
        target: String,
    },
    Stop {
        request_id: Uuid,
        target: String,
    },
    Approve {
        request_id: Uuid,
        target: String,
        approval_id: String,
        decision: ApprovalDecision,
    },
}

impl SubagentAction {
    pub fn request_id(&self) -> Uuid {
        match self {
            Self::Ensure { request_id, .. }
            | Self::List { request_id, .. }
            | Self::Message { request_id, .. }
            | Self::Prompt { request_id, .. }
            | Self::RecallPrompt { request_id, .. }
            | Self::ClearContext { request_id, .. }
            | Self::Interrupt { request_id, .. }
            | Self::Stop { request_id, .. }
            | Self::Approve { request_id, .. } => *request_id,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
#[ts(export)]
pub enum SubagentControlOutcome {
    Listed {
        agents: Vec<crate::SubagentSnapshot>,
    },
    Accepted {
        agent: Box<crate::SubagentSnapshot>,
    },
    Failed {
        message: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct WorkspaceCommandRequest {
    pub request_id: Uuid,
    pub workspace_id: Uuid,
    pub root_path: PathBuf,
    pub cwd: PathBuf,
    pub command: Vec<String>,
    pub timeout_ms: u64,
    pub output_max_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct WorkspaceCommandOutput {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub workspace_root: PathBuf,
    pub cwd: PathBuf,
    pub command: Vec<String>,
    pub timeout_seconds: u64,
    pub timed_out: bool,
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
    pub output_max_bytes: u64,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub manifest_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
#[ts(export)]
pub enum WorkspaceCommandOutcome {
    Success {
        output: Box<WorkspaceCommandOutput>,
    },
    Failure {
        code: WorkspaceCommandErrorCode,
        message: String,
        retryable: bool,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum WorkspaceCommandErrorCode {
    InvalidRoot,
    InvalidCwd,
    InvalidCommand,
    PermissionDenied,
    TimedOut,
    Cancelled,
    Indeterminate,
    Io,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct WorkspaceCommandResponse {
    pub request_id: Uuid,
    pub workspace_id: Uuid,
    pub outcome: WorkspaceCommandOutcome,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct WorkspaceFilesystemRequest {
    pub request_id: Uuid,
    pub workspace_id: Uuid,
    pub root_path: PathBuf,
    pub timeout_ms: u64,
    pub operation: WorkspaceFilesystemOperation,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
#[ts(export)]
pub enum WorkspaceFilesystemOperation {
    List {
        path: PathBuf,
        limit: usize,
    },
    Stat {
        path: PathBuf,
    },
    ReadText {
        path: PathBuf,
        max_bytes: u64,
    },
    ReadBytes {
        path: PathBuf,
        max_bytes: u64,
    },
    WriteText {
        path: PathBuf,
        text: String,
        overwrite: bool,
        create_parent_dirs: bool,
    },
    WriteBytes {
        path: PathBuf,
        content_base64: String,
        overwrite: bool,
        create_parent_dirs: bool,
    },
    Mkdir {
        path: PathBuf,
        recursive: bool,
    },
    Move {
        from_path: PathBuf,
        to_path: PathBuf,
        overwrite: bool,
        create_parent_dirs: bool,
    },
    Copy {
        from_path: PathBuf,
        to_path: PathBuf,
        overwrite: bool,
        create_parent_dirs: bool,
        recursive: bool,
        max_entries: usize,
    },
    Delete {
        path: PathBuf,
        archive: bool,
        recursive: bool,
    },
}

impl WorkspaceFilesystemOperation {
    pub fn is_mutating(&self) -> bool {
        !matches!(
            self,
            Self::List { .. } | Self::Stat { .. } | Self::ReadText { .. } | Self::ReadBytes { .. }
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct WorkspaceFileEntry {
    pub name: String,
    pub path: PathBuf,
    pub kind: WorkspaceFileKind,
    pub bytes: Option<u64>,
    pub modified_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum WorkspaceFileKind {
    File,
    Directory,
    Symlink,
    Other,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
#[ts(export)]
pub enum WorkspaceFilesystemOutput {
    Listed {
        path: PathBuf,
        entries: Vec<WorkspaceFileEntry>,
        limit: usize,
        truncated: bool,
    },
    Stat {
        entry: WorkspaceFileEntry,
    },
    Text {
        path: PathBuf,
        text: String,
        bytes: u64,
        max_bytes: u64,
        modified_at: Option<DateTime<Utc>>,
    },
    Bytes {
        path: PathBuf,
        content_base64: String,
        bytes: u64,
        max_bytes: u64,
        modified_at: Option<DateTime<Utc>>,
    },
    Mutated {
        operation: String,
        path: Option<PathBuf>,
        from_path: Option<PathBuf>,
        to_path: Option<PathBuf>,
        archived_path: Option<PathBuf>,
        bytes: Option<u64>,
        created: Option<bool>,
        changed: bool,
        files: Option<u64>,
        directories: Option<u64>,
        entries: Option<u64>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
#[ts(export)]
pub enum WorkspaceFilesystemOutcome {
    Success {
        output: WorkspaceFilesystemOutput,
    },
    Failure {
        code: WorkspaceFilesystemErrorCode,
        message: String,
        retryable: bool,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum WorkspaceFilesystemErrorCode {
    InvalidPath,
    NotFound,
    AlreadyExists,
    NotAFile,
    NotADirectory,
    PayloadTooLarge,
    InvalidEncoding,
    PermissionDenied,
    TimedOut,
    Cancelled,
    Indeterminate,
    Io,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct WorkspaceFilesystemResponse {
    pub request_id: Uuid,
    pub workspace_id: Uuid,
    pub outcome: WorkspaceFilesystemOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
#[ts(export)]
pub enum GoalAction {
    Set {
        objective: String,
        token_budget: Option<u64>,
    },
    Pause,
    Resume,
    Clear,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum GoalStatus {
    Active,
    Paused,
    Blocked,
    UsageLimited,
    BudgetLimited,
    Complete,
}

impl GoalStatus {
    pub fn is_active(self) -> bool {
        self == Self::Active
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct SessionGoal {
    pub id: Uuid,
    pub objective: String,
    pub status: GoalStatus,
    pub token_budget: Option<u64>,
    pub tokens_used: u64,
    pub time_used_seconds: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl SessionGoal {
    pub fn new(objective: String, token_budget: Option<u64>) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            objective,
            status: GoalStatus::Active,
            token_budget,
            tokens_used: 0,
            time_used_seconds: 0,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn remaining_tokens(&self) -> Option<u64> {
        self.token_budget
            .map(|budget| budget.saturating_sub(self.tokens_used))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelGoalStatus {
    Complete,
    Blocked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SessionGoalToolRequest {
    Get,
    Create {
        objective: String,
        token_budget: Option<u64>,
    },
    Update {
        status: ModelGoalStatus,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionGoalToolResponse {
    pub goal: Option<SessionGoal>,
    pub remaining_tokens: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
#[ts(export)]
pub enum TodoAction {
    Replace { items: Vec<TodoItemUpdate> },
    Add { content: String },
    SetStatus { id: Uuid, status: PlanItemStatus },
    Remove { id: Uuid },
    Clear,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SessionTodoToolRequest {
    Get,
    Update { items: Vec<TodoItemUpdate> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionTodoToolResponse {
    pub items: Vec<PlanItem>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum PromptDelivery {
    /// Admit immediately and promote at the next safe provider-turn boundary.
    Steer,
    /// Keep pending until the session would otherwise become idle.
    Queue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ApprovalDecision {
    AllowOnce,
    AllowSession,
    Deny,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum EventActor {
    User,
    Assistant,
    Tool,
    System,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum MessageStatus {
    Queued,
    InProgress,
    Complete,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum SessionPayloadKind {
    ToolInput,
    ToolOutput,
    ToolResultInput,
}

impl SessionPayloadKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ToolInput => "tool_input",
            Self::ToolOutput => "tool_output",
            Self::ToolResultInput => "tool_result_input",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct SessionPayloadRef {
    pub id: Uuid,
    pub kind: SessionPayloadKind,
    pub byte_len: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum RuntimeProcessStream {
    Stdout,
    Stderr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum RuntimeProcessStatus {
    Exited,
    TimedOut,
    Terminated,
    Failed,
    Orphaned,
}

/// The execution engine selected for an extension workflow. `blu` remains the
/// compatibility default; the other variants are supervised user-provided
/// runtimes rather than engines linked into Borg.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum WorkflowRuntime {
    #[default]
    Blu,
    Python,
    Ipython,
    Javascript,
    Typescript,
}

impl WorkflowRuntime {
    pub fn label(self) -> &'static str {
        match self {
            Self::Blu => "blu",
            Self::Python => "python",
            Self::Ipython => "ipython",
            Self::Javascript => "javascript",
            Self::Typescript => "typescript",
        }
    }

    pub fn source_extension(self) -> &'static str {
        self.source_extensions()[0]
    }

    /// Source suffixes accepted by this runtime. Blu owns the Lua-family
    /// entrypoints; `.lua` and `.luau` are not separate external runtimes.
    pub fn source_extensions(self) -> &'static [&'static str] {
        match self {
            Self::Blu => &["blu", "lua", "luau"],
            Self::Python | Self::Ipython => &["py"],
            Self::Javascript => &["js"],
            Self::Typescript => &["ts"],
        }
    }

    pub fn accepts_source_extension(self, extension: &str) -> bool {
        self.source_extensions()
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(extension))
    }

    /// Best-effort executable defaults. A manifest may override the command;
    /// discovery deliberately does not fail just because an optional runtime
    /// is not installed on this host.
    pub fn default_command(self) -> &'static str {
        match self {
            Self::Blu => "",
            Self::Python => "python3",
            Self::Ipython => "ipython",
            Self::Javascript => "bun",
            Self::Typescript => "bun",
        }
    }

    pub fn is_embedded(self) -> bool {
        matches!(self, Self::Blu)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
#[ts(export)]
pub enum SessionEventKind {
    SessionStarted,
    SessionConfigured {
        cwd: PathBuf,
        provider: CodingProvider,
        model: Option<String>,
        effort: Option<String>,
        #[serde(default)]
        fast: bool,
        #[serde(default)]
        response_language: ResponseLanguage,
        permission_mode: PermissionMode,
    },
    /// Host-local, secret-free provider admission state captured when the
    /// session actor starts or resumes. It is durable metadata, not model
    /// context, so every provider lane can make the same spawn decision after
    /// a crash or reconnect.
    ProviderCapabilitiesUpdated {
        providers: Vec<ProviderCapability>,
    },
    /// Effective provider configuration captured for one admitted turn.
    ///
    /// `SessionConfigured` may change while this turn is still running; this
    /// snapshot keeps provider telemetry and assistant attribution attached
    /// to the configuration that actually produced them.
    TurnStarted {
        message_id: Uuid,
        provider: CodingProvider,
        model: Option<String>,
        effort: Option<String>,
        #[serde(default)]
        fast: bool,
    },
    StatusChanged {
        status: SessionStatus,
        detail: Option<String>,
    },
    Message {
        message_id: Uuid,
        actor: EventActor,
        text: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        attachments: Vec<PathBuf>,
        status: MessageStatus,
        delivery: Option<PromptDelivery>,
    },
    ReasoningDelta {
        text: String,
    },
    /// The provider has ended its reasoning item. This freezes the displayed
    /// thinking duration independently from any later tool boundary.
    ReasoningCompleted,
    ToolStarted {
        tool_call_id: String,
        name: String,
        input: Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        input_ref: Option<SessionPayloadRef>,
    },
    ToolCompleted {
        tool_call_id: String,
        output: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        output_ref: Option<SessionPayloadRef>,
        is_error: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        input: Option<Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        input_ref: Option<SessionPayloadRef>,
    },
    /// A native process has been admitted to the session runtime. Process
    /// handles are host-local, so these events are durable for recovery but
    /// deliberately never become model context or fork history.
    RuntimeProcessStarted {
        process_id: Uuid,
        pid: u32,
        command: String,
        cwd: PathBuf,
    },
    RuntimeProcessOutput {
        process_id: Uuid,
        stream: RuntimeProcessStream,
        chunk: String,
    },
    RuntimeProcessCompleted {
        process_id: Uuid,
        pid: u32,
        status: RuntimeProcessStatus,
        exit_code: Option<i32>,
        timed_out: bool,
        stdout: String,
        stderr: String,
        stdout_omitted_bytes: usize,
        stderr_omitted_bytes: usize,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    /// Durable lifecycle for an embedded Blu workflow. These events are
    /// runtime journal entries rather than model transcript content.
    BluWorkflowStarted {
        workflow_id: Uuid,
        source_hash: String,
        name: String,
    },
    BluWorkflowCallRequested {
        workflow_id: Uuid,
        call_id: u64,
        operation: String,
        request: Value,
    },
    BluWorkflowCallCompleted {
        workflow_id: Uuid,
        call_id: u64,
        operation: String,
        response: Option<Value>,
        error: Option<String>,
    },
    BluWorkflowCompleted {
        workflow_id: Uuid,
        source_hash: String,
        success: bool,
        result: Option<Value>,
        error: Option<String>,
    },
    /// Durable lifecycle for a user-selected external workflow runtime.
    /// These events intentionally carry artifact identity so changing a
    /// script, interpreter selection, or entrypoint cannot silently replay a
    /// previous result under different semantics.
    RuntimeWorkflowStarted {
        workflow_id: Uuid,
        runtime: WorkflowRuntime,
        artifact_hash: String,
        name: String,
    },
    RuntimeWorkflowCallRequested {
        workflow_id: Uuid,
        call_id: u64,
        operation: String,
        request: Value,
    },
    RuntimeWorkflowCallCompleted {
        workflow_id: Uuid,
        call_id: u64,
        operation: String,
        response: Option<Value>,
        error: Option<String>,
    },
    RuntimeWorkflowCompleted {
        workflow_id: Uuid,
        runtime: WorkflowRuntime,
        artifact_hash: String,
        success: bool,
        result: Option<Value>,
        stdout: String,
        stderr: String,
        exit_code: Option<i32>,
        error: Option<String>,
    },
    ApprovalRequested {
        approval_id: String,
        title: String,
        detail: String,
        command: Option<String>,
    },
    ApprovalResolved {
        approval_id: String,
        decision: ApprovalDecision,
    },
    ProviderInteractionRequested {
        interaction_id: String,
        kind: String,
        title: String,
        detail: String,
        payload: Value,
    },
    ProviderInteractionResolved {
        interaction_id: String,
        response: Value,
    },
    PlanUpdated {
        items: Vec<PlanItem>,
    },
    UsageUpdated {
        #[serde(default)]
        provider_duration_ms: u64,
        /// The durable user-turn message this usage belongs to. Older events
        /// omit it; clients must treat those as legacy telemetry only.
        #[serde(default)]
        turn_id: Option<Uuid>,
        /// Whether the subscription adapter reused its native context for
        /// this turn. `None` means the route did not expose this distinction.
        #[serde(default)]
        provider_context_reused: Option<bool>,
        input_tokens: u64,
        output_tokens: u64,
        cached_input_tokens: u64,
        #[serde(default)]
        cache_creation_input_tokens: u64,
        #[serde(default)]
        total_tokens: u64,
        #[serde(default)]
        cost_microusd: Option<u64>,
        #[serde(default)]
        cost_basis: String,
        cost_usd: Option<f64>,
        #[serde(default)]
        context_tokens: Option<u64>,
        #[serde(default)]
        context_window_tokens: Option<u64>,
    },
    ContextWindowUpdated {
        context_tokens: u64,
        context_window_tokens: u64,
    },
    ContextCleared,
    GoalUpdated {
        goal: SessionGoal,
    },
    GoalCleared {
        goal_id: Uuid,
    },
    SubagentActivity {
        activity: crate::SubagentActivityKind,
        agent: crate::SubagentSnapshot,
        event: Option<Box<SessionEvent>>,
    },
    SubagentControl {
        request_id: Uuid,
        outcome: SubagentControlOutcome,
    },
    ProviderSessionLinked {
        provider_session_id: String,
    },
    PromptRecalled {
        message_id: Uuid,
        text: String,
        attachments: Vec<PathBuf>,
    },
    /// Terminal boundary for exactly one admitted prompt.
    ///
    /// Session readiness is lifecycle state and must not be used to infer
    /// that a particular turn completed: a session can emit `Ready` while
    /// another queued prompt is being admitted. Consumers correlate on the
    /// original prompt message id instead.
    TurnCompleted {
        message_id: Uuid,
        provider_session_id: Option<String>,
        final_text: String,
        error: Option<String>,
    },
    ProviderEvent {
        provider: CodingProvider,
        kind: String,
        payload: Value,
    },
    Error {
        message: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct PlanItem {
    pub id: Uuid,
    pub content: String,
    pub status: PlanItemStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, TS)]
#[ts(export)]
pub struct TodoItemUpdate {
    pub id: Option<Uuid>,
    pub content: String,
    pub status: PlanItemStatus,
}

impl<'de> Deserialize<'de> for TodoItemUpdate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Wire {
            #[serde(default)]
            id: Option<String>,
            content: String,
            status: PlanItemStatus,
        }

        let wire = Wire::deserialize(deserializer)?;
        Ok(Self {
            id: wire.id.and_then(|value| Uuid::parse_str(&value).ok()),
            content: wire.content,
            status: wire.status,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum PlanItemStatus {
    Pending,
    InProgress,
    Completed,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct SessionEvent {
    pub id: Uuid,
    pub session_id: Uuid,
    pub sequence: u64,
    pub created_at: DateTime<Utc>,
    pub kind: SessionEventKind,
}

impl SessionEvent {
    pub fn new(session_id: Uuid, sequence: u64, kind: SessionEventKind) -> Self {
        Self {
            id: Uuid::new_v4(),
            session_id,
            sequence,
            created_at: Utc::now(),
            kind,
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    #[test]
    fn workflow_runtime_profiles_are_stable_and_user_selectable() {
        let profiles = [
            (WorkflowRuntime::Blu, "blu", "blu", ""),
            (WorkflowRuntime::Python, "python", "py", "python3"),
            (WorkflowRuntime::Ipython, "ipython", "py", "ipython"),
            (WorkflowRuntime::Javascript, "javascript", "js", "bun"),
            (WorkflowRuntime::Typescript, "typescript", "ts", "bun"),
        ];

        for (runtime, label, extension, command) in profiles {
            assert_eq!(runtime.label(), label);
            assert_eq!(runtime.source_extension(), extension);
            assert_eq!(runtime.default_command(), command);
            assert_eq!(runtime.is_embedded(), runtime == WorkflowRuntime::Blu);
            assert_eq!(serde_json::to_value(runtime).unwrap(), json!(label));
        }
        assert!(WorkflowRuntime::Blu.accepts_source_extension("blu"));
        assert!(WorkflowRuntime::Blu.accepts_source_extension("lua"));
        assert!(WorkflowRuntime::Blu.accepts_source_extension("LUAU"));
        assert!(!WorkflowRuntime::Blu.accepts_source_extension("js"));
    }

    #[test]
    fn models_resolve_back_to_the_provider_that_serves_them() {
        assert_eq!(
            CodingProvider::for_model("claude-opus-5"),
            Some(CodingProvider::Claude)
        );
        assert_eq!(
            CodingProvider::for_model("gpt-5.6-sol"),
            Some(CodingProvider::Codex)
        );
        assert_eq!(
            CodingProvider::for_model("openrouter/auto"),
            Some(CodingProvider::OpenRouter)
        );
        assert_eq!(CodingProvider::for_model("some/openrouter-model"), None);
    }

    #[test]
    fn effective_capabilities_explain_dependency_inactivation() {
        let capabilities = SessionCapabilities {
            multiplayer: false,
            shared_work: true,
            presence: true,
            ..Default::default()
        };
        let effective = capabilities.effective();
        assert!(!effective.active.contains(&SessionCapability::Multiplayer));
        assert_eq!(
            effective
                .inactive
                .iter()
                .find(|item| item.capability == SessionCapability::SharedWork)
                .unwrap()
                .reason,
            "requires multiplayer"
        );
    }

    use super::*;

    #[test]
    fn goal_command_has_stable_cross_process_shape() {
        let session_id = Uuid::nil();
        let command = HostCommand::Goal {
            session_id,
            action: GoalAction::Set {
                objective: "Finish the remote CLI".to_string(),
                token_budget: Some(50_000),
            },
        };

        assert_eq!(
            serde_json::to_value(command).unwrap(),
            json!({
                "type": "goal",
                "session_id": session_id,
                "action": {
                    "type": "set",
                    "objective": "Finish the remote CLI",
                    "token_budget": 50_000
                }
            })
        );
    }

    #[test]
    fn workspace_attachment_fields_are_optional_for_legacy_host_payloads() {
        let capabilities: HostCapabilities = serde_json::from_value(json!({
            "protocol_version": 5,
            "providers": [],
            "roots": [],
            "can_launch": true
        }))
        .unwrap();
        assert!(capabilities.workspace_attachment.is_none());

        let command: HostCommand = serde_json::from_value(json!({
            "type": "launch",
            "session_id": Uuid::new_v4(),
            "request": {
                "request_id": Uuid::new_v4(), "cwd": "/tmp", "provider": "codex",
                "model": null, "effort": null, "permission_mode": "manual",
                "name": null, "initial_prompt": null
            }
        }))
        .unwrap();
        assert!(matches!(
            command,
            HostCommand::Launch {
                attachment: None,
                ..
            }
        ));

        let attachment: WorkspaceAttachment = serde_json::from_value(json!({
            "workspace_id": Uuid::new_v4(),
            "participant_id": Uuid::new_v4(),
            "host_identity": null,
            "host_capabilities": null,
            "presence_lease": null,
            "approval_provenance": null,
            "reconnect_sync_cursors": null
        }))
        .unwrap();
        assert!(attachment.command_authority.is_none());
    }

    #[test]
    fn turn_completion_has_a_correlated_cross_process_shape() {
        let message_id = Uuid::nil();
        let event = SessionEventKind::TurnCompleted {
            message_id,
            provider_session_id: Some("provider-session".to_string()),
            final_text: "done".to_string(),
            error: None,
        };

        assert_eq!(
            serde_json::to_value(event).unwrap(),
            json!({
                "type": "turn_completed",
                "message_id": message_id,
                "provider_session_id": "provider-session",
                "final_text": "done",
                "error": null,
            })
        );
    }

    #[test]
    fn subagent_control_is_correlated_and_targets_the_parent_actor() {
        let parent_session_id = Uuid::nil();
        let request_id = Uuid::from_u128(1);
        let child_session_id = Uuid::from_u128(2);
        let command = HostCommand::Subagent {
            session_id: parent_session_id,
            action: SubagentAction::Interrupt {
                request_id,
                target: child_session_id.to_string(),
            },
        };

        assert_eq!(command.session_id(), Some(parent_session_id));
        assert_eq!(
            serde_json::to_value(command).unwrap(),
            json!({
                "type": "subagent",
                "session_id": parent_session_id,
                "action": {
                    "type": "interrupt",
                    "request_id": request_id,
                    "target": child_session_id.to_string(),
                }
            })
        );
    }

    #[test]
    fn focused_child_prompt_preserves_human_input_identity_and_delivery() {
        let parent_session_id = Uuid::nil();
        let child_session_id = Uuid::from_u128(2);
        let request_id = Uuid::from_u128(3);
        let message_id = Uuid::from_u128(4);
        let command = HostCommand::Subagent {
            session_id: parent_session_id,
            action: SubagentAction::Prompt {
                request_id,
                target: child_session_id.to_string(),
                message_id,
                text: "inspect this directly".to_string(),
                attachments: vec![PathBuf::from("trace.txt")],
                delivery: PromptDelivery::Steer,
            },
        };

        assert_eq!(command.session_id(), Some(parent_session_id));
        assert_eq!(
            serde_json::to_value(command).unwrap(),
            json!({
                "type": "subagent",
                "session_id": parent_session_id,
                "action": {
                    "type": "prompt",
                    "request_id": request_id,
                    "target": child_session_id.to_string(),
                    "message_id": message_id,
                    "text": "inspect this directly",
                    "attachments": ["trace.txt"],
                    "delivery": "steer",
                }
            })
        );
    }

    #[test]
    fn response_language_has_stable_codes_and_accepts_cli_names() {
        let expected = [
            "auto", "en", "zh-Hans", "zh-Hant", "es", "pt-BR", "fr", "de", "ja", "ko", "ru", "ar",
        ];
        assert_eq!(ResponseLanguage::ALL.map(ResponseLanguage::code), expected);
        for (language, code) in ResponseLanguage::ALL.into_iter().zip(expected) {
            assert_eq!(serde_json::to_value(language).unwrap(), json!(code));
            assert_eq!(ResponseLanguage::parse(code), Some(language));
            assert_eq!(ResponseLanguage::parse(language.name()), Some(language));
        }
        assert_eq!(
            ResponseLanguage::parse("Brazilian Portuguese"),
            Some(ResponseLanguage::PortugueseBrazil)
        );
        assert_eq!(ResponseLanguage::parse("not-a-language"), None);
    }

    #[test]
    fn plan_update_treats_invented_item_labels_as_new_items() {
        let update: TodoItemUpdate = serde_json::from_value(json!({
            "id": "inventory",
            "content": "Inventory the workspace",
            "status": "in_progress"
        }))
        .unwrap();

        assert_eq!(update.id, None);
        assert_eq!(update.content, "Inventory the workspace");
        assert_eq!(update.status, PlanItemStatus::InProgress);
    }

    #[test]
    fn launch_session_accepts_legacy_payload_without_extension_skill_roots() {
        let launch: LaunchSession = serde_json::from_value(json!({
            "request_id": Uuid::nil(), "cwd": "/workspace", "provider": "codex",
            "model": null, "effort": null, "permission_mode": "manual",
            "name": null, "initial_prompt": null
        }))
        .unwrap();
        assert!(launch.extension_skill_roots.is_empty());
        assert!(
            serde_json::to_value(&launch)
                .unwrap()
                .get("extension_skill_roots")
                .is_none()
        );
    }

    #[test]
    fn provider_prompt_exposes_safe_auth_and_spawn_state_without_secrets() {
        let secret = "sk-do-not-render";
        let prompt = provider_capabilities_prompt(&[
            ProviderCapability {
                provider: CodingProvider::Codex,
                installed: true,
                version: Some("test".to_string()),
                authenticated: true,
                auth_detail: Some("Codex subscription authenticated".to_string()),
                auth_methods: vec![ProviderAuthMethod::Subscription],
                can_spawn: true,
            },
            ProviderCapability {
                provider: CodingProvider::OpenRouter,
                installed: true,
                version: Some("test".to_string()),
                authenticated: true,
                auth_detail: Some(format!("API key configured: {secret}")),
                auth_methods: vec![ProviderAuthMethod::ApiKey],
                can_spawn: true,
            },
        ]);
        assert!(prompt.contains("Codex: READY"));
        assert!(prompt.contains("subscription"));
        assert!(prompt.contains("OpenRouter: READY"));
        assert!(prompt.contains("API key"));
        assert!(!prompt.contains(secret));
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct HostCommandEnvelope {
    pub id: Uuid,
    pub sequence: u64,
    pub created_at: DateTime<Utc>,
    pub command: HostCommand,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct HostHeartbeat {
    pub name: String,
    pub platform: String,
    pub hostname: String,
    pub capabilities: HostCapabilities,
    pub acknowledged_command_sequence: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<RemoteHostIdentity>,
}
