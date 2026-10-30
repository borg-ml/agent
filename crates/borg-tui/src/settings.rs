//! The fullscreen settings screen: every interactive preference in one place,
//! grouped into a few sections and changed in place with the keyboard or mouse.
//!
//! Rows are rebuilt from live values on every frame, so what the screen shows
//! is what is in force. A change is sent as the same typed [`UiAction`] the
//! slash commands use; the host applies and persists it.

use super::*;
mod providers;
use ratatui::Frame;
use ratatui::buffer::Buffer;

const MAX_WIDTH: u16 = 112;
/// Taller windows centre the screen instead of opening a gap above the help.
const MAX_HEIGHT: u16 = 34;
const SIDEBAR_WIDTH: u16 = 18;
/// Choices with more options than this open a list instead of a segment row.
const MAX_SEGMENTS: usize = 4;
/// An optimistic choice is shown until the host confirms it, or this long.
const PENDING_TTL: Duration = Duration::from_secs(3);

const PANEL: Color = Color::Rgb(28, 28, 30);
const ROW_SELECTED: Color = Color::Rgb(36, 36, 40);
const SEGMENT: Color = Color::Rgb(48, 48, 52);
const SEGMENT_HOVER: Color = Color::Rgb(72, 72, 78);
const MUTED: Color = Color::Rgb(128, 128, 134);
const LIST_PANEL: Color = Color::Rgb(40, 40, 44);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SettingsSection {
    Agent,
    Providers,
    Transcript,
    Names,
    Appearance,
    Notifications,
    System,
}

impl SettingsSection {
    pub(crate) const ALL: [Self; 7] = [
        Self::Agent,
        Self::Providers,
        Self::Transcript,
        Self::Names,
        Self::Appearance,
        Self::Notifications,
        Self::System,
    ];

    fn label(self) -> &'static str {
        match self {
            Self::Agent => "Agent",
            Self::Providers => "Providers",
            Self::Transcript => "Transcript",
            Self::Names => "Names & colours",
            Self::Appearance => "Appearance",
            Self::Notifications => "Notifications",
            Self::System => "System",
        }
    }

    fn summary(self) -> &'static str {
        match self {
            Self::Agent => "Model, reasoning and how Borg works",
            Self::Providers => "Custom endpoints, credentials, models and variants",
            Self::Transcript => "What the conversation shows",
            Self::Names => "How you and Borg appear in the conversation",
            Self::Appearance => "Language, cursor, icons and motion",
            Self::Notifications => "How Borg tells you it is done",
            Self::System => "Your machine, clipboard and imports",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SettingId {
    ProviderAdd,
    ProviderEntry(usize),
    ProviderBack,
    ProviderId,
    ProviderName,
    ProviderEndpoint,
    ProviderKeyEnv,
    ProviderKey,
    ProviderClearAuth,
    ProviderHeaders,
    ProviderModelAdd,
    ProviderModelEntry(usize),
    ProviderSave,
    ProviderDelete,
    ProviderModelId,
    ProviderModelName,
    ProviderModelContext,
    ProviderModelOutput,
    ProviderModelCache,
    ProviderModelBody,
    ProviderModelVariants,
    ProviderModelSave,
    ProviderModelDelete,

    Model,
    CommitModel,
    CommitEffort,
    CustomCommitModel,
    StreamingRefreshRate,
    HorizontalMargin,
    ComposerHeight,
    Footer,
    Dictation,
    Effort,
    Speed,
    Access,
    ResponseLanguage,
    ActiveMessages,
    Streaming,
    EditDiffs,
    ToolDetails,
    Thinking,
    ToolClick,
    ActionDescriptors,
    WrapActionRows,
    CompletionTimes,
    UiLanguage,
    UserLabel,
    AssistantLabel,
    UserLabelColour,
    UserMessageColour,
    AssistantLabelColour,
    AssistantMessageColour,
    Cursor,
    Icons,
    Animations,
    RefreshRate,
    Notifications,
    Sound,
    KeepAwake,
    AutoCopy,
    KeyHints,
    LunaTitles,
    LanguageServers,
    Import,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SettingControl {
    Choice {
        options: Vec<String>,
        selected: Option<usize>,
        disabled: Vec<bool>,
    },
    Text(String),
    Secret(String),
    Colour(String, Color),
    Action(String),
}

#[derive(Clone, Debug)]
pub(crate) struct SettingRow {
    pub(crate) id: SettingId,
    pub(crate) section: SettingsSection,
    pub(crate) label: &'static str,
    pub(crate) description: String,
    pub(crate) control: SettingControl,
    pub(crate) enabled: bool,
}

/// Everything the screen displays, read from live state each frame.
#[derive(Clone, Debug)]
pub(crate) struct SettingsValues {
    pub(crate) provider: Option<CodingProvider>,
    pub(crate) model: Option<String>,
    pub(crate) effort: Option<String>,
    pub(crate) fast: bool,
    pub(crate) ultrafast: bool,
    pub(crate) speed_support: borg_remote::SpeedSupport,
    pub(crate) permission_mode: Option<PermissionMode>,
    pub(crate) response_language: Option<ResponseLanguage>,
    pub(crate) steer_active: bool,
    pub(crate) streaming: ResponseStreaming,
    pub(crate) diff_expansion: DiffExpansionPolicy,
    pub(crate) auto_expand_tools: bool,
    pub(crate) auto_expand_thinking: bool,
    pub(crate) tool_click: ToolClickBehavior,
    pub(crate) action_descriptors: bool,
    pub(crate) wrap_action_rows: bool,
    pub(crate) show_done_at: bool,
    pub(crate) ui_language: UiLanguage,
    pub(crate) user_label: String,
    pub(crate) assistant_label: String,
    /// User label, user message, assistant label, assistant message.
    pub(crate) colours: [(String, Color); 4],
    pub(crate) cursor: ComposerCursorStyle,
    pub(crate) icons: DictationIconStyle,
    pub(crate) running_sweeps: bool,
    pub(crate) refresh_fps: u64,
    pub(crate) notifications: CompletionAlertPolicy,
    pub(crate) sound: CompletionAlertPolicy,
    pub(crate) prevent_sleep: bool,
    pub(crate) prevent_lid_sleep: bool,
    pub(crate) auto_copy: bool,
    pub(crate) key_hints: bool,
    pub(crate) luna_titles: bool,
    pub(crate) commit_model: String,
    pub(crate) configured_models: Vec<borg_provider::DynamicModelEntry>,
    pub(crate) custom_providers:
        std::collections::BTreeMap<String, borg_ui::providers::ConfiguredProvider>,
    pub(crate) streaming_refresh_fps: u64,
    pub(crate) layout: borg_ui::preferences::LayoutPreferences,
}

const REFRESH_RATES: [u64; 7] = [30, 60, 90, 120, 144, 165, 240];
const ALERT_OPTIONS: [&str; 3] = ["Off", "When unfocused", "Always"];
const COLOUR_TARGETS: [&str; 4] = [
    "user-label",
    "user-message",
    "assistant-label",
    "assistant-message",
];

fn choice(options: &[&str], selected: Option<usize>) -> SettingControl {
    SettingControl::Choice {
        options: options.iter().map(|option| (*option).to_string()).collect(),
        selected,
        disabled: vec![false; options.len()],
    }
}

fn toggle(on: bool) -> SettingControl {
    choice(&["Off", "On"], Some(usize::from(on)))
}

fn alert_index(policy: CompletionAlertPolicy) -> usize {
    match policy {
        CompletionAlertPolicy::Off => 0,
        CompletionAlertPolicy::Unfocused => 1,
        CompletionAlertPolicy::Always => 2,
    }
}

fn alert_policy(index: usize) -> CompletionAlertPolicy {
    match index {
        0 => CompletionAlertPolicy::Off,
        2 => CompletionAlertPolicy::Always,
        _ => CompletionAlertPolicy::Unfocused,
    }
}

pub(crate) fn setting_rows(values: &SettingsValues) -> Vec<SettingRow> {
    use SettingId as Id;
    use SettingsSection as Section;
    let row = |id, section, label, description: &str, control| SettingRow {
        id,
        section,
        label,
        description: description.to_string(),
        control,
        enabled: true,
    };
    let model_label = match (&values.model, values.provider) {
        (Some(model), Some(provider)) => format!("{model} · {}", provider.label()),
        (Some(model), None) => model.clone(),
        (None, Some(provider)) => format!("Default · {}", provider.label()),
        (None, None) => "Default".to_string(),
    };
    let commit = CommitMessageModel::parse(&values.commit_model);
    let commit_efforts =
        borg_provider::gateway_effort_levels(commit.model_id(), &values.configured_models)
            .unwrap_or_else(|| effort_picker_options(commit.provider(), Some(commit.model_id())));
    let efforts = values
        .model
        .as_deref()
        .and_then(|model| borg_provider::gateway_effort_levels(model, &values.configured_models))
        .unwrap_or_else(|| effort_picker_options(values.provider, values.model.as_deref()));
    let effort = if efforts.is_empty() {
        SettingRow {
            enabled: false,
            ..row(
                Id::Effort,
                Section::Agent,
                "Reasoning effort",
                "This model has no adjustable reasoning effort.",
                choice(&["Not adjustable"], Some(0)),
            )
        }
    } else {
        let optional = values.model.as_deref().is_some_and(|model| {
            borg_provider::gateway_effort_is_optional(model, &values.configured_models)
        }) || effort_is_optional(values.provider, values.model.as_deref());
        row(
            Id::Effort,
            Section::Agent,
            "Reasoning effort",
            "How long the model thinks before it answers. Higher is slower and more thorough.",
            SettingControl::Choice {
                selected: values
                    .effort
                    .as_deref()
                    .and_then(|effort| efforts.iter().position(|option| option == effort)),
                disabled: efforts
                    .iter()
                    .map(|option| !optional && option == "none")
                    .collect(),
                options: efforts,
            },
        )
    };
    let speeds = [
        ("Standard", true),
        ("Fast", values.speed_support.fast),
        ("Ultrafast", values.speed_support.ultrafast),
    ]
    .into_iter()
    .filter_map(|(tier, offered)| offered.then_some(tier))
    .collect::<Vec<_>>();
    let speed_now = if values.ultrafast {
        "Ultrafast"
    } else if values.fast {
        "Fast"
    } else {
        "Standard"
    };
    let speed = SettingRow {
        enabled: speeds.len() > 1,
        ..row(
            Id::Speed,
            Section::Agent,
            "Speed",
            if speeds.len() > 1 {
                "Faster tiers answer sooner where the model offers them."
            } else {
                "This model offers no faster tier."
            },
            choice(&speeds, speeds.iter().position(|tier| *tier == speed_now)),
        )
    };
    let languages = ResponseLanguage::ALL.map(ResponseLanguage::name);
    let ui_languages = UiLanguage::ALL.map(|language| ui_text(values.ui_language, language.name()));
    let refresh_labels = REFRESH_RATES.map(|fps| format!("{fps} fps"));
    let nerd_icons = format!("Nerd Font {DICTATION_NERD_FONT_ICON}");
    let emoji_icons = format!("Emoji {DICTATION_EMOJI_ICON}");
    let colour = |index: usize| {
        let (hex, colour) = &values.colours[index];
        SettingControl::Colour(hex.clone(), *colour)
    };
    let mut rows = vec![
        row(
            Id::Model,
            Section::Agent,
            "Model",
            "The model this session uses. Opens the model list.",
            SettingControl::Action(model_label),
        ),
        effort,
        speed,
        row(
            Id::Access,
            Section::Agent,
            "Access",
            "How much Borg may do before it asks you first.",
            choice(
                &["Full access", "Auto approvals", "Manual approvals"],
                values.permission_mode.map(|mode| match mode {
                    PermissionMode::FullAccess => 0,
                    PermissionMode::Auto => 1,
                    PermissionMode::Manual => 2,
                }),
            ),
        ),
        row(
            Id::ResponseLanguage,
            Section::Agent,
            "Reply language",
            "The language Borg replies and drafts in.",
            choice(
                &languages,
                values.response_language.and_then(|current| {
                    ResponseLanguage::ALL
                        .iter()
                        .position(|language| *language == current)
                }),
            ),
        ),
        row(
            Id::ActiveMessages,
            Section::Agent,
            "Messages while working",
            "A message sent while Borg works can redirect it right away, or wait until the turn ends.",
            choice(
                &["Send right away", "Wait until done"],
                Some(usize::from(!values.steer_active)),
            ),
        ),
        row(
            Id::CommitModel,
            Section::Agent,
            "Commit message model",
            "Independent of chat. Search all supported models and configured providers for click-to-commit drafts.",
            SettingControl::Action(commit.model.clone()),
        ),
        row(
            Id::CustomCommitModel,
            Section::Agent,
            "Custom commit model",
            "Any model-only route: MODEL@EFFORT or PROVIDER::MODEL@EFFORT (for example open_router::vendor/model). External agent-loop routes cannot draft safely.",
            SettingControl::Text(values.commit_model.clone()),
        ),
        SettingRow {
            enabled: !commit_efforts.is_empty(),
            ..row(
                Id::CommitEffort,
                Section::Agent,
                "Commit reasoning",
                "How much reasoning the commit drafter uses. Does not change chat reasoning.",
                SettingControl::Choice {
                    selected: commit_efforts
                        .iter()
                        .position(|effort| *effort == commit.effort),
                    disabled: commit_efforts
                        .iter()
                        .map(|effort| {
                            effort == "none"
                                && !borg_provider::gateway_effort_is_optional(
                                    commit.model_id(),
                                    &values.configured_models,
                                )
                                && !effort_is_optional(commit.provider(), Some(commit.model_id()))
                        })
                        .collect(),
                    options: commit_efforts,
                },
            )
        },
        row(
            Id::Streaming,
            Section::Transcript,
            "Replies appear",
            "Show replies a paragraph at a time, or word by word as they arrive.",
            choice(
                &["By paragraph", "Word by word"],
                Some(usize::from(values.streaming == ResponseStreaming::Token)),
            ),
        ),
        row(
            Id::EditDiffs,
            Section::Transcript,
            "File edits",
            "How file changes appear. \"Until next action\" keeps the latest edit open until Borg moves on.",
            choice(
                &["Expanded", "Collapsed", "Until next action"],
                Some(match values.diff_expansion {
                    DiffExpansionPolicy::Expanded => 0,
                    DiffExpansionPolicy::Collapsed => 1,
                    DiffExpansionPolicy::UntilNextAction => 2,
                }),
            ),
        ),
        row(
            Id::ToolDetails,
            Section::Transcript,
            "Open tool details",
            "Expand the details of every tool call automatically, not just edits.",
            toggle(values.auto_expand_tools),
        ),
        row(
            Id::Thinking,
            Section::Transcript,
            "Show thinking",
            "Keep the model's reasoning expanded while it streams.",
            toggle(values.auto_expand_thinking),
        ),
        row(
            Id::ToolClick,
            Section::Transcript,
            "Clicking a tool",
            "Open a tool's details fullscreen, or expand them where they are.",
            choice(
                &["Fullscreen", "In place"],
                Some(usize::from(values.tool_click == ToolClickBehavior::Inline)),
            ),
        ),
        row(
            Id::ActionDescriptors,
            Section::Transcript,
            "Action summaries",
            "Show the short line Borg writes before each tool call.",
            toggle(values.action_descriptors),
        ),
        row(
            Id::WrapActionRows,
            Section::Transcript,
            "Wrap long actions",
            "Let long action rows wrap onto more lines instead of being cut off.",
            toggle(values.wrap_action_rows),
        ),
        row(
            Id::CompletionTimes,
            Section::Transcript,
            "Completion times",
            "Show when each group of actions finished.",
            toggle(values.show_done_at),
        ),
        row(
            Id::UiLanguage,
            Section::Appearance,
            "Interface language",
            "The language of menus, hints and labels.",
            choice(
                &ui_languages,
                UiLanguage::ALL
                    .iter()
                    .position(|language| *language == values.ui_language),
            ),
        ),
        row(
            Id::UserLabel,
            Section::Names,
            "Your name",
            "The name shown above your messages.",
            SettingControl::Text(values.user_label.clone()),
        ),
        row(
            Id::AssistantLabel,
            Section::Names,
            "Borg's name",
            "The name shown above Borg's replies.",
            SettingControl::Text(values.assistant_label.clone()),
        ),
        row(
            Id::UserLabelColour,
            Section::Names,
            "Your name colour",
            "Colour of your name. Type a hex colour such as #4aa3ff.",
            colour(0),
        ),
        row(
            Id::UserMessageColour,
            Section::Names,
            "Your message colour",
            "Colour of your message text. Type a hex colour such as #ffffff.",
            colour(1),
        ),
        row(
            Id::AssistantLabelColour,
            Section::Names,
            "Borg's name colour",
            "Colour of Borg's name. Type a hex colour such as #ff8e24.",
            colour(2),
        ),
        row(
            Id::AssistantMessageColour,
            Section::Names,
            "Borg's message colour",
            "Colour of Borg's reply text. Type a hex colour such as #ffffff.",
            colour(3),
        ),
        row(
            Id::Cursor,
            Section::Appearance,
            "Cursor",
            "The shape of the text cursor in the message box.",
            choice(
                &["Underline", "Bar", "Block"],
                Some(match values.cursor {
                    ComposerCursorStyle::Underline => 0,
                    ComposerCursorStyle::Bar => 1,
                    ComposerCursorStyle::Block => 2,
                }),
            ),
        ),
        row(
            Id::Icons,
            Section::Appearance,
            "Icons",
            "Pick the set that draws correctly in your terminal font. Used for the microphone and settings buttons.",
            choice(
                &[nerd_icons.as_str(), emoji_icons.as_str()],
                Some(usize::from(values.icons == DictationIconStyle::Emoji)),
            ),
        ),
        row(
            Id::Animations,
            Section::Appearance,
            "Animations",
            "Animate the sweep across rows that are still running.",
            toggle(values.running_sweeps),
        ),
        row(
            Id::RefreshRate,
            Section::Appearance,
            "Refresh rate",
            "How often the screen redraws. Match your display for the smoothest scrolling.",
            choice(
                &refresh_labels
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>(),
                REFRESH_RATES
                    .iter()
                    .position(|fps| *fps == values.refresh_fps),
            ),
        ),
        row(
            Id::StreamingRefreshRate,
            Section::Appearance,
            "Streaming refresh rate",
            "Independent redraw rate while replies stream.",
            choice(
                &refresh_labels
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>(),
                REFRESH_RATES
                    .iter()
                    .position(|fps| *fps == values.streaming_refresh_fps),
            ),
        ),
        row(
            Id::HorizontalMargin,
            Section::Appearance,
            "Horizontal margin",
            "Space on each side of the conversation, in columns.",
            SettingControl::Text(values.layout.horizontal_margin.to_string()),
        ),
        row(
            Id::ComposerHeight,
            Section::Appearance,
            "Message box height",
            "Maximum composer height, in rows (3–30).",
            SettingControl::Text(values.layout.composer_max_height.to_string()),
        ),
        row(
            Id::Footer,
            Section::Appearance,
            "Status footer",
            "Show the bottom status line and working directory.",
            toggle(values.layout.show_footer),
        ),
        row(
            Id::Dictation,
            Section::System,
            "Dictation",
            "Choose local speech recognition, accelerator and icons without starting the microphone.",
            SettingControl::Action("Configure…".to_string()),
        ),
        row(
            Id::Notifications,
            Section::Notifications,
            "Desktop notification",
            "Show a desktop notification when Borg finishes a turn.",
            choice(&ALERT_OPTIONS, Some(alert_index(values.notifications))),
        ),
        row(
            Id::Sound,
            Section::Notifications,
            "Sound",
            "Play a sound when Borg finishes a turn.",
            choice(&ALERT_OPTIONS, Some(alert_index(values.sound))),
        ),
        row(
            Id::KeepAwake,
            Section::System,
            "Keep computer awake",
            "Stop the computer from sleeping while Borg is working.",
            choice(
                &["Off", "On", "Even with lid closed"],
                Some(match (values.prevent_sleep, values.prevent_lid_sleep) {
                    (false, _) => 0,
                    (true, false) => 1,
                    (true, true) => 2,
                }),
            ),
        ),
        row(
            Id::AutoCopy,
            Section::System,
            "Copy on select",
            "Copy text to the clipboard as soon as you select it with the mouse.",
            toggle(values.auto_copy),
        ),
        row(
            Id::KeyHints,
            Section::System,
            "Shortcut numbers",
            "Show numbered Ctrl/Cmd shortcuts on the status controls.",
            toggle(values.key_hints),
        ),
        row(
            Id::LunaTitles,
            Section::System,
            "Luna thread titles",
            "Send each new thread's first message to your Codex subscription so Luna can name it, whichever provider runs the thread.",
            toggle(values.luna_titles),
        ),
        row(
            Id::LanguageServers,
            Section::System,
            "Language servers",
            "See which language servers Borg can use for code navigation and diagnostics.",
            SettingControl::Action("Show status".to_string()),
        ),
        row(
            Id::Import,
            Section::System,
            "Import",
            "Bring threads and memory over from Codex, Claude Code, Claude Desktop or other apps. Originals stay untouched.",
            SettingControl::Action("Choose a source".to_string()),
        ),
    ];
    rows.extend(providers::list_rows(&values.custom_providers));
    rows
}

/// The action that applies option `index` of a choice row.
fn choice_action(row: &SettingRow, index: usize) -> Option<UiAction> {
    use SettingId as Id;
    let SettingControl::Choice { options, .. } = &row.control else {
        return None;
    };
    let option = options.get(index)?;
    let on = index == 1;
    Some(match row.id {
        Id::CommitEffort => UiAction::SetCommitMessageEffort(option.clone()),
        Id::StreamingRefreshRate => UiAction::Submit {
            target: None,
            text: format!("/refresh streaming {}", REFRESH_RATES[index]),
            attachments: Vec::new(),
        },
        Id::Footer => UiAction::SetShowFooter(on),
        Id::Effort => UiAction::SetEffort(option.clone()),
        Id::Speed => match option.as_str() {
            "Ultrafast" => UiAction::SetUltrafast(true),
            "Fast" => UiAction::SetFast(true),
            _ => UiAction::SetFast(false),
        },
        Id::Access => UiAction::SetPermissionMode(match index {
            0 => PermissionMode::FullAccess,
            1 => PermissionMode::Auto,
            _ => PermissionMode::Manual,
        }),
        Id::ResponseLanguage => UiAction::SetResponseLanguage(*ResponseLanguage::ALL.get(index)?),
        Id::ActiveMessages => UiAction::SetSteerActive(index == 0),
        Id::Streaming => UiAction::SetResponseStreaming(if on {
            ResponseStreaming::Token
        } else {
            ResponseStreaming::Paragraph
        }),
        Id::EditDiffs => UiAction::SetDiffExpansion(match index {
            0 => DiffExpansionPolicy::Expanded,
            1 => DiffExpansionPolicy::Collapsed,
            _ => DiffExpansionPolicy::UntilNextAction,
        }),
        Id::ToolDetails => UiAction::SetAutoExpandTools(on),
        Id::Thinking => UiAction::SetAutoExpandThinking(on),
        Id::ToolClick => UiAction::SetToolClickBehavior(if on {
            ToolClickBehavior::Inline
        } else {
            ToolClickBehavior::Fullscreen
        }),
        Id::ActionDescriptors => UiAction::SetActionDescriptors(on),
        Id::WrapActionRows => UiAction::SetWrapActionRows(on),
        Id::CompletionTimes => UiAction::SetShowActionDoneAt(on),
        Id::UiLanguage => UiAction::SetUiLanguage(*UiLanguage::ALL.get(index)?),
        Id::Cursor => UiAction::SetComposerCursorStyle(match index {
            0 => ComposerCursorStyle::Underline,
            1 => ComposerCursorStyle::Bar,
            _ => ComposerCursorStyle::Block,
        }),
        Id::Icons => UiAction::SetDictationIcon(if on {
            DictationIconStyle::Emoji
        } else {
            DictationIconStyle::NerdFont
        }),
        Id::Animations => UiAction::SetRunningSweeps(on),
        Id::RefreshRate => UiAction::SetRefreshRate(*REFRESH_RATES.get(index)?),
        Id::Notifications => UiAction::SetCompletionNotifications(alert_policy(index)),
        Id::Sound => UiAction::SetCompletionSound(alert_policy(index)),
        Id::KeepAwake => UiAction::SetPreventSleep {
            enabled: index > 0,
            lid: index == 2,
        },
        Id::AutoCopy => UiAction::SetAutoCopySelection(on),
        Id::KeyHints => UiAction::SetKeyHints(on),
        Id::LunaTitles => UiAction::SetLunaTitlesForAllProviders(on),
        _ => return None,
    })
}

/// What the host must do after the screen handled an input.
#[derive(Debug)]
pub(crate) enum SettingsEffect {
    None,
    Close,
    Apply(Box<UiAction>),
    OpenModelPicker,
    OpenCommitModelPicker,
    OpenDictation,
    /// Close the screen, then run a slash command whose result shows in the
    /// conversation.
    CloseAndRun(&'static str),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Target {
    Close,
    Section(usize),
    Row(usize),
    Option(usize, usize),
    Previous(usize),
    Next(usize),
    Value(usize),
    ListOption(usize),
}

#[derive(Default)]
pub(crate) struct SettingsScreen {
    custom_providers: std::collections::BTreeMap<String, borg_ui::providers::ConfiguredProvider>,
    provider_editor: Option<providers::ProviderEditor>,
    section: usize,
    /// Index into the visible rows.
    selected: usize,
    scroll: usize,
    query: String,
    editing: Option<(SettingId, String)>,
    error: Option<String>,
    /// An open option list: the row id and the highlighted option.
    list: Option<(SettingId, usize)>,
    pending: Vec<(SettingId, usize, Instant)>,
    hits: Vec<(Rect, Target)>,
    hovered: Option<Target>,
}

impl SettingsScreen {
    /// Rows to show, with choices the host has not confirmed yet shown as
    /// chosen so a click never appears to bounce back.
    pub(crate) fn rows(&mut self, values: &SettingsValues) -> Vec<SettingRow> {
        self.custom_providers = values.custom_providers.clone();
        let mut rows = setting_rows(values);
        if let Some(editor) = &self.provider_editor {
            rows.retain(|row| row.section != SettingsSection::Providers);
            rows.extend(providers::editor_rows(editor));
        }
        self.pending.retain(|(id, index, at)| {
            if at.elapsed() > PENDING_TTL {
                return false;
            }
            let Some(SettingControl::Choice { selected, .. }) = rows
                .iter_mut()
                .find(|row| row.id == *id)
                .map(|row| &mut row.control)
            else {
                return false;
            };
            if *selected == Some(*index) {
                return false;
            }
            *selected = Some(*index);
            true
        });
        rows
    }

    fn visible(&self, rows: &[SettingRow]) -> Vec<usize> {
        let query = self.query.trim().to_lowercase();
        rows.iter()
            .enumerate()
            .filter(|(_, row)| {
                if query.is_empty() {
                    row.section == SettingsSection::ALL[self.section]
                } else {
                    row.label.to_lowercase().contains(&query)
                        || row.description.to_lowercase().contains(&query)
                        || row.section.label().to_lowercase().contains(&query)
                }
            })
            .map(|(index, _)| index)
            .collect()
    }

    fn selected_row<'a>(&self, rows: &'a [SettingRow]) -> Option<&'a SettingRow> {
        self.visible(rows)
            .get(self.selected)
            .map(|index| &rows[*index])
    }

    fn set_section(&mut self, section: usize) {
        self.section = section % SettingsSection::ALL.len();
        self.query.clear();
        self.selected = 0;
        self.scroll = 0;
        self.list = None;
        self.editing = None;
        self.error = None;
    }

    fn move_selection(&mut self, rows: &[SettingRow], delta: isize) {
        let count = self.visible(rows).len();
        if count == 0 {
            return;
        }
        self.selected = self.selected.saturating_add_signed(delta).min(count - 1);
        self.error = None;
    }

    fn choose(&mut self, row: &SettingRow, index: usize) -> SettingsEffect {
        self.list = None;
        if !row.enabled {
            return SettingsEffect::None;
        }
        let SettingControl::Choice {
            selected, disabled, ..
        } = &row.control
        else {
            return SettingsEffect::None;
        };
        if disabled.get(index).copied().unwrap_or(true) || *selected == Some(index) {
            return SettingsEffect::None;
        }
        match choice_action(row, index) {
            Some(action) => {
                self.pending.retain(|(id, ..)| *id != row.id);
                self.pending.push((row.id, index, Instant::now()));
                SettingsEffect::Apply(Box::new(action))
            }
            None => SettingsEffect::None,
        }
    }

    /// The next enabled option after the current one, wrapping or not.
    fn step(row: &SettingRow, forward: bool, wrap: bool) -> Option<usize> {
        let SettingControl::Choice {
            options,
            selected,
            disabled,
        } = &row.control
        else {
            return None;
        };
        let count = options.len();
        let mut index = selected.unwrap_or(if forward { count - 1 } else { 0 });
        for _ in 0..count {
            index = match (forward, index) {
                (true, index) if index + 1 < count => index + 1,
                (true, _) if wrap => 0,
                (false, 0) if wrap => count - 1,
                (false, index) if index > 0 => index - 1,
                _ => return None,
            };
            if !disabled[index] {
                return Some(index);
            }
        }
        None
    }

    fn activate(&mut self, row: &SettingRow) -> SettingsEffect {
        if !row.enabled {
            return SettingsEffect::None;
        }
        if matches!(row.control, SettingControl::Action(_))
            && let Some(effect) = self.provider_action(row.id)
        {
            return effect;
        }
        match &row.control {
            SettingControl::Secret(_) => {
                self.editing = Some((row.id, String::new()));
                self.error = None;
                SettingsEffect::None
            }
            SettingControl::Choice {
                options, selected, ..
            } => {
                if options.len() > MAX_SEGMENTS {
                    self.list = Some((row.id, selected.unwrap_or(0)));
                    SettingsEffect::None
                } else {
                    match Self::step(row, true, true) {
                        Some(index) => self.choose(row, index),
                        None => SettingsEffect::None,
                    }
                }
            }
            SettingControl::Text(value) | SettingControl::Colour(value, _) => {
                self.editing = Some((row.id, value.clone()));
                self.error = None;
                SettingsEffect::None
            }
            SettingControl::Action(_) => match row.id {
                SettingId::CommitModel => SettingsEffect::OpenCommitModelPicker,
                SettingId::Dictation => SettingsEffect::OpenDictation,
                SettingId::Model => SettingsEffect::OpenModelPicker,
                SettingId::LanguageServers => SettingsEffect::CloseAndRun("/lsp"),
                SettingId::Import => SettingsEffect::CloseAndRun("/import"),
                _ => SettingsEffect::None,
            },
        }
    }

    fn commit_edit(&mut self) -> SettingsEffect {
        let Some((id, value)) = self.editing.take() else {
            return SettingsEffect::None;
        };
        let value = value.trim();
        if let Some(result) = self.provider_edit(id, value) {
            match result {
                Ok(()) => self.error = None,
                Err(error) => {
                    self.error = Some(error);
                    self.editing = Some((id, value.to_string()));
                }
            }
            return SettingsEffect::None;
        }
        let command = match id {
            SettingId::CustomCommitModel => {
                let model = CommitMessageModel::parse(value);
                if value.is_empty()
                    || model.model_id().is_empty()
                    || matches!(
                        model.provider(),
                        Some(CodingProvider::Grok | CodingProvider::Muse)
                    )
                    || (model.model.starts_with("opencode/")
                        && !model.model.starts_with("opencode-go/"))
                {
                    self.error =
                        Some("Use a model-only route, not an external agent loop.".to_string());
                    self.editing = Some((id, value.to_string()));
                    return SettingsEffect::None;
                }
                self.error = None;
                return SettingsEffect::Apply(Box::new(UiAction::SetCommitMessageModel(
                    model.label(),
                )));
            }
            SettingId::HorizontalMargin | SettingId::ComposerHeight => {
                let max = if id == SettingId::HorizontalMargin {
                    40
                } else {
                    30
                };
                let min = if id == SettingId::HorizontalMargin {
                    0
                } else {
                    3
                };
                let Ok(number) = value.parse::<u16>() else {
                    self.error = Some(format!("Enter a number from {min} to {max}."));
                    self.editing = Some((id, value.to_string()));
                    return SettingsEffect::None;
                };
                if !(min..=max).contains(&number) {
                    self.error = Some(format!("Enter a number from {min} to {max}."));
                    self.editing = Some((id, value.to_string()));
                    return SettingsEffect::None;
                }
                self.error = None;
                return SettingsEffect::Apply(Box::new(if id == SettingId::HorizontalMargin {
                    UiAction::SetHorizontalMargin(number)
                } else {
                    UiAction::SetComposerMaxHeight(number)
                }));
            }
            SettingId::UserLabel | SettingId::AssistantLabel => {
                if value.is_empty() {
                    self.error = Some("A name can't be empty.".to_string());
                    self.editing = Some((id, String::new()));
                    return SettingsEffect::None;
                }
                let command = if id == SettingId::UserLabel {
                    "/user-label"
                } else {
                    "/assistant-label"
                };
                format!("{command} {value}")
            }
            _ => {
                let target = match id {
                    SettingId::UserLabelColour => COLOUR_TARGETS[0],
                    SettingId::UserMessageColour => COLOUR_TARGETS[1],
                    SettingId::AssistantLabelColour => COLOUR_TARGETS[2],
                    _ => COLOUR_TARGETS[3],
                };
                let hex = if value.starts_with('#') {
                    value.to_string()
                } else {
                    format!("#{value}")
                };
                if parse_hex_color(&hex).is_err() {
                    self.error = Some("Use a hex colour such as #4aa3ff.".to_string());
                    self.editing = Some((id, value.to_string()));
                    return SettingsEffect::None;
                }
                format!("/color {target} {}", hex.to_ascii_lowercase())
            }
        };
        self.error = None;
        SettingsEffect::Apply(Box::new(UiAction::Submit {
            target: None,
            text: command,
            attachments: Vec::new(),
        }))
    }

    /// Pasted text goes to the field being edited, otherwise to the search.
    pub(crate) fn paste(&mut self, text: &str) {
        let joined;
        let text = if self.editing.as_ref().is_some_and(|(id, _)| {
            matches!(
                id,
                SettingId::ProviderHeaders
                    | SettingId::ProviderModelBody
                    | SettingId::ProviderModelVariants
            )
        }) {
            joined = text.replace(['\n', '\r'], " ");
            &joined
        } else {
            text.lines().next().unwrap_or_default()
        };
        match self.editing.as_mut() {
            Some((_, buffer)) => buffer.push_str(text),
            None => {
                self.query.push_str(text);
                self.selected = 0;
                self.scroll = 0;
            }
        }
    }

    pub(crate) fn handle_key(&mut self, key: KeyEvent, rows: &[SettingRow]) -> SettingsEffect {
        if key.kind == KeyEventKind::Release {
            return SettingsEffect::None;
        }
        let plain = !key
            .modifiers
            .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT | KeyModifiers::SUPER);
        if let Some((_, buffer)) = self.editing.as_mut() {
            match key.code {
                KeyCode::Esc => {
                    self.editing = None;
                    self.error = None;
                }
                KeyCode::Enter => return self.commit_edit(),
                KeyCode::Backspace => {
                    buffer.pop();
                }
                KeyCode::Char(character) if plain => buffer.push(character),
                _ => {}
            }
            return SettingsEffect::None;
        }
        if let Some((id, highlighted)) = self.list {
            let Some(row) = rows.iter().find(|row| row.id == id) else {
                self.list = None;
                return SettingsEffect::None;
            };
            let SettingControl::Choice { options, .. } = &row.control else {
                self.list = None;
                return SettingsEffect::None;
            };
            match key.code {
                KeyCode::Esc => self.list = None,
                KeyCode::Up => self.list = Some((id, highlighted.saturating_sub(1))),
                KeyCode::Down => {
                    self.list = Some((id, (highlighted + 1).min(options.len() - 1)));
                }
                KeyCode::Home => self.list = Some((id, 0)),
                KeyCode::End => self.list = Some((id, options.len() - 1)),
                KeyCode::Enter | KeyCode::Char(' ') => return self.choose(row, highlighted),
                _ => {}
            }
            return SettingsEffect::None;
        }
        match key.code {
            KeyCode::Esc if !self.query.is_empty() => {
                self.query.clear();
                self.selected = 0;
                self.scroll = 0;
            }
            KeyCode::Esc if self.provider_editor.is_some() => {
                return self
                    .provider_action(SettingId::ProviderBack)
                    .unwrap_or(SettingsEffect::None);
            }
            KeyCode::Esc => return SettingsEffect::Close,
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                return SettingsEffect::Close;
            }
            KeyCode::Up => self.move_selection(rows, -1),
            KeyCode::Down => self.move_selection(rows, 1),
            KeyCode::Home => self.selected = 0,
            KeyCode::End => self.move_selection(rows, isize::MAX),
            KeyCode::Tab | KeyCode::PageDown => self.set_section(self.section + 1),
            KeyCode::BackTab | KeyCode::PageUp => {
                self.set_section(self.section + SettingsSection::ALL.len() - 1);
            }
            KeyCode::Left | KeyCode::Right => {
                if let Some(row) = self.selected_row(rows).filter(|row| row.enabled) {
                    let forward = key.code == KeyCode::Right;
                    if let Some(index) = Self::step(row, forward, false) {
                        return self.choose(row, index);
                    }
                    if forward && matches!(row.control, SettingControl::Action(_)) {
                        return self.activate(row);
                    }
                }
            }
            KeyCode::Enter => {
                if let Some(row) = self.selected_row(rows) {
                    return self.activate(row);
                }
            }
            KeyCode::Char(' ') if self.query.is_empty() => {
                if let Some(row) = self.selected_row(rows) {
                    return self.activate(row);
                }
            }
            KeyCode::Backspace => {
                self.query.pop();
                self.selected = 0;
                self.scroll = 0;
            }
            KeyCode::Char(character) if plain => {
                self.query.push(character);
                self.selected = 0;
                self.scroll = 0;
                self.error = None;
            }
            _ => {}
        }
        SettingsEffect::None
    }

    /// Handle a mouse event; the flag reports whether the screen changed.
    pub(crate) fn handle_mouse(
        &mut self,
        mouse: MouseEvent,
        rows: &[SettingRow],
    ) -> (SettingsEffect, bool) {
        let pointer = Position::new(mouse.column, mouse.row);
        let target = self
            .hits
            .iter()
            .rev()
            .find_map(|(area, target)| area.contains(pointer).then_some(*target));
        let hover_changed = self.hovered != target;
        self.hovered = target;
        match mouse.kind {
            MouseEventKind::ScrollUp => {
                self.move_selection(rows, -1);
                (SettingsEffect::None, true)
            }
            MouseEventKind::ScrollDown => {
                self.move_selection(rows, 1);
                (SettingsEffect::None, true)
            }
            MouseEventKind::Down(MouseButton::Left) => (self.click(target, rows), true),
            _ => (SettingsEffect::None, hover_changed),
        }
    }

    fn click(&mut self, target: Option<Target>, rows: &[SettingRow]) -> SettingsEffect {
        // Clicking away from the field being edited saves it, like a blur.
        if self.editing.is_some()
            && !matches!(target, Some(Target::Value(index)) if self.is_editing(rows, index))
        {
            return self.commit_edit();
        }
        let visible = self.visible(rows);
        let row_at = |index: usize| visible.get(index).map(|row| &rows[*row]);
        let list_open = self.list.take();
        match target {
            None => SettingsEffect::None,
            Some(Target::Close) => SettingsEffect::Close,
            Some(Target::Section(section)) => {
                self.set_section(section);
                SettingsEffect::None
            }
            Some(Target::ListOption(index)) => {
                let Some((id, _)) = list_open else {
                    return SettingsEffect::None;
                };
                match rows.iter().find(|row| row.id == id) {
                    Some(row) => self.choose(row, index),
                    None => SettingsEffect::None,
                }
            }
            Some(Target::Row(index)) => {
                self.selected = index;
                self.error = None;
                match row_at(index) {
                    Some(row) if matches!(row.control, SettingControl::Action(_)) => {
                        self.activate(row)
                    }
                    _ => SettingsEffect::None,
                }
            }
            Some(Target::Option(index, option)) => {
                self.selected = index;
                match row_at(index) {
                    Some(row) => self.choose(row, option),
                    None => SettingsEffect::None,
                }
            }
            Some(Target::Previous(index) | Target::Next(index)) => {
                self.selected = index;
                let forward = matches!(target, Some(Target::Next(_)));
                match row_at(index)
                    .and_then(|row| Self::step(row, forward, false).map(|i| (row, i)))
                {
                    Some((row, option)) => self.choose(row, option),
                    None => SettingsEffect::None,
                }
            }
            Some(Target::Value(index)) => {
                self.selected = index;
                if list_open.is_some_and(|(id, _)| row_at(index).is_some_and(|row| row.id == id)) {
                    return SettingsEffect::None;
                }
                if self.is_editing(rows, index) {
                    return SettingsEffect::None;
                }
                match row_at(index) {
                    Some(row) => self.activate(row),
                    None => SettingsEffect::None,
                }
            }
        }
    }

    fn is_editing(&self, rows: &[SettingRow], index: usize) -> bool {
        let visible = self.visible(rows);
        self.editing
            .as_ref()
            .is_some_and(|(id, _)| visible.get(index).is_some_and(|row| rows[*row].id == *id))
    }

    /// Draw the whole screen. Returns nothing; the caret is placed on the
    /// frame when a value is being edited.
    pub(crate) fn render(
        &mut self,
        frame: &mut Frame<'_>,
        rows: &[SettingRow],
        notice: Option<&str>,
        icon: &str,
    ) {
        self.hits.clear();
        let full = frame.area();
        frame.render_widget(Clear, full);
        let buffer = frame.buffer_mut();
        buffer.set_style(full, Style::default().bg(Color::Reset));
        let width = full.width.min(MAX_WIDTH);
        let margin = if width >= 48 { 2 } else { 0 };
        let height = full.height.min(MAX_HEIGHT);
        let area = Rect {
            x: full.x + (full.width - width) / 2 + margin,
            y: full.y + (full.height - height) / 2,
            width: width.saturating_sub(margin * 2),
            height,
        };
        if area.width < 20 || area.height < 8 {
            buffer.set_string(
                area.x,
                area.y,
                "Settings need a larger window · Esc to close",
                Style::default().fg(MUTED),
            );
            return;
        }
        let visible = self.visible(rows);
        self.selected = self.selected.min(visible.len().saturating_sub(1));

        // Header: title, search, close.
        let header_y = area.y + 1;
        let title = format!("{icon} Settings");
        let mut x = area.x;
        x = put(
            buffer,
            x,
            header_y,
            area.right(),
            &title,
            bold(Color::White),
        );
        let close = " ✕ Close ";
        let close_width = close.width() as u16;
        let close_x = area.right().saturating_sub(close_width);
        let close_area = Rect::new(close_x, header_y, close_width, 1);
        let close_style = if self.hovered == Some(Target::Close) {
            Style::default().fg(Color::Black).bg(Color::White)
        } else {
            Style::default().fg(MUTED)
        };
        put(buffer, close_x, header_y, area.right(), close, close_style);
        self.hits.push((close_area, Target::Close));
        let search_right = close_x.saturating_sub(2);
        let search_width = 34.min(search_right.saturating_sub(x + 3));
        if search_width >= 12 {
            let search_x = search_right - search_width;
            let field = Rect::new(search_x, header_y, search_width, 1);
            buffer.set_style(field, Style::default().bg(PANEL));
            let (text, style) = if self.query.is_empty() {
                (
                    "⌕ Type to search".to_string(),
                    Style::default().fg(MUTED).bg(PANEL),
                )
            } else {
                (
                    format!("⌕ {}▏", self.query),
                    Style::default().fg(Color::White).bg(PANEL),
                )
            };
            put(buffer, search_x + 1, header_y, field.right(), &text, style);
        }
        let rule_y = header_y + 1;
        put(
            buffer,
            area.x,
            rule_y,
            area.right(),
            &"─".repeat(area.width as usize),
            Style::default().fg(Color::Rgb(54, 54, 58)),
        );

        // Footer and description occupy the bottom.
        let footer_y = area.bottom().saturating_sub(1);
        let description_y = footer_y.saturating_sub(3);
        let body = Rect {
            x: area.x,
            y: rule_y + 2,
            width: area.width,
            height: description_y.saturating_sub(rule_y + 3),
        };

        // Sidebar.
        let sidebar = if area.width >= 60 { SIDEBAR_WIDTH } else { 0 };
        if sidebar > 0 {
            let spacing = if body.height as usize >= SettingsSection::ALL.len() * 2 - 1 {
                2
            } else {
                1
            };
            for (index, section) in SettingsSection::ALL.iter().enumerate() {
                let y = body.y + (index as u16) * spacing;
                if y >= body.bottom() {
                    break;
                }
                let current = self.query.is_empty() && index == self.section;
                let hovered = self.hovered == Some(Target::Section(index));
                let cell = Rect::new(body.x, y, sidebar, 1);
                let (marker, style) = if current {
                    ("▌ ", bold(BORG_ORANGE).bg(ROW_SELECTED))
                } else if hovered {
                    ("  ", Style::default().fg(Color::White).bg(PANEL))
                } else {
                    ("  ", Style::default().fg(Color::Gray))
                };
                buffer.set_style(cell, style);
                put(
                    buffer,
                    cell.x,
                    y,
                    cell.right(),
                    &format!("{marker}{}", section.label()),
                    style,
                );
                self.hits.push((cell, Target::Section(index)));
            }
        }
        let content = Rect {
            x: body.x + if sidebar > 0 { sidebar + 3 } else { 0 },
            y: body.y,
            width: body
                .width
                .saturating_sub(if sidebar > 0 { sidebar + 3 } else { 0 }),
            height: body.height,
        };

        // Content heading.
        if self.query.is_empty() {
            let section = SettingsSection::ALL[self.section];
            let x = put(
                buffer,
                content.x + 2,
                content.y,
                content.right(),
                section.label(),
                bold(Color::White),
            );
            put(
                buffer,
                x + 2,
                content.y,
                content.right(),
                section.summary(),
                Style::default().fg(MUTED),
            );
        } else {
            let heading = match visible.len() {
                0 => format!("Nothing matches “{}”", self.query.trim()),
                1 => "1 setting".to_string(),
                count => format!("{count} settings"),
            };
            put(
                buffer,
                content.x + 2,
                content.y,
                content.right(),
                &heading,
                bold(Color::White),
            );
        }

        // Rows.
        let list = Rect {
            x: content.x,
            y: content.y + 2,
            width: content.width,
            height: content.height.saturating_sub(2),
        };
        let spacing: u16 = if list.height as usize >= (visible.len() * 2).saturating_sub(1) {
            2
        } else {
            1
        };
        let capacity = list.height.div_ceil(spacing).max(1) as usize;
        if self.selected < self.scroll {
            self.scroll = self.selected;
        } else if self.selected >= self.scroll + capacity {
            self.scroll = self.selected + 1 - capacity;
        }
        self.scroll = self.scroll.min(visible.len().saturating_sub(capacity));
        let mut list_anchor = None;
        let mut caret = None;
        for (slot, (index, row)) in visible
            .iter()
            .map(|row| &rows[*row])
            .enumerate()
            .skip(self.scroll)
            .take(capacity)
            .enumerate()
        {
            let y = list.y + slot as u16 * spacing;
            let selected = index == self.selected;
            let row_area = Rect::new(list.x, y, list.width, 1);
            if selected {
                buffer.set_style(row_area, Style::default().bg(ROW_SELECTED));
            }
            self.hits.push((row_area, Target::Row(index)));
            let label_style = match (row.enabled, selected) {
                (false, _) => Style::default().fg(Color::DarkGray),
                (true, true) => bold(Color::White),
                (true, false) => Style::default().fg(Color::Gray),
            };
            let marker = if selected { "▌ " } else { "  " };
            let marker_style = Style::default().fg(BORG_ORANGE);
            let x = put(buffer, list.x, y, list.right(), marker, marker_style);
            let mut label = row.label.to_string();
            if !self.query.is_empty() {
                label = format!("{label}  ");
            }
            let after_label = put(buffer, x, y, list.right(), &label, label_style);
            if !self.query.is_empty() {
                put(
                    buffer,
                    after_label,
                    y,
                    list.right(),
                    row.section.label(),
                    Style::default().fg(Color::DarkGray),
                );
            }
            let control_right = list.right().saturating_sub(1);
            let control_left = (list.x + 2 + 26).min(control_right);
            let editing = self
                .editing
                .as_ref()
                .filter(|(id, _)| *id == row.id)
                .map(|(_, buffer)| buffer.clone());
            if let Some(position) = self.render_control(
                buffer,
                row,
                index,
                y,
                control_left,
                control_right,
                editing.as_deref(),
            ) {
                caret = Some(position);
            }
            if self.list.is_some_and(|(id, _)| id == row.id) {
                list_anchor = Some((row, y));
            }
        }
        if visible.len() > self.scroll + capacity {
            put(
                buffer,
                list.right().saturating_sub(8),
                list.bottom().saturating_sub(1),
                list.right(),
                "↓ more",
                Style::default().fg(Color::DarkGray),
            );
        }

        // Description of the focused row.
        put(
            buffer,
            content.x,
            description_y,
            content.right(),
            &"─".repeat(content.width as usize),
            Style::default().fg(Color::Rgb(44, 44, 48)),
        );
        let (description, description_style) = if let Some(error) = &self.error {
            (error.clone(), Style::default().fg(Color::LightRed))
        } else if self.editing.is_some() {
            (
                "Type, then Enter to save · Esc to cancel".to_string(),
                Style::default().fg(Color::Gray),
            )
        } else {
            (
                self.selected_row(rows)
                    .map(|row| row.description.clone())
                    .unwrap_or_default(),
                Style::default().fg(Color::Gray),
            )
        };
        for (line, text) in wrap_display(&description, content.width.saturating_sub(2) as usize)
            .into_iter()
            .take(2)
            .enumerate()
        {
            put(
                buffer,
                content.x + 2,
                description_y + 1 + line as u16,
                content.right(),
                &text,
                description_style,
            );
        }

        // Footer: keys on the left, the latest confirmation on the right.
        let keys: &[&str] = if self.editing.is_some() {
            &["Enter save", "Esc cancel"]
        } else if self.list.is_some() {
            &["↑↓ choose", "Enter select", "Esc close list"]
        } else if !self.query.is_empty() {
            &["↑↓ move", "←→ change", "Enter select", "Esc clear search"]
        } else {
            &[
                "↑↓ move",
                "←→ change",
                "Enter select",
                "Tab next section",
                "type to search",
                "Esc close",
            ]
        };
        let notice = notice
            .filter(|notice| !notice.is_empty())
            .map(|notice| format!("✓ {notice}"));
        let notice_width = notice.as_ref().map_or(0, |notice| notice.width() + 3);
        // Drop hints from the middle until the hints and the latest
        // confirmation both fit; Esc to close always stays.
        let mut shown = keys.to_vec();
        while shown.len() > 1 && shown.join(" · ").width() + notice_width > area.width as usize {
            shown.remove(shown.len() - 2);
        }
        let keys_end = put(
            buffer,
            area.x,
            footer_y,
            area.right(),
            &shown.join(" · "),
            Style::default().fg(Color::DarkGray),
        );
        if let Some(notice) = notice {
            let room = area.right().saturating_sub(keys_end + 3) as usize;
            if room > 8 {
                let text = truncate_display(&notice, room);
                let x = area.right().saturating_sub(text.width() as u16);
                put(
                    buffer,
                    x,
                    footer_y,
                    area.right(),
                    &text,
                    Style::default().fg(Color::LightGreen),
                );
            }
        }

        if let Some((row, y)) = list_anchor {
            self.render_list(buffer, row, y, list, area.bottom().saturating_sub(1));
        }
        if let Some(caret) = caret {
            frame.set_cursor_position(caret);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn render_control(
        &mut self,
        buffer: &mut Buffer,
        row: &SettingRow,
        index: usize,
        y: u16,
        left: u16,
        right: u16,
        editing: Option<&str>,
    ) -> Option<Position> {
        let enabled = row.enabled;
        match &row.control {
            SettingControl::Choice {
                options,
                selected,
                disabled,
            } => {
                let segments_width: usize = options
                    .iter()
                    .map(|option| option.width() + 2)
                    .sum::<usize>()
                    + options.len().saturating_sub(1);
                let room = right.saturating_sub(left) as usize;
                if options.len() <= MAX_SEGMENTS && segments_width <= room {
                    let mut x = right.saturating_sub(segments_width as u16);
                    for (option_index, option) in options.iter().enumerate() {
                        let text = format!(" {option} ");
                        let width = text.width() as u16;
                        let target = Target::Option(index, option_index);
                        let style = segment_style(
                            enabled && !disabled[option_index],
                            *selected == Some(option_index),
                            self.hovered == Some(target),
                        );
                        put(buffer, x, y, right, &text, style);
                        self.hits.push((Rect::new(x, y, width, 1), target));
                        x += width + 1;
                    }
                } else {
                    let value = selected
                        .and_then(|selected| options.get(selected))
                        .map_or("Choose", String::as_str);
                    let value = format!(" {value} ▾ ");
                    let width = value.width() as u16;
                    let start = right.saturating_sub(width + 4);
                    let previous = Target::Previous(index);
                    let next = Target::Next(index);
                    let arrow = |hovered: bool| {
                        if !enabled {
                            Style::default().fg(Color::DarkGray)
                        } else if hovered {
                            bold(Color::White)
                        } else {
                            Style::default().fg(MUTED)
                        }
                    };
                    put(
                        buffer,
                        start,
                        y,
                        right,
                        "‹ ",
                        arrow(self.hovered == Some(previous)),
                    );
                    self.hits.push((Rect::new(start, y, 2, 1), previous));
                    let value_target = Target::Value(index);
                    let style = segment_style(
                        enabled,
                        selected.is_some(),
                        self.hovered == Some(value_target),
                    );
                    put(buffer, start + 2, y, right, &value, style);
                    self.hits
                        .push((Rect::new(start + 2, y, width, 1), value_target));
                    put(
                        buffer,
                        start + 2 + width,
                        y,
                        right + 1,
                        " ›",
                        arrow(self.hovered == Some(next)),
                    );
                    self.hits
                        .push((Rect::new(start + 2 + width, y, 2, 1), next));
                }
                None
            }
            SettingControl::Text(value)
            | SettingControl::Colour(value, _)
            | SettingControl::Secret(value) => {
                let secret = matches!(row.control, SettingControl::Secret(_));
                let masked_value = if value.is_empty() {
                    "Not set"
                } else {
                    "••••••••"
                };
                let masked_editing = editing.map(|value| "•".repeat(value.chars().count()));
                let value = if secret { masked_value } else { value.as_str() };
                let editing = if secret {
                    masked_editing.as_deref()
                } else {
                    editing
                };
                // While a colour is typed, the swatch previews it once valid.
                let swatch = match &row.control {
                    SettingControl::Colour(_, colour) => Some(
                        editing
                            .map(|typed| {
                                if typed.starts_with('#') {
                                    typed.to_string()
                                } else {
                                    format!("#{typed}")
                                }
                            })
                            .and_then(|typed| parse_hex_color(&typed).ok())
                            .map_or(*colour, |(red, green, blue)| Color::Rgb(red, green, blue)),
                    ),
                    _ => None,
                };
                let target = Target::Value(index);
                let (text, style) = match editing {
                    Some(buffer) => (
                        format!(" {buffer} "),
                        Style::default().fg(Color::White).bg(SEGMENT_HOVER),
                    ),
                    None => (
                        format!(" {value} ✎ "),
                        segment_style(enabled, false, self.hovered == Some(target)),
                    ),
                };
                let field_width = (text.width() as u16).max(16);
                let swatch_width = if swatch.is_some() { 3 } else { 0 };
                let start = right.saturating_sub(field_width + swatch_width).max(left);
                if let Some(colour) = swatch {
                    put(buffer, start, y, right, "██", Style::default().fg(colour));
                }
                let field_x = start + swatch_width;
                let field = Rect::new(field_x, y, right.saturating_sub(field_x), 1);
                buffer.set_style(field, style);
                put(buffer, field_x, y, right, &text, style);
                self.hits.push((field, target));
                editing.map(|buffer| {
                    Position::new((field_x + 1 + buffer.width() as u16).min(right), y)
                })
            }
            SettingControl::Action(value) => {
                let target = Target::Row(index);
                let hovered = self.hovered == Some(target);
                let text = format!("{value}  ›");
                let start = right.saturating_sub(text.width() as u16).max(left);
                let style = if hovered {
                    bold(BORG_ORANGE)
                } else {
                    Style::default().fg(Color::Gray)
                };
                put(buffer, start, y, right, &text, style);
                None
            }
        }
    }

    fn render_list(
        &mut self,
        buffer: &mut Buffer,
        row: &SettingRow,
        y: u16,
        list: Rect,
        bottom: u16,
    ) {
        let SettingControl::Choice {
            options,
            selected,
            disabled,
        } = &row.control
        else {
            return;
        };
        let Some((_, highlighted)) = self.list else {
            return;
        };
        let width = options
            .iter()
            .map(|option| option.width() as u16 + 6)
            .max()
            .unwrap_or(10)
            .max(18)
            .min(list.width);
        let x = list.right().saturating_sub(width + 1);
        let below = bottom.saturating_sub(y + 1);
        let above = y.saturating_sub(list.y);
        let height = (options.len() as u16 + 2).min(below.max(above));
        if height < 3 {
            return;
        }
        let top = if below >= height { y + 1 } else { y - height };
        let frame = Rect::new(x, top, width, height);
        Clear.render(frame, buffer);
        buffer.set_style(frame, Style::default().bg(LIST_PANEL));
        let inner = frame.inner(ratatui::layout::Margin::new(1, 1));
        let capacity = inner.height as usize;
        let first = highlighted.saturating_sub(capacity.saturating_sub(1));
        for (slot, option_index) in (first..options.len()).take(capacity).enumerate() {
            let line_y = inner.y + slot as u16;
            let cell = Rect::new(inner.x, line_y, inner.width, 1);
            let target = Target::ListOption(option_index);
            let current = *selected == Some(option_index);
            let style = if disabled[option_index] {
                Style::default().fg(Color::DarkGray).bg(LIST_PANEL)
            } else if option_index == highlighted || self.hovered == Some(target) {
                Style::default().fg(Color::Black).bg(BORG_ORANGE)
            } else {
                Style::default().fg(Color::Gray).bg(LIST_PANEL)
            };
            buffer.set_style(cell, style);
            let mark = if current { "✓ " } else { "  " };
            put(
                buffer,
                cell.x + 1,
                line_y,
                cell.right(),
                &format!("{mark}{}", options[option_index]),
                style,
            );
            self.hits.push((cell, target));
        }
    }
}

use ratatui::widgets::Widget;

fn bold(colour: Color) -> Style {
    Style::default().fg(colour).add_modifier(Modifier::BOLD)
}

fn segment_style(enabled: bool, selected: bool, hovered: bool) -> Style {
    match (enabled, selected, hovered) {
        (false, true, _) => Style::default().fg(Color::Gray).bg(SEGMENT),
        (false, false, _) => Style::default().fg(Color::DarkGray).bg(PANEL),
        (true, true, _) => Style::default()
            .fg(Color::Black)
            .bg(BORG_ORANGE)
            .add_modifier(Modifier::BOLD),
        (true, false, true) => Style::default().fg(Color::White).bg(SEGMENT_HOVER),
        (true, false, false) => Style::default().fg(Color::Gray).bg(SEGMENT),
    }
}

/// Write `text` at (x, y), clipped before `right`; returns the column after it.
fn put(buffer: &mut Buffer, x: u16, y: u16, right: u16, text: &str, style: Style) -> u16 {
    if x >= right || !buffer.area.contains(Position::new(x, y)) {
        return x;
    }
    let right = right.min(buffer.area.right());
    buffer
        .set_stringn(x, y, text, (right - x) as usize, style)
        .0
}

fn truncate_display(text: &str, width: usize) -> String {
    if text.width() <= width {
        return text.to_string();
    }
    let mut out = String::new();
    for grapheme in text.graphemes(true) {
        if out.width() + grapheme.width() + 1 > width {
            break;
        }
        out.push_str(grapheme);
    }
    out.push('…');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn values() -> SettingsValues {
        SettingsValues {
            provider: Some(CodingProvider::Codex),
            model: Some("gpt-6.1-sol".into()),
            effort: Some("high".into()),
            fast: false,
            ultrafast: false,
            speed_support: borg_remote::SpeedSupport {
                fast: true,
                ultrafast: false,
            },
            permission_mode: Some(PermissionMode::FullAccess),
            response_language: Some(ResponseLanguage::English),
            steer_active: true,
            streaming: ResponseStreaming::Paragraph,
            diff_expansion: DiffExpansionPolicy::UntilNextAction,
            auto_expand_tools: false,
            auto_expand_thinking: true,
            tool_click: ToolClickBehavior::Fullscreen,
            action_descriptors: true,
            wrap_action_rows: false,
            show_done_at: true,
            ui_language: UiLanguage::English,
            user_label: "user".into(),
            assistant_label: "borg".into(),
            colours: [
                ("#4aa3ff".into(), Color::Rgb(74, 163, 255)),
                ("#ffffff".into(), Color::White),
                ("#ff8e24".into(), Color::Rgb(255, 142, 36)),
                ("#ffffff".into(), Color::White),
            ],
            cursor: ComposerCursorStyle::Bar,
            icons: DictationIconStyle::Emoji,
            running_sweeps: true,
            refresh_fps: 120,
            notifications: CompletionAlertPolicy::Unfocused,
            sound: CompletionAlertPolicy::Off,
            prevent_sleep: true,
            prevent_lid_sleep: false,
            auto_copy: true,
            key_hints: true,
            luna_titles: false,
            commit_model: DEFAULT_COMMIT_MESSAGE_MODEL.to_string(),
            configured_models: Vec::new(),
            custom_providers: Default::default(),
            streaming_refresh_fps: 120,
            layout: Default::default(),
        }
    }

    /// A choice the screen draws but cannot apply is a dead control: the
    /// click does nothing and the setting silently stays as it was.
    #[test]
    fn every_offered_choice_applies() {
        for row in setting_rows(&values()) {
            if let SettingControl::Choice { options, .. } = &row.control
                && row.enabled
            {
                for index in 0..options.len() {
                    assert!(
                        choice_action(&row, index).is_some(),
                        "{} option {index} has no action",
                        row.label
                    );
                }
            }
        }
    }
}
