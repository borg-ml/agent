use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::localization::UiLanguage;

const DEFAULT_REFRESH_RATE_FPS: u16 = 60;
const MIN_REFRESH_RATE_FPS: u16 = 15;
const MAX_REFRESH_RATE_FPS: u16 = 240;
const MAX_TRANSCRIPT_LABEL_CHARS: usize = 32;
const HEX_COLOR_LENGTH: usize = 7;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
#[derive(Default)]
pub struct EditorPreferences {
    pub transcript: TranscriptPreferences,
    pub interaction: InteractionPreferences,
    pub presentation: PresentationPreferences,
    pub layout: LayoutPreferences,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct LayoutPreferences {
    pub horizontal_margin: u16,
    pub composer_max_height: u16,
    pub show_footer: bool,
}

impl Default for LayoutPreferences {
    fn default() -> Self {
        Self {
            horizontal_margin: 0,
            composer_max_height: 8,
            show_footer: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct TranscriptPreferences {
    pub user_label: String,
    pub assistant_label: String,
    pub user_label_color: String,
    pub user_message_color: String,
    pub assistant_label_color: String,
    pub assistant_message_color: String,
}

impl Default for TranscriptPreferences {
    fn default() -> Self {
        Self {
            user_label: "user".to_string(),
            assistant_label: "borg".to_string(),
            user_label_color: "#4aa3ff".to_string(),
            user_message_color: "#c6e4ff".to_string(),
            assistant_label_color: "#ff8e24".to_string(),
            assistant_message_color: "#ffffff".to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActiveMessageBehavior {
    Steer,
    Queue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompletionAlertPolicy {
    Off,
    Unfocused,
    Always,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DictationIconStyle {
    NerdFont,
    Emoji,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiffExpansionPolicy {
    Expanded,
    Collapsed,
    UntilNextAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolClickBehavior {
    Fullscreen,
    Inline,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct InteractionPreferences {
    pub active_messages: ActiveMessageBehavior,
    pub prevent_sleep: bool,
    pub completion_notifications: CompletionAlertPolicy,
    pub completion_sound: CompletionAlertPolicy,
    /// Set once the user has completed the enable-dictation flow (which also
    /// grants microphone access). Until then, the dictation key opens that
    /// flow instead of recording.
    pub dictation_enabled: bool,
}

impl Default for InteractionPreferences {
    fn default() -> Self {
        Self {
            active_messages: ActiveMessageBehavior::Steer,
            prevent_sleep: true,
            completion_notifications: CompletionAlertPolicy::Unfocused,
            completion_sound: CompletionAlertPolicy::Unfocused,
            dictation_enabled: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct PresentationPreferences {
    pub ui_language: UiLanguage,
    pub refresh_rate_fps: u16,
    pub diff_expansion: Option<DiffExpansionPolicy>,
    /// Legacy compatibility for editor.toml files written before diff_expansion.
    pub auto_expand_edits: bool,
    pub auto_expand_tools: bool,
    pub tool_click_behavior: ToolClickBehavior,
    pub action_descriptors: bool,
    pub running_sweeps: bool,
    pub dictation_icon: Option<DictationIconStyle>,
    /// Selected managed dictation model id (e.g. "parakeet-v2"); `None` uses
    /// the built-in default.
    pub dictation_model: Option<String>,
    /// Selected dictation inference accelerator id (e.g. "auto", "nvidia",
    /// "vulkan"); `None` uses the per-platform default.
    pub dictation_accelerator: Option<String>,
}

impl Default for PresentationPreferences {
    fn default() -> Self {
        Self {
            ui_language: UiLanguage::Auto,
            refresh_rate_fps: DEFAULT_REFRESH_RATE_FPS,
            diff_expansion: None,
            auto_expand_edits: true,
            auto_expand_tools: false,
            tool_click_behavior: ToolClickBehavior::Fullscreen,
            action_descriptors: true,
            running_sweeps: true,
            dictation_icon: None,
            dictation_model: None,
            dictation_accelerator: None,
        }
    }
}

impl PresentationPreferences {
    pub fn effective_diff_expansion(&self) -> DiffExpansionPolicy {
        self.diff_expansion.unwrap_or(if self.auto_expand_edits {
            DiffExpansionPolicy::Expanded
        } else {
            DiffExpansionPolicy::Collapsed
        })
    }
}

impl EditorPreferences {
    pub fn load() -> Result<Self> {
        Self::load_from(&default_path()?)
    }

    pub fn load_from(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let source = fs::read_to_string(path)
            .with_context(|| format!("failed to read editor preferences {}", path.display()))?;
        // A newer Borg may already have written keys this build does not
        // know. Refusing to start over them would strand the user on every
        // downgrade or side-by-side install, so they are reported and kept.
        let (preferences, unknown_keys) = from_toml_str_lenient::<Self>(&source)
            .with_context(|| format!("invalid editor preferences {}", path.display()))?;
        if !unknown_keys.is_empty() {
            tracing::warn!(
                path = %path.display(),
                keys = %unknown_keys.join(", "),
                "editor preferences contain keys this Borg build does not understand; they are kept but have no effect"
            );
        }
        preferences
            .validate()
            .with_context(|| format!("invalid editor preferences {}", path.display()))?;
        Ok(preferences)
    }

    pub fn save(&self) -> Result<()> {
        self.save_to(&default_path()?)
    }

    pub fn save_to(&self, path: &Path) -> Result<()> {
        self.validate()
            .context("refusing to save invalid editor preferences")?;
        let parent = path
            .parent()
            .context("editor preferences path has no parent directory")?;
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
        let toml::Value::Table(mut document) =
            toml::Value::try_from(self).context("failed to encode editor preferences")?
        else {
            anyhow::bail!("editor preferences did not encode as a TOML table");
        };
        // Keys written by a newer Borg survive a save from this build, so a
        // downgrade or a side-by-side install never erases settings the user
        // has already made.
        if let Ok(existing_source) = fs::read_to_string(path)
            && let Ok((_, unknown_keys)) = from_toml_str_lenient::<Self>(&existing_source)
            && let Ok(existing) = existing_source.parse::<toml::Table>()
        {
            for key_path in unknown_keys {
                copy_toml_path(&existing, &mut document, &key_path);
            }
        }
        let source =
            toml::to_string_pretty(&document).context("failed to encode editor preferences")?;
        let mut temporary = tempfile::NamedTempFile::new_in(parent)
            .with_context(|| format!("failed to create temporary file in {}", parent.display()))?;
        temporary
            .write_all(source.as_bytes())
            .context("failed to write editor preferences")?;
        temporary
            .as_file()
            .sync_all()
            .context("failed to sync editor preferences")?;
        temporary
            .persist(path)
            .map_err(|error| error.error)
            .with_context(|| format!("failed to replace editor preferences {}", path.display()))?;
        Ok(())
    }

    pub fn validate(&self) -> Result<()> {
        validate_label("user", &self.transcript.user_label)?;
        validate_label("assistant", &self.transcript.assistant_label)?;
        for (name, color) in [
            ("user label", &self.transcript.user_label_color),
            ("user message", &self.transcript.user_message_color),
            ("assistant label", &self.transcript.assistant_label_color),
            (
                "assistant message",
                &self.transcript.assistant_message_color,
            ),
        ] {
            parse_hex_color(color)
                .with_context(|| format!("{name} colour must use #RRGGBB notation"))?;
        }
        anyhow::ensure!(
            (MIN_REFRESH_RATE_FPS..=MAX_REFRESH_RATE_FPS)
                .contains(&self.presentation.refresh_rate_fps),
            "refresh rate must be between {MIN_REFRESH_RATE_FPS} and {MAX_REFRESH_RATE_FPS} FPS"
        );
        anyhow::ensure!(
            self.layout.horizontal_margin <= 40,
            "horizontal margin must be at most 40 cells"
        );
        anyhow::ensure!(
            (3..=30).contains(&self.layout.composer_max_height),
            "composer maximum height must be between 3 and 30 rows"
        );
        Ok(())
    }
}

/// Deserialize `T` and report the dotted paths of every key `T` ignored.
///
/// Config structs accept unknown keys so that a file written by a newer Borg
/// still loads in an older one; callers decide whether the unknown paths are
/// a warning (user files) or an error (extension manifests).
pub fn deserialize_lenient<'de, T, D>(
    deserializer: D,
) -> std::result::Result<(T, Vec<String>), D::Error>
where
    T: Deserialize<'de>,
    D: serde::Deserializer<'de>,
{
    let mut unknown_keys = Vec::new();
    let value = serde_ignored::deserialize(deserializer, |path| {
        unknown_keys.push(path.to_string());
    })?;
    Ok((value, unknown_keys))
}

/// Parse a TOML document leniently; see [`deserialize_lenient`].
pub fn from_toml_str_lenient<T: DeserializeOwned>(
    source: &str,
) -> std::result::Result<(T, Vec<String>), toml::de::Error> {
    deserialize_lenient(toml::Deserializer::parse(source)?)
}

/// Copy the value at a dotted `key_path` from `source` into `target`,
/// creating intermediate tables. Paths that address array elements are
/// skipped; preferences never nest tables inside arrays.
fn copy_toml_path(source: &toml::Table, target: &mut toml::Table, key_path: &str) {
    if key_path.contains('[') {
        return;
    }
    let mut segments = key_path.split('.').peekable();
    let mut source_table = source;
    let mut target_table = target;
    while let Some(segment) = segments.next() {
        let Some(value) = source_table.get(segment) else {
            return;
        };
        if segments.peek().is_none() {
            target_table.insert(segment.to_string(), value.clone());
            return;
        }
        let Some(next_source) = value.as_table() else {
            return;
        };
        source_table = next_source;
        let entry = target_table
            .entry(segment.to_string())
            .or_insert_with(|| toml::Value::Table(toml::Table::new()));
        if !entry.is_table() {
            *entry = toml::Value::Table(toml::Table::new());
        }
        let Some(next_target) = entry.as_table_mut() else {
            return;
        };
        target_table = next_target;
    }
}

pub fn parse_hex_color(value: &str) -> Result<(u8, u8, u8)> {
    anyhow::ensure!(
        value.len() == HEX_COLOR_LENGTH && value.starts_with('#'),
        "expected #RRGGBB"
    );
    let red = u8::from_str_radix(&value[1..3], 16).context("invalid red channel")?;
    let green = u8::from_str_radix(&value[3..5], 16).context("invalid green channel")?;
    let blue = u8::from_str_radix(&value[5..7], 16).context("invalid blue channel")?;
    Ok((red, green, blue))
}

pub fn default_path() -> Result<PathBuf> {
    let root = config_root(
        std::env::var_os("XDG_CONFIG_HOME").map(PathBuf::from),
        std::env::var_os("HOME").map(PathBuf::from),
        platform_config_dir(),
    )
    .context("unable to determine a config directory for editor preferences")?;
    Ok(root.join("borg").join("editor.toml"))
}

#[cfg(any(unix, windows))]
fn platform_config_dir() -> Option<PathBuf> {
    dirs::config_dir()
}

#[cfg(not(any(unix, windows)))]
fn platform_config_dir() -> Option<PathBuf> {
    None
}

fn config_root(
    xdg_config_home: Option<PathBuf>,
    home: Option<PathBuf>,
    platform_config_dir: Option<PathBuf>,
) -> Option<PathBuf> {
    xdg_config_home
        .or_else(|| home.map(|home| home.join(".config")))
        .or(platform_config_dir)
}

fn validate_label(kind: &str, value: &str) -> Result<()> {
    anyhow::ensure!(!value.is_empty(), "{kind} transcript label cannot be empty");
    anyhow::ensure!(
        value.trim() == value,
        "{kind} transcript label cannot start or end with whitespace"
    );
    anyhow::ensure!(
        value.chars().count() <= MAX_TRANSCRIPT_LABEL_CHARS,
        "{kind} transcript label cannot exceed {MAX_TRANSCRIPT_LABEL_CHARS} characters"
    );
    anyhow::ensure!(
        !value.chars().any(char::is_control),
        "{kind} transcript label cannot contain control characters"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_preserves_label_casing_and_all_preferences() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("borg/editor.toml");
        let preferences = EditorPreferences {
            transcript: TranscriptPreferences {
                user_label: "shulgin".to_string(),
                assistant_label: "cLaNkEr".to_string(),
                user_label_color: "#ff70b7".to_string(),
                user_message_color: "#ffc0df".to_string(),
                assistant_label_color: "#89ddff".to_string(),
                assistant_message_color: "#e6edf3".to_string(),
            },
            interaction: InteractionPreferences {
                active_messages: ActiveMessageBehavior::Queue,
                prevent_sleep: false,
                completion_notifications: CompletionAlertPolicy::Always,
                completion_sound: CompletionAlertPolicy::Off,
                dictation_enabled: true,
            },
            presentation: PresentationPreferences {
                ui_language: UiLanguage::SimplifiedChinese,
                refresh_rate_fps: 144,
                diff_expansion: Some(DiffExpansionPolicy::UntilNextAction),
                auto_expand_edits: false,
                auto_expand_tools: true,
                tool_click_behavior: ToolClickBehavior::Inline,
                action_descriptors: false,
                running_sweeps: false,
                dictation_icon: Some(DictationIconStyle::NerdFont),
                dictation_model: Some("lightweight".to_string()),
                dictation_accelerator: Some("auto".to_string()),
            },
            layout: LayoutPreferences {
                horizontal_margin: 5,
                composer_max_height: 14,
                show_footer: false,
            },
        };

        preferences.save_to(&path).unwrap();

        assert_eq!(EditorPreferences::load_from(&path).unwrap(), preferences);
        let source = fs::read_to_string(path).unwrap();
        assert!(source.contains("assistant_label = \"cLaNkEr\""));
        assert!(source.contains("assistant_message_color = \"#e6edf3\""));
        assert!(source.contains("dictation_icon = \"nerd_font\""));
    }

    #[test]
    fn missing_file_uses_current_editor_defaults() {
        let temp = tempfile::tempdir().unwrap();
        let preferences = EditorPreferences::load_from(&temp.path().join("missing.toml")).unwrap();

        assert_eq!(preferences, EditorPreferences::default());
        assert_eq!(
            preferences.presentation.tool_click_behavior,
            ToolClickBehavior::Fullscreen
        );
    }

    #[test]
    fn config_root_falls_back_to_the_platform_config_directory() {
        let platform_config_dir = PathBuf::from("native-config");

        assert_eq!(
            config_root(None, None, Some(platform_config_dir.clone())),
            Some(platform_config_dir)
        );
        assert_eq!(
            config_root(
                Some(PathBuf::from("xdg-config")),
                Some(PathBuf::from("home")),
                Some(PathBuf::from("native-config")),
            ),
            Some(PathBuf::from("xdg-config"))
        );
        assert_eq!(
            config_root(
                None,
                Some(PathBuf::from("home")),
                Some(PathBuf::from("native-config")),
            ),
            Some(PathBuf::from("home").join(".config"))
        );
    }

    #[test]
    fn checked_in_example_matches_the_typed_editor_preferences() {
        let preferences: EditorPreferences =
            toml::from_str(include_str!("../../../configs/editor.example.toml")).unwrap();
        preferences.validate().unwrap();
        assert_eq!(
            preferences.interaction.active_messages,
            ActiveMessageBehavior::Steer
        );
    }

    #[test]
    fn validation_protects_terminal_rendering_and_refresh_bounds() {
        let mut preferences = EditorPreferences::default();
        preferences.transcript.user_label = " user".to_string();
        assert!(preferences.validate().is_err());

        preferences.transcript.user_label = "user\nlabel".to_string();
        assert!(preferences.validate().is_err());

        preferences.transcript.user_label = "user".to_string();
        preferences.presentation.refresh_rate_fps = 241;
        assert!(preferences.validate().is_err());

        preferences.presentation.refresh_rate_fps = 165;
        preferences.transcript.user_message_color = "pink".to_string();
        assert!(preferences.validate().is_err());
    }

    #[test]
    fn partial_files_gain_defaults_without_losing_explicit_values() {
        let preferences: EditorPreferences = toml::from_str(
            r#"
                [transcript]
                user_label = "ShUlGiN"
            "#,
        )
        .unwrap();

        assert_eq!(preferences.transcript.user_label, "ShUlGiN");
        assert_eq!(preferences.transcript.assistant_label, "borg");
        assert_eq!(preferences.transcript.user_message_color, "#c6e4ff");
        assert!(preferences.interaction.prevent_sleep);
        assert_eq!(preferences.presentation.refresh_rate_fps, 60);
        assert_eq!(preferences.layout.horizontal_margin, 0);
    }

    #[test]
    fn unknown_keys_are_reported_and_survive_a_save() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("editor.toml");
        fs::write(
            &path,
            "[interaction]\nprevent_sleep = false\nfuture_flag = \"authorized\"\n\n[future_section]\nenabled = true\n",
        )
        .unwrap();
        let (loaded, mut unknown) =
            from_toml_str_lenient::<EditorPreferences>(&fs::read_to_string(&path).unwrap())
                .unwrap();
        assert!(!loaded.interaction.prevent_sleep);
        unknown.sort();
        assert_eq!(unknown, vec!["future_section", "interaction.future_flag"]);

        let mut loaded = EditorPreferences::load_from(&path).unwrap();
        loaded.interaction.prevent_sleep = true;
        loaded.save_to(&path).unwrap();
        let saved = fs::read_to_string(&path)
            .unwrap()
            .parse::<toml::Table>()
            .unwrap();
        assert_eq!(saved["interaction"]["prevent_sleep"].as_bool(), Some(true));
        assert_eq!(
            saved["interaction"]["future_flag"].as_str(),
            Some("authorized")
        );
        assert_eq!(saved["future_section"]["enabled"].as_bool(), Some(true));
        let reloaded = EditorPreferences::load_from(&path).unwrap();
        assert!(reloaded.interaction.prevent_sleep);
    }

    #[test]
    fn diff_expansion_supports_new_policy_and_legacy_boolean() {
        let configured: EditorPreferences =
            toml::from_str("[presentation]\ndiff_expansion = \"until_next_action\"\n").unwrap();
        assert_eq!(
            configured.presentation.effective_diff_expansion(),
            DiffExpansionPolicy::UntilNextAction
        );

        let legacy: EditorPreferences =
            toml::from_str("[presentation]\nauto_expand_edits = false\n").unwrap();
        assert_eq!(
            legacy.presentation.effective_diff_expansion(),
            DiffExpansionPolicy::Collapsed
        );
    }
}
