use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

const DEFAULT_REFRESH_RATE_FPS: u16 = 60;
const MIN_REFRESH_RATE_FPS: u16 = 15;
const MAX_REFRESH_RATE_FPS: u16 = 240;
const MAX_TRANSCRIPT_LABEL_CHARS: usize = 32;
const HEX_COLOR_LENGTH: usize = 7;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[derive(Default)]
pub(crate) struct EditorPreferences {
    pub(crate) transcript: TranscriptPreferences,
    pub(crate) interaction: InteractionPreferences,
    pub(crate) presentation: PresentationPreferences,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct TranscriptPreferences {
    pub(crate) user_label: String,
    pub(crate) assistant_label: String,
    pub(crate) user_label_color: String,
    pub(crate) user_message_color: String,
    pub(crate) assistant_label_color: String,
    pub(crate) assistant_message_color: String,
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
pub(crate) enum ActiveMessageBehavior {
    Steer,
    Queue,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct InteractionPreferences {
    pub(crate) active_messages: ActiveMessageBehavior,
    pub(crate) prevent_sleep: bool,
}

impl Default for InteractionPreferences {
    fn default() -> Self {
        Self {
            active_messages: ActiveMessageBehavior::Steer,
            prevent_sleep: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct PresentationPreferences {
    pub(crate) refresh_rate_fps: u16,
    pub(crate) auto_expand_edits: bool,
    pub(crate) auto_expand_tools: bool,
}

impl Default for PresentationPreferences {
    fn default() -> Self {
        Self {
            refresh_rate_fps: DEFAULT_REFRESH_RATE_FPS,
            auto_expand_edits: true,
            auto_expand_tools: false,
        }
    }
}

impl EditorPreferences {
    pub(crate) fn load() -> Result<Self> {
        Self::load_from(&default_path()?)
    }

    pub(crate) fn load_from(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let source = fs::read_to_string(path)
            .with_context(|| format!("failed to read editor preferences {}", path.display()))?;
        let preferences: Self = toml::from_str(&source)
            .with_context(|| format!("invalid editor preferences {}", path.display()))?;
        preferences
            .validate()
            .with_context(|| format!("invalid editor preferences {}", path.display()))?;
        Ok(preferences)
    }

    pub(crate) fn save(&self) -> Result<()> {
        self.save_to(&default_path()?)
    }

    pub(crate) fn save_to(&self, path: &Path) -> Result<()> {
        self.validate()
            .context("refusing to save invalid editor preferences")?;
        let parent = path
            .parent()
            .context("editor preferences path has no parent directory")?;
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
        let source = toml::to_string_pretty(self).context("failed to encode editor preferences")?;
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

    pub(crate) fn validate(&self) -> Result<()> {
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
        Ok(())
    }
}

pub(crate) fn parse_hex_color(value: &str) -> Result<(u8, u8, u8)> {
    anyhow::ensure!(
        value.len() == HEX_COLOR_LENGTH && value.starts_with('#'),
        "expected #RRGGBB"
    );
    let red = u8::from_str_radix(&value[1..3], 16).context("invalid red channel")?;
    let green = u8::from_str_radix(&value[3..5], 16).context("invalid green channel")?;
    let blue = u8::from_str_radix(&value[5..7], 16).context("invalid blue channel")?;
    Ok((red, green, blue))
}

fn default_path() -> Result<PathBuf> {
    let root = config_root(
        std::env::var_os("XDG_CONFIG_HOME").map(PathBuf::from),
        std::env::var_os("HOME").map(PathBuf::from),
        platform_config_dir(),
    )
    .context("unable to determine a config directory for editor preferences")?;
    Ok(root.join("borg").join("editor.toml"))
}

#[cfg(windows)]
fn platform_config_dir() -> Option<PathBuf> {
    dirs::config_dir()
}

#[cfg(not(windows))]
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
            },
            presentation: PresentationPreferences {
                refresh_rate_fps: 144,
                auto_expand_edits: false,
                auto_expand_tools: true,
            },
        };

        preferences.save_to(&path).unwrap();

        assert_eq!(EditorPreferences::load_from(&path).unwrap(), preferences);
        let source = fs::read_to_string(path).unwrap();
        assert!(source.contains("assistant_label = \"cLaNkEr\""));
        assert!(source.contains("assistant_message_color = \"#e6edf3\""));
    }

    #[test]
    fn missing_file_uses_current_editor_defaults() {
        let temp = tempfile::tempdir().unwrap();

        assert_eq!(
            EditorPreferences::load_from(&temp.path().join("missing.toml")).unwrap(),
            EditorPreferences::default()
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
    }
}
