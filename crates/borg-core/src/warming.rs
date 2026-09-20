//! Prompt cache warming mode.
//!
//! This lives in the shared contract crate because two layers need the same
//! three states and the same spellings: the configuration surface the operator
//! writes, and the native harness worker that acts on it. Defining it twice
//! invites the two to drift into disagreeing about what `idle` means.

use serde::{Deserialize, Serialize};

/// How aggressively Borg keeps prompt cache entries alive.
///
/// [`Self::Streaming`] is the default: it protects an expensive prefix across
/// a long tool run, which is where an entry is most likely to lapse without
/// anyone choosing to let it. [`Self::Idle`] keeps spending while the human is
/// away, so it is opt-in.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheWarmingMode {
    Off,
    #[default]
    Streaming,
    Idle,
}

impl CacheWarmingMode {
    /// Accepts the canonical spellings plus the boolean-ish forms an operator
    /// reaches for when a setting reads like a switch.
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "off" | "false" | "0" => Some(Self::Off),
            "streaming" | "on" | "true" | "1" => Some(Self::Streaming),
            "idle" => Some(Self::Idle),
            _ => None,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Streaming => "streaming",
            Self::Idle => "idle",
        }
    }
}
