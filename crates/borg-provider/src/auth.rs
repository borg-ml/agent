use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ProviderAuthProvider {
    Claude,
    Openai,
}

impl ProviderAuthProvider {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Claude => "claude",
            Self::Openai => "openai",
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderAuthFile {
    pub path: String,
    pub contents_b64: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderAuthBundle {
    #[serde(default)]
    pub files: Vec<ProviderAuthFile>,
}
