use crate::wire::{AssetRef, TextField};
use anyhow::{Result, ensure};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};

/// An opaque immutable prefix/version address; scope authorization remains the host's responsibility.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TextAssetAddress {
    pub row_id: String,
    pub revision: String,
    pub field: TextField,
    pub bytes: String,
}
impl TextAssetAddress {
    pub fn asset_ref(&self) -> AssetRef {
        let bytes = serde_json::to_vec(self).expect("text address serialization");
        AssetRef {
            id: format!("view-text:{}", URL_SAFE_NO_PAD.encode(bytes)),
            media_type: "text/plain".into(),
            bytes: self.bytes.clone(),
        }
    }
    pub fn parse(asset_id: &str) -> Result<Self> {
        let encoded = asset_id
            .strip_prefix("view-text:")
            .ok_or_else(|| anyhow::anyhow!("not a view text asset"))?;
        ensure!(encoded.len() <= 2048, "oversized text asset address");
        let address: Self = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(encoded)?)?;
        ensure!(!address.row_id.is_empty(), "empty text asset row");
        address.revision.parse::<u64>()?;
        address.bytes.parse::<u64>()?;
        Ok(address)
    }
}
