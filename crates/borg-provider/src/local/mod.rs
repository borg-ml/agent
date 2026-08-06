//! Discovery and metadata for models that can be served by a local
//! OpenAI-compatible runtime.
//!
//! This module deliberately stops at model discovery. It does not start a
//! server, load weights, or choose llama.cpp offload flags. A caller that owns
//! local-provider configuration can construct the sources below and use the
//! model's `path` when it wires a selected entry into its server lifecycle.

mod fit;
mod gguf;
mod sources;

use std::collections::HashSet;
use std::env;
use std::path::{Path, PathBuf};

use crate::runtime::DynamicModelEntry;

pub use fit::{FitReport, FitStatus, available_vram_bytes, format_bytes};
pub use gguf::{GgufError, GgufHeader, GgufValue, parse_gguf_file};
pub use sources::{
    HuggingFaceCacheSource, ModelSource, ModelSourceError, NativeGgufSource, OllamaBlobSource,
};

/// A model discovered from a local source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalModel {
    /// Stable picker/runtime identifier. This is intentionally separate from
    /// `path`: a future server supervisor can resolve this id to the path
    /// without exposing filesystem paths in the picker.
    pub id: String,
    pub display_name: String,
    pub path: PathBuf,
    pub architecture: String,
    pub quant: String,
    pub size_bytes: u64,
    pub block_count: Option<u32>,
    pub train_ctx: Option<u32>,
    pub expert_count: Option<u32>,
    pub source: &'static str,
}

impl LocalModel {
    pub(crate) fn picker_entry(&self) -> DynamicModelEntry {
        let mut label = self.display_name.clone();
        if !self.quant.is_empty() && self.quant != "unknown" {
            label.push_str(" · ");
            label.push_str(&self.quant);
        }
        label.push_str(" · ");
        label.push_str(&format_bytes(self.size_bytes));

        let fit = FitReport::for_model(self);
        let mut details = vec![self.architecture.clone()];
        if let Some(block_count) = self.block_count {
            details.push(format!("{block_count} blocks"));
        }
        if let Some(context) = self.train_ctx {
            details.push(format_context(context));
        }
        if let Some(expert_count) = self.expert_count {
            let spill = matches!(fit.status, FitStatus::MaySpill);
            details.push(if spill {
                format!("MoE · {expert_count} experts may spill to RAM")
            } else {
                format!("MoE · {expert_count} experts")
            });
        }
        details.push(fit.label());
        details.push(format!("source: {}", self.source));

        DynamicModelEntry {
            id: self.id.clone(),
            label,
            detail: Some(details.join(" · ")),
        }
    }
}

fn format_context(tokens: u32) -> String {
    if tokens >= 1_000_000 {
        format!("{}M ctx", tokens / 1_000_000)
    } else if tokens >= 1_000 {
        format!("{}k ctx", tokens / 1_000)
    } else {
        format!("{tokens} ctx")
    }
}

/// Explicit inputs for local model discovery. No configuration is read by
/// the source implementations; callers can populate this from `agent.toml`
/// or another host-owned configuration object.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LocalModelDiscoveryConfig {
    pub model_dirs: Vec<PathBuf>,
    pub include_ollama_store: bool,
    pub ollama_models_dir: Option<PathBuf>,
    pub include_hf_cache: bool,
    pub hf_cache_dir: Option<PathBuf>,
}

impl LocalModelDiscoveryConfig {
    /// Build conservative standard locations for a known home directory.
    ///
    /// This does not read the environment. The terminal UI uses
    /// `from_standard_environment` only as a fallback until the local
    /// provider configuration is passed through its session state.
    pub fn standard_for_home(home: &Path) -> Self {
        Self {
            model_dirs: vec![home.join("models"), home.join(".local/share/models")],
            include_ollama_store: true,
            ollama_models_dir: Some(home.join(".ollama/models")),
            include_hf_cache: false,
            hf_cache_dir: Some(home.join(".cache/huggingface/hub")),
        }
    }

    /// Build the standard-location fallback used by the picker. This only
    /// consults HOME/XDG-style location information; it does not invent
    /// provider configuration variables.
    pub fn from_standard_environment() -> Self {
        let home = env::var_os("HOME").map(PathBuf::from);
        home.as_deref()
            .map(Self::standard_for_home)
            .unwrap_or_default()
    }
}

/// Discover all configured local models, continuing past malformed individual
/// model files. Source-level I/O failures remain typed so a host can surface a
/// diagnostic instead of silently claiming that discovery succeeded.
pub fn discover_models(
    config: &LocalModelDiscoveryConfig,
) -> Result<Vec<LocalModel>, ModelSourceError> {
    let mut sources: Vec<Box<dyn ModelSource>> = Vec::new();
    if !config.model_dirs.is_empty() {
        sources.push(Box::new(NativeGgufSource::new(config.model_dirs.clone())));
    }
    if config.include_ollama_store
        && let Some(models_dir) = config.ollama_models_dir.clone()
    {
        sources.push(Box::new(OllamaBlobSource::new(models_dir)));
    }
    if config.include_hf_cache
        && let Some(cache_dir) = config.hf_cache_dir.clone()
    {
        sources.push(Box::new(HuggingFaceCacheSource::new(cache_dir)));
    }

    let mut models = Vec::new();
    let mut seen_paths = HashSet::new();
    for source in sources {
        for model in source.discover()? {
            let path_key = model
                .path
                .canonicalize()
                .unwrap_or_else(|_| model.path.clone());
            if seen_paths.insert(path_key) {
                models.push(model);
            }
        }
    }
    models.sort_by(|left, right| {
        left.display_name
            .to_ascii_lowercase()
            .cmp(&right.display_name.to_ascii_lowercase())
            .then_with(|| left.id.cmp(&right.id))
    });
    Ok(models)
}

/// Convert discovery results to the runtime-owned entries consumed by the
/// model picker. Keeping this conversion here gives Stream B a single seam to
/// replace with its resolved LocalProviderConfig later.
pub fn discover_dynamic_model_entries(
    config: &LocalModelDiscoveryConfig,
) -> Result<Vec<DynamicModelEntry>, ModelSourceError> {
    Ok(discover_models(config)?
        .iter()
        .map(LocalModel::picker_entry)
        .collect())
}

pub(crate) fn stable_model_id(source: &str, path: &Path) -> String {
    let stem = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or("model");
    let mut slug = String::with_capacity(stem.len());
    let mut previous_dash = false;
    for character in stem.chars() {
        if character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-') {
            slug.push(character.to_ascii_lowercase());
            previous_dash = false;
        } else if !previous_dash {
            slug.push('-');
            previous_dash = true;
        }
    }
    let slug = slug.trim_matches('-');
    if slug.is_empty() {
        format!("{source}:model")
    } else {
        format!("{source}:{slug}")
    }
}
