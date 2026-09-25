pub mod credentials;
pub mod image_tiles;
pub mod local;
pub mod mcp;
pub mod openai_subscription;
pub mod provider;
pub mod provider_auth;
pub mod provider_bin;
pub mod provider_install;
pub mod runtime;
pub mod subscription;

mod auth;
mod bounded_io;
mod env;
pub mod subprocess;

pub use auth::{ProviderAuthBundle, ProviderAuthFile, ProviderAuthProvider};
pub use provider_bin::{
    CODEX_BIN_ENV, Runtime, command as runtime_command, executable as runtime_executable,
};
pub use provider_install::Healed;
pub use runtime::{
    CLAUDE_EFFORT_LEVELS, CLAUDE_MODEL_CATALOG, CLAUDE_SELECTABLE_MODELS, CODEX_EFFORT_LEVELS,
    CODEX_MODEL_CATALOG, CODEX_SELECTABLE_MODELS, CostBasis, DynamicModelEntry, MODEL_CATALOGS,
    ProviderCallUsage, ProviderChannel, ProviderModelCatalog, claude_default_effort,
    claude_product_model, codex_default_effort, codex_effort_levels, codex_effort_supported,
    codex_product_model, dynamic_models_for_backend, glm_product_model, grok_product_model,
    kimi_default_effort, kimi_product_model, model_catalog_for_backend, muse_product_model,
    opencode_go_model_entries, openrouter_model_entries, openrouter_product_model,
    qwen_default_effort, qwen_product_model, refresh_opencode_go_model_catalog,
    refresh_openrouter_model_catalog, set_openrouter_model_entries,
};
pub use subscription::Plan;

pub use local::{
    FitReport, FitStatus, GgufError, GgufHeader, GgufValue, HuggingFaceCacheSource, LocalModel,
    LocalModelDiscoveryConfig, ModelSource, ModelSourceError, NativeGgufSource, OllamaBlobSource,
    available_vram_bytes, discover_dynamic_model_entries, discover_models, format_bytes,
    parse_gguf_file,
};
