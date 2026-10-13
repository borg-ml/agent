//! Authoritative, renderer-independent session view contracts.
pub mod cache_diagnostics;
pub mod core;
pub mod entry;
pub mod grouping;
pub mod intent;
pub mod policy;
pub mod projection_helpers;
pub mod reducer;
pub mod semantics;
pub mod streaming;
pub mod wire;
pub use reducer::ViewReducer;
pub mod checkpoint;
pub use checkpoint::{CheckpointTextField, CheckpointTextRef};
pub mod assets;
