//! Host-local coordination primitives for parallel work on resource-heavy projects.
//! The interfaces in this crate are engine-agnostic; adapters supply policy and commands.
//! Unix only: jobs and services rely on process groups, flock and Unix sockets.
#![cfg(unix)]

pub mod adapter;
pub mod lanes;
pub mod services;
pub mod workspace;
