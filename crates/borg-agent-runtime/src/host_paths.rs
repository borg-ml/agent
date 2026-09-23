//! Filesystem location of the durable host state, shared by the runtime
//! (imported memory, session store) and the transport layer (enrollment).

use std::path::PathBuf;

pub fn host_home() -> PathBuf {
    std::env::var_os("BORG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".borg")))
        .or_else(|| dirs::home_dir().map(|home| home.join(".borg")))
        .unwrap_or_else(|| PathBuf::from(".borg"))
}

pub fn default_host_config_path() -> PathBuf {
    host_home().join("remote").join("host.json")
}
