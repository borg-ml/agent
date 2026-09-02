use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use reqwest::Client;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::agent_config::UsageCountConfig;

const ENDPOINT: &str = "https://borg.ml/api/v1/active-install";
const SEND_INTERVAL_SECS: u64 = 24 * 60 * 60;
const ROTATION_INTERVAL_SECS: u64 = 31 * 24 * 60 * 60;
static STARTED: AtomicBool = AtomicBool::new(false);

#[derive(Debug, Deserialize, Serialize)]
struct UsageCountState {
    seed: Uuid,
    #[serde(default)]
    last_attempt_unix: u64,
}

#[derive(Serialize)]
struct ActiveInstall<'a> {
    anonymous_period_id: &'a str,
}

pub(crate) fn spawn_background(config: UsageCountConfig) {
    if cfg!(debug_assertions)
        || !config.enabled
        || std::env::var_os("BORG_DISABLE_USAGE_COUNT").is_some()
        || STARTED.swap(true, Ordering::AcqRel)
    {
        return;
    }
    let mut state = read_state().unwrap_or_else(|| UsageCountState {
        seed: Uuid::new_v4(),
        last_attempt_unix: 0,
    });
    let now = unix_now();
    if now.saturating_sub(state.last_attempt_unix) < SEND_INTERVAL_SECS {
        return;
    }
    state.last_attempt_unix = now;
    write_state(&state);
    tokio::spawn(async move {
        let _ = send(&state, now).await;
    });
}

async fn send(state: &UsageCountState, now: u64) -> bool {
    let period_id = period_id(state.seed, now);
    let Ok(client) = Client::builder()
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(5))
        .build()
    else {
        return false;
    };
    client
        .post(ENDPOINT)
        .json(&ActiveInstall {
            anonymous_period_id: &period_id,
        })
        .send()
        .await
        .is_ok_and(|response| response.status().is_success())
}

fn period_id(seed: Uuid, now: u64) -> String {
    let period = now / ROTATION_INTERVAL_SECS;
    let digest = Sha256::new()
        .chain_update(seed.as_bytes())
        .chain_update(period.to_le_bytes())
        .finalize();
    hex::encode(&digest[..16])
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn state_path() -> Option<PathBuf> {
    let root = std::env::var_os("XDG_STATE_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".local/state")))
        .or_else(dirs::data_local_dir)?;
    Some(root.join("borg").join("usage-count.json"))
}

fn read_state() -> Option<UsageCountState> {
    serde_json::from_slice(&fs::read(state_path()?).ok()?).ok()
}

fn write_state(state: &UsageCountState) {
    let Some(path) = state_path() else { return };
    let Some(parent) = path.parent() else { return };
    if fs::create_dir_all(parent).is_err() {
        return;
    }
    if let Ok(bytes) = serde_json::to_vec(state) {
        let _ = fs::write(path, bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifier_is_stable_within_a_period_and_rotates_between_periods() {
        let seed = Uuid::nil();
        assert_eq!(
            period_id(seed, 1),
            period_id(seed, ROTATION_INTERVAL_SECS - 1)
        );
        assert_ne!(period_id(seed, 1), period_id(seed, ROTATION_INTERVAL_SECS));
        assert_eq!(period_id(seed, 1).len(), 32);
    }
}
