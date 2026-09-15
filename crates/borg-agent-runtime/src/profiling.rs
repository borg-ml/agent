use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const PROFILE_FILE_SUFFIX: &str = ".profile.json";
const PROFILE_BUCKET_COUNT: usize = 13;
#[cfg(feature = "profiling")]
const PROFILE_SCHEMA_VERSION: u32 = 1;
#[cfg(feature = "profiling")]
const PROFILE_BUCKET_UPPER_BOUNDS_MS: [u64; PROFILE_BUCKET_COUNT] = [
    1, 5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 30_000,
];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeProfileSnapshot {
    pub schema_version: u32,
    pub session_id: Uuid,
    pub pid: u32,
    pub started_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub active_turn: Option<RuntimeProfileActiveTurn>,
    pub current_phase: Option<String>,
    pub current_phase_started_at: Option<DateTime<Utc>>,
    pub turns_completed: u64,
    pub last_turn: Option<RuntimeProfileTurn>,
    pub phases: BTreeMap<String, RuntimeProfilePhase>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeProfileActiveTurn {
    pub provider: String,
    pub started_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeProfileTurn {
    pub provider: String,
    pub duration_ms: u64,
    pub success: bool,
    pub completed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeProfilePhase {
    pub count: u64,
    pub total_ms: u64,
    pub max_ms: u64,
    #[serde(default)]
    pub average_ms: u64,
    #[serde(default)]
    pub p95_ms: u64,
    pub buckets: [u64; PROFILE_BUCKET_COUNT],
}

#[cfg(feature = "profiling")]
impl RuntimeProfilePhase {
    fn record(&mut self, elapsed_ms: u64) {
        self.count = self.count.saturating_add(1);
        self.total_ms = self.total_ms.saturating_add(elapsed_ms);
        self.max_ms = self.max_ms.max(elapsed_ms);
        let bucket = PROFILE_BUCKET_UPPER_BOUNDS_MS
            .iter()
            .position(|bound| elapsed_ms < *bound)
            .unwrap_or(PROFILE_BUCKET_COUNT - 1);
        self.buckets[bucket] = self.buckets[bucket].saturating_add(1);
        self.average_ms = self.total_ms / self.count;
        self.p95_ms = percentile_ms(&self.buckets, self.count, 95);
    }
}

#[cfg(feature = "profiling")]
fn percentile_ms(buckets: &[u64; PROFILE_BUCKET_COUNT], count: u64, percentile: u64) -> u64 {
    if count == 0 {
        return 0;
    }
    let rank = count.saturating_mul(percentile).saturating_add(99) / 100;
    let mut seen = 0_u64;
    for (index, bucket) in buckets.iter().enumerate() {
        seen = seen.saturating_add(*bucket);
        if seen >= rank {
            return PROFILE_BUCKET_UPPER_BOUNDS_MS
                .get(index)
                .copied()
                .unwrap_or(u64::MAX);
        }
    }
    u64::MAX
}

pub fn runtime_profile_path(sessions_dir: &Path, session_id: Uuid) -> PathBuf {
    sessions_dir.join(format!("{session_id}{PROFILE_FILE_SUFFIX}"))
}

pub fn read_runtime_profile(path: &Path) -> Result<RuntimeProfileSnapshot> {
    let bytes = std::fs::read(path)
        .with_context(|| format!("failed to read runtime profile {}", path.display()))?;
    let snapshot = serde_json::from_slice(&bytes)
        .with_context(|| format!("invalid runtime profile {}", path.display()))?;
    Ok(snapshot)
}

#[cfg(feature = "profiling")]
mod enabled {
    use super::*;
    use std::fs::{self, OpenOptions};
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    };
    use std::time::Instant;

    use crate::CodingProvider;

    struct ProfileState {
        snapshot: RuntimeProfileSnapshot,
        phase_started_at: Option<Instant>,
    }

    pub struct RuntimeProfiler {
        path: PathBuf,
        state: Mutex<ProfileState>,
        persist_sequence: AtomicU64,
    }

    impl RuntimeProfiler {
        pub fn start(sessions_dir: &Path, session_id: Uuid) -> Result<Option<Arc<Self>>> {
            if !profile_requested() {
                return Ok(None);
            }
            fs::create_dir_all(sessions_dir).with_context(|| {
                format!(
                    "failed to create profile directory {}",
                    sessions_dir.display()
                )
            })?;
            let now = Utc::now();
            let profiler = Arc::new(Self {
                path: runtime_profile_path(sessions_dir, session_id),
                state: Mutex::new(ProfileState {
                    snapshot: RuntimeProfileSnapshot {
                        schema_version: PROFILE_SCHEMA_VERSION,
                        session_id,
                        pid: std::process::id(),
                        started_at: now,
                        updated_at: now,
                        active_turn: None,
                        current_phase: None,
                        current_phase_started_at: None,
                        turns_completed: 0,
                        last_turn: None,
                        phases: BTreeMap::new(),
                    },
                    phase_started_at: None,
                }),
                persist_sequence: AtomicU64::new(0),
            });
            profiler.persist()?;
            Ok(Some(profiler))
        }

        pub fn begin_turn(&self, provider: CodingProvider) -> Instant {
            let now = Utc::now();
            let started = Instant::now();
            let mut state = self.lock_state();
            state.phase_started_at = Some(started);
            state.snapshot.active_turn = Some(RuntimeProfileActiveTurn {
                provider: format!("{provider:?}"),
                started_at: now,
            });
            state.snapshot.current_phase = Some("prepare".to_string());
            state.snapshot.current_phase_started_at = Some(now);
            state.snapshot.updated_at = now;
            drop(state);
            let _ = self.persist();
            started
        }

        pub fn set_phase(&self, phase: &str) {
            let now = Utc::now();
            let instant = Instant::now();
            let mut state = self.lock_state();
            self.finish_current_phase(&mut state, instant);
            state.phase_started_at = Some(instant);
            state.snapshot.current_phase = Some(phase.to_string());
            state.snapshot.current_phase_started_at = Some(now);
            state.snapshot.updated_at = now;
            drop(state);
            let _ = self.persist();
        }

        pub fn finish_turn(&self, provider: CodingProvider, started: Instant, success: bool) {
            let now = Utc::now();
            let mut state = self.lock_state();
            self.finish_current_phase(&mut state, Instant::now());
            let duration_ms = elapsed_millis(started);
            state.snapshot.turns_completed = state.snapshot.turns_completed.saturating_add(1);
            state.snapshot.last_turn = Some(RuntimeProfileTurn {
                provider: format!("{provider:?}"),
                duration_ms,
                success,
                completed_at: now,
            });
            state.snapshot.active_turn = None;
            state.snapshot.current_phase = None;
            state.snapshot.current_phase_started_at = None;
            state.snapshot.updated_at = now;
            state.phase_started_at = None;
            drop(state);
            let _ = self.persist();
        }

        fn finish_current_phase(&self, state: &mut ProfileState, now: Instant) {
            let (Some(phase), Some(started)) = (
                state.snapshot.current_phase.as_deref(),
                state.phase_started_at,
            ) else {
                return;
            };
            let elapsed_ms = elapsed_millis(started.min(now));
            state
                .snapshot
                .phases
                .entry(phase.to_string())
                .or_insert_with(|| RuntimeProfilePhase {
                    count: 0,
                    total_ms: 0,
                    max_ms: 0,
                    average_ms: 0,
                    p95_ms: 0,
                    buckets: [0; PROFILE_BUCKET_COUNT],
                })
                .record(elapsed_ms);
        }

        fn lock_state(&self) -> std::sync::MutexGuard<'_, ProfileState> {
            self.state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
        }

        fn persist(&self) -> Result<()> {
            let snapshot = self.lock_state().snapshot.clone();
            let temporary = self.path.with_extension(format!(
                "profile.{}.{}.tmp",
                std::process::id(),
                self.persist_sequence.fetch_add(1, Ordering::Relaxed)
            ));
            let bytes = serde_json::to_vec_pretty(&snapshot)?;
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temporary)
                .with_context(|| format!("failed to create {}", temporary.display()))?;
            use std::io::Write;
            file.write_all(&bytes)?;
            drop(file);
            publish_profile(&temporary, &self.path)?;
            Ok(())
        }
    }

    #[cfg(not(windows))]
    fn publish_profile(temporary: &Path, destination: &Path) -> Result<()> {
        fs::rename(temporary, destination).with_context(|| {
            format!(
                "failed to publish runtime profile {}",
                destination.display()
            )
        })
    }

    #[cfg(windows)]
    fn publish_profile(temporary: &Path, destination: &Path) -> Result<()> {
        if destination.exists() {
            fs::remove_file(destination).with_context(|| {
                format!(
                    "failed to replace runtime profile {}",
                    destination.display()
                )
            })?;
        }
        fs::rename(temporary, destination).with_context(|| {
            format!(
                "failed to publish runtime profile {}",
                destination.display()
            )
        })
    }

    impl Drop for RuntimeProfiler {
        fn drop(&mut self) {
            let _ = self.persist();
        }
    }

    fn profile_requested() -> bool {
        matches!(
            std::env::var("BORG_PROFILE").ok().as_deref().map(str::trim),
            Some("1") | Some("true") | Some("yes") | Some("on")
        )
    }

    fn elapsed_millis(started: Instant) -> u64 {
        u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    pub use RuntimeProfiler as PublicRuntimeProfiler;
}

#[cfg(feature = "profiling")]
pub use enabled::PublicRuntimeProfiler as RuntimeProfiler;

#[cfg(all(test, feature = "profiling"))]
mod tests {
    use super::*;

    #[test]
    fn phase_statistics_keep_bounded_percentile_data() {
        let mut phase = RuntimeProfilePhase {
            count: 0,
            total_ms: 0,
            max_ms: 0,
            average_ms: 0,
            p95_ms: 0,
            buckets: [0; PROFILE_BUCKET_COUNT],
        };
        phase.record(2);
        phase.record(12);
        phase.record(120);

        assert_eq!(phase.count, 3);
        assert_eq!(phase.total_ms, 134);
        assert_eq!(phase.average_ms, 44);
        assert_eq!(phase.max_ms, 120);
        assert_eq!(phase.p95_ms, 250);
        assert_eq!(phase.buckets.iter().sum::<u64>(), 3);
    }
}
