#[cfg(target_os = "linux")]
use std::fs;
#[cfg(target_os = "linux")]
use std::path::Path;

use super::LocalModel;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FitStatus {
    Fits,
    MaySpill,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FitReport {
    pub status: FitStatus,
    pub model_bytes: u64,
    pub available_vram_bytes: Option<u64>,
}

impl FitReport {
    pub fn for_model(model: &LocalModel) -> Self {
        let available_vram_bytes = available_vram_bytes();
        let status = match available_vram_bytes {
            Some(available) if model.size_bytes <= available => FitStatus::Fits,
            Some(_) => FitStatus::MaySpill,
            None => FitStatus::Unknown,
        };
        Self {
            status,
            model_bytes: model.size_bytes,
            available_vram_bytes,
        }
    }

    pub fn label(self) -> String {
        match self.status {
            FitStatus::Fits => "fits in available VRAM".to_string(),
            FitStatus::MaySpill => {
                "may spill to system RAM; llama.cpp will fit automatically".to_string()
            }
            FitStatus::Unknown => "VRAM fit unknown; llama.cpp will fit automatically".to_string(),
        }
    }
}

/// Return aggregate free VRAM for the Linux DRM devices that expose the
/// standard AMD/Intel memory counters. The result is intentionally an
/// estimate for display only; llama.cpp remains the authority on fitting.
pub fn available_vram_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let drm = Path::new("/sys/class/drm");
        let mut total = 0_u64;
        let mut used = 0_u64;
        let mut found = false;
        let entries = fs::read_dir(drm).ok()?;
        for entry in entries.flatten() {
            let device = entry.path().join("device");
            let total_path = device.join("mem_info_vram_total");
            let used_path = device.join("mem_info_vram_used");
            let Some(device_total) = read_u64(&total_path) else {
                continue;
            };
            let Some(device_used) = read_u64(&used_path) else {
                continue;
            };
            found = true;
            total = total.saturating_add(device_total);
            used = used.saturating_add(device_used);
        }
        found.then_some(total.saturating_sub(used))
    }

    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

#[cfg(target_os = "linux")]
fn read_u64(path: &Path) -> Option<u64> {
    fs::read_to_string(path).ok()?.trim().parse().ok()
}

pub fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}
