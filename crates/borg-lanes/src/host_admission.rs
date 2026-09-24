//! One RAM admission lock across lane journals and migrating external adapters.
use anyhow::Result;
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};
use uuid::Uuid;

pub(crate) fn directory(root: &Path) -> PathBuf {
    if cfg!(test) {
        return root.join("host-admission");
    }
    std::env::var_os("BORG_HOST_ADMISSION_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let runtime = PathBuf::from(format!("/run/user/{}", unsafe { libc::geteuid() }));
            if runtime.is_dir() {
                runtime.join("borg-host-admission")
            } else {
                PathBuf::from(format!("/tmp/borg-host-admission-{}", unsafe {
                    libc::geteuid()
                }))
            }
        })
}

pub(crate) fn journals(root: &Path) -> Result<Vec<PathBuf>> {
    let path = directory(root).join("journals.json");
    if !path.exists() {
        return Ok(vec![]);
    }
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

/// Always before the local journal lock. Adapters use this same flock before
/// publishing a claim; admission and its durable journal write are one transaction.
pub(crate) fn lock(root: &Path) -> Result<File> {
    use std::os::unix::fs::OpenOptionsExt;
    let dir = directory(root);
    fs::create_dir_all(dir.join("claims"))?;
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(dir.join("admission.lock"))?;
    lock.lock()?;
    let mut paths = journals(root)?;
    let before = paths.clone();
    paths.retain(|p| p.parent().is_some_and(Path::is_dir));
    let own = root.canonicalize()?.join("state.json");
    if !paths.contains(&own) {
        paths.push(own);
    }
    if paths != before {
        let temp = dir.join(format!("journals.{}.tmp", Uuid::new_v4()));
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&temp)?;
        file.write_all(&serde_json::to_vec(&paths)?)?;
        file.sync_all()?;
        fs::rename(temp, dir.join("journals.json"))?;
        File::open(&dir)?.sync_all()?;
    }
    Ok(lock)
}

/// Kernel-held bridge claims survive another worker's independent preflight.
/// A lost supervisor cannot release capacity while its cgroup is populated.
pub(crate) fn bridge_reservations(root: &Path) -> Result<Vec<(u64, Option<String>)>> {
    let dir = directory(root).join("claims");
    if !dir.exists() {
        return Ok(vec![]);
    }
    let mut out = vec![];
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().is_none_or(|s| s != "json") {
            continue;
        }
        let file = OpenOptions::new().read(true).write(true).open(&path)?;
        let value: serde_json::Value = serde_json::from_slice(&fs::read(&path)?)?;
        let scope = value["cgroup"]
            .as_str()
            .filter(|s| !s.is_empty())
            .map(str::to_owned);
        let populated = scope.as_ref().is_some_and(|s| {
            fs::read_to_string(
                Path::new("/sys/fs/cgroup")
                    .join(s.trim_start_matches('/'))
                    .join("cgroup.events"),
            )
            .is_ok_and(|text| text.lines().any(|l| l == "populated 1"))
        });
        match file.try_lock() {
            Err(std::fs::TryLockError::WouldBlock) => (),
            Err(std::fs::TryLockError::Error(error)) => return Err(error.into()),
            Ok(()) if !populated => {
                continue;
            }
            Ok(()) => (),
        }
        out.push((
            value["reserve_ram_bytes"]
                .as_u64()
                .ok_or_else(|| anyhow::anyhow!("invalid RAM claim {}", path.display()))?,
            scope,
        ));
    }
    Ok(out)
}
