//! The `borg` libraries a command imports to call Borg from code: Python's
//! `import borg` and Bun's `import borg from "borg"` (Node: `require`). They
//! speak the tool-socket protocol `borg call` uses, so every language reaches
//! the same dispatcher and journals its calls as steps of the command.

use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::path::Path;

use anyhow::{Context, Result};

const FILES: [(&str, &str); 3] = [
    ("python/borg.py", include_str!("clients/python/borg.py")),
    (
        "js/borg/index.mjs",
        include_str!("clients/js/borg/index.mjs"),
    ),
    (
        "js/borg/package.json",
        include_str!("clients/js/borg/package.json"),
    ),
];

/// Write the clients under `root` and put them on the command import paths.
/// The directory is named by content, so sessions running different Borg
/// builds never overwrite each other's copy.
pub(crate) fn install(root: &Path, environment: &mut BTreeMap<String, String>) -> Result<()> {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    FILES.hash(&mut hasher);
    let dir = root
        .join("clients")
        .join(format!("{:016x}", hasher.finish()));
    for (relative, contents) in FILES {
        let path = dir.join(relative);
        if std::fs::read_to_string(&path).is_ok_and(|existing| existing == contents) {
            continue;
        }
        let parent = path.parent().context("client path has a parent")?;
        std::fs::create_dir_all(parent)?;
        let temporary = tempfile::NamedTempFile::new_in(parent)?;
        std::fs::write(temporary.path(), contents)?;
        temporary
            .persist(&path)
            .with_context(|| format!("failed to install {}", path.display()))?;
    }
    for (variable, subdir) in [("PYTHONPATH", "python"), ("NODE_PATH", "js")] {
        let inherited = environment
            .get(variable)
            .cloned()
            .or_else(|| std::env::var(variable).ok())
            .filter(|value| !value.is_empty());
        let paths = std::iter::once(dir.join(subdir))
            .chain(inherited.iter().flat_map(std::env::split_paths));
        let joined = std::env::join_paths(paths)?;
        environment.insert(variable.to_string(), joined.to_string_lossy().into_owned());
    }
    Ok(())
}
