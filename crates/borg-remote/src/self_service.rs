use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail, ensure};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

const MAX_PLUGIN_TEXT: usize = 512 * 1024;
const MAX_BLU_WORKFLOW_SOURCE: usize = 256 * 1024;
const MAX_RETRIEVAL_ADAPTER_SOURCE: usize = 256 * 1024;
const MAX_RETRIEVAL_ADAPTERS: usize = 128;
const MAX_BLU_EXTENSIONS: usize = 128;
const MAX_EXTENSION_HISTORY_VERSIONS: usize = 32;
const SETTINGS_SECTIONS: &[&str] = &[
    "capabilities",
    "extensions",
    "team",
    "commands",
    "keybindings",
    "mcp",
    "approvals",
    "updates",
    "providers",
];

#[derive(Debug, Clone)]
pub(crate) struct SelfServiceContext {
    cwd: PathBuf,
}

impl SelfServiceContext {
    pub(crate) fn new(cwd: PathBuf) -> Self {
        Self { cwd }
    }

    pub(crate) fn call(&self, name: &str, arguments: Value) -> Result<Value> {
        match name {
            "list_plugins" => {
                let _: NoArgs = serde_json::from_value(arguments)?;
                self.list_plugins()
            }
            "read_plugin" => {
                let args: PluginIdArgs = serde_json::from_value(arguments)?;
                self.read_plugin(&args.id)
            }
            "get_agent_settings" => {
                let args: SettingsScopeArgs = serde_json::from_value(arguments)?;
                self.get_settings(args.scope.as_deref().unwrap_or("user"))
            }
            "update_agent_settings" => {
                let args: UpdateSettingsArgs = serde_json::from_value(arguments)?;
                self.update_settings(args.scope.as_deref().unwrap_or("user"), args.updates)
            }
            "create_plugin" => {
                let args: CreatePluginArgs = serde_json::from_value(arguments)?;
                self.create_plugin(args)
            }
            "rollback_plugin" => {
                let args: RollbackPluginArgs = serde_json::from_value(arguments)?;
                self.rollback_plugin(&args.id, &args.revision)
            }
            "list_retrieval_adapters" => {
                let _: NoArgs = serde_json::from_value(arguments)?;
                self.list_retrieval_adapters()
            }
            "read_retrieval_adapter" => {
                let args: RetrievalAdapterIdArgs = serde_json::from_value(arguments)?;
                self.read_retrieval_adapter(&args.id)
            }
            "create_retrieval_adapter" => {
                let args: CreateRetrievalAdapterArgs = serde_json::from_value(arguments)?;
                self.create_retrieval_adapter(args)
            }
            "rollback_retrieval_adapter" => {
                let args: RollbackRetrievalAdapterArgs = serde_json::from_value(arguments)?;
                self.rollback_retrieval_adapter(&args.id, &args.revision)
            }
            "list_blu_extensions" => {
                let _: NoArgs = serde_json::from_value(arguments)?;
                self.list_blu_extensions()
            }
            "read_blu_extension" => {
                let args: BluExtensionIdArgs = serde_json::from_value(arguments)?;
                self.read_blu_extension(&args.id, args.scope.as_deref().unwrap_or("project"))
            }
            "create_blu_extension" => {
                let args: CreateBluExtensionArgs = serde_json::from_value(arguments)?;
                self.create_blu_extension(args)
            }
            "create_extension" => {
                let args: CreateBluExtensionArgs = serde_json::from_value(arguments)?;
                self.create_blu_extension(args)
            }
            "set_blu_extension_enabled" => {
                let args: SetBluExtensionEnabledArgs = serde_json::from_value(arguments)?;
                self.set_blu_extension_enabled(
                    &args.id,
                    args.scope.as_deref().unwrap_or("project"),
                    args.enabled,
                )
            }
            "remove_blu_extension" => {
                let args: BluExtensionIdArgs = serde_json::from_value(arguments)?;
                self.remove_blu_extension(&args.id, args.scope.as_deref().unwrap_or("project"))
            }
            "rollback_blu_extension" => {
                let args: RollbackBluExtensionArgs = serde_json::from_value(arguments)?;
                self.rollback_blu_extension(
                    &args.id,
                    args.scope.as_deref().unwrap_or("project"),
                    &args.revision,
                )
            }
            "reload_blu_extensions" => {
                let args: BluScopeArgs = serde_json::from_value(arguments)?;
                let scope = args.scope.as_deref().unwrap_or("project");
                let path = self.reload_blu_extensions(scope)?;
                let audit = self.audit_blu(scope, "reload", "catalog")?;
                Ok(json!({
                    "scope": scope,
                    "reload_signal": path,
                    "audit": audit,
                    "hot_reload": "next native turn boundary",
                }))
            }
            other => bail!("unknown self-service tool: {other}"),
        }
    }

    fn get_settings(&self, scope: &str) -> Result<Value> {
        let path = self.settings_path(scope)?;
        let exists = path.is_file();
        let settings = if exists {
            let source = fs::read_to_string(&path)
                .with_context(|| format!("failed to read agent settings {}", path.display()))?;
            let parsed: toml::Value = toml::from_str(&source)
                .with_context(|| format!("invalid agent settings {}", path.display()))?;
            redact_toml(parsed)
        } else {
            toml::Value::Table(toml::map::Map::new())
        };
        Ok(json!({
            "scope": scope,
            "path": path,
            "exists": exists,
            "settings": serde_json::to_value(settings)?,
            "hot_reload": ["commands.aliases", "keybindings"],
            "next_turn_reload": ["extensions", "mcp"],
            "restart_required": [
                "capabilities", "team", "approvals", "updates", "providers"
            ]
        }))
    }

    fn list_plugins(&self) -> Result<Value> {
        let root = self.cwd.join(".borg").join("skills");
        let mut plugins = Vec::new();
        if root.is_dir() {
            let mut entries = fs::read_dir(&root)?.collect::<std::io::Result<Vec<_>>>()?;
            entries.sort_by_key(|entry| entry.file_name());
            for entry in entries.into_iter().take(128) {
                let id = entry.file_name().to_string_lossy().to_string();
                if validate_plugin_id(&id).is_err() {
                    continue;
                }
                let path = entry.path().join("SKILL.md");
                if !path.is_file() {
                    continue;
                }
                let size_bytes = fs::metadata(&path)?.len();
                if size_bytes > MAX_PLUGIN_TEXT as u64 {
                    plugins.push(json!({
                        "id": id,
                        "path": path,
                        "size_bytes": size_bytes,
                        "description": "Plugin is too large to inspect through the Borg tool"
                    }));
                    continue;
                }
                let content = fs::read_to_string(&path)?;
                let (version, revision) = plugin_version_and_revision(&content);
                plugins.push(json!({
                    "id": id,
                    "path": path,
                    "size_bytes": size_bytes,
                    "description": plugin_description(&content),
                    "version": version,
                    "revision": revision,
                    "versions": self.plugin_versions(&id)?,
                }));
            }
        }
        Ok(json!({ "root": root, "plugins": plugins }))
    }

    fn read_plugin(&self, id: &str) -> Result<Value> {
        validate_plugin_id(id)?;
        let root = self.cwd.join(".borg").join("skills");
        let path = root.join(id).join("SKILL.md");
        let canonical_root = root.canonicalize().unwrap_or(root);
        let canonical = path
            .canonicalize()
            .with_context(|| format!("plugin `{id}` does not exist"))?;
        ensure!(
            canonical.starts_with(&canonical_root),
            "plugin path escapes the project skill root"
        );
        let metadata = fs::metadata(&canonical)?;
        ensure!(
            metadata.len() <= MAX_PLUGIN_TEXT as u64,
            "plugin `{id}` is too large"
        );
        let content = fs::read_to_string(&canonical)?;
        let (version, revision) = plugin_version_and_revision(&content);
        Ok(json!({
            "id": id,
            "path": canonical,
            "version": version,
            "revision": revision,
            "content": content
        }))
    }

    fn update_settings(&self, scope: &str, updates: Map<String, Value>) -> Result<Value> {
        let path = self.settings_path(scope)?;
        ensure!(
            !updates.is_empty(),
            "updates must contain at least one setting section"
        );
        for key in updates.keys() {
            ensure!(
                SETTINGS_SECTIONS.contains(&key.as_str()),
                "unsupported settings section `{key}`; allowed sections: {}",
                SETTINGS_SECTIONS.join(", ")
            );
        }
        let updated_sections = updates.keys().cloned().collect::<Vec<_>>();
        let mut root = if path.is_file() {
            let source = fs::read_to_string(&path)
                .with_context(|| format!("failed to read agent settings {}", path.display()))?;
            toml::from_str::<toml::Value>(&source)
                .with_context(|| format!("invalid agent settings {}", path.display()))?
        } else {
            toml::Value::Table(toml::map::Map::new())
        };
        let table = root
            .as_table_mut()
            .context("agent settings root must be a TOML table")?;
        for (key, patch) in updates {
            if patch.is_null() {
                table.remove(&key);
                continue;
            }
            let patch = json_to_toml(patch)
                .with_context(|| format!("settings section `{key}` is not TOML-compatible"))?;
            merge_toml(
                table
                    .entry(key)
                    .or_insert_with(|| toml::Value::Table(toml::map::Map::new())),
                patch,
            );
        }
        validate_settings_shape(&root)?;
        let rendered = toml::to_string_pretty(&root).context("serialize agent settings")?;
        write_atomic(&path, rendered.as_bytes())?;
        Ok(json!({
            "scope": scope,
            "path": path,
            "updated_sections": updated_sections,
            "hot_reloaded": ["commands.aliases", "keybindings"],
            "next_turn_reloaded": ["extensions", "mcp"],
            "restart_required": [
                "capabilities", "team", "approvals", "updates"
            ],
            "note": "Aliases and keybindings reload in the running TUI. Blu and base MCP catalogs swap at the next turn boundary; capability/team/approval/update policy changes require a new session."
        }))
    }

    fn create_plugin(&self, args: CreatePluginArgs) -> Result<Value> {
        validate_plugin_id(&args.id)?;
        let version = args.version.as_deref().unwrap_or("0.1.0");
        validate_extension_version(version)?;
        ensure!(
            args.description.len() <= MAX_PLUGIN_TEXT,
            "plugin description is too large"
        );
        ensure!(
            args.instructions.len() <= MAX_PLUGIN_TEXT,
            "plugin instructions are too large"
        );
        ensure!(
            !args.description.contains('\0'),
            "plugin description contains NUL"
        );
        ensure!(
            !args.instructions.contains('\0'),
            "plugin instructions contain NUL"
        );
        let skills_root = self.cwd.join(".borg").join("skills");
        let skill_dir = skills_root.join(&args.id);
        let skill_path = skill_dir.join("SKILL.md");
        if skill_path.exists() && !args.overwrite {
            bail!(
                "plugin `{}` already exists at {}; pass overwrite=true to replace it",
                args.id,
                skill_path.display()
            );
        }
        let revision = plugin_revision(&args, version);
        let content = format!(
            "---\nname: {}\ndescription: {}\nversion: {}\nrevision: {}\n---\n\n# {}\n\n{}\n",
            args.id,
            yaml_scalar(&args.description),
            version,
            revision,
            args.id,
            args.instructions.trim()
        );
        fs::create_dir_all(&skills_root)?;
        let staging_root = tempfile::tempdir_in(&skills_root)?;
        let staging = staging_root.path().join(&args.id);
        fs::create_dir_all(&staging)?;
        write_atomic(&staging.join("SKILL.md"), content.as_bytes())?;
        let backup = skills_root.join(format!(".{}.backup-{}", args.id, Uuid::new_v4()));
        if skill_dir.exists() {
            fs::rename(&skill_dir, &backup)?;
        }
        if let Err(error) = fs::rename(&staging, &skill_dir) {
            if backup.exists() {
                let _ = fs::rename(&backup, &skill_dir);
            }
            return Err(error).context("activate staged Borg plugin");
        }
        if backup.exists()
            && let Err(error) = self.archive_plugin_package(&args.id, &backup)
        {
            let _ = fs::remove_dir_all(&skill_dir);
            let _ = fs::rename(&backup, &skill_dir);
            return Err(error).context("archive previous Borg plugin revision");
        }
        Ok(json!({
            "id": args.id,
            "path": skill_path,
            "version": version,
            "revision": revision,
            "hot_reload": "next native turn",
            "restart_required": false,
            "note": "Project skills are rescanned at the start of every native turn. Blu MCP manifests also reload at the next turn boundary."
        }))
    }

    fn plugin_history_root(&self, id: &str) -> PathBuf {
        self.cwd
            .join(".borg")
            .join("skills")
            .join(".versions")
            .join(id)
    }

    fn archive_plugin_package(&self, id: &str, package: &Path) -> Result<String> {
        let history_root = self.plugin_history_root(id);
        fs::create_dir_all(&history_root)?;
        let revision = plugin_package_revision(package)?;
        let mut archive_key = revision.clone();
        let mut archive = history_root.join(&archive_key);
        if archive.exists() {
            archive_key = format!("{revision}-{}", Uuid::new_v4());
            archive = history_root.join(&archive_key);
        }
        fs::rename(package, &archive)?;
        self.prune_plugin_history(&history_root)?;
        Ok(archive_key)
    }

    fn prune_plugin_history(&self, history_root: &Path) -> Result<()> {
        let mut entries = fs::read_dir(history_root)?
            .filter_map(std::result::Result::ok)
            .filter(|entry| entry.path().is_dir())
            .map(|entry| {
                let modified = entry
                    .metadata()
                    .and_then(|metadata| metadata.modified())
                    .unwrap_or(UNIX_EPOCH);
                (modified, entry.path())
            })
            .collect::<Vec<_>>();
        entries.sort_by_key(|(modified, path)| (*modified, path.clone()));
        while entries.len() > MAX_EXTENSION_HISTORY_VERSIONS {
            let (_, path) = entries.remove(0);
            fs::remove_dir_all(path)?;
        }
        Ok(())
    }

    fn plugin_versions(&self, id: &str) -> Result<Vec<Value>> {
        let history_root = self.plugin_history_root(id);
        if !history_root.is_dir() {
            return Ok(Vec::new());
        }
        let mut versions = fs::read_dir(history_root)?
            .filter_map(std::result::Result::ok)
            .filter(|entry| entry.path().is_dir())
            .filter_map(|entry| {
                let path = entry.path().join("SKILL.md");
                let content = fs::read_to_string(path).ok()?;
                let (version, revision) = plugin_version_and_revision(&content);
                Some(json!({
                    "revision": entry.file_name().to_string_lossy(),
                    "version": version,
                    "path": entry.path(),
                    "content_revision": revision,
                }))
            })
            .collect::<Vec<_>>();
        versions.sort_by_key(|value| value["revision"].as_str().unwrap_or_default().to_string());
        Ok(versions)
    }

    fn rollback_plugin(&self, id: &str, revision: &str) -> Result<Value> {
        validate_plugin_id(id)?;
        validate_extension_revision(revision)?;
        let root = self.cwd.join(".borg").join("skills");
        let destination = root.join(id);
        let history_root = self.plugin_history_root(id);
        let canonical_root = history_root.canonicalize().unwrap_or(history_root.clone());
        let selected = history_root.join(revision);
        let canonical_selected = selected
            .canonicalize()
            .with_context(|| format!("plugin revision `{revision}` does not exist"))?;
        ensure!(
            canonical_selected.starts_with(&canonical_root)
                && canonical_selected.join("SKILL.md").is_file(),
            "invalid plugin revision `{revision}`"
        );
        let current = plugin_package_revision(&destination)?;
        ensure!(
            current != revision,
            "plugin revision `{revision}` is already active"
        );
        let current_staging = root.join(format!(".{}.rollback-{}", id, Uuid::new_v4()));
        fs::rename(&destination, &current_staging)?;
        if let Err(error) = fs::rename(&selected, &destination) {
            let _ = fs::rename(&current_staging, &destination);
            return Err(error).context("activate rolled-back plugin revision");
        }
        if let Err(error) = self.archive_plugin_package(id, &current_staging) {
            let _ = fs::remove_dir_all(&destination);
            let _ = fs::rename(&current_staging, &destination);
            return Err(error).context("archive current plugin during rollback");
        }
        Ok(json!({
            "id": id,
            "revision": revision,
            "hot_reload": "next native turn",
        }))
    }

    fn list_retrieval_adapters(&self) -> Result<Value> {
        let root = self.retrieval_adapters_root();
        let mut adapters = Vec::new();
        if root.is_dir() {
            let mut entries = fs::read_dir(&root)?.collect::<std::io::Result<Vec<_>>>()?;
            entries.sort_by_key(|entry| entry.file_name());
            for entry in entries.into_iter().take(MAX_RETRIEVAL_ADAPTERS) {
                let package = entry.path();
                if !package.is_dir() {
                    continue;
                }
                let id = entry.file_name().to_string_lossy().to_string();
                if validate_plugin_id(&id).is_err() {
                    continue;
                }
                let manifest_path = package.join("manifest.json");
                let manifest = match fs::read_to_string(&manifest_path)
                    .ok()
                    .and_then(|source| serde_json::from_str::<Value>(&source).ok())
                {
                    Some(manifest) => manifest,
                    None => continue,
                };
                adapters.push(json!({
                    "id": id,
                    "path": package,
                    "manifest": manifest,
                    "source_bytes": fs::metadata(package.join("adapter.source"))?.len(),
                    "tests_present": package.join("tests.source").is_file(),
                    "versions": self.retrieval_adapter_versions(&id)?,
                }));
            }
        }
        Ok(json!({ "root": root, "adapters": adapters }))
    }

    fn read_retrieval_adapter(&self, id: &str) -> Result<Value> {
        validate_plugin_id(id)?;
        let package = self.retrieval_adapters_root().join(id);
        let canonical_root = self
            .retrieval_adapters_root()
            .canonicalize()
            .unwrap_or_else(|_| self.retrieval_adapters_root());
        let canonical = package
            .canonicalize()
            .with_context(|| format!("retrieval adapter `{id}` does not exist"))?;
        ensure!(
            canonical.starts_with(&canonical_root),
            "retrieval adapter path escapes the project retriever root"
        );
        let manifest = read_retrieval_adapter_file(&canonical, "manifest.json")?;
        let source = read_bounded_retrieval_source(&canonical.join("adapter.source"))?;
        let tests = if canonical.join("tests.source").is_file() {
            Some(read_bounded_retrieval_source(
                &canonical.join("tests.source"),
            )?)
        } else {
            None
        };
        Ok(json!({
            "id": id,
            "path": canonical,
            "manifest": manifest,
            "source": source,
            "tests": tests,
            "versions": self.retrieval_adapter_versions(id)?,
        }))
    }

    fn create_retrieval_adapter(&self, args: CreateRetrievalAdapterArgs) -> Result<Value> {
        validate_plugin_id(&args.id)?;
        let version = args.version.as_deref().unwrap_or("0.1.0");
        validate_extension_version(version)?;
        let language = args.language.as_deref().unwrap_or("python");
        ensure!(
            matches!(language, "python" | "javascript"),
            "retrieval adapter language must be python or javascript"
        );
        ensure!(
            !args.description.trim().is_empty(),
            "retrieval adapter description must not be empty"
        );
        ensure!(
            args.description.len() <= 4_096,
            "retrieval adapter description is too large"
        );
        ensure!(
            !args.source.trim().is_empty(),
            "retrieval adapter source must not be empty"
        );
        ensure!(
            args.source.len() <= MAX_RETRIEVAL_ADAPTER_SOURCE,
            "retrieval adapter source is too large"
        );
        ensure!(
            args.source.contains("retrieve"),
            "retrieval adapter source must define a retrieve function"
        );
        ensure!(
            !args.description.contains('\0') && !args.source.contains('\0'),
            "retrieval adapter contains NUL"
        );
        if let Some(tests) = args.tests.as_deref() {
            ensure!(
                tests.len() <= MAX_RETRIEVAL_ADAPTER_SOURCE,
                "retrieval adapter tests are too large"
            );
            ensure!(
                tests.contains("test"),
                "retrieval adapter tests must define a test function"
            );
            ensure!(!tests.contains('\0'), "retrieval adapter tests contain NUL");
        }
        let root = self.retrieval_adapters_root();
        let package = root.join(&args.id);
        if package.exists() && !args.overwrite {
            bail!(
                "retrieval adapter `{}` already exists at {}; pass overwrite=true to replace it",
                args.id,
                package.display()
            );
        }
        let revision = retrieval_adapter_revision(
            &args.id,
            &args.description,
            language,
            version,
            &args.source,
            args.tests.as_deref(),
        );
        let manifest = json!({
            "schema": "borg.retrieval-adapter.v1",
            "id": args.id,
            "description": args.description,
            "language": language,
            "version": version,
            "revision": revision,
            "entrypoint": "retrieve",
            "test_entrypoint": args.tests.as_ref().map(|_| "test"),
            "authority": "canonical_history",
            "created_by": "agent",
        });
        fs::create_dir_all(&root)?;
        let staging_root = tempfile::tempdir_in(&root)?;
        let staging = staging_root.path().join(&args.id);
        fs::create_dir_all(&staging)?;
        write_atomic(
            &staging.join("manifest.json"),
            serde_json::to_vec_pretty(&manifest)?.as_slice(),
        )?;
        write_atomic(&staging.join("adapter.source"), args.source.as_bytes())?;
        if let Some(tests) = args.tests.as_deref() {
            write_atomic(&staging.join("tests.source"), tests.as_bytes())?;
        }
        let backup = root.join(format!(".{}.backup-{}", args.id, Uuid::new_v4()));
        if package.exists() {
            fs::rename(&package, &backup)?;
        }
        if let Err(error) = fs::rename(&staging, &package) {
            if backup.exists() {
                let _ = fs::rename(&backup, &package);
            }
            return Err(error).context("activate staged retrieval adapter");
        }
        if backup.exists()
            && let Err(error) = self.archive_retrieval_adapter_package(&args.id, &backup)
        {
            let _ = fs::remove_dir_all(&package);
            let _ = fs::rename(&backup, &package);
            return Err(error).context("archive previous retrieval adapter revision");
        }
        Ok(json!({
            "id": args.id,
            "path": package,
            "version": version,
            "revision": revision,
            "language": language,
            "tests_present": args.tests.is_some(),
            "authority": "canonical_history",
            "reload": "next runtime call",
            "note": "The adapter may rank or transform history-index documents, but every result must be resolved through query_history before it is treated as evidence."
        }))
    }

    fn rollback_retrieval_adapter(&self, id: &str, revision: &str) -> Result<Value> {
        validate_plugin_id(id)?;
        validate_extension_revision(revision)?;
        let root = self.retrieval_adapters_root();
        let history_root = self.retrieval_adapter_history_root(id);
        let canonical_root = history_root
            .canonicalize()
            .unwrap_or_else(|_| history_root.clone());
        let selected = history_root.join(revision);
        let canonical_selected = selected
            .canonicalize()
            .with_context(|| format!("retrieval adapter revision `{revision}` does not exist"))?;
        ensure!(
            canonical_selected.starts_with(&canonical_root)
                && canonical_selected.join("manifest.json").is_file()
                && canonical_selected.join("adapter.source").is_file(),
            "invalid retrieval adapter revision `{revision}`"
        );
        let package = root.join(id);
        let current = retrieval_adapter_package_revision(&package)?;
        ensure!(
            current != revision,
            "retrieval adapter revision `{revision}` is already active"
        );
        let current_staging = root.join(format!(".{}.rollback-{}", id, Uuid::new_v4()));
        fs::rename(&package, &current_staging)?;
        if let Err(error) = fs::rename(&selected, &package) {
            let _ = fs::rename(&current_staging, &package);
            return Err(error).context("activate rolled-back retrieval adapter revision");
        }
        if let Err(error) = self.archive_retrieval_adapter_package(id, &current_staging) {
            let _ = fs::remove_dir_all(&package);
            let _ = fs::rename(&current_staging, &package);
            return Err(error).context("archive current retrieval adapter during rollback");
        }
        Ok(json!({
            "id": id,
            "revision": revision,
            "reload": "next runtime call",
            "authority": "canonical_history",
        }))
    }

    fn retrieval_adapters_root(&self) -> PathBuf {
        self.cwd.join(".borg").join("retrievers")
    }

    fn retrieval_adapter_history_root(&self, id: &str) -> PathBuf {
        self.retrieval_adapters_root().join(".versions").join(id)
    }

    fn archive_retrieval_adapter_package(&self, id: &str, package: &Path) -> Result<String> {
        let history_root = self.retrieval_adapter_history_root(id);
        fs::create_dir_all(&history_root)?;
        let revision = retrieval_adapter_package_revision(package)?;
        let mut archive_key = revision.clone();
        let mut archive = history_root.join(&archive_key);
        if archive.exists() {
            archive_key = format!("{revision}-{}", Uuid::new_v4());
            archive = history_root.join(&archive_key);
        }
        fs::rename(package, &archive)?;
        self.prune_retrieval_adapter_history(&history_root)?;
        Ok(archive_key)
    }

    fn prune_retrieval_adapter_history(&self, history_root: &Path) -> Result<()> {
        let mut entries = fs::read_dir(history_root)?
            .filter_map(std::result::Result::ok)
            .filter(|entry| entry.path().is_dir())
            .map(|entry| {
                let modified = entry
                    .metadata()
                    .and_then(|metadata| metadata.modified())
                    .unwrap_or(UNIX_EPOCH);
                (modified, entry.path())
            })
            .collect::<Vec<_>>();
        entries.sort_by_key(|(modified, path)| (*modified, path.clone()));
        while entries.len() > MAX_EXTENSION_HISTORY_VERSIONS {
            let (_, path) = entries.remove(0);
            fs::remove_dir_all(path)?;
        }
        Ok(())
    }

    fn retrieval_adapter_versions(&self, id: &str) -> Result<Vec<Value>> {
        let history_root = self.retrieval_adapter_history_root(id);
        if !history_root.is_dir() {
            return Ok(Vec::new());
        }
        let mut versions = fs::read_dir(&history_root)?
            .filter_map(std::result::Result::ok)
            .filter(|entry| entry.path().is_dir())
            .filter_map(|entry| {
                let manifest = read_retrieval_adapter_file(&entry.path(), "manifest.json").ok()?;
                Some(json!({
                    "revision": entry.file_name().to_string_lossy(),
                    "manifest": manifest,
                    "path": entry.path(),
                }))
            })
            .collect::<Vec<_>>();
        versions.sort_by_key(|value| value["revision"].as_str().unwrap_or_default().to_string());
        Ok(versions)
    }

    fn list_blu_extensions(&self) -> Result<Value> {
        let mut extensions = Vec::new();
        for scope in ["project", "user"] {
            let root = self.blu_extensions_root(scope)?;
            if !root.is_dir() {
                continue;
            }
            let mut entries = fs::read_dir(&root)?.collect::<std::io::Result<Vec<_>>>()?;
            entries.sort_by_key(|entry| entry.file_name());
            for entry in entries.into_iter().take(MAX_BLU_EXTENSIONS) {
                let package = entry.path();
                if !package.is_dir() {
                    continue;
                }
                let id = entry.file_name().to_string_lossy().to_string();
                if validate_plugin_id(&id).is_err() {
                    continue;
                }
                let manifest_path = package.join("blu.toml");
                if !manifest_path.is_file() {
                    continue;
                }
                let manifest = match fs::read_to_string(&manifest_path)
                    .ok()
                    .and_then(|source| toml::from_str::<toml::Value>(&source).ok())
                {
                    Some(manifest) => manifest,
                    None => continue,
                };
                let state = self.blu_extension_state(scope, &id)?;
                let enabled = state
                    .as_ref()
                    .and_then(|state| state.get("enabled"))
                    .and_then(toml::Value::as_bool)
                    .or_else(|| manifest.get("enabled").and_then(toml::Value::as_bool))
                    .unwrap_or(true);
                let workflows = manifest
                    .get("workflows")
                    .and_then(toml::Value::as_table)
                    .map(|workflows| workflows.keys().cloned().collect::<Vec<_>>())
                    .unwrap_or_default();
                extensions.push(json!({
                    "id": id,
                    "scope": scope,
                    "path": package,
                    "manifest": manifest_path,
                    "version": manifest.get("version"),
                    "revision": manifest.get("revision"),
                    "versions": self.extension_versions(scope, &id)?,
                    "enabled": enabled,
                    "workflows": workflows,
                    "runtimes": manifest
                        .get("workflows")
                        .and_then(toml::Value::as_table)
                        .map(|workflows| {
                            workflows
                                .iter()
                                .filter_map(|(name, workflow)| {
                                    workflow
                                        .get("runtime")
                                        .and_then(toml::Value::as_str)
                                        .map(|runtime| (name, runtime))
                                })
                                .collect::<BTreeMap<_, _>>()
                        })
                        .unwrap_or_default(),
                    "activation": "next native turn boundary",
                }));
            }
        }
        Ok(json!({ "extensions": extensions }))
    }

    fn read_blu_extension(&self, id: &str, scope: &str) -> Result<Value> {
        validate_plugin_id(id)?;
        let package = self.blu_package_path(scope, id)?;
        let manifest_path = package.join("blu.toml");
        let manifest_source = fs::read_to_string(&manifest_path)
            .with_context(|| format!("Blu extension {id} does not exist"))?;
        ensure!(
            manifest_source.len() <= MAX_PLUGIN_TEXT,
            "Blu manifest is too large"
        );
        let manifest: toml::Value = toml::from_str(&manifest_source)
            .with_context(|| format!("invalid Blu manifest {}", manifest_path.display()))?;
        let mut files = BTreeMap::new();
        let skill_root = package.join("skills");
        if skill_root.is_dir() {
            let skill_path = skill_root.join(id).join("SKILL.md");
            if skill_path.is_file() {
                let metadata = fs::metadata(&skill_path)?;
                ensure!(
                    metadata.len() <= MAX_PLUGIN_TEXT as u64,
                    "Blu skill file is too large"
                );
                files.insert(
                    format!("skills/{id}/SKILL.md"),
                    fs::read_to_string(skill_path)?,
                );
            }
        }
        if let Some(workflows) = manifest.get("workflows").and_then(toml::Value::as_table) {
            for (name, workflow) in workflows {
                validate_plugin_id(name)?;
                let entrypoint = workflow
                    .get("entrypoint")
                    .and_then(toml::Value::as_str)
                    .context("Blu workflow is missing an entrypoint")?;
                validate_relative_path(Path::new(entrypoint), "Blu workflow entrypoint")?;
                let path = package.join(entrypoint);
                let canonical_root = package.canonicalize()?;
                let canonical = path.canonicalize()?;
                ensure!(
                    canonical.starts_with(&canonical_root),
                    "Blu workflow entrypoint escapes its package"
                );
                let metadata = fs::metadata(&canonical)?;
                ensure!(
                    metadata.len() <= MAX_BLU_WORKFLOW_SOURCE as u64,
                    "Blu workflow is too large"
                );
                files.insert(entrypoint.to_string(), fs::read_to_string(canonical)?);
            }
        }
        Ok(json!({
            "id": id,
            "scope": scope,
            "path": package,
            "version": manifest.get("version"),
            "revision": manifest.get("revision"),
            "manifest": manifest,
            "files": files,
        }))
    }

    fn create_blu_extension(&self, args: CreateBluExtensionArgs) -> Result<Value> {
        validate_plugin_id(&args.id)?;
        let scope = args.scope.as_deref().unwrap_or("project");
        validate_blu_scope(scope)?;
        let version = args.version.as_deref().unwrap_or("0.1.0");
        validate_extension_version(version)?;
        let runtime = parse_workflow_runtime(args.runtime.as_deref().unwrap_or("blu"))?;
        let source_extension = args
            .source_extension
            .as_deref()
            .unwrap_or(runtime.source_extension())
            .to_ascii_lowercase();
        ensure!(
            runtime.accepts_source_extension(&source_extension),
            "workflow runtime {} does not accept .{} entrypoints",
            runtime.label(),
            source_extension
        );
        ensure!(
            runtime.is_embedded() || source_extension == runtime.source_extension(),
            "external workflow runtime {} requires a .{} entrypoint",
            runtime.label(),
            runtime.source_extension()
        );
        if runtime.is_embedded() {
            ensure!(
                args.command.is_none() && args.args.is_empty(),
                "Blu workflows do not accept an external command or arguments"
            );
        }
        if let Some(command) = &args.command {
            ensure!(
                !command.trim().is_empty() && !command.contains('\0'),
                "workflow runtime command must be a nonempty string without NUL bytes"
            );
        }
        ensure!(
            args.args.iter().all(|argument| !argument.contains('\0')),
            "workflow runtime arguments must not contain NUL bytes"
        );
        ensure!(
            !args.description.trim().is_empty(),
            "extension description must not be empty"
        );
        ensure!(
            !args.instructions.trim().is_empty(),
            "extension instructions must not be empty"
        );
        ensure!(
            args.description.len() <= 4 * 1024,
            "extension description is too large"
        );
        ensure!(
            args.instructions.len() <= MAX_PLUGIN_TEXT,
            "extension instructions are too large"
        );
        ensure!(
            !args.description.contains('\0'),
            "extension description contains NUL"
        );
        ensure!(
            !args.instructions.contains('\0'),
            "extension instructions contain NUL"
        );
        let workflow = match (&args.workflow_name, &args.workflow_source) {
            (None, None) => None,
            (Some(name), Some(source)) => {
                validate_plugin_id(name)?;
                ensure!(
                    !source.trim().is_empty(),
                    "workflow source must not be empty"
                );
                ensure!(
                    source.len() <= MAX_BLU_WORKFLOW_SOURCE,
                    "workflow source is too large"
                );
                ensure!(!source.contains('\0'), "workflow source contains NUL");
                Some((name, source))
            }
            _ => bail!("workflow_name and workflow_source must be provided together"),
        };
        let revision = extension_revision(&args, version, runtime, workflow.as_ref());
        let extensions_root = self.blu_extensions_root(scope)?;
        fs::create_dir_all(&extensions_root)?;
        let destination = extensions_root.join(&args.id);
        ensure!(
            !destination.exists() || args.overwrite,
            "Blu extension {} already exists at {}; pass overwrite=true to replace it",
            args.id,
            destination.display()
        );
        let staging_root = tempfile::tempdir_in(&extensions_root)?;
        let staging = staging_root.path().join(&args.id);
        fs::create_dir_all(staging.join("skills").join(&args.id))?;

        let mut manifest = toml::map::Map::new();
        manifest.insert("manifest_version".into(), toml::Value::Integer(1));
        manifest.insert("id".into(), toml::Value::String(args.id.clone()));
        manifest.insert("name".into(), toml::Value::String(args.id.clone()));
        manifest.insert("version".into(), toml::Value::String(version.into()));
        manifest.insert("revision".into(), toml::Value::String(revision.clone()));
        manifest.insert(
            "description".into(),
            toml::Value::String(args.description.clone()),
        );
        manifest.insert("enabled".into(), toml::Value::Boolean(true));
        manifest.insert(
            "skill_roots".into(),
            toml::Value::Array(vec![toml::Value::String("skills".into())]),
        );
        if let Some((name, _)) = workflow.as_ref() {
            let mut definition = toml::map::Map::new();
            definition.insert(
                "entrypoint".into(),
                toml::Value::String(format!("workflows/{name}.{source_extension}")),
            );
            definition.insert(
                "description".into(),
                toml::Value::String(format!("{name} {} workflow", runtime.label())),
            );
            definition.insert(
                "runtime".into(),
                toml::Value::String(runtime.label().to_string()),
            );
            if let Some(command) = &args.command {
                definition.insert("command".into(), toml::Value::String(command.clone()));
            }
            if !args.args.is_empty() {
                definition.insert(
                    "args".into(),
                    toml::Value::Array(
                        args.args.iter().cloned().map(toml::Value::String).collect(),
                    ),
                );
            }
            let mut workflows = toml::map::Map::new();
            workflows.insert(name.to_string(), toml::Value::Table(definition));
            manifest.insert("workflows".into(), toml::Value::Table(workflows));
        }
        let manifest_text = toml::to_string_pretty(&toml::Value::Table(manifest))?;
        write_atomic(&staging.join("blu.toml"), manifest_text.as_bytes())?;
        write_atomic(
            &staging.join("skills").join(&args.id).join("SKILL.md"),
            format!(
                "---\nname: {}\ndescription: {}\n---\n\n# {}\n\n{}\n",
                args.id,
                yaml_scalar(&args.description),
                args.id,
                args.instructions.trim()
            )
            .as_bytes(),
        )?;
        if let Some((name, source)) = workflow {
            let workflow_path = staging
                .join("workflows")
                .join(format!("{name}.{source_extension}"));
            write_atomic(&workflow_path, source.as_bytes())?;
        }

        let backup = extensions_root.join(format!(".{}.backup-{}", args.id, Uuid::new_v4()));
        if destination.exists() {
            fs::rename(&destination, &backup)?;
        }
        if let Err(error) = fs::rename(&staging, &destination) {
            if backup.exists() {
                let _ = fs::rename(&backup, &destination);
            }
            return Err(error).context("activate staged Blu extension");
        }
        if backup.exists()
            && let Err(error) = self.archive_extension_package(scope, &args.id, &backup)
        {
            let _ = fs::remove_dir_all(&destination);
            let _ = fs::rename(&backup, &destination);
            return Err(error).context("archive previous Blu extension revision");
        }
        let reload_signal = self.reload_blu_extensions(scope)?;
        let audit = self.audit_blu(scope, "create", &args.id)?;
        Ok(json!({
            "id": args.id,
            "scope": scope,
            "path": destination,
            "workflow": args.workflow_name,
            "reload_signal": reload_signal,
            "audit": audit,
            "runtime": runtime,
            "source_extension": source_extension,
            "version": version,
            "revision": revision,
            "hot_reload": "next native turn boundary",
        }))
    }

    fn set_blu_extension_enabled(&self, id: &str, scope: &str, enabled: bool) -> Result<Value> {
        validate_plugin_id(id)?;
        let _ = self.blu_package_path(scope, id)?;
        let path = self.blu_state_path(scope)?;
        let mut root = if path.is_file() {
            toml::from_str::<toml::Value>(&fs::read_to_string(&path)?)?
        } else {
            toml::Value::Table(toml::map::Map::new())
        };
        let table = root
            .as_table_mut()
            .context("Blu state must be a TOML table")?;
        table
            .entry("state_version")
            .or_insert_with(|| toml::Value::Integer(1));
        let extensions = table
            .entry("extensions")
            .or_insert_with(|| toml::Value::Table(toml::map::Map::new()))
            .as_table_mut()
            .context("Blu state extensions must be a table")?;
        let state = extensions
            .entry(id.to_string())
            .or_insert_with(|| toml::Value::Table(toml::map::Map::new()))
            .as_table_mut()
            .context("Blu extension state must be a table")?;
        state.insert("enabled".into(), toml::Value::Boolean(enabled));
        write_atomic(&path, toml::to_string_pretty(&root)?.as_bytes())?;
        let reload_signal = self.reload_blu_extensions(scope)?;
        let operation = if enabled { "enable" } else { "disable" };
        let audit = self.audit_blu(scope, operation, id)?;
        Ok(json!({
            "id": id,
            "scope": scope,
            "enabled": enabled,
            "state": path,
            "reload_signal": reload_signal,
            "audit": audit,
            "hot_reload": "next native turn boundary",
        }))
    }

    fn remove_blu_extension(&self, id: &str, scope: &str) -> Result<Value> {
        validate_plugin_id(id)?;
        let package = self.blu_package_path(scope, id)?;
        let staging = package.with_extension(format!("remove-{}", Uuid::new_v4()));
        fs::rename(&package, &staging)?;
        if let Err(error) = self.clear_blu_extension_state(scope, id) {
            let _ = fs::rename(&staging, &package);
            return Err(error);
        }
        if let Err(error) = self.archive_extension_package(scope, id, &staging) {
            let _ = fs::rename(&staging, &package);
            return Err(error).context("archive removed Blu extension revision");
        }
        let reload_signal = self.reload_blu_extensions(scope)?;
        let audit = self.audit_blu(scope, "remove", id)?;
        Ok(json!({
            "id": id,
            "scope": scope,
            "removed": true,
            "reload_signal": reload_signal,
            "audit": audit,
            "hot_reload": "next native turn boundary",
        }))
    }

    fn rollback_blu_extension(&self, id: &str, scope: &str, revision: &str) -> Result<Value> {
        validate_plugin_id(id)?;
        validate_extension_revision(revision)?;
        let package = self.blu_package_path(scope, id)?;
        let history_root = self.extension_history_root(scope, id)?;
        let selected = history_root.join(revision);
        let canonical_root = history_root.canonicalize().unwrap_or(history_root.clone());
        let canonical_selected = selected
            .canonicalize()
            .with_context(|| format!("Blu extension revision `{revision}` does not exist"))?;
        ensure!(
            canonical_selected.starts_with(&canonical_root)
                && canonical_selected.is_dir()
                && canonical_selected.join("blu.toml").is_file(),
            "invalid Blu extension revision `{revision}`"
        );
        let current = package_revision(&package)?;
        ensure!(
            current != revision,
            "Blu extension revision `{revision}` is already active"
        );
        let current_staging = package.with_extension(format!("rollback-{}", Uuid::new_v4()));
        fs::rename(&package, &current_staging)?;
        if let Err(error) = fs::rename(&selected, &package) {
            let _ = fs::rename(&current_staging, &package);
            return Err(error).context("activate rolled-back Blu extension revision");
        }
        if let Err(error) = self.archive_extension_package(scope, id, &current_staging) {
            let _ = fs::remove_dir_all(&package);
            let _ = fs::rename(&current_staging, &package);
            return Err(error).context("archive current Blu extension during rollback");
        }
        let reload_signal = self.reload_blu_extensions(scope)?;
        let audit = self.audit_blu(scope, "rollback", id)?;
        Ok(json!({
            "id": id,
            "scope": scope,
            "revision": revision,
            "reload_signal": reload_signal,
            "audit": audit,
            "hot_reload": "next native turn boundary",
        }))
    }

    fn extension_history_root(&self, scope: &str, id: &str) -> Result<PathBuf> {
        Ok(self.blu_extensions_root(scope)?.join(".versions").join(id))
    }

    fn archive_extension_package(&self, scope: &str, id: &str, package: &Path) -> Result<String> {
        let history_root = self.extension_history_root(scope, id)?;
        fs::create_dir_all(&history_root)?;
        let revision = package_revision(package)?;
        let mut archive_key = revision.clone();
        let mut archive = history_root.join(&archive_key);
        if archive.exists() {
            archive_key = format!("{revision}-{}", Uuid::new_v4());
            archive = history_root.join(&archive_key);
        }
        fs::rename(package, &archive)?;
        self.prune_extension_history(&history_root)?;
        Ok(archive_key)
    }

    fn prune_extension_history(&self, history_root: &Path) -> Result<()> {
        let mut entries = fs::read_dir(history_root)?
            .filter_map(std::result::Result::ok)
            .filter(|entry| entry.path().is_dir())
            .map(|entry| {
                let modified = entry
                    .metadata()
                    .and_then(|metadata| metadata.modified())
                    .unwrap_or(UNIX_EPOCH);
                (modified, entry.path())
            })
            .collect::<Vec<_>>();
        entries.sort_by_key(|(modified, path)| (*modified, path.clone()));
        while entries.len() > MAX_EXTENSION_HISTORY_VERSIONS {
            let (_, path) = entries.remove(0);
            fs::remove_dir_all(path)?;
        }
        Ok(())
    }

    fn extension_versions(&self, scope: &str, id: &str) -> Result<Vec<Value>> {
        let history_root = self.extension_history_root(scope, id)?;
        if !history_root.is_dir() {
            return Ok(Vec::new());
        }
        let mut versions = fs::read_dir(&history_root)?
            .filter_map(std::result::Result::ok)
            .filter(|entry| entry.path().is_dir())
            .filter_map(|entry| {
                let revision = entry.file_name().to_string_lossy().to_string();
                let manifest_path = entry.path().join("blu.toml");
                let manifest = fs::read_to_string(manifest_path)
                    .ok()
                    .and_then(|source| toml::from_str::<toml::Value>(&source).ok())?;
                Some(json!({
                    "revision": revision,
                    "version": manifest.get("version"),
                    "path": entry.path(),
                }))
            })
            .collect::<Vec<_>>();
        versions.sort_by_key(|value| value["revision"].as_str().unwrap_or_default().to_string());
        Ok(versions)
    }

    fn reload_blu_extensions(&self, scope: &str) -> Result<PathBuf> {
        let path = match scope {
            "project" => self.cwd.join(".borg/blu.reload"),
            "user" => self.blu_config_root()?.join("blu.reload"),
            other => bail!("unknown Blu scope {other}; use project or user"),
        };
        write_atomic(
            &path,
            format!(
                "{}\n",
                SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
            )
            .as_bytes(),
        )?;
        Ok(path)
    }

    fn blu_extensions_root(&self, scope: &str) -> Result<PathBuf> {
        validate_blu_scope(scope)?;
        Ok(match scope {
            "project" => self.cwd.join(".borg/extensions"),
            "user" => self.blu_config_root()?.join("extensions"),
            _ => unreachable!("validated Blu scope"),
        })
    }

    fn blu_config_root(&self) -> Result<PathBuf> {
        std::env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".config")))
            .map(|root| root.join("borg"))
            .context("cannot locate the Blu user config; set HOME or XDG_CONFIG_HOME")
    }

    fn blu_package_path(&self, scope: &str, id: &str) -> Result<PathBuf> {
        let root = self.blu_extensions_root(scope)?;
        let package = root.join(id);
        let canonical_root = root.canonicalize().unwrap_or(root);
        let canonical = package
            .canonicalize()
            .with_context(|| format!("Blu extension {id} does not exist"))?;
        ensure!(
            canonical.starts_with(&canonical_root),
            "Blu package path escapes its root"
        );
        ensure!(
            canonical.is_dir(),
            "Blu extension {id} is not a package directory"
        );
        ensure!(
            canonical.join("blu.toml").is_file(),
            "Blu extension {id} has no blu.toml"
        );
        Ok(canonical)
    }

    fn blu_state_path(&self, scope: &str) -> Result<PathBuf> {
        validate_blu_scope(scope)?;
        Ok(match scope {
            "project" => self.cwd.join(".borg/blu.toml"),
            "user" => self.blu_config_root()?.join("blu.toml"),
            _ => unreachable!("validated Blu scope"),
        })
    }

    fn blu_extension_state(&self, scope: &str, id: &str) -> Result<Option<toml::Value>> {
        let path = self.blu_state_path(scope)?;
        let Some(source) = path
            .is_file()
            .then(|| fs::read_to_string(&path))
            .transpose()?
        else {
            return Ok(None);
        };
        let root = toml::from_str::<toml::Value>(&source)?;
        Ok(root
            .get("extensions")
            .and_then(toml::Value::as_table)
            .and_then(|extensions| extensions.get(id))
            .cloned())
    }

    fn clear_blu_extension_state(&self, scope: &str, id: &str) -> Result<()> {
        let path = self.blu_state_path(scope)?;
        if !path.is_file() {
            return Ok(());
        }
        let mut root = toml::from_str::<toml::Value>(&fs::read_to_string(&path)?)?;
        if let Some(extensions) = root
            .get_mut("extensions")
            .and_then(toml::Value::as_table_mut)
        {
            extensions.remove(id);
        }
        if let Some(sources) = root.get_mut("sources").and_then(toml::Value::as_table_mut) {
            sources.remove(id);
        }
        write_atomic(&path, toml::to_string_pretty(&root)?.as_bytes())
    }

    fn audit_blu(&self, scope: &str, operation: &str, id: &str) -> Result<PathBuf> {
        validate_blu_scope(scope)?;
        let path = match scope {
            "project" => self.cwd.join(".borg/blu.audit.jsonl"),
            "user" => self.blu_config_root()?.join("blu.audit.jsonl"),
            _ => unreachable!("validated Blu scope"),
        };
        let parent = path.parent().context("Blu audit path has no parent")?;
        fs::create_dir_all(parent)?;
        let mut file = OpenOptions::new().create(true).append(true).open(&path)?;
        writeln!(
            file,
            "{}",
            serde_json::to_string(&json!({
                "timestamp_unix_ms": SystemTime::now()
                    .duration_since(UNIX_EPOCH)?
                    .as_millis(),
                "operation": operation,
                "id": id,
                "scope": scope,
            }))?
        )?;
        file.sync_data()?;
        Ok(path)
    }

    fn settings_path(&self, scope: &str) -> Result<PathBuf> {
        match scope {
            "user" => default_settings_path()
                .context("cannot locate the user agent config; set HOME or XDG_CONFIG_HOME"),
            other => bail!("unknown settings scope `{other}`; use `user`"),
        }
    }
}

#[derive(Debug, Deserialize)]
struct SettingsScopeArgs {
    #[serde(default)]
    scope: Option<String>,
}

#[derive(Debug, Deserialize)]
struct NoArgs {}

#[derive(Debug, Deserialize)]
struct PluginIdArgs {
    id: String,
}

#[derive(Debug, Deserialize)]
struct UpdateSettingsArgs {
    #[serde(default)]
    scope: Option<String>,
    updates: Map<String, Value>,
}

#[derive(Debug, Deserialize)]
struct CreatePluginArgs {
    id: String,
    description: String,
    instructions: String,
    #[serde(default)]
    version: Option<String>,
    #[serde(default)]
    overwrite: bool,
}

#[derive(Debug, Deserialize)]
struct RollbackPluginArgs {
    id: String,
    revision: String,
}

#[derive(Debug, Deserialize)]
struct RetrievalAdapterIdArgs {
    id: String,
}

#[derive(Debug, Deserialize)]
struct CreateRetrievalAdapterArgs {
    id: String,
    description: String,
    source: String,
    #[serde(default)]
    tests: Option<String>,
    #[serde(default)]
    language: Option<String>,
    #[serde(default)]
    version: Option<String>,
    #[serde(default)]
    overwrite: bool,
}

#[derive(Debug, Deserialize)]
struct RollbackRetrievalAdapterArgs {
    id: String,
    revision: String,
}

#[derive(Debug, Deserialize)]
struct BluScopeArgs {
    #[serde(default)]
    scope: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BluExtensionIdArgs {
    id: String,
    #[serde(default)]
    scope: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CreateBluExtensionArgs {
    id: String,
    description: String,
    instructions: String,
    #[serde(default)]
    version: Option<String>,
    #[serde(default)]
    scope: Option<String>,
    #[serde(default)]
    workflow_name: Option<String>,
    #[serde(default)]
    workflow_source: Option<String>,
    #[serde(default)]
    overwrite: bool,
    #[serde(default)]
    runtime: Option<String>,
    #[serde(default)]
    source_extension: Option<String>,
    #[serde(default)]
    command: Option<String>,
    #[serde(default)]
    args: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct RollbackBluExtensionArgs {
    id: String,
    revision: String,
    #[serde(default)]
    scope: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SetBluExtensionEnabledArgs {
    id: String,
    #[serde(default)]
    scope: Option<String>,
    enabled: bool,
}

pub(crate) fn is_tool(name: &str) -> bool {
    matches!(
        name,
        "list_plugins"
            | "read_plugin"
            | "get_agent_settings"
            | "update_agent_settings"
            | "create_plugin"
            | "rollback_plugin"
            | "list_retrieval_adapters"
            | "read_retrieval_adapter"
            | "create_retrieval_adapter"
            | "rollback_retrieval_adapter"
            | "list_blu_extensions"
            | "read_blu_extension"
            | "create_blu_extension"
            | "create_extension"
            | "set_blu_extension_enabled"
            | "remove_blu_extension"
            | "rollback_blu_extension"
            | "reload_blu_extensions"
    )
}

pub(crate) fn tool_specs() -> Vec<Value> {
    vec![
        json!({
            "name": "list_plugins",
            "description": "List project-local Borg plugins in .borg/skills. This is live filesystem state and includes plugins created during the current session.",
            "inputSchema": {
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }
        }),
        json!({
            "name": "read_plugin",
            "description": "Read one project-local Borg plugin skill from .borg/skills/<id>/SKILL.md. Use this after list_plugins when a plugin's workflow applies.",
            "inputSchema": {
                "type": "object",
                "properties": { "id": { "type": "string", "pattern": "^[A-Za-z0-9_-]+$", "maxLength": 64 } },
                "required": ["id"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": "get_agent_settings",
            "description": "Read the effective Borg agent settings with sensitive environment values redacted. Borg's typed agent config is user-scoped and shared across sessions.",
            "inputSchema": {
                "type": "object",
                "properties": { "scope": { "type": "string", "enum": ["user"] } },
                "additionalProperties": false
            }
        }),
        json!({
            "name": "update_agent_settings",
            "description": "Safely merge typed Borg settings into the user agent.toml. Aliases/keybindings hot-reload in the TUI; Blu and MCP catalogs reload at the next turn boundary; capability and policy changes require a new session.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "scope": { "type": "string", "enum": ["user"] },
                    "updates": { "type": "object", "description": "Top-level Borg config sections to merge, such as {commands:{aliases:{review:\"/effort high\"}}}." }
                },
                "required": ["updates"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": "create_plugin",
            "description": "Create a project-local Borg plugin skill at .borg/skills/<id>/SKILL.md. It is picked up automatically on the next native turn, so no restart is needed for skill-only plugins.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "id": { "type": "string", "pattern": "^[A-Za-z0-9_-]+$", "maxLength": 64 },
                    "description": { "type": "string", "minLength": 1, "maxLength": 512 },
                    "instructions": { "type": "string", "minLength": 1, "maxLength": 524288 },
                    "version": { "type": "string", "pattern": "^[A-Za-z0-9._+-]+$", "maxLength": 128, "default": "0.1.0" },
                    "overwrite": { "type": "boolean" }
                },
                "required": ["id", "description", "instructions"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": "rollback_plugin",
            "description": "Restore one bounded, previously persisted project skill revision while keeping the current revision in history.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "id": { "type": "string", "pattern": "^[A-Za-z0-9_-]+$", "maxLength": 64 },
                    "revision": { "type": "string", "pattern": "^[A-Za-z0-9._-]+$", "maxLength": 160 }
                },
                "required": ["id", "revision"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": "list_retrieval_adapters",
            "description": "List bounded, project-local retrieval adapters in .borg/retrievers. Adapters are versioned code over canonical history-index documents; they are not an evidence authority.",
            "inputSchema": {
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }
        }),
        json!({
            "name": "read_retrieval_adapter",
            "description": "Read one retrieval adapter manifest, source, optional tests, and prior revisions. Runtime helpers execute only its declared retrieve entrypoint against the Borg host boundary.",
            "inputSchema": {
                "type": "object",
                "properties": { "id": { "type": "string", "pattern": "^[A-Za-z0-9_-]+$", "maxLength": 64 } },
                "required": ["id"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": "create_retrieval_adapter",
            "description": "Create or replace a versioned Python or JavaScript retrieval adapter at .borg/retrievers/<id>. Define retrieve(query) and optionally test(retrieve, borg). The adapter can rank or transform history_index results, but authoritative evidence must be re-read with query_history.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "id": { "type": "string", "pattern": "^[A-Za-z0-9_-]+$", "maxLength": 64 },
                    "description": { "type": "string", "minLength": 1, "maxLength": 4096 },
                    "source": { "type": "string", "minLength": 1, "maxLength": 262144 },
                    "tests": { "type": "string", "maxLength": 262144, "description": "Optional source defining test(retrieve, borg)." },
                    "language": { "type": "string", "enum": ["python", "javascript"], "default": "python" },
                    "version": { "type": "string", "pattern": "^[A-Za-z0-9._+-]+$", "maxLength": 128, "default": "0.1.0" },
                    "overwrite": { "type": "boolean" }
                },
                "required": ["id", "description", "source"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": "rollback_retrieval_adapter",
            "description": "Restore one bounded, previously persisted retrieval adapter revision while keeping the current revision in history.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "id": { "type": "string", "pattern": "^[A-Za-z0-9_-]+$", "maxLength": 64 },
                    "revision": { "type": "string", "pattern": "^[A-Za-z0-9._-]+$", "maxLength": 160 }
                },
                "required": ["id", "revision"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": "list_blu_extensions",
            "description": "List project and user Blu extension packages, including executable workflow entrypoints. This reads live package state; changes apply at the next native turn boundary.",
            "inputSchema": {
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }
        }),
        json!({
            "name": "read_blu_extension",
            "description": "Read a bounded Blu extension manifest, skill file, and workflow sources from the selected scope.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "id": { "type": "string", "pattern": "^[A-Za-z0-9_-]+$", "maxLength": 64 },
                    "scope": { "type": "string", "enum": ["project", "user"] }
                },
                "required": ["id"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": "create_blu_extension",
            "description": "Create or replace a live extension package with a skill and optional executable workflow. Select embedded Blu for .blu/.lua/.luau or a supervised Python, IPython, JavaScript, or TypeScript worker; the package is atomically installed and rescanned at the next native turn boundary.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "id": { "type": "string", "pattern": "^[A-Za-z0-9_-]+$", "maxLength": 64 },
                    "description": { "type": "string", "minLength": 1, "maxLength": 4096 },
                    "instructions": { "type": "string", "minLength": 1, "maxLength": 524288 },
                    "version": { "type": "string", "pattern": "^[A-Za-z0-9._+-]+$", "maxLength": 128, "default": "0.1.0" },
                    "scope": { "type": "string", "enum": ["project", "user"], "default": "project" },
                    "workflow_name": { "type": "string", "pattern": "^[A-Za-z0-9_-]+$", "maxLength": 64 },
                    "workflow_source": { "type": "string", "minLength": 1, "maxLength": 262144 },
                    "overwrite": { "type": "boolean" },
                    "runtime": { "type": "string", "enum": ["blu", "python", "ipython", "javascript", "typescript"], "default": "blu" },
                    "source_extension": { "type": "string", "enum": ["blu", "lua", "luau", "py", "js", "ts"], "description": "Optional source suffix. Blu accepts .blu, .lua, and .luau; other runtimes use their standard suffix." },
                    "command": { "type": "string", "description": "Optional executable override for external runtimes." },
                    "args": { "type": "array", "items": { "type": "string" }, "maxItems": 32 }
                },
                "required": ["id", "description", "instructions"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": "create_extension",
            "description": "Generic alias for create_blu_extension: create a live, hot-reloadable Blu/Lua/Luau/Python/IPython/JavaScript/TypeScript extension package for self-extension.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "id": { "type": "string", "pattern": "^[A-Za-z0-9_-]+$", "maxLength": 64 },
                    "description": { "type": "string", "minLength": 1, "maxLength": 4096 },
                    "instructions": { "type": "string", "minLength": 1, "maxLength": 524288 },
                    "version": { "type": "string", "pattern": "^[A-Za-z0-9._+-]+$", "maxLength": 128, "default": "0.1.0" },
                    "scope": { "type": "string", "enum": ["project", "user"], "default": "project" },
                    "workflow_name": { "type": "string", "pattern": "^[A-Za-z0-9_-]+$", "maxLength": 64 },
                    "workflow_source": { "type": "string", "minLength": 1, "maxLength": 262144 },
                    "overwrite": { "type": "boolean" },
                    "runtime": { "type": "string", "enum": ["blu", "python", "ipython", "javascript", "typescript"], "default": "blu" },
                    "source_extension": { "type": "string", "enum": ["blu", "lua", "luau", "py", "js", "ts"], "description": "Optional source suffix. Blu accepts .blu, .lua, and .luau; other runtimes use their standard suffix." },
                    "command": { "type": "string" },
                    "args": { "type": "array", "items": { "type": "string" }, "maxItems": 32 }
                },
                "required": ["id", "description", "instructions"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": "set_blu_extension_enabled",
            "description": "Enable or disable an installed Blu extension in durable scope state; the running catalog swaps at the next native turn boundary.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "id": { "type": "string", "pattern": "^[A-Za-z0-9_-]+$", "maxLength": 64 },
                    "scope": { "type": "string", "enum": ["project", "user"] },
                    "enabled": { "type": "boolean" }
                },
                "required": ["id", "enabled"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": "remove_blu_extension",
            "description": "Atomically remove an installed Blu package and its durable activation state.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "id": { "type": "string", "pattern": "^[A-Za-z0-9_-]+$", "maxLength": 64 },
                    "scope": { "type": "string", "enum": ["project", "user"] }
                },
                "required": ["id"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": "rollback_blu_extension",
            "description": "Restore one bounded, previously persisted Blu extension revision and keep the current revision in history.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "id": { "type": "string", "pattern": "^[A-Za-z0-9_-]+$", "maxLength": 64 },
                    "revision": { "type": "string", "pattern": "^[A-Za-z0-9._-]+$", "maxLength": 160 },
                    "scope": { "type": "string", "enum": ["project", "user"] }
                },
                "required": ["id", "revision"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": "reload_blu_extensions",
            "description": "Write the Blu reload signal for a project or user catalog. Interactive Borg also polls catalogs automatically; native turns always refresh their snapshot.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "scope": { "type": "string", "enum": ["project", "user"] }
                },
                "additionalProperties": false
            }
        }),
    ]
}

fn validate_blu_scope(scope: &str) -> Result<()> {
    ensure!(
        matches!(scope, "project" | "user"),
        "unknown Blu scope {scope}; use project or user"
    );
    Ok(())
}

fn read_retrieval_adapter_file(package: &Path, name: &str) -> Result<Value> {
    let path = package.join(name);
    let source = fs::read_to_string(&path)
        .with_context(|| format!("failed to read retrieval adapter file {}", path.display()))?;
    serde_json::from_str(&source)
        .with_context(|| format!("invalid retrieval adapter manifest {}", path.display()))
}

fn read_bounded_retrieval_source(path: &Path) -> Result<String> {
    let metadata = fs::metadata(path).with_context(|| {
        format!(
            "failed to inspect retrieval adapter source {}",
            path.display()
        )
    })?;
    ensure!(
        metadata.len() <= MAX_RETRIEVAL_ADAPTER_SOURCE as u64,
        "retrieval adapter source is too large"
    );
    fs::read_to_string(path)
        .with_context(|| format!("failed to read retrieval adapter source {}", path.display()))
}

fn retrieval_adapter_revision(
    id: &str,
    description: &str,
    language: &str,
    version: &str,
    source: &str,
    tests: Option<&str>,
) -> String {
    let mut digest = Sha256::new();
    for value in [id, description, language, version, source] {
        digest.update(value.as_bytes());
        digest.update([0]);
    }
    if let Some(tests) = tests {
        digest.update(tests.as_bytes());
    }
    format!("{:x}", digest.finalize())
}

fn retrieval_adapter_package_revision(package: &Path) -> Result<String> {
    let manifest = read_retrieval_adapter_file(package, "manifest.json")?;
    let revision = manifest
        .get("revision")
        .and_then(Value::as_str)
        .context("retrieval adapter manifest has no revision")?;
    validate_extension_revision(revision)?;
    Ok(revision.to_string())
}

fn parse_workflow_runtime(value: &str) -> Result<crate::WorkflowRuntime> {
    match value {
        "blu" => Ok(crate::WorkflowRuntime::Blu),
        "python" => Ok(crate::WorkflowRuntime::Python),
        "ipython" => Ok(crate::WorkflowRuntime::Ipython),
        "javascript" => Ok(crate::WorkflowRuntime::Javascript),
        "typescript" => Ok(crate::WorkflowRuntime::Typescript),
        other => bail!(
            "unknown workflow runtime `{other}`; use blu, python, ipython, javascript, or typescript"
        ),
    }
}

fn validate_relative_path(path: &Path, label: &str) -> Result<()> {
    ensure!(!path.as_os_str().is_empty(), "{label} must not be empty");
    ensure!(!path.is_absolute(), "{label} must be relative");
    ensure!(
        !path.components().any(|component| matches!(
            component,
            std::path::Component::ParentDir
                | std::path::Component::RootDir
                | std::path::Component::Prefix(_)
        )),
        "invalid {label}"
    );
    Ok(())
}

fn default_settings_path() -> Option<PathBuf> {
    std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".config")))
        .map(|root| root.join("borg").join("agent.toml"))
}

fn validate_plugin_id(id: &str) -> Result<()> {
    ensure!(
        !id.is_empty() && id.len() <= 64,
        "plugin id must be 1-64 characters"
    );
    ensure!(
        id.bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-')),
        "plugin id may contain only ASCII letters, digits, `_`, or `-`"
    );
    Ok(())
}

fn validate_extension_version(version: &str) -> Result<()> {
    ensure!(
        !version.is_empty() && version.len() <= 128,
        "extension version must be 1-128 characters"
    );
    ensure!(
        version.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'+' | b'-' | b'_')
        }),
        "extension version contains unsupported characters"
    );
    Ok(())
}

fn validate_extension_revision(revision: &str) -> Result<()> {
    ensure!(
        !revision.is_empty()
            && revision.len() <= 160
            && revision != "."
            && revision != ".."
            && !revision.contains(".."),
        "invalid extension revision"
    );
    ensure!(
        revision
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_')),
        "extension revision contains unsupported characters"
    );
    Ok(())
}

fn extension_revision(
    args: &CreateBluExtensionArgs,
    version: &str,
    runtime: crate::WorkflowRuntime,
    workflow: Option<&(&String, &String)>,
) -> String {
    let workflow_name = workflow.map(|(name, _)| name.as_str());
    let workflow_source = workflow.map(|(_, source)| source.as_str());
    let input = json!({
        "id": args.id,
        "version": version,
        "description": args.description,
        "instructions": args.instructions,
        "runtime": runtime.label(),
        "source_extension": args.source_extension,
        "command": args.command,
        "args": args.args,
        "workflow_name": workflow_name,
        "workflow_source": workflow_source,
    });
    format!("{:x}", Sha256::digest(input.to_string().as_bytes()))
}

fn plugin_revision(args: &CreatePluginArgs, version: &str) -> String {
    let input = json!({
        "id": args.id,
        "version": version,
        "description": args.description,
        "instructions": args.instructions,
    });
    format!("{:x}", Sha256::digest(input.to_string().as_bytes()))
}

fn plugin_version_and_revision(content: &str) -> (Option<String>, Option<String>) {
    let mut version = None;
    let mut revision = None;
    if content.starts_with("---") {
        for line in content.lines().skip(1).take_while(|line| *line != "---") {
            if let Some(value) = line.strip_prefix("version:") {
                version = Some(value.trim().trim_matches('"').to_string());
            } else if let Some(value) = line.strip_prefix("revision:") {
                revision = Some(value.trim().trim_matches('"').to_string());
            }
        }
    }
    (version, revision)
}

fn plugin_package_revision(package: &Path) -> Result<String> {
    let path = package.join("SKILL.md");
    let content = fs::read_to_string(&path)
        .with_context(|| format!("read plugin skill {}", path.display()))?;
    if let (_, Some(revision)) = plugin_version_and_revision(&content) {
        validate_extension_revision(&revision)?;
        return Ok(revision);
    }
    Ok(format!("legacy-{:x}", Sha256::digest(content.as_bytes())))
}

fn package_revision(package: &Path) -> Result<String> {
    let manifest_path = package.join("blu.toml");
    let source = fs::read_to_string(&manifest_path)
        .with_context(|| format!("read extension manifest {}", manifest_path.display()))?;
    let manifest = toml::from_str::<toml::Value>(&source)
        .with_context(|| format!("parse extension manifest {}", manifest_path.display()))?;
    if let Some(revision) = manifest.get("revision").and_then(toml::Value::as_str) {
        validate_extension_revision(revision)?;
        return Ok(revision.to_string());
    }
    Ok(format!("legacy-{:x}", Sha256::digest(source.as_bytes())))
}

fn yaml_scalar(value: &str) -> String {
    let escaped = value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace(['\r', '\n'], " ");
    format!("\"{escaped}\"")
}

fn plugin_description(content: &str) -> String {
    if content.starts_with("---")
        && let Some(description) = content
            .lines()
            .skip(1)
            .take_while(|line| *line != "---")
            .find_map(|line| line.strip_prefix("description:").map(str::trim))
    {
        return description.trim_matches('"').to_string();
    }
    "Reusable Borg workflow".to_string()
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().context("settings path has no parent")?;
    fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    let mut temp = tempfile::NamedTempFile::new_in(parent)
        .with_context(|| format!("create temporary file beside {}", path.display()))?;
    temp.as_file_mut().write_all(bytes)?;
    temp.as_file_mut().sync_all()?;
    temp.persist(path)
        .map_err(|error| error.error)
        .with_context(|| format!("atomically replace {}", path.display()))?;
    #[cfg(unix)]
    {
        let _ = OpenOptions::new().read(true).open(parent)?.sync_all();
    }
    Ok(())
}

fn json_to_toml(value: Value) -> Result<toml::Value> {
    match value {
        Value::Null => bail!("null is only supported for removing a top-level section"),
        Value::Bool(value) => Ok(toml::Value::Boolean(value)),
        Value::Number(value) => value
            .as_i64()
            .map(toml::Value::Integer)
            .or_else(|| value.as_f64().map(toml::Value::Float))
            .context("number is not representable in TOML"),
        Value::String(value) => Ok(toml::Value::String(value)),
        Value::Array(values) => values
            .into_iter()
            .map(json_to_toml)
            .collect::<Result<Vec<_>>>()
            .map(toml::Value::Array),
        Value::Object(values) => values
            .into_iter()
            .map(|(key, value)| Ok((key, json_to_toml(value)?)))
            .collect::<Result<toml::map::Map<_, _>>>()
            .map(toml::Value::Table),
    }
}

fn merge_toml(existing: &mut toml::Value, patch: toml::Value) {
    if let (Some(existing), Some(patch)) = (existing.as_table_mut(), patch.as_table()) {
        for (key, value) in patch {
            if let Some(current) = existing.get_mut(key) {
                merge_toml(current, value.clone());
            } else {
                existing.insert(key.clone(), value.clone());
            }
        }
    } else {
        *existing = patch;
    }
}

fn validate_settings_shape(root: &toml::Value) -> Result<()> {
    let Some(root) = root.as_table() else {
        bail!("agent settings root must be a TOML table");
    };
    for (section, value) in root {
        ensure!(
            SETTINGS_SECTIONS.contains(&section.as_str()),
            "unsupported settings section `{section}`"
        );
        ensure!(
            value.is_table(),
            "settings section `{section}` must be a table"
        );
    }
    if let Some(capabilities) = root.get("capabilities") {
        check_keys(
            capabilities,
            &[
                "multiplayer",
                "subagents",
                "autonomous_team",
                "shared_work",
                "presence",
                "cloud_sync",
                "web_relay",
                "telemetry",
            ],
            "capabilities",
        )?;
        check_bool_values(capabilities, "capabilities")?;
    }
    if let Some(extensions) = root.get("extensions") {
        check_keys(extensions, &["allow_project_mcp"], "extensions")?;
        check_bool_values(extensions, "extensions")?;
    }
    if let Some(team) = root.get("team") {
        check_keys(
            team,
            &[
                "preset",
                "worker_concurrency",
                "max_total_assignments",
                "max_total_reports",
                "max_total_escalations",
                "max_specialists",
                "max_tokens",
                "max_cost_microusd",
                "max_wall_time_ms",
            ],
            "team",
        )?;
        if let Some(preset) = team.get("preset") {
            ensure!(
                preset.as_str() == Some("xhigh_director_low_workers"),
                "team.preset must be `xhigh_director_low_workers`"
            );
        }
        if let Some(worker_concurrency) = team.get("worker_concurrency") {
            check_positive_integer(worker_concurrency, "team.worker_concurrency")?;
        }
        for name in [
            "max_total_assignments",
            "max_total_reports",
            "max_total_escalations",
            "max_specialists",
            "max_tokens",
            "max_cost_microusd",
            "max_wall_time_ms",
        ] {
            if let Some(value) = team.get(name) {
                check_positive_integer(value, &format!("team.{name}"))?;
            }
        }
    }
    if let Some(commands) = root.get("commands") {
        check_keys(commands, &["aliases"], "commands")?;
        if let Some(aliases) = commands.get("aliases") {
            let Some(aliases) = aliases.as_table() else {
                bail!("commands.aliases must be a table");
            };
            for (name, target) in aliases {
                ensure!(
                    !name.is_empty()
                        && name
                            .bytes()
                            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')),
                    "invalid command alias `{name}`"
                );
                ensure!(
                    target
                        .as_str()
                        .is_some_and(|value| value.starts_with('/') && value.len() > 1),
                    "command alias `{name}` must target a slash command"
                );
            }
        }
    }
    if let Some(keybindings) = root.get("keybindings") {
        check_keys(
            keybindings,
            &[
                "send",
                "queue",
                "newline",
                "keybindings",
                "interrupt",
                "clear_or_exit",
                "exit",
                "attach_image",
                "copy",
                "scroll_up",
                "scroll_down",
                "select_previous",
                "select_next",
                "approve",
                "deny",
            ],
            "keybindings",
        )?;
        for (name, values) in keybindings.as_table().expect("checked table") {
            let Some(values) = values.as_array() else {
                bail!("keybindings.{name} must be an array");
            };
            ensure!(!values.is_empty(), "keybindings.{name} must not be empty");
            ensure!(
                values
                    .iter()
                    .all(|value| value.as_str().is_some_and(|value| !value.trim().is_empty())),
                "keybindings.{name} must contain strings"
            );
        }
    }
    if let Some(mcp) = root.get("mcp") {
        check_keys(mcp, &["servers"], "mcp")?;
        if let Some(servers) = mcp.get("servers") {
            let Some(servers) = servers.as_table() else {
                bail!("mcp.servers must be a table");
            };
            for (name, server) in servers {
                validate_plugin_id(name).with_context(|| {
                    format!(
                        "MCP server name `{name}` must contain only ASCII letters, digits, `_`, or `-`"
                    )
                })?;
                check_keys(
                    server,
                    &["enabled", "command", "args", "env", "allowed_tools"],
                    &format!("mcp.servers.{name}"),
                )?;
                if let Some(enabled) = server.get("enabled") {
                    ensure!(
                        enabled.as_bool().is_some(),
                        "mcp.servers.{name}.enabled must be a boolean"
                    );
                }
                let enabled = server
                    .get("enabled")
                    .and_then(toml::Value::as_bool)
                    .unwrap_or(true);
                let command = server.get("command");
                if let Some(command) = command {
                    ensure!(
                        command.as_str().is_some(),
                        "mcp.servers.{name}.command must be a string"
                    );
                }
                ensure!(
                    !enabled
                        || command
                            .and_then(toml::Value::as_str)
                            .is_some_and(|command| !command.trim().is_empty()),
                    "enabled MCP server `{name}` must define a command"
                );
                if let Some(args) = server.get("args") {
                    check_string_array(args, &format!("mcp.servers.{name}.args"), false)?;
                }
                if let Some(env) = server.get("env") {
                    let Some(env) = env.as_table() else {
                        bail!("mcp.servers.{name}.env must be a table");
                    };
                    for (key, value) in env {
                        ensure!(
                            !key.is_empty() && !key.contains(['=', '\0']),
                            "MCP server `{name}` has an invalid environment key"
                        );
                        ensure!(
                            value.as_str().is_some_and(|value| !value.contains('\0')),
                            "MCP server `{name}` environment values must be strings without NUL bytes"
                        );
                    }
                }
                if let Some(allowed_tools) = server.get("allowed_tools") {
                    check_string_array(
                        allowed_tools,
                        &format!("mcp.servers.{name}.allowed_tools"),
                        true,
                    )?;
                }
            }
        }
    }
    if let Some(approvals) = root.get("approvals") {
        check_keys(
            approvals,
            &["reviewer_model", "reviewer_effort"],
            "approvals",
        )?;
        for name in ["reviewer_model", "reviewer_effort"] {
            if let Some(value) = approvals.get(name) {
                ensure!(
                    value.as_str().is_some_and(|value| !value.trim().is_empty()),
                    "approvals.{name} must be a non-empty string"
                );
            }
        }
    }
    if let Some(updates) = root.get("updates") {
        check_keys(
            updates,
            &["auto_install", "check_interval_hours"],
            "updates",
        )?;
        if let Some(auto_install) = updates.get("auto_install") {
            ensure!(
                auto_install.as_bool().is_some(),
                "updates.auto_install must be a boolean"
            );
        }
        if let Some(interval) = updates.get("check_interval_hours") {
            ensure!(
                interval
                    .as_integer()
                    .is_some_and(|value| (1..=720).contains(&value)),
                "updates.check_interval_hours must be between 1 and 720"
            );
        }
    }
    if let Some(providers) = root.get("providers") {
        let Some(providers) = providers.as_table() else {
            bail!("providers must be a table");
        };
        for (provider_id, provider) in providers {
            let Some(provider) = provider.as_table() else {
                bail!("providers.{provider_id} must be a table");
            };
            check_table_keys(
                provider,
                &[
                    "protocol",
                    "name",
                    "base_url",
                    "api_key_env",
                    "api_key",
                    "headers",
                    "models",
                ],
                &format!("providers.{provider_id}"),
            )?;
            for key in ["protocol", "name", "base_url", "api_key_env", "api_key"] {
                if let Some(value) = provider.get(key) {
                    ensure!(
                        value
                            .as_str()
                            .is_some_and(|value| !value.contains(['\0', '\r', '\n'])),
                        "providers.{provider_id}.{key} must be a valid string"
                    );
                }
            }
            let models = provider
                .get("models")
                .context("providers entries must declare models")?;
            let Some(models) = models.as_table() else {
                bail!("providers.{provider_id}.models must be a table");
            };
            ensure!(
                !models.is_empty(),
                "providers.{provider_id}.models must not be empty"
            );
            for (model_id, model) in models {
                let Some(model) = model.as_table() else {
                    bail!("providers.{provider_id}.models.{model_id} must be a table");
                };
                check_table_keys(
                    model,
                    &[
                        "name",
                        "context_window_tokens",
                        "max_output_tokens",
                        "variants",
                        "body",
                    ],
                    &format!("providers.{provider_id}.models.{model_id}"),
                )?;
                for key in ["context_window_tokens", "max_output_tokens"] {
                    if let Some(value) = model.get(key) {
                        ensure!(
                            value.as_integer().is_some_and(|value| value > 0),
                            "providers.{provider_id}.models.{model_id}.{key} must be positive"
                        );
                    }
                }
                if let Some(variants) = model.get("variants") {
                    let Some(variants) = variants.as_table() else {
                        bail!("providers.{provider_id}.models.{model_id}.variants must be a table");
                    };
                    for (variant_id, variant) in variants {
                        let Some(variant) = variant.as_table() else {
                            bail!(
                                "providers.{provider_id}.models.{model_id}.variants.{variant_id} must be a table"
                            );
                        };
                        check_table_keys(
                            variant,
                            &["body"],
                            &format!(
                                "providers.{provider_id}.models.{model_id}.variants.{variant_id}"
                            ),
                        )?;
                        if let Some(body) = variant.get("body") {
                            ensure!(body.is_table(), "configured variant body must be a table");
                        }
                    }
                }
                if let Some(body) = model.get("body") {
                    ensure!(body.is_table(), "configured model body must be a table");
                }
            }
        }
    }
    Ok(())
}

fn check_keys(value: &toml::Value, allowed: &[&str], section: &str) -> Result<()> {
    let Some(table) = value.as_table() else {
        bail!("settings section `{section}` must be a table");
    };
    check_table_keys(table, allowed, section)
}

fn check_table_keys(
    table: &toml::map::Map<String, toml::Value>,
    allowed: &[&str],
    section: &str,
) -> Result<()> {
    for key in table.keys() {
        ensure!(
            allowed.contains(&key.as_str()),
            "unsupported setting `{section}.{key}`"
        );
    }
    Ok(())
}

fn check_bool_values(value: &toml::Value, section: &str) -> Result<()> {
    for (key, value) in value.as_table().expect("checked table") {
        ensure!(
            value.as_bool().is_some(),
            "{section}.{key} must be a boolean"
        );
    }
    Ok(())
}

fn check_positive_integer(value: &toml::Value, field: &str) -> Result<()> {
    ensure!(
        value.as_integer().is_some_and(|value| value > 0),
        "{field} must be a positive integer"
    );
    Ok(())
}

fn check_string_array(value: &toml::Value, field: &str, require_non_empty: bool) -> Result<()> {
    let Some(values) = value.as_array() else {
        bail!("{field} must be an array");
    };
    for value in values {
        ensure!(
            value.as_str().is_some_and(|value| {
                !value.contains('\0') && (!require_non_empty || !value.trim().is_empty())
            }),
            "{field} must contain valid strings"
        );
    }
    Ok(())
}

fn redact_toml(value: toml::Value) -> toml::Value {
    match value {
        toml::Value::Table(table) => toml::Value::Table(
            table
                .into_iter()
                .map(|(key, value)| {
                    let sensitive = key.eq_ignore_ascii_case("env")
                        || key.eq_ignore_ascii_case("headers")
                        || key.to_ascii_lowercase().contains("token")
                        || key.to_ascii_lowercase().contains("secret")
                        || key.to_ascii_lowercase().contains("password")
                        || key.eq_ignore_ascii_case("api_key");
                    (
                        key,
                        if sensitive {
                            toml::Value::String("[redacted]".to_string())
                        } else {
                            redact_toml(value)
                        },
                    )
                })
                .collect(),
        ),
        toml::Value::Array(values) => {
            toml::Value::Array(values.into_iter().map(redact_toml).collect())
        }
        value => value,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_merge_preserves_unmentioned_sections() {
        let mut root = toml::Value::Table(toml::map::Map::new());
        let patch = json_to_toml(json!({"aliases": {"review": "/effort high"}})).unwrap();
        let table = root.as_table_mut().unwrap();
        table.insert("commands".into(), toml::Value::Table(toml::map::Map::new()));
        merge_toml(table.get_mut("commands").unwrap(), patch);
        assert_eq!(
            root["commands"]["aliases"]["review"].as_str(),
            Some("/effort high")
        );
    }

    #[test]
    fn settings_validation_rejects_unknown_nested_fields_before_writing() {
        let valid = toml::from_str::<toml::Value>(
            r#"
            [team]
            preset = "xhigh_director_low_workers"
            worker_concurrency = 2
            [mcp.servers.docs]
            command = "docs-mcp"
            args = ["--stdio"]
            [approvals]
            reviewer_effort = "low"
            "#,
        )
        .unwrap();
        validate_settings_shape(&valid).unwrap();

        let invalid = toml::from_str::<toml::Value>(
            r#"
            [mcp.servers.docs]
            command = "docs-mcp"
            unexpected = true
            "#,
        )
        .unwrap();
        assert!(validate_settings_shape(&invalid).is_err());
    }

    #[test]
    fn plugin_ids_are_path_safe() {
        assert!(validate_plugin_id("review_tools").is_ok());
        assert!(validate_plugin_id("../escape").is_err());
    }

    #[test]
    fn create_plugin_writes_a_native_skill_that_can_be_rescanned() {
        let workspace = tempfile::tempdir().unwrap();
        let context = SelfServiceContext::new(workspace.path().to_path_buf());
        let result = context
            .call(
                "create_plugin",
                json!({
                    "id": "review-tools",
                    "description": "Review project changes",
                    "instructions": "Inspect the diff and report risks."
                }),
            )
            .unwrap();
        assert_eq!(result["hot_reload"], "next native turn");
        let skill = fs::read_to_string(workspace.path().join(".borg/skills/review-tools/SKILL.md"))
            .unwrap();
        assert!(skill.contains("name: review-tools"));
        assert!(skill.contains("Inspect the diff"));
        let listed = context.call("list_plugins", json!({})).unwrap();
        assert_eq!(listed["plugins"][0]["id"], "review-tools");
        let read = context
            .call("read_plugin", json!({"id": "review-tools"}))
            .unwrap();
        assert!(
            read["content"]
                .as_str()
                .unwrap()
                .contains("Inspect the diff")
        );
    }

    #[test]
    fn plugin_revisions_are_persisted_and_can_be_rolled_back() {
        let workspace = tempfile::tempdir().unwrap();
        let context = SelfServiceContext::new(workspace.path().to_path_buf());
        let first = context
            .call(
                "create_plugin",
                json!({
                    "id": "versioned-skill",
                    "description": "Version one",
                    "instructions": "Use version one.",
                    "version": "1.0.0"
                }),
            )
            .unwrap();
        let first_revision = first["revision"].as_str().unwrap().to_string();
        context
            .call(
                "create_plugin",
                json!({
                    "id": "versioned-skill",
                    "description": "Version two",
                    "instructions": "Use version two.",
                    "version": "2.0.0",
                    "overwrite": true
                }),
            )
            .unwrap();
        let listed = context.call("list_plugins", json!({})).unwrap();
        assert_eq!(listed["plugins"][0]["version"], "2.0.0");
        assert_eq!(
            listed["plugins"][0]["versions"][0]["content_revision"],
            first_revision
        );

        context
            .call(
                "rollback_plugin",
                json!({"id": "versioned-skill", "revision": first_revision}),
            )
            .unwrap();
        let read = context
            .call("read_plugin", json!({"id": "versioned-skill"}))
            .unwrap();
        assert_eq!(read["version"], "1.0.0");
        assert!(
            read["content"]
                .as_str()
                .unwrap()
                .contains("Use version one.")
        );
    }

    #[test]
    fn retrieval_adapter_revisions_are_persisted_and_can_be_rolled_back() {
        let workspace = tempfile::tempdir().unwrap();
        let context = SelfServiceContext::new(workspace.path().to_path_buf());
        let first = context
            .call(
                "create_retrieval_adapter",
                json!({
                    "id": "history-ranker",
                    "description": "Rank history evidence",
                    "version": "1.0.0",
                    "source": "def retrieve(query):\n    return {'query': query, 'version': 1}\n",
                    "tests": "def test(retrieve, borg):\n    assert retrieve('x')['version'] == 1\n    return {'ok': True}\n"
                }),
            )
            .unwrap();
        let first_revision = first["revision"].as_str().unwrap().to_string();
        context
            .call(
                "create_retrieval_adapter",
                json!({
                    "id": "history-ranker",
                    "description": "Rank newer history evidence",
                    "version": "2.0.0",
                    "source": "def retrieve(query):\n    return {'query': query, 'version': 2}\n",
                    "overwrite": true
                }),
            )
            .unwrap();

        let listed = context.call("list_retrieval_adapters", json!({})).unwrap();
        assert_eq!(listed["adapters"][0]["manifest"]["version"], "2.0.0");
        assert_eq!(
            listed["adapters"][0]["versions"][0]["revision"],
            first_revision
        );

        context
            .call(
                "rollback_retrieval_adapter",
                json!({"id": "history-ranker", "revision": first_revision}),
            )
            .unwrap();
        let read = context
            .call("read_retrieval_adapter", json!({"id": "history-ranker"}))
            .unwrap();
        assert_eq!(read["manifest"]["version"], "1.0.0");
        assert!(read["source"].as_str().unwrap().contains("version': 1"));
        assert!(read["tests"].as_str().unwrap().contains("assert retrieve"));
    }

    #[test]
    fn retrieval_adapter_requires_a_retrieve_entrypoint() {
        let workspace = tempfile::tempdir().unwrap();
        let context = SelfServiceContext::new(workspace.path().to_path_buf());
        assert!(
            context
                .call(
                    "create_retrieval_adapter",
                    json!({
                        "id": "invalid",
                        "description": "No entrypoint",
                        "source": "def rank(query): return query"
                    }),
                )
                .is_err()
        );
    }

    #[test]
    fn blu_extension_lifecycle_is_atomic_audited_and_live() {
        let workspace = tempfile::tempdir().unwrap();
        let context = SelfServiceContext::new(workspace.path().to_path_buf());
        let created = context
            .call(
                "create_blu_extension",
                json!({
                    "id": "review-tools",
                    "description": "Review changes",
                    "instructions": "Use the review workflow when asked.",
                    "workflow_name": "review",
                    "workflow_source": "borg_emit(\"audit\", \"review\", \"{}\")"
                }),
            )
            .unwrap();
        assert_eq!(created["hot_reload"], "next native turn boundary");
        assert!(
            workspace
                .path()
                .join(".borg/extensions/review-tools/workflows/review.blu")
                .is_file()
        );
        assert!(workspace.path().join(".borg/blu.reload").is_file());
        assert!(workspace.path().join(".borg/blu.audit.jsonl").is_file());

        let listed = context.call("list_blu_extensions", json!({})).unwrap();
        assert_eq!(listed["extensions"][0]["id"], "review-tools");
        assert_eq!(listed["extensions"][0]["workflows"][0], "review");
        let read = context
            .call(
                "read_blu_extension",
                json!({"id": "review-tools", "scope": "project"}),
            )
            .unwrap();
        assert_eq!(
            read["files"]["workflows/review.blu"],
            "borg_emit(\"audit\", \"review\", \"{}\")"
        );

        context
            .call(
                "set_blu_extension_enabled",
                json!({"id": "review-tools", "enabled": false}),
            )
            .unwrap();
        let state = fs::read_to_string(workspace.path().join(".borg/blu.toml")).unwrap();
        assert!(state.contains("enabled = false"));

        context
            .call(
                "remove_blu_extension",
                json!({"id": "review-tools", "scope": "project"}),
            )
            .unwrap();
        assert!(
            !workspace
                .path()
                .join(".borg/extensions/review-tools")
                .exists()
        );
        let audit = fs::read_to_string(workspace.path().join(".borg/blu.audit.jsonl")).unwrap();
        assert!(audit.contains("\"operation\":\"create\""));
        assert!(audit.contains("\"operation\":\"disable\""));
        assert!(audit.contains("\"operation\":\"remove\""));
    }

    #[test]
    fn create_extension_writes_a_selected_external_runtime() {
        let workspace = tempfile::tempdir().unwrap();
        let context = SelfServiceContext::new(workspace.path().to_path_buf());
        let created = context
            .call(
                "create_extension",
                json!({
                    "id": "analysis-tools",
                    "description": "Run project analysis",
                    "instructions": "Use the analysis workflow when asked.",
                    "workflow_name": "report",
                    "workflow_source": "console.log('analysis-ok')",
                    "runtime": "typescript",
                    "args": ["--smol"]
                }),
            )
            .unwrap();
        assert_eq!(created["runtime"], "typescript");
        assert_eq!(created["hot_reload"], "next native turn boundary");
        let workflow = workspace
            .path()
            .join(".borg/extensions/analysis-tools/workflows/report.ts");
        assert_eq!(
            fs::read_to_string(workflow).unwrap(),
            "console.log('analysis-ok')"
        );
        let manifest = fs::read_to_string(
            workspace
                .path()
                .join(".borg/extensions/analysis-tools/blu.toml"),
        )
        .unwrap();
        assert!(manifest.contains("runtime = \"typescript\""));
        assert!(manifest.contains("args = [\"--smol\"]"));
    }

    #[test]
    fn extension_revisions_are_persisted_and_can_be_rolled_back() {
        let workspace = tempfile::tempdir().unwrap();
        let context = SelfServiceContext::new(workspace.path().to_path_buf());
        let first = context
            .call(
                "create_extension",
                json!({
                    "id": "versioned-tools",
                    "description": "Version one",
                    "instructions": "Use version one.",
                    "version": "1.0.0"
                }),
            )
            .unwrap();
        let first_revision = first["revision"].as_str().unwrap().to_string();
        context
            .call(
                "create_extension",
                json!({
                    "id": "versioned-tools",
                    "description": "Version two",
                    "instructions": "Use version two.",
                    "version": "2.0.0",
                    "overwrite": true
                }),
            )
            .unwrap();
        let listed = context.call("list_blu_extensions", json!({})).unwrap();
        assert_eq!(listed["extensions"][0]["version"], "2.0.0");
        assert_eq!(
            listed["extensions"][0]["versions"][0]["revision"],
            first_revision
        );

        context
            .call(
                "rollback_blu_extension",
                json!({"id": "versioned-tools", "revision": first_revision}),
            )
            .unwrap();
        let read = context
            .call(
                "read_blu_extension",
                json!({"id": "versioned-tools", "scope": "project"}),
            )
            .unwrap();
        assert_eq!(read["version"], "1.0.0");
        assert_eq!(
            read["files"]["skills/versioned-tools/SKILL.md"],
            "---\nname: versioned-tools\ndescription: \"Version one\"\n---\n\n# versioned-tools\n\nUse version one.\n"
        );
    }

    #[test]
    fn create_extension_can_write_a_luau_workflow_under_blu() {
        let workspace = tempfile::tempdir().unwrap();
        let context = SelfServiceContext::new(workspace.path().to_path_buf());
        let created = context
            .call(
                "create_extension",
                json!({
                    "id": "luau-tools",
                    "description": "Run Luau analysis",
                    "instructions": "Use the Luau workflow when asked.",
                    "workflow_name": "analyze",
                    "workflow_source": "local answer: number = 42\nreturn answer",
                    "runtime": "blu",
                    "source_extension": "luau"
                }),
            )
            .unwrap();
        assert_eq!(created["runtime"], "blu");
        assert_eq!(created["source_extension"], "luau");
        assert!(
            workspace
                .path()
                .join(".borg/extensions/luau-tools/workflows/analyze.luau")
                .is_file()
        );
        let manifest = fs::read_to_string(
            workspace
                .path()
                .join(".borg/extensions/luau-tools/blu.toml"),
        )
        .unwrap();
        assert!(manifest.contains("entrypoint = \"workflows/analyze.luau\""));
    }

    #[test]
    fn blu_extension_lifecycle_rejects_incomplete_or_oversized_workflows() {
        let workspace = tempfile::tempdir().unwrap();
        let context = SelfServiceContext::new(workspace.path().to_path_buf());
        assert!(
            context
                .call(
                    "create_blu_extension",
                    json!({
                        "id": "broken",
                        "description": "Broken",
                        "instructions": "Instructions",
                        "workflow_name": "review"
                    }),
                )
                .is_err()
        );
        assert!(
            context
                .call(
                    "create_blu_extension",
                    json!({
                        "id": "large",
                        "description": "Large",
                        "instructions": "Instructions",
                        "workflow_name": "review",
                        "workflow_source": "x".repeat(MAX_BLU_WORKFLOW_SOURCE + 1)
                    }),
                )
                .is_err()
        );
        assert!(!workspace.path().join(".borg/extensions").exists());
    }
}
