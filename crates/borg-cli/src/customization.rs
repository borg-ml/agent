use std::collections::BTreeMap;
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};

use crate::agent_config::AgentConfig;
use crate::cli::{CustomizeArgs, CustomizeCommand};
use crate::editor_preferences::EditorPreferences;

const PROFILE_VERSION: u32 = 1;

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CustomizationProfile {
    profile_version: u32,
    #[serde(default)]
    agent_toml: Option<String>,
    #[serde(default)]
    editor_toml: Option<String>,
    #[serde(default)]
    user_blu_toml: Option<String>,
    #[serde(default)]
    project_blu_toml: Option<String>,
}

#[derive(Serialize)]
struct EffectiveCustomization {
    agent_config: Option<PathBuf>,
    editor_config: PathBuf,
    user_extension_state: PathBuf,
    project_extension_state: PathBuf,
    editor: EditorPreferences,
    keybindings: BTreeMap<String, Vec<String>>,
    command_aliases: BTreeMap<String, String>,
    extension_default_access: String,
    project_extension_access: String,
    native_extension_access: String,
    extensions: crate::extensions::ExtensionCatalog,
}

pub(crate) fn run(args: CustomizeArgs) -> Result<()> {
    let cwd = std::env::current_dir()?;
    match args.command {
        CustomizeCommand::Inspect { json } => inspect(&cwd, json),
        CustomizeCommand::Export { output, force } => export(&cwd, &output, force),
        CustomizeCommand::Import { input, force } => import(&cwd, &input, force),
    }
}

fn inspect(cwd: &Path, json: bool) -> Result<()> {
    let mut agent = AgentConfig::load(None)?;
    let mut editor = EditorPreferences::load()?;
    let (extensions, _, _) =
        crate::extensions::discover(cwd, &agent.capabilities, &agent.extensions)?;
    extensions.apply_editor_customization(&mut editor, &mut agent)?;
    let effective = EffectiveCustomization {
        agent_config: AgentConfig::path(None),
        editor_config: crate::editor_preferences::default_path()?,
        user_extension_state: user_blu_path()?,
        project_extension_state: cwd.join(".borg/blu.toml"),
        editor,
        keybindings: agent
            .keybindings
            .entries()
            .into_iter()
            .map(|(name, bindings)| (name.to_string(), bindings.to_vec()))
            .collect(),
        command_aliases: agent.commands.aliases.clone(),
        extension_default_access: agent.extensions.default_access.label().to_string(),
        project_extension_access: agent.extensions.project_access.label().to_string(),
        native_extension_access: format!("{:?}", agent.extensions.native_access).to_lowercase(),
        extensions,
    };
    if json {
        println!("{}", serde_json::to_string_pretty(&effective)?);
    } else {
        println!("Customization");
        println!(
            "  agent: {}",
            display_optional(effective.agent_config.as_deref())
        );
        println!("  editor: {}", effective.editor_config.display());
        println!(
            "  user extensions: {}",
            effective.user_extension_state.display()
        );
        println!(
            "  project extensions: {}",
            effective.project_extension_state.display()
        );
        println!("  catalog revision: {}", effective.extensions.revision);
        for extension in &effective.extensions.extensions {
            println!(
                "  {}: {} · {:?} · {}",
                extension.id,
                if extension.active {
                    "active"
                } else {
                    "inactive"
                },
                extension.requested_access,
                extension.reason.as_deref().unwrap_or("admitted")
            );
        }
    }
    Ok(())
}

fn export(cwd: &Path, output: &Path, force: bool) -> Result<()> {
    let profile = CustomizationProfile {
        profile_version: PROFILE_VERSION,
        agent_toml: read_optional(AgentConfig::path(None).as_deref())?,
        editor_toml: read_optional(Some(&crate::editor_preferences::default_path()?))?,
        user_blu_toml: read_optional(Some(&user_blu_path()?))?,
        project_blu_toml: read_optional(Some(&cwd.join(".borg/blu.toml")))?,
    };
    write_atomic(
        output,
        serde_json::to_string_pretty(&profile)?.as_bytes(),
        force,
    )?;
    println!("Exported customization profile to {}", output.display());
    Ok(())
}

fn import(cwd: &Path, input: &Path, force: bool) -> Result<()> {
    let source = fs::read_to_string(input)
        .with_context(|| format!("failed to read profile {}", input.display()))?;
    let profile: CustomizationProfile = serde_json::from_str(&source)
        .with_context(|| format!("invalid customization profile {}", input.display()))?;
    ensure!(
        profile.profile_version == PROFILE_VERSION,
        "unsupported profile version {}",
        profile.profile_version
    );

    validate_profile(&profile)?;
    let targets = [
        (AgentConfig::path(None), profile.agent_toml),
        (
            Some(crate::editor_preferences::default_path()?),
            profile.editor_toml,
        ),
        (Some(user_blu_path()?), profile.user_blu_toml),
        (Some(cwd.join(".borg/blu.toml")), profile.project_blu_toml),
    ];
    for (path, _) in &targets {
        if let Some(path) = path {
            ensure!(
                force || !path.exists(),
                "{} already exists; pass --force to replace it",
                path.display()
            );
        }
    }
    for (path, contents) in targets {
        if let Some(path) = path {
            match contents {
                Some(contents) => write_atomic(&path, contents.as_bytes(), true)?,
                None if path.exists() => fs::remove_file(&path)
                    .with_context(|| format!("failed to remove {}", path.display()))?,
                None => {}
            }
        }
    }
    println!("Imported customization profile from {}", input.display());
    Ok(())
}

fn validate_profile(profile: &CustomizationProfile) -> Result<()> {
    if let Some(source) = &profile.agent_toml {
        let agent: AgentConfig = toml::from_str(source).context("invalid agent.toml in profile")?;
        agent.validate().context("invalid agent.toml in profile")?;
    }
    if let Some(source) = &profile.editor_toml {
        let editor: EditorPreferences =
            toml::from_str(source).context("invalid editor.toml in profile")?;
        editor
            .validate()
            .context("invalid editor.toml in profile")?;
    }
    for (label, source) in [
        ("user blu.toml", &profile.user_blu_toml),
        ("project blu.toml", &profile.project_blu_toml),
    ] {
        if let Some(source) = source {
            toml::from_str::<toml::Value>(source)
                .with_context(|| format!("invalid {label} in profile"))?;
        }
    }
    Ok(())
}

fn read_optional(path: Option<&Path>) -> Result<Option<String>> {
    path.filter(|path| path.exists())
        .map(|path| {
            fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))
        })
        .transpose()
}

fn write_atomic(path: &Path, bytes: &[u8], force: bool) -> Result<()> {
    ensure!(
        force || !path.exists(),
        "{} already exists; pass --force to replace it",
        path.display()
    );
    let parent = path.parent().context("customization path has no parent")?;
    fs::create_dir_all(parent)?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary.write_all(bytes)?;
    temporary.as_file().sync_all()?;
    temporary.persist(path).map_err(|error| error.error)?;
    Ok(())
}

fn user_blu_path() -> Result<PathBuf> {
    let root = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".config")))
        .or_else(dirs::config_dir)
        .context("unable to determine a user config directory")?;
    Ok(root.join("borg/blu.toml"))
}

fn display_optional(path: Option<&Path>) -> String {
    path.map(|path| path.display().to_string())
        .unwrap_or_else(|| "defaults".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_validation_rejects_invalid_editor_values() {
        let profile = CustomizationProfile {
            profile_version: PROFILE_VERSION,
            agent_toml: None,
            editor_toml: Some("[presentation]\nrefresh_rate_fps = 1\n".to_string()),
            user_blu_toml: None,
            project_blu_toml: None,
        };
        assert!(validate_profile(&profile).is_err());
    }

    #[test]
    fn atomic_profile_writes_require_explicit_replacement() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("profile.json");
        write_atomic(&path, b"one", false).unwrap();
        assert!(write_atomic(&path, b"two", false).is_err());
        write_atomic(&path, b"two", true).unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"two");
    }
}
