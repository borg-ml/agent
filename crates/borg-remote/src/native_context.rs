use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};

const MAX_PROJECT_INSTRUCTIONS_BYTES: usize = 512 * 1024;
const MAX_SKILL_BYTES: u64 = 512 * 1024;
const MAX_SKILLS: usize = 128;

#[derive(Debug, Clone, Default)]
pub(crate) struct NativeContext {
    project_instructions: String,
    skills: BTreeMap<String, SkillEntry>,
}

#[derive(Debug, Clone)]
struct SkillEntry {
    description: String,
    path: PathBuf,
}

impl NativeContext {
    pub(crate) async fn load(cwd: PathBuf) -> Result<Self> {
        tokio::task::spawn_blocking(move || Self::load_blocking(&cwd))
            .await
            .context("native context loader stopped")?
    }

    fn load_blocking(cwd: &Path) -> Result<Self> {
        let cwd = cwd.canonicalize().context("canonicalize agent workspace")?;
        let project_chain = project_chain(&cwd);
        let mut project_instructions = String::new();
        for directory in &project_chain {
            let path = directory.join("AGENTS.md");
            let Ok(metadata) = path.metadata() else {
                continue;
            };
            if !metadata.is_file() {
                continue;
            }
            let content = std::fs::read_to_string(&path)
                .with_context(|| format!("read project guidance {}", path.display()))?;
            if project_instructions.len().saturating_add(content.len())
                > MAX_PROJECT_INSTRUCTIONS_BYTES
            {
                bail!(
                    "applicable AGENTS.md files exceed the {} KiB native context limit",
                    MAX_PROJECT_INSTRUCTIONS_BYTES / 1024
                );
            }
            project_instructions.push_str(&format!(
                "\n\n<project_guidance path=\"{}\">\n{}\n</project_guidance>",
                path.display(),
                content.trim()
            ));
        }

        let mut skill_roots = user_skill_roots();
        for directory in &project_chain {
            skill_roots.push(directory.join(".agents").join("skills"));
            skill_roots.push(directory.join(".borg").join("skills"));
        }
        let mut skills = BTreeMap::new();
        for root in skill_roots {
            load_skill_root(&root, &mut skills)?;
            if skills.len() >= MAX_SKILLS {
                break;
            }
        }
        Ok(Self {
            project_instructions,
            skills,
        })
    }

    pub(crate) fn prompt_appendix(&self) -> String {
        let mut appendix = self.project_instructions.clone();
        if !self.skills.is_empty() {
            appendix.push_str(
                "\n\nAvailable skills are listed below. When a user names one or the task clearly matches its description, call read_skill and follow the complete SKILL.md before acting.",
            );
            for (name, skill) in &self.skills {
                appendix.push_str(&format!("\n- {name}: {}", skill.description));
            }
        }
        appendix
    }

    pub(crate) fn has_skills(&self) -> bool {
        !self.skills.is_empty()
    }

    pub(crate) fn skill_tool_spec(&self) -> Value {
        json!({
            "name": "read_skill",
            "description": "Read the complete SKILL.md for one skill from the catalog in the system prompt.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "enum": self.skills.keys().collect::<Vec<_>>()
                    }
                },
                "required": ["name"],
                "additionalProperties": false
            }
        })
    }

    pub(crate) async fn read_skill(&self, name: &str) -> Result<Value> {
        let skill = self
            .skills
            .get(name)
            .with_context(|| format!("unknown skill `{name}`"))?;
        let metadata = tokio::fs::metadata(&skill.path).await?;
        if metadata.len() > MAX_SKILL_BYTES {
            bail!(
                "skill `{name}` exceeds the {} KiB limit",
                MAX_SKILL_BYTES / 1024
            );
        }
        let content = tokio::fs::read_to_string(&skill.path)
            .await
            .with_context(|| format!("read skill {}", skill.path.display()))?;
        Ok(json!({
            "name": name,
            "path": skill.path,
            "content": content,
        }))
    }
}

fn project_chain(cwd: &Path) -> Vec<PathBuf> {
    let mut ancestors = cwd
        .ancestors()
        .take(32)
        .map(Path::to_path_buf)
        .collect::<Vec<_>>();
    let root_index = ancestors
        .iter()
        .position(|path| path.join(".git").exists())
        .unwrap_or(0);
    ancestors.truncate(root_index + 1);
    ancestors.reverse();
    ancestors
}

fn user_skill_roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Some(home) = std::env::var_os("HOME").map(PathBuf::from) {
        roots.push(home.join(".agents").join("skills"));
        roots.push(home.join(".borg").join("skills"));
        roots.push(home.join(".codex").join("skills"));
    }
    if let Some(codex_home) = std::env::var_os("CODEX_HOME").map(PathBuf::from) {
        roots.push(codex_home.join("skills"));
    }
    roots
}

fn load_skill_root(root: &Path, skills: &mut BTreeMap<String, SkillEntry>) -> Result<()> {
    let Ok(entries) = std::fs::read_dir(root) else {
        return Ok(());
    };
    for entry in entries {
        if skills.len() >= MAX_SKILLS {
            break;
        }
        let entry = entry?;
        let path = entry.path().join("SKILL.md");
        let Ok(metadata) = path.metadata() else {
            continue;
        };
        if !metadata.is_file() || metadata.len() > MAX_SKILL_BYTES {
            continue;
        }
        let content = std::fs::read_to_string(&path)
            .with_context(|| format!("inspect skill {}", path.display()))?;
        let fallback_name = entry.file_name().to_string_lossy().to_string();
        let (name, description) = skill_metadata(&content, &fallback_name);
        let canonical = path.canonicalize()?;
        skills.insert(
            name,
            SkillEntry {
                description,
                path: canonical,
            },
        );
    }
    Ok(())
}

fn skill_metadata(content: &str, fallback_name: &str) -> (String, String) {
    let mut name = None;
    let mut description = None;
    if content.starts_with("---") {
        for line in content.lines().skip(1).take_while(|line| *line != "---") {
            if let Some(value) = line.strip_prefix("name:") {
                name = Some(value.trim().trim_matches('"').to_string());
            } else if let Some(value) = line.strip_prefix("description:") {
                description = Some(value.trim().trim_matches('"').to_string());
            }
        }
    }
    let name = name
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| fallback_name.to_string());
    let description = description
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "Reusable agent workflow".to_string());
    (name, description)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn project_skills_override_user_skills_by_name() {
        let root = tempfile::tempdir().expect("root");
        let user = tempfile::tempdir().expect("user");
        for (base, description) in [(user.path(), "user"), (root.path(), "project")] {
            let directory = base.join("skills").join("review");
            std::fs::create_dir_all(&directory).expect("skill directory");
            std::fs::write(
                directory.join("SKILL.md"),
                format!("---\nname: review\ndescription: {description}\n---\n"),
            )
            .expect("skill");
        }
        let mut skills = BTreeMap::new();
        load_skill_root(&user.path().join("skills"), &mut skills).expect("user skills");
        load_skill_root(&root.path().join("skills"), &mut skills).expect("project skills");
        assert_eq!(skills["review"].description, "project");
    }
}
