use std::path::{Path, PathBuf};

use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportedMemory {
    pub source: String,
    pub source_id: String,
    pub title: String,
    pub content: String,
    #[serde(default)]
    pub cwd: Option<PathBuf>,
    #[serde(default)]
    pub project: Option<String>,
    pub updated_at: DateTime<Utc>,
}

pub fn imported_memory_directory() -> PathBuf {
    crate::default_host_config_path()
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("imports/memory")
}

pub async fn copy_imported_memory(directory: &Path, memory: &ImportedMemory) -> Result<bool> {
    ensure!(!memory.content.trim().is_empty(), "memory is empty");
    ensure!(
        memory.title.len() <= 512 && memory.source.len() <= 512 && memory.source_id.len() <= 4096,
        "memory metadata exceeds size limits"
    );
    ensure!(
        memory.content.len() <= 4 * 1024 * 1024,
        "memory exceeds 4 MiB"
    );
    let id = Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        format!("borg-import:{}:memory:{}", memory.source, memory.source_id).as_bytes(),
    );
    tokio::fs::create_dir_all(directory).await?;
    let path = directory.join(format!("{id}.json"));
    let temporary = directory.join(format!(".{}.tmp", Uuid::new_v4()));
    tokio::fs::write(&temporary, serde_json::to_vec_pretty(memory)?).await?;
    let result = tokio::fs::hard_link(&temporary, &path).await;
    let _ = tokio::fs::remove_file(&temporary).await;
    match result {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
        Err(error) => Err(error).context("copy imported memory"),
    }
}

pub(crate) async fn prompt_appendix(root: &Path) -> Result<String> {
    prompt_appendix_from(&imported_memory_directory(), root).await
}

async fn prompt_appendix_from(directory: &Path, root: &Path) -> Result<String> {
    let mut files = match tokio::fs::read_dir(directory).await {
        Ok(files) => files,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(String::new()),
        Err(error) => return Err(error.into()),
    };
    let root = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    let mut memories = Vec::new();
    while let Some(file) = files.next_entry().await? {
        if file.path().extension().is_none_or(|ext| ext != "json")
            || !file.file_type().await?.is_file()
        {
            continue;
        }
        if file.metadata().await?.len() > 5 * 1024 * 1024 {
            continue;
        }
        let memory: ImportedMemory = match serde_json::from_slice(
            &tokio::fs::read(file.path()).await?,
        ) {
            Ok(memory) => memory,
            Err(error) => {
                tracing::warn!(path = %file.path().display(), %error, "invalid imported memory");
                continue;
            }
        };
        if memory.project.is_some() && memory.cwd.is_none() {
            continue;
        }
        if memory
            .cwd
            .as_ref()
            .is_some_and(|cwd| cwd.canonicalize().unwrap_or_else(|_| cwd.clone()) != root)
        {
            continue;
        }
        memories.push((file.path(), memory));
    }
    if memories.is_empty() {
        return Ok(String::new());
    }
    memories.sort_by(|a, b| b.1.updated_at.cmp(&a.1.updated_at).then(a.0.cmp(&b.0)));
    let mut text = format!(
        "\n\n## Imported memory\nThe user copied {} memory entries from other assistants. Treat them as historical context, with current user instructions taking precedence. Full entries are stored in {}. Only entries with no project scope or matching this directory apply. Read additional entries there when needed.\n",
        memories.len(),
        directory.display()
    );
    for (path, memory) in memories {
        let available = 16_384usize.saturating_sub(text.len());
        if available < 512 {
            break;
        }
        let mut content = memory.content;
        if content.len() > available.saturating_sub(512) {
            let mut end = available.saturating_sub(512);
            while !content.is_char_boundary(end) {
                end -= 1;
            }
            content.truncate(end);
            content.push_str("\n[Excerpt; read the full entry if needed.]");
        }
        text.push_str(&format!(
            "\n### {} ({})\nFile: {}\n{}\n",
            memory.title,
            memory.source,
            path.display(),
            content
        ));
    }
    Ok(text)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn imported_memory_is_copy_only_and_respects_project_scope() {
        let directory = tempfile::tempdir().unwrap();
        let project = tempfile::tempdir().unwrap();
        let other = tempfile::tempdir().unwrap();
        let memory = ImportedMemory {
            source: "claude-code".into(),
            source_id: "test".into(),
            title: "Project preference".into(),
            content: "Use the project-specific build command".into(),
            cwd: Some(project.path().into()),
            project: Some("source-project".into()),
            updated_at: Utc::now(),
        };
        assert!(
            copy_imported_memory(directory.path(), &memory)
                .await
                .unwrap()
        );
        let mut changed = memory.clone();
        changed.content = "Changed in source".into();
        assert!(
            !copy_imported_memory(directory.path(), &changed)
                .await
                .unwrap()
        );
        let context = prompt_appendix_from(directory.path(), project.path())
            .await
            .unwrap();
        assert!(context.contains(&memory.content));
        assert!(!context.contains(&changed.content));
        assert!(
            prompt_appendix_from(directory.path(), other.path())
                .await
                .unwrap()
                .is_empty()
        );
        changed.source_id = "unmapped".into();
        changed.cwd = None;
        assert!(
            copy_imported_memory(directory.path(), &changed)
                .await
                .unwrap()
        );
        assert!(
            !prompt_appendix_from(directory.path(), other.path())
                .await
                .unwrap()
                .contains(&changed.content)
        );
    }
}
