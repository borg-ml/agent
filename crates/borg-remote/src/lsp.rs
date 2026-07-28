use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::Mutex;
use tokio::time::{Duration, timeout};
use url::Url;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_LSP_MESSAGE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone)]
pub struct LspService {
    root: PathBuf,
    clients: std::sync::Arc<Mutex<HashMap<&'static str, LspClient>>>,
}

struct ServerSpec {
    id: &'static str,
    command: &'static str,
    args: &'static [&'static str],
    language_id: &'static str,
}

struct LspClient {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    next_id: u64,
    opened_versions: HashMap<PathBuf, i32>,
    published_diagnostics: HashMap<String, Value>,
}

impl LspService {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            clients: std::sync::Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn status(&self) -> Value {
        let clients = self.clients.lock().await;
        let active = clients.keys().copied().collect::<Vec<_>>();
        json!({
            "root": self.root,
            "active_servers": active,
            "supported_servers": server_specs().iter().map(|spec| spec.id).collect::<Vec<_>>()
        })
    }

    pub async fn diagnostics(&self, path: &Path) -> Result<Value> {
        let (path, uri, spec) = self.resolve_document(path).await?;
        let mut clients = self.clients.lock().await;
        let client = ensure_client(&mut clients, spec, &self.root).await?;
        client.open_document(&path, &uri, spec.language_id).await?;
        match client
            .request(
                "textDocument/diagnostic",
                json!({ "textDocument": { "uri": uri } }),
            )
            .await
        {
            Ok(result) => Ok(result),
            Err(pull_error) => client
                .wait_for_published_diagnostics(&uri)
                .await
                .with_context(|| format!("pull diagnostics failed ({pull_error:#})")),
        }
    }

    pub async fn hover(&self, path: &Path, line: u32, character: u32) -> Result<Value> {
        self.position_request(path, "textDocument/hover", line, character, json!({}))
            .await
    }

    pub async fn definition(&self, path: &Path, line: u32, character: u32) -> Result<Value> {
        self.position_request(path, "textDocument/definition", line, character, json!({}))
            .await
    }

    pub async fn references(&self, path: &Path, line: u32, character: u32) -> Result<Value> {
        self.position_request(
            path,
            "textDocument/references",
            line,
            character,
            json!({ "context": { "includeDeclaration": true } }),
        )
        .await
    }

    pub async fn document_symbols(&self, path: &Path) -> Result<Value> {
        self.document_request(path, "textDocument/documentSymbol", json!({}))
            .await
    }

    pub async fn workspace_symbols(&self, query: &str) -> Result<Value> {
        let mut clients = self.clients.lock().await;
        if clients.is_empty() {
            bail!("no language server is active; inspect a supported source file first");
        }
        let mut results = serde_json::Map::new();
        for (id, client) in clients.iter_mut() {
            let value = client
                .request("workspace/symbol", json!({ "query": query }))
                .await
                .with_context(|| format!("{id} workspace symbol request failed"))?;
            results.insert((*id).to_string(), value);
        }
        Ok(Value::Object(results))
    }

    async fn document_request(&self, path: &Path, method: &str, extra: Value) -> Result<Value> {
        let (path, uri, spec) = self.resolve_document(path).await?;
        let mut clients = self.clients.lock().await;
        let client = ensure_client(&mut clients, spec, &self.root).await?;
        client.open_document(&path, &uri, spec.language_id).await?;
        let mut params = extra.as_object().cloned().unwrap_or_default();
        params.insert("textDocument".to_string(), json!({ "uri": uri }));
        client.request(method, Value::Object(params)).await
    }

    async fn position_request(
        &self,
        path: &Path,
        method: &str,
        line: u32,
        character: u32,
        extra: Value,
    ) -> Result<Value> {
        let (path, uri, spec) = self.resolve_document(path).await?;
        let mut clients = self.clients.lock().await;
        let client = ensure_client(&mut clients, spec, &self.root).await?;
        client.open_document(&path, &uri, spec.language_id).await?;
        let mut params = extra.as_object().cloned().unwrap_or_default();
        params.insert("textDocument".to_string(), json!({ "uri": uri }));
        params.insert(
            "position".to_string(),
            json!({
                "line": line.saturating_sub(1),
                "character": character.saturating_sub(1)
            }),
        );
        client.request(method, Value::Object(params)).await
    }

    async fn resolve_document(
        &self,
        requested: &Path,
    ) -> Result<(PathBuf, String, &'static ServerSpec)> {
        let joined = if requested.is_absolute() {
            requested.to_path_buf()
        } else {
            self.root.join(requested)
        };
        let path = tokio::fs::canonicalize(&joined)
            .await
            .with_context(|| format!("cannot resolve {}", joined.display()))?;
        let root = tokio::fs::canonicalize(&self.root)
            .await
            .with_context(|| format!("cannot resolve workspace root {}", self.root.display()))?;
        if !path.starts_with(&root) {
            bail!("LSP path must stay inside the session workspace");
        }
        let spec = spec_for_path(&path).ok_or_else(|| {
            anyhow::anyhow!(
                "no configured language server for {}",
                path.extension()
                    .and_then(|value| value.to_str())
                    .unwrap_or("this file type")
            )
        })?;
        let uri = Url::from_file_path(&path)
            .map_err(|_| anyhow::anyhow!("cannot convert {} to a file URI", path.display()))?
            .to_string();
        Ok((path, uri, spec))
    }
}

impl LspClient {
    async fn start(spec: &ServerSpec, root: &Path) -> Result<Self> {
        let mut child = Command::new(spec.command)
            .args(spec.args)
            .current_dir(root)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| {
                format!(
                    "{} is not available; install it on PATH to enable {} LSP support",
                    spec.command, spec.id
                )
            })?;
        let stdin = child
            .stdin
            .take()
            .context("language server stdin unavailable")?;
        let stdout = child
            .stdout
            .take()
            .context("language server stdout unavailable")?;
        let mut client = Self {
            child,
            stdin,
            stdout: BufReader::new(stdout),
            next_id: 1,
            opened_versions: HashMap::new(),
            published_diagnostics: HashMap::new(),
        };
        let root_uri = Url::from_directory_path(root)
            .map_err(|_| anyhow::anyhow!("cannot convert workspace root to URI"))?
            .to_string();
        client
            .request(
                "initialize",
                json!({
                    "processId": std::process::id(),
                    "rootUri": root_uri,
                    "capabilities": {
                        "textDocument": {
                            "hover": { "contentFormat": ["markdown", "plaintext"] },
                            "definition": { "linkSupport": true },
                            "documentSymbol": { "hierarchicalDocumentSymbolSupport": true },
                            "diagnostic": {}
                        },
                        "workspace": { "symbol": { "resolveSupport": { "properties": [] } } }
                    },
                    "clientInfo": { "name": "borg", "version": env!("CARGO_PKG_VERSION") }
                }),
            )
            .await?;
        client.notify("initialized", json!({})).await?;
        Ok(client)
    }

    async fn open_document(&mut self, path: &Path, uri: &str, language_id: &str) -> Result<()> {
        let text = tokio::fs::read_to_string(path)
            .await
            .with_context(|| format!("cannot read {}", path.display()))?;
        let version = self.opened_versions.entry(path.to_path_buf()).or_insert(0);
        *version += 1;
        let method = if *version == 1 {
            "textDocument/didOpen"
        } else {
            "textDocument/didChange"
        };
        let params = if *version == 1 {
            json!({
                "textDocument": {
                    "uri": uri,
                    "languageId": language_id,
                    "version": *version,
                    "text": text
                }
            })
        } else {
            json!({
                "textDocument": { "uri": uri, "version": *version },
                "contentChanges": [{ "text": text }]
            })
        };
        self.notify(method, params).await
    }

    async fn notify(&mut self, method: &str, params: Value) -> Result<()> {
        self.write_message(&json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params
        }))
        .await
    }

    async fn request(&mut self, method: &str, params: Value) -> Result<Value> {
        let id = self.next_id;
        self.next_id += 1;
        self.write_message(&json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params
        }))
        .await?;
        timeout(REQUEST_TIMEOUT, async {
            loop {
                let message = self.read_message().await?;
                self.capture_diagnostics(&message);
                if message.get("method").is_some()
                    && let Some(server_request_id) = message.get("id").cloned()
                {
                    self.write_message(&json!({
                        "jsonrpc": "2.0",
                        "id": server_request_id,
                        "result": Value::Null
                    }))
                    .await?;
                    continue;
                }
                if message.get("id").and_then(Value::as_u64) != Some(id) {
                    continue;
                }
                if let Some(error) = message.get("error") {
                    bail!("{method} failed: {error}");
                }
                return Ok(message.get("result").cloned().unwrap_or(Value::Null));
            }
        })
        .await
        .with_context(|| format!("{method} timed out"))?
    }

    fn capture_diagnostics(&mut self, message: &Value) {
        if message.get("method").and_then(Value::as_str) != Some("textDocument/publishDiagnostics")
        {
            return;
        }
        let Some(uri) = message.pointer("/params/uri").and_then(Value::as_str) else {
            return;
        };
        let diagnostics = message
            .pointer("/params/diagnostics")
            .cloned()
            .unwrap_or_else(|| json!([]));
        self.published_diagnostics
            .insert(uri.to_string(), diagnostics);
    }

    async fn wait_for_published_diagnostics(&mut self, uri: &str) -> Result<Value> {
        if let Some(items) = self.published_diagnostics.remove(uri) {
            return Ok(json!({ "kind": "full", "items": items }));
        }
        timeout(Duration::from_secs(3), async {
            loop {
                let message = self.read_message().await?;
                self.capture_diagnostics(&message);
                if let Some(server_request_id) = message.get("id").cloned()
                    && message.get("method").is_some()
                {
                    self.write_message(&json!({
                        "jsonrpc": "2.0",
                        "id": server_request_id,
                        "result": Value::Null
                    }))
                    .await?;
                }
                if let Some(items) = self.published_diagnostics.remove(uri) {
                    return Ok(json!({ "kind": "full", "items": items }));
                }
            }
        })
        .await
        .context("language server did not publish diagnostics")?
    }

    async fn write_message(&mut self, message: &Value) -> Result<()> {
        let body = serde_json::to_vec(message)?;
        self.stdin
            .write_all(format!("Content-Length: {}\r\n\r\n", body.len()).as_bytes())
            .await?;
        self.stdin.write_all(&body).await?;
        self.stdin.flush().await?;
        Ok(())
    }

    async fn read_message(&mut self) -> Result<Value> {
        let mut content_length = None;
        loop {
            let mut header = String::new();
            let bytes = self.stdout.read_line(&mut header).await?;
            if bytes == 0 {
                let status = self.child.try_wait()?;
                bail!("language server closed stdout (status: {status:?})");
            }
            if header == "\r\n" || header == "\n" {
                break;
            }
            if let Some(value) = header
                .strip_prefix("Content-Length:")
                .and_then(|value| value.trim().parse::<usize>().ok())
            {
                content_length = Some(value);
            }
        }
        let length = content_length.context("LSP response omitted Content-Length")?;
        if length > MAX_LSP_MESSAGE_BYTES {
            bail!("LSP response exceeds {} bytes", MAX_LSP_MESSAGE_BYTES);
        }
        let mut body = vec![0; length];
        self.stdout.read_exact(&mut body).await?;
        serde_json::from_slice(&body).context("language server returned invalid JSON")
    }
}

async fn ensure_client<'a>(
    clients: &'a mut HashMap<&'static str, LspClient>,
    spec: &'static ServerSpec,
    root: &Path,
) -> Result<&'a mut LspClient> {
    if !clients.contains_key(spec.id) {
        clients.insert(spec.id, LspClient::start(spec, root).await?);
    }
    Ok(clients.get_mut(spec.id).expect("inserted LSP client"))
}

fn spec_for_path(path: &Path) -> Option<&'static ServerSpec> {
    let extension = path.extension()?.to_str()?.to_ascii_lowercase();
    server_specs().iter().find(|spec| match spec.id {
        "rust-analyzer" => extension == "rs",
        "typescript-language-server" => matches!(extension.as_str(), "ts" | "tsx" | "js" | "jsx"),
        "pyright" => extension == "py",
        "gopls" => extension == "go",
        "clangd" => matches!(extension.as_str(), "c" | "h" | "cc" | "cpp" | "cxx" | "hpp"),
        _ => false,
    })
}

fn server_specs() -> &'static [ServerSpec] {
    &[
        ServerSpec {
            id: "rust-analyzer",
            command: "rust-analyzer",
            args: &[],
            language_id: "rust",
        },
        ServerSpec {
            id: "typescript-language-server",
            command: "typescript-language-server",
            args: &["--stdio"],
            language_id: "typescript",
        },
        ServerSpec {
            id: "pyright",
            command: "pyright-langserver",
            args: &["--stdio"],
            language_id: "python",
        },
        ServerSpec {
            id: "gopls",
            command: "gopls",
            args: &[],
            language_id: "go",
        },
        ServerSpec {
            id: "clangd",
            command: "clangd",
            args: &[],
            language_id: "cpp",
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn rust_analyzer_answers_a_real_diagnostic_request_when_available() {
        if !Command::new("rust-analyzer")
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await
            .is_ok_and(|status| status.success())
        {
            return;
        }
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let result = LspService::new(&root)
            .diagnostics(Path::new("src/lib.rs"))
            .await
            .expect("rust-analyzer diagnostic request");
        assert!(result.is_object() || result.is_array() || result.is_null());
    }

    #[tokio::test]
    async fn clangd_publish_diagnostics_are_returned_when_available() {
        if !Command::new("clangd")
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await
            .is_ok_and(|status| status.success())
        {
            return;
        }
        let root = tempfile::tempdir().expect("temp workspace");
        tokio::fs::write(
            root.path().join("broken.c"),
            "int main() { return missing; }\n",
        )
        .await
        .expect("write C source");
        let result = LspService::new(root.path())
            .diagnostics(Path::new("broken.c"))
            .await
            .expect("clangd diagnostics");
        assert!(
            result
                .get("items")
                .and_then(Value::as_array)
                .is_some_and(|items| !items.is_empty())
        );
    }
}
