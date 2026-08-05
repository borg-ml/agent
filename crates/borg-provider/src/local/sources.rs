use std::fmt;
use std::fs::{self, File, ReadDir};
use std::io::{self, Read};
use std::path::{Path, PathBuf};

use serde::Deserialize;

use super::gguf::{GgufHeader, parse_gguf_file};
use super::{LocalModel, stable_model_id};

const MAX_MANIFEST_BYTES: u64 = 4 << 20;
const MODEL_MEDIA_TYPE: &str = "application/vnd.ollama.image.model";

/// A source of locally-runnable models.
pub trait ModelSource: Send + Sync {
    fn name(&self) -> &str;
    fn discover(&self) -> Result<Vec<LocalModel>, ModelSourceError>;
}

#[derive(Debug)]
pub enum ModelSourceError {
    Io {
        source: &'static str,
        path: PathBuf,
        error: io::Error,
    },
}

impl fmt::Display for ModelSourceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io {
                source,
                path,
                error,
            } => {
                write!(
                    formatter,
                    "{source} discovery failed at {}: {error}",
                    path.display()
                )
            }
        }
    }
}

impl std::error::Error for ModelSourceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { error, .. } => Some(error),
        }
    }
}

/// Scans explicitly configured directories for `.gguf` files.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeGgufSource {
    directories: Vec<PathBuf>,
}

impl NativeGgufSource {
    pub fn new(directories: Vec<PathBuf>) -> Self {
        Self { directories }
    }

    pub fn directories(&self) -> &[PathBuf] {
        &self.directories
    }
}

impl ModelSource for NativeGgufSource {
    fn name(&self) -> &str {
        "configured GGUF directories"
    }

    fn discover(&self) -> Result<Vec<LocalModel>, ModelSourceError> {
        discover_gguf_directories(&self.directories, "dir")
    }
}

/// Reads Ollama manifests and resolves only model layers to local blobs. It
/// never contacts or starts the Ollama daemon.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OllamaBlobSource {
    models_dir: PathBuf,
}

impl OllamaBlobSource {
    pub fn new(models_dir: PathBuf) -> Self {
        Self { models_dir }
    }

    pub fn models_dir(&self) -> &Path {
        &self.models_dir
    }
}

impl ModelSource for OllamaBlobSource {
    fn name(&self) -> &str {
        "Ollama blob store"
    }

    fn discover(&self) -> Result<Vec<LocalModel>, ModelSourceError> {
        let manifests_dir = self.models_dir.join("manifests");
        if !manifests_dir.is_dir() {
            return Ok(Vec::new());
        }

        let mut manifests = Vec::new();
        if collect_regular_files(&manifests_dir, &mut manifests, "ollama").is_err() {
            // A partially removed or permission-restricted Ollama store is a
            // normal absence from the picker, not a reason to fail the whole
            // local-model discovery pass.
            return Ok(Vec::new());
        }
        manifests.sort();

        let blobs_dir = self.models_dir.join("blobs");
        let mut models = Vec::new();
        let mut seen_blobs = std::collections::HashSet::new();
        for manifest_path in manifests {
            let Some(digest) = read_model_digest(&manifest_path) else {
                // Ollama can leave partial or old manifests in place. One
                // bad tag must not hide healthy tags from the picker.
                continue;
            };
            if !seen_blobs.insert(digest.clone()) {
                continue;
            }
            let blob_path = blobs_dir.join(format!("sha256-{}", &digest[7..]));
            if !is_regular_file(&blob_path) {
                continue;
            }
            let Ok(header) = parse_gguf_file(&blob_path) else {
                continue;
            };
            let Ok(size_bytes) = fs::metadata(&blob_path).map(|metadata| metadata.len()) else {
                continue;
            };
            let reference = manifest_reference(&manifests_dir, &manifest_path);
            models.push(local_model_from_header(
                &blob_path,
                size_bytes,
                &header,
                "ollama",
                Some(format!("ollama:{reference}")),
            ));
        }
        Ok(models)
    }
}

/// Optional scanner for Hugging Face's on-disk cache. It is explicit because
/// a cache can contain many revisions and symlinked copies of the same model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HuggingFaceCacheSource {
    cache_dir: PathBuf,
}

impl HuggingFaceCacheSource {
    pub fn new(cache_dir: PathBuf) -> Self {
        Self { cache_dir }
    }

    pub fn cache_dir(&self) -> &Path {
        &self.cache_dir
    }
}

impl ModelSource for HuggingFaceCacheSource {
    fn name(&self) -> &str {
        "Hugging Face cache"
    }

    fn discover(&self) -> Result<Vec<LocalModel>, ModelSourceError> {
        discover_gguf_directories(std::slice::from_ref(&self.cache_dir), "hf")
    }
}

fn discover_gguf_directories(
    directories: &[PathBuf],
    source: &'static str,
) -> Result<Vec<LocalModel>, ModelSourceError> {
    let mut paths = Vec::new();
    for directory in directories {
        if !directory.exists() {
            continue;
        }
        collect_gguf_files(directory, &mut paths, source)?;
    }
    paths.sort();
    paths.dedup();

    let mut models = Vec::new();
    for path in paths {
        let Ok(size_bytes) = fs::metadata(&path).map(|metadata| metadata.len()) else {
            continue;
        };
        let Ok(header) = parse_gguf_file(&path) else {
            // Discovery is best-effort. The parser remains strict and typed
            // for callers that need to report a malformed file directly.
            continue;
        };
        models.push(local_model_from_header(
            &path, size_bytes, &header, source, None,
        ));
    }
    Ok(models)
}

fn collect_gguf_files(
    directory: &Path,
    output: &mut Vec<PathBuf>,
    source: &'static str,
) -> Result<(), ModelSourceError> {
    let entries = fs::read_dir(directory).map_err(|error| ModelSourceError::Io {
        source,
        path: directory.to_path_buf(),
        error,
    })?;
    collect_gguf_entries(entries, output, source)
}

fn collect_gguf_entries(
    entries: ReadDir,
    output: &mut Vec<PathBuf>,
    source: &'static str,
) -> Result<(), ModelSourceError> {
    for entry in entries {
        let entry = entry.map_err(|error| ModelSourceError::Io {
            source,
            path: PathBuf::new(),
            error,
        })?;
        let path = entry.path();
        let file_type = entry.file_type().map_err(|error| ModelSourceError::Io {
            source,
            path: path.clone(),
            error,
        })?;
        if file_type.is_dir() {
            collect_gguf_files(&path, output, source)?;
        } else if file_type.is_file()
            && path
                .extension()
                .is_some_and(|extension| extension.eq_ignore_ascii_case("gguf"))
        {
            output.push(path);
        }
    }
    Ok(())
}

fn collect_regular_files(
    directory: &Path,
    output: &mut Vec<PathBuf>,
    source: &'static str,
) -> Result<(), ModelSourceError> {
    let entries = fs::read_dir(directory).map_err(|error| ModelSourceError::Io {
        source,
        path: directory.to_path_buf(),
        error,
    })?;
    for entry in entries {
        let entry = entry.map_err(|error| ModelSourceError::Io {
            source,
            path: directory.to_path_buf(),
            error,
        })?;
        let path = entry.path();
        let file_type = entry.file_type().map_err(|error| ModelSourceError::Io {
            source,
            path: path.clone(),
            error,
        })?;
        if file_type.is_dir() {
            collect_regular_files(&path, output, source)?;
        } else if file_type.is_file() {
            output.push(path);
        }
    }
    Ok(())
}

fn is_regular_file(path: &Path) -> bool {
    fs::symlink_metadata(path)
        .map(|metadata| metadata.file_type().is_file())
        .unwrap_or(false)
}

#[derive(Debug, Deserialize)]
struct OllamaManifest {
    #[serde(default)]
    layers: Vec<OllamaLayer>,
}

#[derive(Debug, Deserialize)]
struct OllamaLayer {
    #[serde(rename = "mediaType")]
    media_type: String,
    digest: String,
}

fn read_model_digest(path: &Path) -> Option<String> {
    let mut file = File::open(path).ok()?;
    let length = file.metadata().ok()?.len();
    if length > MAX_MANIFEST_BYTES {
        return None;
    }
    let mut bytes = Vec::with_capacity(length as usize);
    file.read_to_end(&mut bytes).ok()?;
    let manifest = serde_json::from_slice::<OllamaManifest>(&bytes).ok()?;
    manifest
        .layers
        .into_iter()
        .find(|layer| layer.media_type == MODEL_MEDIA_TYPE)
        .and_then(|layer| valid_digest(&layer.digest))
}

fn valid_digest(digest: &str) -> Option<String> {
    let hex = digest.strip_prefix("sha256:")?;
    if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    Some(format!("sha256:{hex}"))
}

fn manifest_reference(root: &Path, manifest: &Path) -> String {
    let relative = manifest.strip_prefix(root).unwrap_or(manifest);
    let mut components = relative.components().filter_map(|component| {
        let value = component.as_os_str().to_str()?;
        (!value.is_empty() && value != ".").then_some(value.to_string())
    });
    let first = components.next();
    let pieces = components.collect::<Vec<_>>();
    if first.as_deref() == Some("registry.ollama.ai") {
        let mut pieces = pieces;
        if pieces.first().is_some_and(|piece| piece == "library") {
            pieces.remove(0);
        }
        return ollama_tag_reference(&pieces);
    }
    let pieces = first.into_iter().chain(pieces).collect::<Vec<_>>();
    ollama_tag_reference(&pieces)
}

fn ollama_tag_reference(pieces: &[String]) -> String {
    match pieces {
        [] => "unknown".to_string(),
        [model] => model.clone(),
        _ => {
            let mut pieces = pieces.to_vec();
            let tag = pieces.pop().unwrap_or_default();
            format!("{}:{tag}", pieces.join("/"))
        }
    }
}

fn local_model_from_header(
    path: &Path,
    size_bytes: u64,
    header: &GgufHeader,
    source: &'static str,
    id: Option<String>,
) -> LocalModel {
    let architecture = header
        .string("general.architecture")
        .unwrap_or("unknown")
        .to_string();
    let display_name = header
        .string("general.name")
        .or_else(|| path.file_stem().and_then(|name| name.to_str()))
        .unwrap_or("Unnamed GGUF model")
        .to_string();
    let quant = quantization_label(path, header);
    let block_count = header
        .positive_u64(&format!("{architecture}.block_count"))
        .and_then(|value| u32::try_from(value).ok());
    let train_ctx = header
        .positive_u64(&format!("{architecture}.context_length"))
        .and_then(|value| u32::try_from(value).ok());
    let expert_count = header
        .positive_u64(&format!("{architecture}.expert_count"))
        .and_then(|value| u32::try_from(value).ok());

    LocalModel {
        id: id.unwrap_or_else(|| stable_model_id(source, path)),
        display_name,
        path: path.to_path_buf(),
        architecture,
        quant,
        size_bytes,
        block_count,
        train_ctx,
        expert_count,
        source,
    }
}

fn quantization_label(path: &Path, header: &GgufHeader) -> String {
    if let Some(label) = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .and_then(find_quantization_in_name)
    {
        return label;
    }
    header
        .get("general.file_type")
        .and_then(|value| value.as_u64())
        .and_then(quantization_from_file_type)
        .unwrap_or("unknown")
        .to_string()
}

fn find_quantization_in_name(name: &str) -> Option<String> {
    let upper = name.to_ascii_uppercase();
    // Longest first so Q4_K_M wins over Q4_K, and preserve the filename's
    // spelling for custom labels such as Q2_g64.
    const LABELS: &[&str] = &[
        "IQ4_XS", "IQ4_NL", "IQ3_XXS", "IQ3_XS", "IQ3_S", "IQ2_XXS", "IQ2_XS", "Q8_K", "Q6_K",
        "Q5_K_M", "Q5_K_S", "Q4_K_M", "Q4_K_S", "Q3_K_L", "Q3_K_M", "Q3_K_S", "Q2_K", "Q2_G64",
        "Q8_0", "Q6_0", "Q5_1", "Q5_0", "Q4_1", "Q4_0", "F32", "F16", "BF16",
    ];
    LABELS.iter().find_map(|label| {
        let start = upper.find(label)?;
        name.get(start..start + label.len())
            .map(ToString::to_string)
    })
}

fn quantization_from_file_type(file_type: u64) -> Option<&'static str> {
    Some(match file_type {
        0 => "F32",
        1 => "F16",
        2 => "Q4_0",
        3 => "Q4_1",
        6 => "Q5_0",
        7 => "Q5_1",
        8 => "Q8_0",
        10 => "Q2_K",
        11 => "Q3_K_S",
        12 => "Q3_K_M",
        13 => "Q3_K_L",
        14 => "Q4_K_S",
        15 => "Q4_K_M",
        16 => "Q5_K_S",
        17 => "Q5_K_M",
        18 => "Q6_K",
        19 => "TQ1_0",
        20 => "TQ2_0",
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

    fn write_minimal_gguf(path: &Path, name: &str) {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&0x4655_4747_u32.to_le_bytes());
        bytes.extend_from_slice(&3_u32.to_le_bytes());
        bytes.extend_from_slice(&0_u64.to_le_bytes());
        bytes.extend_from_slice(&2_u64.to_le_bytes());
        for (key, value) in [("general.architecture", "qwen35"), ("general.name", name)] {
            bytes.extend_from_slice(&(key.len() as u64).to_le_bytes());
            bytes.extend_from_slice(key.as_bytes());
            bytes.extend_from_slice(&8_u32.to_le_bytes());
            bytes.extend_from_slice(&(value.len() as u64).to_le_bytes());
            bytes.extend_from_slice(value.as_bytes());
        }
        File::create(path).unwrap().write_all(&bytes).unwrap();
    }

    #[test]
    fn native_source_skips_invalid_files_and_discovers_nested_gguf() {
        let temp = tempdir().unwrap();
        let nested = temp.path().join("nested");
        fs::create_dir(&nested).unwrap();
        write_minimal_gguf(&nested.join("Qwen-Q4_K_M.gguf"), "Qwen");
        File::create(temp.path().join("broken.gguf")).unwrap();
        let models = NativeGgufSource::new(vec![temp.path().to_path_buf()])
            .discover()
            .unwrap();
        assert_eq!(models.len(), 1);
        assert_eq!(models[0].display_name, "Qwen");
        assert_eq!(models[0].quant, "Q4_K_M");
        assert_eq!(models[0].source, "dir");
    }

    #[test]
    fn ollama_source_resolves_only_safe_model_digest() {
        let temp = tempdir().unwrap();
        let manifest = temp
            .path()
            .join("manifests/registry.ollama.ai/library/qwen/latest");
        fs::create_dir_all(manifest.parent().unwrap()).unwrap();
        fs::create_dir_all(temp.path().join("blobs")).unwrap();
        let digest = "sha256:".to_string() + &"a".repeat(64);
        let blob = temp.path().join(format!("blobs/sha256-{}", &digest[7..]));
        write_minimal_gguf(&blob, "Ollama Qwen");
        fs::write(
            manifest,
            serde_json::json!({
                "layers": [{
                    "mediaType": MODEL_MEDIA_TYPE,
                    "digest": digest,
                }]
            })
            .to_string(),
        )
        .unwrap();
        let models = OllamaBlobSource::new(temp.path().to_path_buf())
            .discover()
            .unwrap();
        assert_eq!(models.len(), 1);
        assert_eq!(models[0].id, "ollama:qwen:latest");
        assert_eq!(models[0].source, "ollama");
    }

    #[test]
    fn real_fixture_models_report_metadata_without_loading_weights() {
        let fixtures = [
            (
                "/home/shulgin/twilight/target/qwen36-local-gate/Qwen3.6-27B-Q4_K_M.gguf",
                "Qwen3.6-27B",
                "Q4_K_M",
                16_817_244_384_u64,
            ),
            (
                "/home/shulgin/twilight/target/qwen36-local-gate/Qwen3.6-27B-MTP-Q4_K_M.gguf",
                "Qwen3.6-27B",
                "Q4_K_M",
                17_106_773_120_u64,
            ),
            (
                "/home/shulgin/twilight/target/ternary-bonsai27-gate/Ternary-Bonsai-27B-Q2_g64.gguf",
                "Bonsai-27B",
                "Q2_g64",
                7_585_330_240_u64,
            ),
        ];
        for (path, name, quant, size_bytes) in fixtures {
            let path = Path::new(path);
            if !path.is_file() {
                continue;
            }
            let models =
                NativeGgufSource::new(vec![path.parent().expect("fixture parent").to_path_buf()])
                    .discover()
                    .expect("fixture discovery");
            let model = models
                .into_iter()
                .find(|model| model.path == path)
                .expect("fixture model");
            assert_eq!(model.display_name, name);
            assert_eq!(model.architecture, "qwen35");
            assert_eq!(model.quant, quant);
            assert_eq!(model.size_bytes, size_bytes);
        }
    }
}
