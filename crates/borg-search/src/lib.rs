//! A small provider-neutral web-search boundary for Borg agents.
//!
//! The agent sees SearchRequest and SearchResponse, never a vendor's wire
//! format or credential. Backends are deliberately bounded: search is
//! discovery and provenance, not an unbounded page-fetching runtime.

use std::fmt;
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use url::Url;

pub const MAX_QUERY_CHARS: usize = 400;
pub const MAX_RESULTS: usize = 20;
pub const DEFAULT_RESULTS: usize = 5;
pub const MAX_DOMAIN_FILTERS: usize = 20;
pub const MAX_DOMAIN_CHARS: usize = 255;
pub const MAX_SNIPPET_CHARS: usize = 2_000;
const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(20);

/// A concrete backend that produced a search response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SearchBackend {
    Exa,
    Parallel,
    Brave,
}

impl SearchBackend {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Exa => "exa",
            Self::Parallel => "parallel",
            Self::Brave => "brave",
        }
    }
}

impl fmt::Display for SearchBackend {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// The configured backend selection. Auto prefers Exa, then Parallel, then
/// Brave, using only providers with a configured credential.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SearchBackendChoice {
    #[default]
    Auto,
    Exa,
    Parallel,
    Brave,
}

impl SearchBackendChoice {
    pub fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "" | "auto" => Ok(Self::Auto),
            "exa" => Ok(Self::Exa),
            "parallel" => Ok(Self::Parallel),
            "brave" => Ok(Self::Brave),
            other => bail!(
                "unsupported BORG_SEARCH_BACKEND {other}; expected auto, exa, parallel, or brave"
            ),
        }
    }
}

#[derive(Clone)]
pub struct SearchConfig {
    pub backend: SearchBackendChoice,
    pub exa_api_key: Option<String>,
    pub parallel_api_key: Option<String>,
    pub brave_api_key: Option<String>,
    pub timeout: Duration,
}

impl Default for SearchConfig {
    fn default() -> Self {
        Self {
            backend: SearchBackendChoice::Auto,
            exa_api_key: None,
            parallel_api_key: None,
            brave_api_key: None,
            timeout: DEFAULT_TIMEOUT,
        }
    }
}

impl fmt::Debug for SearchConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SearchConfig")
            .field("backend", &self.backend)
            .field(
                "exa_api_key",
                &self.exa_api_key.as_ref().map(|_| "[configured]"),
            )
            .field(
                "parallel_api_key",
                &self.parallel_api_key.as_ref().map(|_| "[configured]"),
            )
            .field(
                "brave_api_key",
                &self.brave_api_key.as_ref().map(|_| "[configured]"),
            )
            .field("timeout", &self.timeout)
            .finish()
    }
}

impl SearchConfig {
    /// Read search selection and credentials without ever logging the values.
    pub fn from_env() -> Result<Self> {
        let backend = SearchBackendChoice::parse(
            &std::env::var("BORG_SEARCH_BACKEND").unwrap_or_else(|_| "auto".to_string()),
        )?;
        let timeout = match std::env::var("BORG_SEARCH_TIMEOUT_SECS") {
            Ok(value) => Duration::from_secs(
                value
                    .trim()
                    .parse()
                    .context("BORG_SEARCH_TIMEOUT_SECS must be a non-negative integer")?,
            ),
            Err(_) => DEFAULT_TIMEOUT,
        };
        Ok(Self {
            backend,
            exa_api_key: first_env(&["EXA_API_KEY", "BORG_EXA_API_KEY"]),
            parallel_api_key: first_env(&["PARALLEL_API_KEY", "BORG_PARALLEL_API_KEY"]),
            brave_api_key: first_env(&[
                "BRAVE_SEARCH_API_KEY",
                "BRAVE_API_KEY",
                "BORG_BRAVE_SEARCH_API_KEY",
            ]),
            timeout,
        })
    }

    fn key_for(&self, backend: SearchBackend) -> Option<&str> {
        match backend {
            SearchBackend::Exa => self.exa_api_key.as_deref(),
            SearchBackend::Parallel => self.parallel_api_key.as_deref(),
            SearchBackend::Brave => self.brave_api_key.as_deref(),
        }
        .filter(|key| !key.trim().is_empty())
    }
}

fn first_env(names: &[&str]) -> Option<String> {
    names.iter().find_map(|name| {
        std::env::var(name)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchRequest {
    pub query: String,
    pub max_results: usize,
    pub include_domains: Vec<String>,
    pub exclude_domains: Vec<String>,
}

impl SearchRequest {
    pub fn bounded(query: impl Into<String>, max_results: Option<usize>) -> Result<Self> {
        let request = Self {
            query: query.into(),
            max_results: max_results.unwrap_or(DEFAULT_RESULTS),
            include_domains: Vec::new(),
            exclude_domains: Vec::new(),
        };
        request.validate()?;
        Ok(request)
    }

    pub fn validate(&self) -> Result<()> {
        let query = self.query.trim();
        ensure!(!query.is_empty(), "web search query cannot be empty");
        ensure!(
            query.chars().count() <= MAX_QUERY_CHARS,
            "web search query exceeds {MAX_QUERY_CHARS} characters"
        );
        ensure!(
            (1..=MAX_RESULTS).contains(&self.max_results),
            "web search max_results must be between 1 and {MAX_RESULTS}"
        );
        validate_domains(&self.include_domains, "include_domains")?;
        validate_domains(&self.exclude_domains, "exclude_domains")?;
        Ok(())
    }
}

fn validate_domains(domains: &[String], field: &str) -> Result<()> {
    ensure!(
        domains.len() <= MAX_DOMAIN_FILTERS,
        "{field} accepts at most {MAX_DOMAIN_FILTERS} domains"
    );
    for domain in domains {
        let domain = domain.trim();
        ensure!(!domain.is_empty(), "{field} cannot contain an empty domain");
        ensure!(
            domain.chars().count() <= MAX_DOMAIN_CHARS,
            "{field} contains a domain longer than {MAX_DOMAIN_CHARS} characters"
        );
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SearchResult {
    pub title: String,
    pub url: String,
    pub snippet: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub published_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SearchResponse {
    pub backend: SearchBackend,
    pub query: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    pub results: Vec<SearchResult>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

/// The only capability the agent/tool layer needs from a search implementation.
#[async_trait]
pub trait WebSearchProvider: Send + Sync {
    async fn search(&self, request: SearchRequest) -> Result<SearchResponse>;
}

/// Selects one configured backend and normalizes its response.
#[derive(Clone)]
pub struct SearchService {
    config: SearchConfig,
    client: reqwest::Client,
}

impl fmt::Debug for SearchService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SearchService")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl SearchService {
    pub fn new(mut config: SearchConfig) -> Result<Self> {
        if config.timeout.is_zero() {
            config.timeout = DEFAULT_TIMEOUT;
        }
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(config.timeout)
            .build()
            .context("build web-search HTTP client")?;
        Ok(Self { config, client })
    }

    /// Construct the service only when at least one usable key is present.
    /// This keeps the web_search tool out of sessions that cannot execute it.
    pub fn from_env() -> Result<Option<Self>> {
        let config = SearchConfig::from_env()?;
        let configured = match config.backend {
            SearchBackendChoice::Auto => [
                SearchBackend::Exa,
                SearchBackend::Parallel,
                SearchBackend::Brave,
            ]
            .into_iter()
            .any(|backend| config.key_for(backend).is_some()),
            SearchBackendChoice::Exa => config.key_for(SearchBackend::Exa).is_some(),
            SearchBackendChoice::Parallel => config.key_for(SearchBackend::Parallel).is_some(),
            SearchBackendChoice::Brave => config.key_for(SearchBackend::Brave).is_some(),
        };
        configured.then(|| Self::new(config)).transpose()
    }

    pub fn configured_backend(&self) -> Option<SearchBackend> {
        match self.config.backend {
            SearchBackendChoice::Auto => [
                SearchBackend::Exa,
                SearchBackend::Parallel,
                SearchBackend::Brave,
            ]
            .into_iter()
            .find(|backend| self.config.key_for(*backend).is_some()),
            SearchBackendChoice::Exa => self
                .config
                .key_for(SearchBackend::Exa)
                .map(|_| SearchBackend::Exa),
            SearchBackendChoice::Parallel => self
                .config
                .key_for(SearchBackend::Parallel)
                .map(|_| SearchBackend::Parallel),
            SearchBackendChoice::Brave => self
                .config
                .key_for(SearchBackend::Brave)
                .map(|_| SearchBackend::Brave),
        }
    }

    async fn search_with_backend(
        &self,
        backend: SearchBackend,
        request: &SearchRequest,
    ) -> Result<SearchResponse> {
        let key = self
            .config
            .key_for(backend)
            .with_context(|| format!("the selected {backend} backend has no configured API key"))?;
        match backend {
            SearchBackend::Exa => self.search_exa(key, request).await,
            SearchBackend::Parallel => self.search_parallel(key, request).await,
            SearchBackend::Brave => self.search_brave(key, request).await,
        }
    }

    async fn search_exa(&self, key: &str, request: &SearchRequest) -> Result<SearchResponse> {
        let mut body = json!({
            "query": request.query.trim(),
            "numResults": request.max_results,
            "contents": { "highlights": true }
        });
        if !request.include_domains.is_empty() {
            body["includeDomains"] = json!(request.include_domains);
        }
        if !request.exclude_domains.is_empty() {
            body["excludeDomains"] = json!(request.exclude_domains);
        }
        let response = self
            .client
            .post("https://api.exa.ai/search")
            .header("x-api-key", key)
            .json(&body)
            .send()
            .await
            .context("Exa web search request failed")?;
        let payload = response_json(response, "Exa web search").await?;
        Ok(SearchResponse {
            backend: SearchBackend::Exa,
            query: request.query.trim().to_string(),
            request_id: payload
                .get("requestId")
                .and_then(Value::as_str)
                .map(str::to_string),
            results: normalize_results(request, parse_exa_results(&payload)),
            warnings: Vec::new(),
        })
    }

    async fn search_parallel(&self, key: &str, request: &SearchRequest) -> Result<SearchResponse> {
        let body = json!({
            "objective": request.query.trim(),
            "search_queries": [request.query.trim()],
            "mode": "turbo",
            "max_chars_total": request.max_results * MAX_SNIPPET_CHARS,
        });
        let response = self
            .client
            .post("https://api.parallel.ai/v1/search")
            .header("x-api-key", key)
            .json(&body)
            .send()
            .await
            .context("Parallel web search request failed")?;
        let payload = response_json(response, "Parallel web search").await?;
        Ok(SearchResponse {
            backend: SearchBackend::Parallel,
            query: request.query.trim().to_string(),
            request_id: payload
                .get("search_id")
                .or_else(|| payload.get("searchId"))
                .or_else(|| payload.get("session_id"))
                .and_then(Value::as_str)
                .map(str::to_string),
            results: normalize_results(request, parse_parallel_results(&payload)),
            warnings: payload
                .get("warnings")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|warning| warning.as_str().map(str::to_string))
                .collect(),
        })
    }

    async fn search_brave(&self, key: &str, request: &SearchRequest) -> Result<SearchResponse> {
        let response = self
            .client
            .get("https://api.search.brave.com/res/v1/web/search")
            .header("Accept", "application/json")
            .header("Accept-Encoding", "gzip")
            .header("X-Subscription-Token", key)
            .query(&[
                ("q", request.query.trim().to_string()),
                ("count", request.max_results.to_string()),
                ("result_filter", "web".to_string()),
            ])
            .send()
            .await
            .context("Brave web search request failed")?;
        let payload = response_json(response, "Brave web search").await?;
        Ok(SearchResponse {
            backend: SearchBackend::Brave,
            query: request.query.trim().to_string(),
            request_id: None,
            results: normalize_results(request, parse_brave_results(&payload)),
            warnings: Vec::new(),
        })
    }
}

#[async_trait]
impl WebSearchProvider for SearchService {
    async fn search(&self, request: SearchRequest) -> Result<SearchResponse> {
        request.validate()?;
        let backend = self.configured_backend().context(
            "web search is unavailable: configure EXA_API_KEY, PARALLEL_API_KEY, or BRAVE_SEARCH_API_KEY",
        )?;
        self.search_with_backend(backend, &request).await
    }
}

async fn response_json(response: reqwest::Response, label: &str) -> Result<Value> {
    let status = response.status();
    ensure!(
        response
            .content_length()
            .is_none_or(|length| length <= MAX_RESPONSE_BYTES as u64),
        "{label} response exceeded {MAX_RESPONSE_BYTES} bytes"
    );
    let mut body = Vec::new();
    let mut response = response;
    while let Some(chunk) = response
        .chunk()
        .await
        .with_context(|| format!("read {label} response"))?
    {
        ensure!(
            body.len().saturating_add(chunk.len()) <= MAX_RESPONSE_BYTES,
            "{label} response exceeded {MAX_RESPONSE_BYTES} bytes"
        );
        body.extend_from_slice(&chunk);
    }
    if !status.is_success() {
        let detail = String::from_utf8_lossy(&body);
        let detail = detail.trim();
        bail!(
            "{label} returned HTTP {}{}",
            status.as_u16(),
            (!detail.is_empty())
                .then(|| format!(": {detail}"))
                .unwrap_or_default()
        );
    }
    serde_json::from_slice(&body).with_context(|| format!("decode {label} JSON response"))
}

fn parse_exa_results(payload: &Value) -> Vec<SearchResult> {
    payload
        .get("results")
        .and_then(Value::as_array)
        .map(|results| {
            results
                .iter()
                .filter_map(|result| {
                    Some(SearchResult {
                        title: result.get("title")?.as_str()?.to_string(),
                        url: result.get("url")?.as_str()?.to_string(),
                        snippet: first_text(result, &["highlights", "text", "summary"]),
                        published_at: result
                            .get("publishedDate")
                            .and_then(Value::as_str)
                            .map(str::to_string),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn parse_parallel_results(payload: &Value) -> Vec<SearchResult> {
    payload
        .get("results")
        .and_then(Value::as_array)
        .map(|results| {
            results
                .iter()
                .filter_map(|result| {
                    Some(SearchResult {
                        title: result.get("title")?.as_str()?.to_string(),
                        url: result.get("url")?.as_str()?.to_string(),
                        snippet: first_text(result, &["excerpts", "excerpt"]),
                        published_at: result
                            .get("publish_date")
                            .or_else(|| result.get("publishedDate"))
                            .and_then(Value::as_str)
                            .map(str::to_string),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn parse_brave_results(payload: &Value) -> Vec<SearchResult> {
    payload
        .pointer("/web/results")
        .and_then(Value::as_array)
        .map(|results| {
            results
                .iter()
                .filter_map(|result| {
                    Some(SearchResult {
                        title: result.get("title")?.as_str()?.to_string(),
                        url: result.get("url")?.as_str()?.to_string(),
                        snippet: first_text(result, &["description"]),
                        published_at: ["page_age", "age", "published"].into_iter().find_map(
                            |field| {
                                result
                                    .get(field)
                                    .and_then(Value::as_str)
                                    .map(str::to_string)
                            },
                        ),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn first_text(value: &Value, fields: &[&str]) -> String {
    for field in fields {
        let Some(value) = value.get(*field) else {
            continue;
        };
        if let Some(text) = value.as_str() {
            return truncate(text);
        }
        if let Some(values) = value.as_array() {
            let joined = values
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join("\n");
            if !joined.is_empty() {
                return truncate(&joined);
            }
        }
    }
    String::new()
}

fn truncate(value: &str) -> String {
    let mut output = value.chars().take(MAX_SNIPPET_CHARS).collect::<String>();
    if value.chars().count() > MAX_SNIPPET_CHARS {
        output.push('…');
    }
    output
}

fn normalize_results(request: &SearchRequest, results: Vec<SearchResult>) -> Vec<SearchResult> {
    results
        .into_iter()
        .filter(|result| domain_allowed(&result.url, request))
        .take(request.max_results)
        .collect()
}

fn domain_allowed(raw_url: &str, request: &SearchRequest) -> bool {
    let Ok(url) = Url::parse(raw_url) else {
        return false;
    };
    let Some(host) = url.host_str().map(str::to_ascii_lowercase) else {
        return false;
    };
    let include = |domain: &str| host_matches(&host, domain);
    let excluded = request.exclude_domains.iter().any(|domain| include(domain));
    let included = request.include_domains.is_empty()
        || request.include_domains.iter().any(|domain| include(domain));
    included && !excluded
}

fn host_matches(host: &str, raw_domain: &str) -> bool {
    let domain = raw_domain
        .trim()
        .trim_start_matches("*.")
        .split('/')
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    host == domain || host.ends_with(&format!(".{domain}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_selection_prefers_exa_then_parallel_then_brave() {
        let config = SearchConfig {
            exa_api_key: Some("exa".to_string()),
            parallel_api_key: Some("parallel".to_string()),
            brave_api_key: Some("brave".to_string()),
            ..SearchConfig::default()
        };
        assert_eq!(config.key_for(SearchBackend::Exa), Some("exa"));
        let service = SearchService::new(config).expect("service");
        assert_eq!(service.configured_backend(), Some(SearchBackend::Exa));
    }

    #[test]
    fn results_are_normalized_and_domain_filtered() {
        let request = SearchRequest {
            query: "borg".to_string(),
            max_results: 2,
            include_domains: vec!["example.com".to_string()],
            exclude_domains: Vec::new(),
        };
        let results = vec![
            SearchResult {
                title: "kept".to_string(),
                url: "https://docs.example.com/a".to_string(),
                snippet: "one\ntwo".to_string(),
                published_at: Some("2026-01-01".to_string()),
            },
            SearchResult {
                title: "filtered".to_string(),
                url: "https://other.test/a".to_string(),
                snippet: "no".to_string(),
                published_at: None,
            },
        ];
        let normalized = normalize_results(&request, results);
        assert_eq!(normalized.len(), 1);
        assert_eq!(normalized[0].snippet, "one\ntwo");
    }

    #[test]
    fn exa_payload_mapping_preserves_request_fields_and_highlights() {
        let payload = json!({
            "requestId": "exa-request",
            "results": [
                {
                    "title": "Exa docs",
                    "url": "https://docs.exa.ai/",
                    "publishedDate": "2026-08-07T00:00:00Z",
                    "highlights": ["first", "second"]
                },
                {"title": "missing url"}
            ]
        });
        let results = parse_exa_results(&payload);

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].title, "Exa docs");
        assert_eq!(results[0].snippet, "first\nsecond");
        assert_eq!(
            results[0].published_at.as_deref(),
            Some("2026-08-07T00:00:00Z")
        );
        assert_eq!(payload["requestId"], "exa-request");
    }

    #[test]
    fn parallel_payload_mapping_uses_excerpts_and_publish_date() {
        let payload = json!({
            "search_id": "parallel-search",
            "results": [{
                "title": "Parallel docs",
                "url": "https://docs.parallel.ai/",
                "publish_date": "2026-08-06",
                "excerpts": ["parallel excerpt"]
            }]
        });
        let results = parse_parallel_results(&payload);

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].title, "Parallel docs");
        assert_eq!(results[0].snippet, "parallel excerpt");
        assert_eq!(results[0].published_at.as_deref(), Some("2026-08-06"));
        assert_eq!(payload["search_id"], "parallel-search");
    }

    #[test]
    fn brave_payload_mapping_reads_nested_web_results() {
        let payload = json!({
            "web": {
                "results": [{
                    "title": "Brave docs",
                    "url": "https://api.search.brave.com/",
                    "description": "Brave description",
                    "age": "2 days ago"
                }]
            }
        });
        let results = parse_brave_results(&payload);

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].title, "Brave docs");
        assert_eq!(results[0].snippet, "Brave description");
        assert_eq!(results[0].published_at.as_deref(), Some("2 days ago"));
    }

    #[test]
    fn request_bounds_reject_unbounded_inputs() {
        let request = SearchRequest {
            query: "x".repeat(MAX_QUERY_CHARS + 1),
            max_results: 1,
            include_domains: Vec::new(),
            exclude_domains: Vec::new(),
        };
        assert!(request.validate().is_err());
    }

    #[test]
    fn backend_choice_accepts_all_supported_backends() {
        assert_eq!(
            SearchBackendChoice::parse("exa").unwrap(),
            SearchBackendChoice::Exa
        );
        assert_eq!(
            SearchBackendChoice::parse("parallel").unwrap(),
            SearchBackendChoice::Parallel
        );
        assert_eq!(
            SearchBackendChoice::parse("brave").unwrap(),
            SearchBackendChoice::Brave
        );
    }
}
