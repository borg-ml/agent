# Borg web search

Borg exposes one provider-neutral web_search tool. It works with no
configuration at all: with no provider credential present it searches through a
keyless route, and when credentials are present it prefers those providers
instead. The model-facing tool never accepts credentials as input; credentials
stay in the host environment. The tool returns bounded normalized results:

- backend: the concrete backend that answered, or `federated` when auto mode
  merged multiple providers;
- query and an optional backend request id;
- result title, source URL, bounded snippet, and optional publication date;
- contributing backends for federated responses;
- optional warnings.

The URL is the provenance boundary. Search snippets are discovery context, not
authoritative page contents, and Borg does not silently turn this tool into an
unbounded page-fetching runtime.

## Configuration

Search needs no configuration. The keyless route speaks the public Exa MCP
endpoint (https://mcp.exa.ai/mcp, a JSON-RPC tools/call of web_search_exa) and
carries no credential at all, so a host with no search keys still gets the tool.

Credentials are optional and are preferred when present:

    EXA_API_KEY
    FIRECRAWL_API_KEY
    PARALLEL_API_KEY
    BRAVE_SEARCH_API_KEY

The aliases BORG_EXA_API_KEY, BORG_FIRECRAWL_API_KEY,
BORG_PARALLEL_API_KEY, and BORG_BRAVE_SEARCH_API_KEY are also accepted.
Select a backend with
BORG_SEARCH_BACKEND=auto|exa|firecrawl|parallel|brave|exa-mcp.

auto is the default and resolves in this order:

- with one or more credentials, it fans out to every configured keyed backend in
  stable order (Exa, Firecrawl, Parallel, then Brave), concurrently. Results are
  URL-deduplicated and capped at the requested count. A provider failure becomes
  a warning when another provider succeeds; if every provider fails, the tool
  returns an error.
- with no credentials at all, auto uses the keyless route, so the tool stays
  available instead of disappearing.

exa-mcp (aliases exa_mcp and keyless) pins the keyless route even when
credentials exist. Naming a keyed backend explicitly stays single-provider and
does not silently switch to another provider; if that backend has no credential,
Borg omits the tool rather than advertising a capability that cannot run.

Two consequences are worth knowing. Adding a credential replaces the keyless
route under auto rather than supplementing it, so there is no automatic fallback
to the free route if a keyed provider errors or rate-limits. And the keyless
route is a public third-party service, so its rate limits and terms apply to
whatever traffic a deployment sends it.

BORG_SEARCH_TIMEOUT_SECS controls the request timeout and defaults to 20
seconds.

## Bounds and selection

The tool accepts a query of at most 400 characters, at most 20 results, and at
most 20 include/exclude domain filters. Snippets are capped at 2,000
characters per result and provider response bodies at 8 MiB. Domain filters are
also applied after normalization so the contract remains consistent across
backends.

The direct adapters follow the vendors' current API contracts:

- [Exa Search API](https://exa.ai/docs/reference/search) uses
  POST /search, the x-api-key header, numResults, and optional domain filters.
- [Firecrawl Search API](https://docs.firecrawl.dev/api-reference/endpoint/search)
  uses POST /v2/search, a bearer token, limit, web sources, and optional domain
  filters.
- [Parallel Search API](https://docs.parallel.ai/api-reference/search/search)
  uses POST /v1/search, the x-api-key header, an objective plus search query,
  and bounded total excerpt characters.
- [Brave Web Search API](https://api-dashboard.search.brave.com/api-reference/web/search/get)
  uses GET /res/v1/web/search, the X-Subscription-Token header, and a bounded
  result count.
- The keyless route uses the public Exa MCP endpoint: POST /mcp, a JSON-RPC 2.0
  tools/call of web_search_exa with a query and a result count, answered as
  server-sent events. That endpoint has no domain-filter parameter, so
  include/exclude filters are applied after normalization like every other
  backend.

These adapters live in borg-search; the agent dispatcher receives only the
WebSearchProvider trait. Native and subscription-backed model lanes therefore
share the same tool and provenance semantics.
