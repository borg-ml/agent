# Borg web search

Borg exposes one provider-neutral web_search tool when one or more host-side
search credentials are configured. The model-facing tool is keyless:
credentials stay in the host environment and are never accepted as tool input.
The tool returns bounded normalized results:

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

Set one or more credentials in the environment:

    EXA_API_KEY
    FIRECRAWL_API_KEY
    PARALLEL_API_KEY
    BRAVE_SEARCH_API_KEY

The aliases BORG_EXA_API_KEY, BORG_FIRECRAWL_API_KEY,
BORG_PARALLEL_API_KEY, and BORG_BRAVE_SEARCH_API_KEY are also accepted.
Select a backend with BORG_SEARCH_BACKEND=auto|exa|firecrawl|parallel|brave.

auto fans out to every configured backend in stable order (Exa, Firecrawl,
Parallel, then Brave), concurrently. Results are URL-deduplicated and capped
at the requested count. A provider failure becomes a warning when another
provider succeeds; if every provider fails, the tool returns an error. If no
usable credential is present, Borg omits the tool instead of advertising a
capability that cannot run. Explicit backend selection remains single-provider
and does not silently switch to another provider.

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

These adapters live in borg-search; the agent dispatcher receives only the
WebSearchProvider trait. Native and subscription-backed model lanes therefore
share the same tool and provenance semantics.
