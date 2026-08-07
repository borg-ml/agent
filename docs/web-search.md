# Borg web search

Borg exposes one provider-neutral web_search tool when a search credential is
configured. The tool returns bounded normalized results:

- backend: the concrete backend that answered (exa, parallel, or brave);
- query and an optional backend request id;
- result title, source URL, bounded snippet, and optional publication date;
- optional warnings.

The URL is the provenance boundary. Search snippets are discovery context, not
authoritative page contents, and Borg does not silently turn this tool into an
unbounded page-fetching runtime.

## Configuration

Set one or more credentials in the environment:

    EXA_API_KEY
    PARALLEL_API_KEY
    BRAVE_SEARCH_API_KEY

The aliases BORG_EXA_API_KEY, BORG_PARALLEL_API_KEY, and
BORG_BRAVE_SEARCH_API_KEY are also accepted. Select a backend with
BORG_SEARCH_BACKEND=auto|exa|parallel|brave.

auto prefers Exa, then Parallel, then Brave. Exa is the recommended default
because it is a strong general-purpose agent search backend; Parallel is a
good alternative with a useful free tier; Brave is a well-established
independent-index option. If no usable credential is present, Borg omits the
tool instead of advertising a capability that cannot run. Explicit backend
selection does not silently switch to another provider.

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
- [Parallel Search API](https://docs.parallel.ai/api-reference/search/search)
  uses POST /v1/search, the x-api-key header, an objective plus search query,
  and bounded total excerpt characters.
- [Brave Web Search API](https://api-dashboard.search.brave.com/api-reference/web/search/get)
  uses GET /res/v1/web/search, the X-Subscription-Token header, and a bounded
  result count.

These adapters live in borg-search; the agent dispatcher receives only the
WebSearchProvider trait. Native and subscription-backed model lanes therefore
share the same tool and provenance semantics.
