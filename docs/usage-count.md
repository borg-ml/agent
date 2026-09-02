# Privacy-minimal usage count

Borg measures active installations, not identifiable people. Release download
totals remain the all-time installation proxy; the runtime heartbeat supports
daily and monthly active-install estimates.

## Client contract

A release build sends at most one `POST` request per 24 hours to
`https://borg.ml/api/v1/active-install`. The JSON body has exactly one field:

```json
{"anonymous_period_id":"32 lowercase hexadecimal characters"}
```

The ID is derived from a locally generated random seed and the current 31-day
period. It is stable inside that period and changes irreversibly at the next
boundary. Borg does not send its version, operating system, architecture,
locale, model, provider, session ID, prompts, paths, tool activity, IP-derived
location, or hardware identifiers. Failed requests are ignored and never
affect agent startup or use.

Development builds do not send the heartbeat. Users can disable it with:

```toml
[usage_count]
enabled = false
```

The environment variable `BORG_DISABLE_USAGE_COUNT=1` is an immediate override.

## Receiver contract

The endpoint may count distinct period IDs by day and by 31-day period. It must
not retain source IP addresses, request headers, or raw access logs; enrich or
join IDs with other data; fingerprint clients; or retain individual IDs after
the period's aggregate has been finalized. Operational rate limiting must use
short-lived in-memory data rather than a durable identity.

This design intentionally cannot calculate lifetime unique users, retention
cohorts across periods, geography, feature use, or per-version adoption. Those
limits are part of the privacy boundary rather than missing telemetry.
