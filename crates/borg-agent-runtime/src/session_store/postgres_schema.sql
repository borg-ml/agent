-- Borg session journal: PostgreSQL schema.
--
-- DEPLOYMENT: this lives in its own DATABASE (default `borg_sessions`), not in
-- a schema beside the relay tier in `borg`. The relay tables (agent_runs,
-- api_keys, auth_events, admin_audit_log...) are a different lifecycle: a
-- different backup cadence, a different retention policy, and a different
-- blast radius. A separate database gives independent autovacuum tuning and
-- connection limits, and makes a stray `drop schema` or an accidental
-- cross-tier join impossible rather than merely discouraged. The cost is that
-- the journal cannot be joined to the relay tier in SQL -- which costs nothing
-- today, because no such join exists.
--
-- NOT PARTITIONED, deliberately. session_events holds 2,497,802 rows
-- (measured, not extrapolated). Partitioning at this size buys nothing and
-- adds planning overhead and constraint-exclusion complexity. Revisit at
-- ~100M rows. Until then the shapes below are kept partition-ready: the
-- primary key leads with session_id, so `partition by hash (session_id)` can
-- be introduced later without changing any key or query.

-- Who has used this journal, by machine and OS user.
--
-- WHY THIS EXISTS: SQLite made the trust boundary physical -- a journal was a
-- file, owned by one OS user on one machine, and nothing else could reach it.
-- A connection string erases that. Two machines, or two users on one machine,
-- can point at the same database without noticing, and workspace membership
-- and `/broadcast` are currently scoped by exactly the boundary that would
-- silently widen.
--
-- Recording the owners does not decide whether sharing is allowed. It makes
-- sharing VISIBLE, so a second owner appearing is a deliberate answer to a
-- question rather than a discovery made later.
create table if not exists borg_journal_owners (
    fingerprint text        primary key,
    display     text        not null,
    first_seen  timestamptz not null default now(),
    last_seen   timestamptz not null default now()
);

create table if not exists borg_session_schema (
    id      integer primary key generated always as identity check (id = 1),
    version bigint  not null
);

-- Versioned, corpus-trained zstd dictionaries for session_events.event_json.
--
-- WHY THIS EXISTS: measured on 2,000 held-out real events, per-record zstd
-- with a 128KB trained dictionary reaches 4.41x, against a 4.44x whole-stream
-- ceiling and only 2.94x for per-record zstd with no dictionary. Postgres
-- TOAST cannot approach this because it compresses each value independently
-- and therefore cannot see the cross-event key redundancy that dominates this
-- corpus: jsonb+TOAST measured 5,208 kB for the same events where
-- bytea+dictionary measured 2,120 kB.
--
-- Dictionaries are append-only and never deleted: a row references the exact
-- dictionary it was written with, so retraining is always safe and never
-- requires rewriting history.
create table if not exists session_event_dicts (
    dict_id    integer primary key,
    dict_bytes bytea       not null,
    trained_at timestamptz not null default now()
);

create table if not exists sessions (
    id                    uuid        primary key,
    parent_session_id     uuid        references sessions (id),
    parent_cut_sequence   bigint,
    owner_session_id      uuid        references sessions (id),
    inherited_event_count bigint      not null default 0,
    -- Monotonic per-session sequence allocator. Claimed under a row lock via
    -- `update ... returning`, which serialises writers for one session without
    -- a database-wide or file-wide lock. This is the one place SQLite forced a
    -- single writer per FILE; here contention is per SESSION ROW.
    next_sequence         bigint      not null default 1,
    live_revision         bigint      not null default 0,
    state_json            text        not null,
    projection_version    integer     not null default 3,
    created_at            timestamptz not null,
    updated_at            timestamptz not null
);

create index if not exists idx_sessions_activity on sessions (updated_at desc);

create table if not exists session_model_access (
    session_id       uuid not null references sessions (id) on delete cascade,
    provider         text not null,
    account_identity text not null,
    primary key (session_id, provider)
);

create table if not exists session_harness_routes (
    session_id uuid    not null references sessions (id) on delete cascade,
    provider   text    not null,
    native     boolean not null,
    primary key (session_id, provider)
);

create table if not exists session_events (
    session_id       uuid        not null references sessions (id) on delete cascade,
    sequence         bigint      not null,
    event_id         uuid        not null,
    event_kind       text        not null,

    -- TWO-TIER BODY STORAGE. Exactly one of these is set per row.
    --
    -- HOT (event_json): readable jsonb, the default for live history. Agents
    -- must be able to read their own and each other's journals and compose
    -- ad-hoc SQL across the whole database, and there is no SQL-callable
    -- zstd-with-dictionary decompressor in Postgres -- a compressed body is
    -- opaque bytes to anything that is not Borg itself. jsonb keeps `->`,
    -- `->>` and jsonb_path_ops indexing available, and Postgres still applies
    -- TOAST/LZ4 underneath, so this is compressed, just not as far.
    --
    -- COLD (event_body + dict_id): zstd against a trained dictionary, reaching
    -- a measured 4.41x where jsonb+TOAST measured 5,208 kB against 2,120 kB
    -- for the same events. Reserved for aged-out history, where the 4.41x is
    -- worth losing plain-SQL readability; nothing writes this tier yet.
    -- dict_id null with event_body set means uncompressed UTF-8 JSON bytes.
    --
    -- Tiering is a per-row property, so a row can move hot -> cold later
    -- without a schema change and without rewriting any other row.
    event_json       jsonb,
    event_body       bytea,
    dict_id          integer     references session_event_dicts (dict_id),
    constraint session_events_body_present
        check (event_json is not null or event_body is not null),

    -- Replayed, never searched, so plain text under ordinary TOAST rather than
    -- jsonb. Do NOT "optimise" this to jsonb: nothing queries inside it, and
    -- jsonb measured ~5% LARGER on this corpus because its decomposed binary
    -- form expands before compressing.
    --
    -- Written only at checkpoints -- local_sequence 1, then every 256th --
    -- exactly as historical_projection_json does. Non-checkpoint rows store
    -- ''. Reads select the newest non-empty row at or below a sequence and
    -- replay forward.
    projection_json  text        not null,

    fork_inheritable boolean     not null,
    recovery_relevant boolean    not null,
    message_id        uuid,
    created_at        timestamptz not null,

    -- Predicate columns lifted OUT of the event body at write time.
    --
    -- SQLite indexed these with partial indexes over json_extract(event_json,
    -- ...). That is impossible here because event_body is compressed and
    -- therefore opaque to the planner. Materialising the handful of fields the
    -- predicates actually read costs a few bytes per row and keeps every hot
    -- lookup index-only, instead of decompressing rows to evaluate a filter.
    subagent_session_id  uuid,
    provider_event_kind  text,

    primary key (session_id, sequence),
    unique (session_id, event_id)
);

-- events_after(session_id, sequence) is served by the primary key.
-- contains_message(session_id, message_id):
create index if not exists idx_session_events_message
    on session_events (session_id, message_id)
    where message_id is not null;

create index if not exists idx_session_events_message_sequence
    on session_events (session_id, sequence desc)
    where event_kind = 'message';

create index if not exists idx_session_events_fork_inheritable
    on session_events (session_id, sequence)
    where fork_inheritable;

create index if not exists idx_session_events_recovery
    on session_events (session_id, sequence)
    where recovery_relevant;

create index if not exists idx_session_events_subagent_recovery
    on session_events (session_id, subagent_session_id, sequence desc)
    where event_kind = 'subagent_activity';

create index if not exists idx_session_events_context_compaction
    on session_events (session_id, sequence desc)
    where event_kind = 'provider_event' and provider_event_kind = 'context_compaction';

create index if not exists idx_session_events_context_clear
    on session_events (session_id, sequence desc)
    where event_kind = 'context_cleared';

-- Ad-hoc containment queries over readable bodies: the capability agents use
-- to query their own and each other's history directly in SQL. jsonb_path_ops
-- is chosen over the default opclass because it is substantially smaller and
-- this index exists for `@>` containment, not for key-existence probes.
create index if not exists idx_session_events_json
    on session_events using gin (event_json jsonb_path_ops)
    where event_json is not null;

-- Checkpoint lookup: newest non-empty projection at or below a sequence.
create index if not exists idx_session_events_projection
    on session_events (session_id, sequence desc)
    where projection_json <> '';

create table if not exists session_live_state (
    session_id uuid        not null references sessions (id) on delete cascade,
    live_key   text        not null,
    revision   bigint      not null,
    event_json jsonb       not null,
    updated_at timestamptz not null,
    primary key (session_id, live_key)
);

-- live_events_after(session_id, revision)
create index if not exists idx_session_live_revision
    on session_live_state (session_id, revision);

create table if not exists session_payloads (
    id           uuid        primary key,
    session_id   uuid        not null references sessions (id) on delete cascade,
    event_id     uuid        not null,
    payload_kind text        not null,
    payload      bytea       not null,
    byte_len     bigint      not null,
    created_at   timestamptz not null
);

create index if not exists idx_session_payloads_event
    on session_payloads (session_id, event_id);

-- Search is a disposable projection, exactly as in SQLite: session_events is
-- the only source of truth and this table can be dropped and rebuilt. It
-- exists so that history stays queryable even though event_body is compressed
-- and opaque -- the compression win and cross-session search are therefore not
-- in tension. Replaces the FTS5 virtual table and its three sync triggers.
create table if not exists session_event_search (
    session_id uuid   not null references sessions (id) on delete cascade,
    sequence   bigint not null,
    event_id   uuid   not null,
    event_kind text   not null,
    actor      text,
    body       text   not null,
    -- `simple`, NOT `english`, and that is a compatibility requirement rather
    -- than a default left unconsidered. The english configuration stems, so
    -- "migration" and "migrate" collapse to one lexeme and a search for either
    -- finds both. SQLite's FTS5 tokenizer does not stem, so adopting english
    -- here would give the two backends different answers to the same query.
    -- Every query in search.rs pins `simple` for the same reason; changing one
    -- without the others silently breaks the index.
    body_tsv   tsvector generated always as (to_tsvector('simple', body)) stored,
    primary key (session_id, event_id)
);

create index if not exists idx_session_event_search_sequence
    on session_event_search (session_id, sequence);

-- Cross-session history search: the capability this migration actually buys.
create index if not exists idx_session_event_search_tsv
    on session_event_search using gin (body_tsv);

-- Substring/trigram recall for identifiers that do not tokenise as words.
create index if not exists idx_session_event_search_trgm
    on session_event_search using gin (body gin_trgm_ops);

create table if not exists session_actions (
    action_id         uuid        primary key,
    session_id        uuid        not null references sessions (id) on delete cascade,
    action_kind       text        not null,
    state             text        not null,
    delivery_policy   text        not null,
    wake_policy       text        not null,
    payload_json      jsonb       not null,
    attempt           bigint      not null default 0,
    error             text,
    created_at        timestamptz not null,
    updated_at        timestamptz not null,
    accepted_at       timestamptz,
    delivered_at      timestamptz,
    completed_at      timestamptz,
    lease_owner       text,
    lease_token       uuid,
    lease_heartbeat_at timestamptz,
    lease_expires_at  timestamptz
);

create index if not exists idx_session_actions_pending
    on session_actions (session_id, state, created_at);

-- Lease recovery sweeps look up expired non-terminal work by deadline.
create index if not exists idx_session_actions_lease_expiry
    on session_actions (lease_expires_at)
    where lease_expires_at is not null;

create table if not exists session_action_transitions (
    action_id     uuid        not null references session_actions (action_id) on delete cascade,
    session_id    uuid        not null references sessions (id) on delete cascade,
    transition_no bigint      not null,
    from_state    text,
    to_state      text        not null,
    error         text,
    created_at    timestamptz not null,
    primary key (action_id, transition_no)
);

create index if not exists idx_session_action_transitions_session
    on session_action_transitions (session_id, created_at, action_id);

create table if not exists session_workspace_bindings (
    session_id     uuid        primary key references sessions (id) on delete cascade,
    workspace_id   uuid        not null,
    participant_id uuid        not null,
    host_id        uuid,
    attached_at    timestamptz not null
);

create index if not exists idx_session_workspace_bindings_workspace
    on session_workspace_bindings (workspace_id, session_id);

create table if not exists host_launches (
    session_id    uuid        primary key,
    metadata_json jsonb       not null,
    created_at    timestamptz not null,
    updated_at    timestamptz not null
);

create table if not exists host_launch_owners (
    session_id   uuid primary key references host_launches (session_id) on delete cascade,
    host_id      uuid not null,
    relay_origin text not null
);

create table if not exists host_journal_cursors (
    session_id    uuid   primary key references host_launches (session_id) on delete cascade,
    event_cursor  bigint not null default 0,
    live_revision bigint not null default 0
);

create table if not exists host_workspace_cursors (
    host_id      uuid   not null,
    session_id   uuid   not null references sessions (id) on delete cascade,
    workspace_id uuid   not null,
    sequence     bigint not null default 0,
    primary key (host_id, session_id, workspace_id)
);

create table if not exists host_bootstraps (
    session_id uuid primary key references host_launches (session_id) on delete cascade
);

create table if not exists runtime_manifests (
    session_id       uuid        primary key references sessions (id) on delete cascade,
    manifest_version bigint      not null,
    runtime          text        not null,
    root             text        not null,
    command          text        not null,
    worker_id        text        not null,
    status           text        not null,
    execution_count  bigint      not null default 0,
    last_code_hash   text,
    last_error       text,
    created_at       timestamptz not null,
    updated_at       timestamptz not null
);

-- `state_json` is text, NOT jsonb, and that is deliberate.
--
-- Every checkpoint carries `content_hash = sha256(state_json)`, verified on
-- read, so a corrupted or truncated row is refused rather than replayed. That
-- check is over the exact stored BYTES. jsonb does not store bytes: it parses
-- to a normalised tree, reordering keys, dropping duplicates, discarding
-- whitespace and renormalising numbers. Round-tripping through jsonb would
-- therefore return text that differs from what was hashed, and the integrity
-- check would fail on rows that are perfectly intact -- or, worse, would have
-- to be weakened to a re-serialise-then-compare that no longer detects the
-- corruption it exists to detect.
--
-- Keeping it text also makes the hash identical across both backends, which is
-- what lets the conformance suite compare them directly. These rows are opaque
-- runtime state fetched whole by key; nothing queries inside them, so jsonb
-- would buy no indexing benefit to trade against this.
create table if not exists runtime_checkpoints (
    session_id     uuid        not null references sessions (id) on delete cascade,
    checkpoint_key text        not null,
    state_json     text        not null,
    content_hash   text        not null,
    revision       bigint      not null,
    created_at     timestamptz not null,
    primary key (session_id, checkpoint_key),
    unique (session_id, revision)
);

create index if not exists idx_runtime_checkpoints_revision
    on runtime_checkpoints (session_id, revision desc);


-- Readable history for humans and agents composing SQL by hand.
--
-- Hot rows expose their body directly. Cold rows surface as null bodies rather
-- than being silently omitted, so a query cannot quietly under-report history:
-- a null body means "compressed, ask Borg to decode it", not "no such event".
create or replace view session_events_readable as
select
    e.session_id,
    e.sequence,
    e.event_id,
    e.event_kind,
    e.event_json                       as body,
    e.event_json is null               as body_compressed,
    e.message_id,
    e.subagent_session_id,
    e.provider_event_kind,
    e.created_at
from session_events e;
