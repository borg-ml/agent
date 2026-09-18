-- Borg satellite tiers: PostgreSQL schema.
--
-- WHY THIS EXISTS: the journal schema covers `sessions` and its immediate
-- children, but four more tiers live in the same SQLite FILE -- workspaces,
-- autonomy, plugin state and receipts. SQLite serialises writers per file, so
-- leaving any of them behind would keep every Borg process queueing on one
-- write lock even after the journal itself became contention-free. Moving them
-- is what makes the migration's central claim true in production rather than
-- only for `session_events`.
--
-- COLUMN TYPES ARE DELIBERATELY FAITHFUL to the SQLite originals: ids stay
-- `text`, timestamps stay RFC3339 `text` or epoch-millisecond `bigint`, and
-- JSON stays `text`. These tiers are driven by code that binds exactly those
-- Rust types, so keeping them identical makes the port a placeholder swap
-- rather than a type rewrite, and removes a whole class of silent coercion
-- bugs. The journal tier uses native `uuid`/`timestamptz`/`jsonb` because it
-- was designed for them; these tiers can follow later, on purpose, with tests.

create table if not exists borg_workspace_schema (
    id      integer primary key generated always as identity check (id = 1),
    version bigint  not null
);

create table if not exists workspaces (
    id            text   primary key,
    name          text   not null,
    next_sequence bigint not null default 1,
    created_at    text   not null
);

create table if not exists workspace_participants (
    id           text primary key,
    display_name text not null,
    kind         text not null,
    created_at   text not null
);

create table if not exists workspace_members (
    workspace_id   text not null references workspaces (id) on delete cascade,
    participant_id text not null references workspace_participants (id),
    role           text not null,
    joined_at      text not null,
    primary key (workspace_id, participant_id)
);

create table if not exists workspace_events (
    workspace_id    text   not null references workspaces (id) on delete cascade,
    sequence        bigint not null,
    id              text   not null,
    author_id       text   not null references workspace_participants (id),
    idempotency_key text   not null,
    canonical_json  text   not null,
    event_json      text   not null,
    created_at      text   not null,
    -- Lifted out of the body so the message index is a plain partial index
    -- rather than an expression the planner must re-evaluate per row. The
    -- expression is immutable, so Postgres can store it.
    is_message      boolean generated always as
        ((event_json::jsonb #>> '{kind,type}') = 'message') stored,
    primary key (workspace_id, sequence),
    unique (workspace_id, author_id, idempotency_key)
);

create index if not exists idx_workspace_events_id
    on workspace_events (workspace_id, id);

create index if not exists idx_workspace_events_messages
    on workspace_events (workspace_id, sequence)
    where is_message;

create table if not exists workspace_deliveries (
    workspace_id      text    not null,
    sequence          bigint  not null,
    recipient_id      text    not null references workspace_participants (id),
    mode              text    not null,
    state             text    not null,
    attempts          bigint  not null default 0,
    last_attempt_json text,
    is_message        boolean not null default false,
    primary key (workspace_id, sequence, recipient_id),
    foreign key (workspace_id, sequence)
        references workspace_events (workspace_id, sequence) on delete cascade
);

create index if not exists idx_workspace_delivery_recipient
    on workspace_deliveries (workspace_id, recipient_id, sequence);

-- The pending-message sweep is the hot read here, so it gets its own partial
-- index. `state` is a JSON-encoded string in this tier, hence the quotes.
create index if not exists idx_workspace_pending_message_deliveries
    on workspace_deliveries (workspace_id, recipient_id, sequence)
    where is_message and state = '"pending"';

create table if not exists workspace_threads (
    id           text primary key,
    workspace_id text not null references workspaces (id) on delete cascade,
    title        text not null,
    created_at   text not null
);

create table if not exists workspace_work_items (
    workspace_id     text   not null references workspaces (id) on delete cascade,
    work_id          text   not null,
    created_sequence bigint not null,
    primary key (workspace_id, work_id)
);

create table if not exists workspace_work_claims (
    workspace_id text   not null references workspaces (id) on delete cascade,
    work_id      text   not null,
    claim_id     text   not null,
    claimant_id  text   not null references workspace_participants (id),
    sequence     bigint not null,
    primary key (workspace_id, work_id)
);

create table if not exists workspace_work_dependencies (
    workspace_id       text   not null references workspaces (id) on delete cascade,
    work_id            text   not null,
    depends_on_work_id text   not null,
    sequence           bigint not null,
    primary key (workspace_id, work_id, depends_on_work_id)
);

create table if not exists workspace_presence_leases (
    workspace_id   text not null references workspaces (id) on delete cascade,
    participant_id text not null references workspace_participants (id),
    client_id      text not null,
    host_id        text,
    expires_at     text not null,
    primary key (workspace_id, participant_id, client_id)
);

create table if not exists agent_instances (
    participant_id text primary key references workspace_participants (id),
    host_id        text,
    workspace_id   text,
    seen_at        text,
    -- Local launch identity. display_name is only the workspace basename, so
    -- sessions in different checkouts are otherwise indistinguishable.
    cwd            text,
    pid            bigint,
    -- Tombstone set by reaping when the local owner is gone; null means the
    -- row is still advertised by discovery.
    exited_at      text
);

create table if not exists borg_autonomy_schema (
    id      integer primary key generated always as identity check (id = 1),
    version bigint  not null
);

create table if not exists autonomy_jobs (
    job_id                text   primary key,
    idempotency_key       text   not null unique,
    kind                  text   not null,
    payload_json          text   not null,
    state                 text   not null,
    due_at_ms             bigint not null,
    attempt               bigint not null default 0,
    max_attempts          bigint not null,
    lease_owner           text,
    lease_token           text,
    lease_heartbeat_at_ms bigint,
    lease_expires_at_ms   bigint,
    session_id            text,
    goal_id               text,
    result_json           text,
    last_error            text,
    created_at_ms         bigint not null,
    updated_at_ms         bigint not null,
    check (state in ('queued', 'claimed', 'running', 'completed', 'failed', 'cancelled')),
    check (attempt >= 0 and max_attempts > 0 and attempt <= max_attempts)
);

create index if not exists idx_autonomy_jobs_due
    on autonomy_jobs (state, due_at_ms, created_at_ms, job_id);

create index if not exists idx_autonomy_jobs_lease_expiry
    on autonomy_jobs (state, lease_expires_at_ms, updated_at_ms, job_id);

create table if not exists autonomy_job_transitions (
    job_id         text   not null references autonomy_jobs (job_id) on delete cascade,
    sequence       bigint not null,
    from_state     text,
    to_state       text   not null,
    attempt        bigint not null,
    reason         text,
    lease_owner    text,
    occurred_at_ms bigint not null,
    primary key (job_id, sequence),
    check (to_state in ('queued', 'claimed', 'running', 'completed', 'failed', 'cancelled')),
    check (from_state is null or from_state in
        ('queued', 'claimed', 'running', 'completed', 'failed', 'cancelled'))
);

create table if not exists autonomy_checkpoints (
    checkpoint_id  text   primary key,
    job_id         text   not null references autonomy_jobs (job_id) on delete cascade,
    checkpoint_key text   not null,
    session_id     text,
    goal_id        text,
    kind           text   not null,
    state_json     text   not null,
    evidence_json  text   not null,
    content_hash   text   not null,
    created_at_ms  bigint not null,
    unique (job_id, checkpoint_key)
);

create index if not exists idx_autonomy_checkpoints_job
    on autonomy_checkpoints (job_id, created_at_ms, checkpoint_id);

create table if not exists plugin_state (
    extension_id     text    not null,
    scope            text    not null,
    scope_id         text    not null,
    key              text    not null,
    value_json       text,
    deleted          boolean not null default false,
    content_hash     text    not null,
    revision         bigint  not null,
    provenance_json  text    not null,
    created_at       text    not null,
    updated_at       text    not null,
    primary key (extension_id, scope, scope_id, key)
);

create index if not exists idx_plugin_state_scope
    on plugin_state (extension_id, scope, scope_id, deleted, key);

create table if not exists plugin_artifacts (
    extension_id    text   not null,
    scope           text   not null,
    scope_id        text   not null,
    artifact_id     text   not null,
    path            text   not null,
    name            text,
    run_id          text,
    media_type      text,
    byte_len        bigint not null,
    content_hash    text   not null,
    metadata_json   text   not null,
    provenance_json text   not null,
    created_at      text   not null,
    primary key (extension_id, scope, scope_id, artifact_id)
);

create index if not exists idx_plugin_artifacts_run
    on plugin_artifacts (extension_id, scope, scope_id, run_id, created_at);

create table if not exists plugin_mutation_receipts (
    extension_id    text not null,
    scope           text not null,
    scope_id        text not null,
    idempotency_key text not null,
    request_hash    text not null,
    result_json     text not null,
    created_at      text not null,
    primary key (extension_id, scope, scope_id, idempotency_key)
);

create table if not exists receipt_records (
    request_id    text primary key,
    version       bigint not null,
    state         text   not null,
    request_json  text   not null,
    response_json text,
    created_at    text   not null,
    updated_at    text   not null
);

create table if not exists receipt_transitions (
    request_id    text   not null references receipt_records (request_id),
    sequence      bigint not null,
    version       bigint not null,
    state         text   not null,
    request_json  text   not null,
    response_json text,
    created_at    text   not null,
    primary key (request_id, sequence)
);

-- The relay's durable command queue. `sequence` is an identity column rather
-- than SQLite's autoincrement rowid, which is the same contract: monotonic,
-- never reused.
create table if not exists host_operation_queue (
    sequence          bigint primary key generated always as identity,
    request_id        text not null unique,
    host_id           text not null,
    command_json      text not null,
    quarantine_reason text
);

create index if not exists idx_host_operation_queue_host_live
    on host_operation_queue (host_id, sequence)
    where quarantine_reason is null;
