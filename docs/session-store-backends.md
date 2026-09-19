# Session store backends

Status: implementation contract

Borg's durable journal runs on either SQLite or PostgreSQL. **PostgreSQL is the
default**, and Borg provisions it for you. SQLite is still supported and is
selected by name.

The default follows from what Borg is for. Several agents commonly run at once
on one machine, and SQLite permits one writer per FILE while every Borg process
here shares one journal file -- so a SQLite default puts every agent behind a
single machine-wide lock whose throughput does not improve as writers are added.

## Which one do you want?

**PostgreSQL unless you are certain you run one agent at a time.** It is the
default and needs no setup: with no configuration Borg initialises a cluster
under its own home directory and starts it. SQLite's throughput plateaus at
about 780 appends/second no matter how many writers are added -- that plateau is
the single-file write lock. Postgres scales roughly linearly with writers and
overtakes SQLite at around 32 concurrent writers. It also brings
dictionary-compressed cold storage and cross-session search.

**SQLite when a single agent is the whole story.** It needs no service and is
roughly 60x faster per append at low concurrency, because there is no network
round trip. That advantage is real and it is why SQLite remains supported; it
simply stops mattering the moment a second writer appears.

Switching does not move history -- see [Migrating](#migrating-an-existing-journal).
See [Concurrency](#concurrency) and [Storage](#storage) for the measurements
behind both claims.

## Choosing a backend

```sh
# PostgreSQL (default): no configuration. Borg provisions and starts a cluster
# in its own home directory the first time it runs.
borg

# An existing PostgreSQL server instead of the managed one.
export BORG_SESSIONS_URL="postgres://borg@localhost:5432/borg"
borg

# SQLite, for a single agent at a time.
export BORG_SESSIONS_BACKEND=sqlite
borg
```

`BORG_SESSIONS_URL` outranks `BORG_SESSIONS_BACKEND`, because naming a specific
server is the more specific instruction.

## The managed cluster

With no configuration, Borg owns a cluster of its own:

| | |
|---|---|
| Data directory | `$BORG_HOME/pgdata` (default `~/.borg/pgdata`) |
| Port | `5433`, or `BORG_SESSIONS_PORT` |
| Role / database | `borg` / `borg_sessions` |
| Log | `$BORG_HOME/logs/postgres.log` |

Port 5433 rather than 5432 is deliberate: a developer machine frequently
already runs a system PostgreSQL on 5432, and adopting someone else's cluster is
not Borg's decision to make.

Borg does **not** install PostgreSQL. It locates the `initdb` and `pg_ctl` that
an installation already provides -- including the version-suffixed directories
distributions keep off `PATH`, such as `/usr/lib/postgresql/17/bin`. When they
are absent it fails with the install command for the platform and with both
escape hatches, rather than silently dropping to SQLite. A silent drop would
split one machine's history across two databases, and the split stays invisible
until someone goes looking for a session that was quietly written elsewhere.

Several Borg processes routinely start at the same moment and all of them run
this. No lock is taken; instead each step treats losing the race as having had
nothing to do. `initdb` refuses a populated directory and `pg_ctl start` refuses
a running cluster, and both outcomes are re-checked against live state before
being treated as failures.

A crash leaves `postmaster.pid` behind and the next start refuses, assuming the
old server is alive. Borg checks whether that process actually exists and clears
the file if it does not, because the machine this runs on does crash.

## Migrating an existing journal

Changing the backend does not move history. A journal written to SQLite stays
there, visible again the moment `BORG_SESSIONS_BACKEND=sqlite` is set. To bring
it across:

```sh
borg session migrate --to "postgres://borg@127.0.0.1:5433/borg_sessions"
```

Migration replays events through the destination's own `append` rather than
copying rows, so the destination derives its own projections and search index.
The source is only ever read, so this is safe to run against a journal still in
use and safe to abandon partway.

`borg doctor` reports which backend a process resolved to:

```
Session backend: postgres
Durable session store: ready
```

The variable is read by every entry point — the agent session, the relay host,
`borg session`, `borg workspaces`, `borg doctor`, ACP, collab, the importer and
the GUI. There is no per-command override, because two commands disagreeing
about where history lives is worse than either choice.

## What the URL does and does not do

**A configured URL always wins.** If `BORG_SESSIONS_URL` is set and the server
is unreachable, Borg exits non-zero with the connection error. It does not fall
back to SQLite. Falling back would split one machine's history across two
databases, and the split would be silent until someone went looking for a
session that had quietly been written somewhere else.

```
$ BORG_SESSIONS_URL="postgres://borg@localhost:5432/missing" borg -p hi
Error: BORG_SESSIONS_URL is set, so Borg requires PostgreSQL

Caused by:
    0: failed to connect to Postgres session store: postgres://borg@localhost:5432/missing
    1: error returned from database: database "missing" does not exist
    2: database "missing" does not exist
$ echo $?
1
```

**Schema is applied automatically.** Both backends create their own tables on
first connection. Postgres does it under an advisory lock, so many processes
starting at once cannot race; every statement is `create ... if not exists`, so
bootstrap is safe to run on every open. You need a database and a role that can
create tables in it. You do not need to run migrations by hand.

**Every tier is resolved before the process commits to running.** The journal
has satellite tiers — workspaces, autonomy jobs, plugin state, receipts — and a
backend that served some but not others would not fail at the point of use. It
would hang: the relay's message-recovery loop treats a missing workspace tier as
a transient fault and retries every two seconds, forever, logging a warning each
time. So the store factory resolves all of them up front and refuses to start
with an error naming the backend and the missing tier.

Resolution is deliberately split from opening. Constructing a tier creates its
schema, which on SQLite takes the machine-wide writer lock, so a process that is
about to discover it lost an ownership race and exit must not pay for tiers it
will never use. `open()` returns the journal; `resolve()` proves the tiers.
Long-running paths call both.

## Rolling back to SQLite

Unset the variable:

```sh
unset BORG_SESSIONS_URL
borg doctor    # Session backend: sqlite
```

Nothing else is required. The SQLite journal at
`~/.borg/sessions/sessions.sqlite3` is untouched by Postgres operation, so it is
exactly as it was when you switched.

**Switching backends does not move history by itself.** Sessions written to
Postgres are not visible from SQLite and vice versa, so rolling back returns you
to the history you had before the switch, not a merge of both. To carry history
across, copy it explicitly — see below.

## Migrating history

```sh
# See what would be copied. Reads only; writes nothing.
borg session migrate --to "postgres://borg@localhost:5432/borg" --dry-run

# Copy it.
borg session migrate --to "postgres://borg@localhost:5432/borg"
```

The source is whatever this machine is configured for, and is **only ever
read**. A migration is therefore safe to run against a journal that is still in
use, and safe to abandon part way.

**Re-running resumes.** A session already complete in the destination is
skipped; one that is present but short — because a previous run was interrupted
mid-session — is carried the rest of the way. Being present is not treated as
being finished, so an interrupted run cannot leave a permanently truncated
session behind.

Rows are copied verbatim rather than replayed — see
[How it copies](#how-it-copies-and-why-not-by-replaying), which matters more
than it sounds. Oversized bodies held in `session_payloads` come across keeping
their ids, since the events reference them.

Useful flags: `--limit N` to stop after N sessions, `--fail-fast` to abandon on
the first error instead of continuing past it, `--json` for a machine-readable
report.

### Rate

Measured against a 56 GB journal of 2,523,336 events across 1,144 sessions: a
dry run takes about 20 seconds, and copying runs at roughly 1,050
events/second, so a full migration of that journal takes about 40 minutes.

Rows are copied in batches of 512 within a single transaction. Doing it one
event at a time — each its own transaction and its own fsync — measured 46
events/second, which would have made the same migration take about fifteen
hours.

### Check disk space first

A migration does not move history, it COPIES it, so the destination needs room
for a second full journal alongside the first. Postgres holds roughly the same
bytes per event as SQLite while hot (see [Storage](#storage)), so budget at
least the size of the source journal, plus headroom for the write-ahead log
during the copy. The cold tier only reclaims space later, once aged sessions
are compressed.

This is worth checking rather than assuming: the incident that motivated this
migration began with a full disk, and a migration is one of the few operations
that can double a journal's footprint in under an hour.

### What is not carried

The journal tier is copied: sessions, events, payloads, fork lineage and
subagent ownership. The satellite tiers are NOT migrated — workspaces, autonomy
jobs, plugin state and receipts stay behind. Those are operational state rather
than history, and the destination creates them empty.

### How it copies, and why not by replaying

Events are copied VERBATIM: the stored row, at the same sequence, with the same
flags, projection checkpoints and event ids. Fork lineage and subagent ownership
are carried as the source recorded them.

The obvious alternative — replaying events through the destination's append path
so it derives its own projections — was built first and does not work on real
history, for two reasons worth stating because both are easy to rediscover:

* **An event's persistence class is computed by the CURRENT code.** A journal
  written by older code holds rows today's rules would not journal at all.
  `subagent_activity` is classified by its NESTED child event and is 46% of
  events in the journal this was developed against, so replay silently dropped
  a large fraction of history and broke sequence contiguity behind it.
* **Lineage is recomputed, and recomputing it can disagree with the record.**
  `fork_before` derives `inherited_event_count` by counting the parent's
  inheritable events. One real fork records 15,245 where recounting gives
  15,169 — and its own events start at 15,246, so a recomputed value opens a
  76-sequence hole in the composed history.

Verbatim copying sidesteps both: the destination receives what the source
recorded rather than what today's rules would produce from it. The search
projection is the one thing rebuilt rather than copied, because Postgres builds
it lazily on first query anyway.

## Storage

The honest summary is that Postgres is slightly larger than SQLite while a
thread is active, and smaller once it goes quiet.

Measured by `storage_footprint_profile`, over 1,800 events across 12 sessions,
both written through the public `append` path and both vacuumed before
measurement:

| Backend | Bytes | Per event | vs SQLite |
| --- | --- | --- | --- |
| SQLite | 1,826,816 | 1,015 B | 1.00x |
| Postgres, hot | 1,957,888 | 1,088 B | 1.07x |
| Postgres, cold | 1,064,960 | 592 B | 0.58x |

A row store with per-row headers, a uuid primary key and several indexes starts
out larger for the same events; that is the 1.07x. The cold tier pays it back.
Seven days after a thread's last activity, its event bodies are compressed with
a trained zstd dictionary and the footprint drops to 0.58x the SQLite baseline.

Body compression alone was 5.99x in that run. On real captured events it is
4.77x — the synthetic fixture is more repetitive than production traffic, so
treat 4.77x as the number to expect and the table above as the shape.

Reheating is automatic: opening a cold session decompresses it back to the hot
tier, so an agent never reads through compressed data.

One exception to the hot tier being readable jsonb: **an event body containing a
NUL is stored as raw bytes instead.** Postgres `jsonb` parses to a tree whose
strings are `text`, and `text` cannot hold a NUL, so such an insert is rejected
outright. SQLite's JSON is plain text and accepts it. 246 events out of 2.5
million in the journal this was measured against carry one, almost all of them
tool output that captured a binary byte. Dropping the byte would corrupt the
evidence, so those rows fall back to the byte tier and are opaque to ad-hoc SQL
— the same trade as any cold row. They read back byte for byte through Borg.

To reproduce:

```sh
BORG_TEST_SESSIONS_URL="postgres://borg@localhost:5432/postgres" \
  cargo test -p borg-agent-runtime --lib storage_footprint_profile \
  -- --ignored --nocapture
```

## Concurrency

This is the reason the port exists, and the numbers need stating carefully,
because **Postgres is not simply faster**.

SQLite serialises writers per FILE. Postgres serialises them per SESSION ROW,
via `select ... for update` on the session's own row in the sequence allocator,
so two agents appending to two different sessions never block each other.

Measured by `concurrent_writer_scaling_profile`, each writer appending to its
own session (appends/second, median of three runs on one machine):

| Writers | SQLite | Postgres |
| --- | --- | --- |
| 1 | ~2,900 | ~47 |
| 4 | ~780 | ~107 |
| 8 | ~780 | ~207 |
| 16 | ~780 | ~420 |
| 32 | ~780 | ~830 |

Read the shape, not any single cell:

* **SQLite is far faster with one writer** — roughly 60x — because an append is
  a local file write with no network round trip. Postgres pays per-statement
  latency that SQLite does not.
* **SQLite has a ceiling.** Adding writers does not raise throughput; it drops
  to about 780/s at four writers and stays there through 32. That plateau IS the
  single-file write lock.
* **Postgres scales roughly linearly** — 18x from 1 to 32 writers — and crosses
  SQLite at around 32 concurrent writers on this hardware.

So the trade is latency for scalability. If a machine runs a handful of agents,
SQLite is the faster choice and remains the default. The port matters when many
agents share one machine, where SQLite's ceiling is a hard wall and Postgres
keeps climbing.

The crossover point is hardware-dependent: it moves down with slower disks or
higher SQLite contention, and up with higher network latency to the database.
Measure on the target machine rather than inheriting the number above.

None of this reproduces the production pathology that motivated the work —
171,861 "database is locked" waits in a single log window against a 56 GB
journal. That regime is dominated by lock waits, not by SQLite's local-file
advantage, and a fresh benchmark database cannot recreate it.

To reproduce:

```sh
BORG_TEST_SESSIONS_URL="postgres://borg@localhost:5432/postgres" \
  cargo test -p borg-agent-runtime --lib concurrent_writer_scaling_profile \
  -- --ignored --nocapture
```

## Search

Cross-session search works on both backends, but they are different engines:
SQLite uses FTS5, Postgres uses `tsvector` with `ts_rank_cd` ranking and
`ts_headline` snippets. Relevance scores and snippet boundaries therefore
differ, and the conformance suite deliberately does not assert them equal.

Search comes in two shapes. `query_history` searches ONE session and supports
both lexical and regex modes. `search_all_sessions` searches every session at
once — the question "have I seen this error before?" is about a whole history,
not one thread — and is lexical only, because a global regex would have to scan
every body in the journal, which is a far more expensive promise. Both backends
implement both.

What is guaranteed to agree is **which events a query selects** — a caller must
not get different evidence from the two backends for the same question. That
covers actor, kind and sequence filters, limits, `newest_first` ordering, regex
mode including case sensitivity, and `event_id` resolution.

**Neither engine stems.** The Postgres schema pins the `simple` text search
configuration rather than `english`, because `english` maps "migration" and
"migrate" to one lexeme while FTS5 does not. Switching it would look like a
search improvement and would silently make the two backends answer the same
query differently. `neither_backend_stems_word_variants` in the conformance
suite is what holds that line; if you change the configuration, that test is the
one that will tell you what you broke.

## Testing against both backends

The store suites are written against the traits and run twice — once per
backend. The Postgres half is skipped when `BORG_TEST_SESSIONS_URL` is unset, so
the suite still runs on a machine with no database:

```sh
BORG_TEST_SESSIONS_URL="postgres://borg@localhost:5432/postgres" \
  cargo test -p borg-agent-runtime --lib -- --test-threads=4
```

Each suite carries a guard test asserting that Postgres coverage follows the
environment variable, so a misconfigured URL cannot turn the whole suite green
while only exercising SQLite. Every test creates and drops its own scratch
database, so the server named by the URL needs `CREATEDB`.

One further guard is not a conformance test at all. `backend_symmetry.rs`
reads the crate's own source and fails if one backend overrides a trait DEFAULT
that the other silently inherits. That asymmetry shipped a real bug here:
`admit_prompt`'s default bails, SQLite overrode it and Postgres did not, so
every interactive and relayed prompt would have failed on Postgres while the
one-shot path kept the suite green. A conformance test only covers a method
someone thought to test; the store factory only proves a TIER is present; and
the compiler cannot help, because a default is a legal implementation.
Intentional asymmetries are allowed by name, each with a written reason.

Coverage at the time of writing: 24 session-store tests (journal, actions,
forks, runtime manifests and checkpoints, harness state, workflow admission and
lease fencing, the cross-tier relay sweep, search parity, and gapless allocation
under 8 concurrent writers), plus 13 workspace, 7 plugin, 7 autonomy and 5
receipt tests. Two further tests are `#[ignore]`d benchmarks rather than gates:
`storage_footprint_profile` and `concurrent_writer_scaling_profile`.

## Sharing a journal between machines or users

`BORG_SESSIONS_URL` can name a server on another machine, and nothing stops two
machines — or two OS users on one machine — pointing at the same database. Doing
so **changes a trust boundary**: workspace membership and `/broadcast` are scoped
per machine and OS user, so every owner of a shared journal can see and address
every other.

SQLite could not be shared by accident, because a journal was a file with an
owner. A connection string can be, and the sharing would stay invisible until
someone noticed a stranger in their workspace. So Borg records who has used a
journal — machine and OS user — and refuses to start when a second owner appears
without being asked for:

```
$ BORG_SESSIONS_URL=... borg doctor
Error: this journal has already been used by alice on laptop, and this process
is shulgin on archlinux. Sharing one journal means sharing a trust boundary:
workspace membership and /broadcast are scoped per machine and OS user, so every
owner of this database can see and address every other. If that is what you
intend, set BORG_SESSIONS_SHARED=1. If it is not, point shulgin on archlinux at
a database of its own.
```

This is deliberately not a decision about whether sharing is a good idea. It is
allowed — set `BORG_SESSIONS_SHARED=1` and it proceeds. What is refused is
arriving at a shared trust boundary by accident.

If you do share one, understand what follows: a workspace is no longer scoped to
a machine, `/broadcast` reaches every participant in the database, and instance
discovery lists agents you do not control. Those may be exactly what you want
for a team journal. They are rarely what someone wants who merely copied a
connection string between two hosts.
