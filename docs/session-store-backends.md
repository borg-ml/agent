# Session store backends

Status: implementation contract

Borg's durable journal runs on PostgreSQL, and Borg provisions it for you. With
no configuration Borg initialises a cluster under its own home directory and
starts it; `BORG_SESSIONS_URL` points it at an existing server instead.

PostgreSQL is the only backend because of what Borg is for. Several agents
commonly run at once on one machine, and the journal serialises writers per
SESSION ROW rather than per file, so two agents appending to two different
sessions never block each other. It also brings dictionary-compressed cold
storage and cross-session search.

## Choosing where the journal lives

```sh
# Default: no configuration. Borg provisions and starts a cluster in its own
# home directory the first time it runs.
borg

# An existing PostgreSQL server instead of the managed one.
export BORG_SESSIONS_URL="postgres://borg@localhost:5432/borg"
borg
```

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
escape hatches. Borg has no second place to put history, and inventing one
would split a machine's journal in a way that stays invisible until someone
goes looking for a session that was quietly written elsewhere.

Several Borg processes routinely start at the same moment and all of them run
this. No lock is taken; instead each step treats losing the race as having had
nothing to do. `initdb` refuses a populated directory and `pg_ctl start` refuses
a running cluster, and both outcomes are re-checked against live state before
being treated as failures.

A crash leaves `postmaster.pid` behind and the next start refuses, assuming the
old server is alive. Borg checks whether that process actually exists and clears
the file if it does not, because the machine this runs on does crash.

## Where the setting is read

`borg doctor` reports the store's health:

```
Durable session store: ready
```

`BORG_SESSIONS_URL` is read by every entry point -- the agent session, the relay
host, `borg session`, `borg workspaces`, `borg doctor`, ACP, collab, the
importer and the GUI. There is no per-command override, because two commands
disagreeing about where history lives is worse than either choice.

## What the URL does and does not do

**A configured URL always wins.** If `BORG_SESSIONS_URL` is set and the server
is unreachable, Borg exits non-zero with the connection error rather than
quietly writing somewhere else. A silent fallback would split one machine's
history across two databases, and the split would stay invisible until someone
went looking for a session that had been written elsewhere.

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

**Schema is applied automatically.** The store creates its own tables on first
connection, under an advisory lock, so many processes
starting at once cannot race; every statement is `create ... if not exists`, so
bootstrap is safe to run on every open. You need a database and a role that can
create tables in it. You do not need to run migrations by hand.

**Every tier is resolved before the process commits to running.** The journal
has satellite tiers — workspaces, autonomy jobs, plugin state, receipts — and a
backend that served some but not others would not fail at the point of use. It
would hang: the relay's message-recovery loop treats a missing workspace tier as
a transient fault and retries every two seconds, forever, logging a warning each
time. So the store factory resolves all of them up front and refuses to start
with an error naming the missing tier.

Resolution is deliberately split from opening. Constructing a tier creates its
schema, so a process that is about to discover it lost an ownership race and
exit must not pay for tiers it will never use. `open()` returns the journal;
`resolve()` proves the tiers. Long-running paths call both.

## Storage

The honest summary is that a thread costs more while it is active and much less
once it goes quiet.

Measured by `storage_footprint_profile`, over 1,800 events across 12 sessions,
written through the public `append` path and vacuumed before measurement:

| Tier | Bytes | Per event |
| --- | --- | --- |
| Hot | 1,957,888 | 1,088 B |
| Cold | 1,064,960 | 592 B |

A row store with per-row headers, a uuid primary key and several indexes is not
cheap per event. The cold tier pays it back: seven days after a thread's last
activity its event bodies are compressed with a trained zstd dictionary, and
the footprint drops to 54% of the hot tier.

Body compression alone was 5.99x in that run. On real captured events it is
4.77x — the synthetic fixture is more repetitive than production traffic, so
treat 4.77x as the number to expect and the table above as the shape.

Reheating is automatic: opening a cold session decompresses it back to the hot
tier, so an agent never reads through compressed data.

One exception to the hot tier being readable jsonb: **an event body containing a
NUL is stored as raw bytes instead.** Postgres `jsonb` parses to a tree whose
strings are `text`, and `text` cannot hold a NUL, so such an insert is rejected
outright. 246 events out of 2.5 million in the journal this was measured
against carry one, almost all of them
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

Writers serialise per SESSION ROW, via `select ... for update` on the session's
own row in the sequence allocator, so two agents appending to two different
sessions never block each other. Throughput therefore rises as writers are
added rather than plateauing behind one machine-wide lock.

Measured by `concurrent_writer_scaling_profile`, each writer appending to its
own session (appends/second, median of three runs on one machine):

| Writers | Appends/second |
| --- | --- |
| 1 | ~47 |
| 4 | ~107 |
| 8 | ~207 |
| 16 | ~420 |
| 32 | ~830 |

Read the shape, not any single cell: throughput scales roughly linearly, 18x
from 1 to 32 writers. The per-append cost at one writer is dominated by
per-statement round-trip latency, which is the price paid for that scaling.
Measure on the target machine rather than inheriting these numbers; they move
with disk speed and with network latency to the database.

To reproduce:

```sh
BORG_TEST_SESSIONS_URL="postgres://borg@localhost:5432/postgres" \
  cargo test -p borg-agent-runtime --lib concurrent_writer_scaling_profile \
  -- --ignored --nocapture
```

## Search

Cross-session search uses `tsvector` with `ts_rank_cd` ranking and
`ts_headline` snippets.

Search comes in two shapes. `query_history` searches ONE session and supports
both lexical and regex modes. `search_all_sessions` searches every session at
once -- the question "have I seen this error before?" is about a whole history,
not one thread -- and is lexical only, because a global regex would have to scan
every body in the journal, which is a far more expensive promise.

**Search does not stem.** The schema pins the `simple` text search
configuration rather than `english`, because `english` maps "migration" and
"migrate" to one lexeme, so a search for one silently answers with the other.
Switching it would look like a search improvement and would quietly change
which events a query selects. The conformance suite holds that line; if you
change the configuration, it is what will tell you what you broke.

## Testing the store

The store suites are written against the traits, and **they require a
PostgreSQL server**. `BORG_TEST_SESSIONS_URL` must name one; without it the
contract tests fail with an actionable message rather than skipping, because a
suite that silently skips its own storage contracts reports green for a
database it never touched:

`just release`, `just release-minor`, and `just release-check` automatically
start an isolated temporary PostgreSQL server and stop/remove it afterward,
including on failure. They use locally installed `initdb` and `pg_ctl` (also
discovered through `pg_config`), so no database URL setup is needed. An explicit
`BORG_TEST_SESSIONS_URL` opts into an existing disposable server; release tests
always set `BORG_SESSIONS_URL` to that same test URL, never the inherited
interactive journal.

Use a disposable test server. For a full workspace run, also pin the runtime
URL to a disposable database: entrypoints that resolve configuration must not
fall back to your normal journal. Neither URL should name a production journal.

```sh
BORG_TEST_SESSIONS_URL="postgres://borg@localhost:5432/postgres" \
BORG_SESSIONS_URL="postgres://borg@localhost:5432/postgres" \
  cargo test --workspace --exclude borg-gui -- --test-threads=4
```

Storage fixtures create and drop their own scratch databases, so the server named by
the URL needs `CREATEDB`. A handful of older optional Postgres tests still skip
when the variable is unset; that convention is being retired, and a full suite
run is not meaningful without a configured server.

Coverage at the time of writing: 24 session-store tests (journal, actions,
forks, runtime manifests and checkpoints, harness state, workflow admission and
lease fencing, the cross-tier relay sweep, search, and gapless allocation
under 8 concurrent writers), plus 13 workspace, 7 plugin, 7 autonomy and 5
receipt tests. Two further tests are `#[ignore]`d benchmarks rather than gates:
`storage_footprint_profile` and `concurrent_writer_scaling_profile`.

## Sharing a journal between machines or users

`BORG_SESSIONS_URL` can name a server on another machine, and nothing stops two
machines — or two OS users on one machine — pointing at the same database. Doing
so **changes a trust boundary**: workspace membership and `/broadcast` are scoped
per machine and OS user, so every owner of a shared journal can see and address
every other.

A connection string can be shared by accident, and the sharing would stay
invisible until
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
