//! The journal's behavioural contracts, exercised against the shipped store.
//!
//! WHY THESE ARE SEPARATE from the query-level tests beside them: these were
//! written against `SessionStore` semantics -- action lifecycles, recovery
//! projections, fork lineage, live-state boundaries -- rather than against the
//! SQL that happens to implement them. Keeping them in their own modules keeps
//! that distinction visible, so a future change to the SQL is checked against a
//! statement of what the journal must DO rather than how it currently does it.
//!
//! Every module here requires a Postgres server. See `super::testing`.

mod support;

mod actions;
mod fork;
mod host;
mod live_state;
mod recovery;
mod roster;
mod routes;
mod sessions;
