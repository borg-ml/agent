//! Guards against a backend silently inheriting a trait default the other one
//! found it necessary to replace.
//!
//! WHY THIS EXISTS: this migration shipped a real bug of exactly that shape.
//! `SessionStore::admit_prompt` has a default that BAILS, SQLite overrode it,
//! and Postgres did not -- so every interactive and relayed prompt would have
//! failed on Postgres while the one-shot path kept working and kept the
//! conformance suite green. Two more methods, `recent_messages` and
//! `recent_user_messages`, silently fell back to reading an entire session
//! where SQLite used an index.
//!
//! None of that was caught by the conformance suites, because a conformance
//! test only covers a method someone thought to write a test FOR. It was not
//! caught by the store factory either, which proves every TIER is present and
//! says nothing about the methods inside one. And it cannot be caught by the
//! compiler, because a default is by definition a legal implementation.
//!
//! WHAT IS AND IS NOT A PROBLEM: a default shared by both backends is
//! deliberate -- `WorkspaceStore::append_message` is policy written once over
//! storage primitives precisely so the two engines cannot disagree about it.
//! The danger is ASYMMETRY. When one backend overrides a default and the other
//! inherits it, the inheriting one is almost always wrong, because the other
//! found the default insufficient for the same trait.
//!
//! This is a source-level check rather than a runtime one because Rust has no
//! reflection over trait impls. It reads the crate's own text, which is crude,
//! but a crude check that fails loudly beats a category of bug that ships
//! silently.

#![cfg(test)]

use std::collections::BTreeSet;

/// One asymmetry that is intentional, with the reason it is allowed.
///
/// Every entry must say why the inheriting backend genuinely has nothing to do,
/// not merely that nobody has written the override yet. If an entry cannot be
/// justified in one sentence, it is a bug, not an exemption.
struct Allowed {
    trait_name: &'static str,
    method: &'static str,
    because: &'static str,
}

const ALLOWED: &[Allowed] = &[
    Allowed {
        trait_name: "SessionStore",
        method: "finish_interactive_open",
        because: "SQLite defers stale-live cleanup during an interactive open so the first \
                  frame is not stuck behind the machine-wide writer lock, then completes it \
                  here. Postgres has no such lock, so nothing was deferred and there is \
                  nothing to finish.",
    },
    Allowed {
        trait_name: "PluginBackend",
        method: "ensure_ready",
        because: "SQLite creates the plugin tables lazily on first use, because that tier \
                  predates the session store owning its schema. Postgres applies the whole \
                  satellite schema at connect, so readiness is already established.",
    },
];

/// The trait bodies and impl blocks this check reads.
///
/// `include_str!` rather than runtime file reads: the paths are then verified at
/// compile time, so a moved module breaks the build instead of quietly
/// narrowing what this test inspects.
struct Tier {
    name: &'static str,
    trait_decl: &'static str,
    trait_source: &'static str,
    sqlite_impl: &'static str,
    sqlite_source: &'static str,
    postgres_impl: &'static str,
    postgres_source: &'static str,
}

const SESSION_STORE: &str = include_str!("../session_store.rs");
const POSTGRES_STORE: &str = include_str!("postgres/store.rs");
const WORKSPACE: &str = include_str!("../workspace.rs");
const WORKSPACE_POSTGRES: &str = include_str!("../workspace_postgres.rs");
const AUTONOMY: &str = include_str!("../autonomy.rs");
const AUTONOMY_POSTGRES: &str = include_str!("../autonomy_postgres.rs");
const RECEIPT: &str = include_str!("../receipt.rs");
const RECEIPT_POSTGRES: &str = include_str!("../receipt_postgres.rs");
const PLUGIN: &str = include_str!("../plugin_store.rs");

fn tiers() -> Vec<Tier> {
    vec![
        Tier {
            name: "SessionStore",
            trait_decl: "pub trait SessionStore",
            trait_source: SESSION_STORE,
            sqlite_impl: "impl SessionStore for SqliteSessionStore",
            sqlite_source: SESSION_STORE,
            postgres_impl: "impl SessionStore for PostgresSessionStore",
            postgres_source: POSTGRES_STORE,
        },
        Tier {
            name: "WorkspaceStore",
            trait_decl: "pub trait WorkspaceStore",
            trait_source: WORKSPACE,
            sqlite_impl: "impl WorkspaceStore for SqliteWorkspaceStore",
            sqlite_source: WORKSPACE,
            postgres_impl: "impl WorkspaceStore for PostgresWorkspaceStore",
            postgres_source: WORKSPACE_POSTGRES,
        },
        Tier {
            name: "AutonomyStore",
            trait_decl: "pub trait AutonomyStore",
            trait_source: AUTONOMY,
            sqlite_impl: "impl AutonomyStore for SqliteAutonomyStore",
            sqlite_source: AUTONOMY,
            postgres_impl: "impl crate::autonomy::AutonomyStore for PostgresAutonomyStore",
            postgres_source: AUTONOMY_POSTGRES,
        },
        Tier {
            name: "ReceiptBackend",
            trait_decl: "pub trait ReceiptBackend",
            trait_source: RECEIPT,
            sqlite_impl: "impl ReceiptBackend for SqliteReceiptStore",
            sqlite_source: RECEIPT,
            postgres_impl: "impl crate::receipt::ReceiptBackend for PostgresReceiptStore",
            postgres_source: RECEIPT_POSTGRES,
        },
        Tier {
            name: "PluginBackend",
            trait_decl: "pub trait PluginBackend",
            trait_source: PLUGIN,
            sqlite_impl: "impl PluginBackend for SqlitePluginStore",
            sqlite_source: PLUGIN,
            postgres_impl: "impl PluginBackend for PostgresPluginStore",
            postgres_source: PLUGIN,
        },
    ]
}

/// The text of the braced block introduced by `marker`, by brace matching.
///
/// Panics rather than returning an error: a marker that no longer matches means
/// this check has stopped inspecting what it claims to, and silently passing
/// would be worse than failing.
fn braced_block<'a>(source: &'a str, marker: &str) -> &'a str {
    let start = source
        .find(marker)
        .unwrap_or_else(|| panic!("`{marker}` not found; this guard is no longer reading it"));
    let open = source[start..]
        .find('{')
        .expect("a trait or impl must have a body")
        + start;
    let mut depth = 0usize;
    for (offset, byte) in source[open..].bytes().enumerate() {
        match byte {
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return &source[open..open + offset];
                }
            }
            _ => {}
        }
    }
    panic!("unbalanced braces after `{marker}`");
}

/// Method names declared at the top level of a trait or impl body.
///
/// Only depth-1 `fn` items count, so helpers nested inside a method body are
/// not mistaken for trait methods.
fn method_names(block: &str, defaulted_only: bool) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    let mut depth = 0usize;
    for line in block.lines() {
        let trimmed = line.trim();
        if depth == 1
            && let Some(rest) = trimmed
                .strip_prefix("pub async fn ")
                .or_else(|| trimmed.strip_prefix("pub fn "))
                .or_else(|| trimmed.strip_prefix("async fn "))
                .or_else(|| trimmed.strip_prefix("fn "))
            && let Some(name) = rest.split(['(', '<']).next()
            && !name.is_empty()
        {
            // A declaration ends in `;` somewhere ahead; a default has a body.
            // Judging by the presence of a brace ANYWHERE in the signature line
            // is wrong for multi-line signatures, so the distinction is made by
            // scanning forward to the first `;` or `{` at this depth.
            let is_default = signature_opens_a_body(block, line);
            if !defaulted_only || is_default {
                names.insert(name.to_string());
            }
        }
        depth += line.matches('{').count();
        depth = depth.saturating_sub(line.matches('}').count());
    }
    names
}

/// Does the signature starting at `line` end in a body rather than a `;`?
fn signature_opens_a_body(block: &str, line: &str) -> bool {
    let offset = block.find(line).expect("the line came from this block") + line.len() - line.len();
    for byte in block[offset..].bytes() {
        match byte {
            b';' => return false,
            b'{' => return true,
            _ => {}
        }
    }
    false
}

/// A trait default that one backend replaces and the other inherits is a bug
/// until someone writes down why it is not.
#[test]
fn no_backend_silently_inherits_a_default_the_other_replaced() {
    let mut findings: Vec<String> = Vec::new();

    for tier in tiers() {
        let trait_body = braced_block(tier.trait_source, tier.trait_decl);
        let defaults = method_names(trait_body, true);
        assert!(
            !defaults.is_empty() || tier.name == "AutonomyStore" || tier.name == "ReceiptBackend",
            "{}: found no trait defaults at all, which means this guard's parser \
             has stopped matching the source it inspects",
            tier.name
        );

        let sqlite = method_names(braced_block(tier.sqlite_source, tier.sqlite_impl), false);
        let postgres = method_names(
            braced_block(tier.postgres_source, tier.postgres_impl),
            false,
        );
        assert!(
            !sqlite.is_empty() && !postgres.is_empty(),
            "{}: one of the impl blocks parsed as empty; the guard is broken",
            tier.name
        );

        for method in &defaults {
            let in_sqlite = sqlite.contains(method);
            let in_postgres = postgres.contains(method);
            if in_sqlite == in_postgres {
                continue;
            }
            let exempt = ALLOWED
                .iter()
                .any(|allowed| allowed.trait_name == tier.name && allowed.method == method);
            if exempt {
                continue;
            }
            let (has, lacks) = if in_sqlite {
                ("sqlite", "postgres")
            } else {
                ("postgres", "sqlite")
            };
            findings.push(format!(
                "{}::{method} is overridden by {has} but inherited by {lacks}. The default \
                 is almost certainly wrong for {lacks}, because {has} found it insufficient \
                 for the same trait. Implement it, or add it to ALLOWED with the reason it \
                 genuinely has nothing to do.",
                tier.name
            ));
        }
    }

    assert!(
        findings.is_empty(),
        "backends disagree about which trait defaults suffice:\n  {}",
        findings.join("\n  ")
    );
}

/// An exemption without a reason is not an exemption.
#[test]
fn every_allowed_asymmetry_is_justified() {
    for allowed in ALLOWED {
        assert!(
            allowed.because.len() > 60,
            "{}::{} is exempt without a real explanation",
            allowed.trait_name,
            allowed.method
        );
    }
}
