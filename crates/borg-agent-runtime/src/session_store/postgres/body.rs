//! Event body storage: the hot readable tier and the cold compressed tier.
//!
//! `session_events` stores exactly one of two forms per row (see the two-tier
//! comment in postgres_schema.sql):
//!
//! * HOT -- `event_json`, readable jsonb. The default. Agents read their own
//!   and each other's journals and compose ad-hoc SQL across the database, and
//!   Postgres has no SQL-callable zstd-with-dictionary decompressor, so a
//!   compressed body is opaque to everything that is not Borg itself.
//! * COLD -- `event_body` plus an optional `dict_id`. zstd against a trained
//!   dictionary, for aged-out history where ratio outweighs plain-SQL reads.
//!
//! This module owns the cold codec and the tier decision. Nothing writes the
//! cold tier yet; the codec exists so ageing history into it later is a
//! per-row operation that needs no schema change and no rewrite of hot rows.

use anyhow::{Context, Result, bail};

/// Compression level for the cold tier. 19 is near the top of zstd's normal
/// range: cold rows are written once by a background ager and then read rarely,
/// so encode cost is close to irrelevant and ratio is what matters.
const COLD_COMPRESSION_LEVEL: i32 = 19;

/// Target size of a trained dictionary. The design measured 4.41x at 128KB
/// against a 4.44x whole-stream ceiling, so a larger dictionary buys almost
/// nothing while being loaded into memory by every process.
pub const DICTIONARY_TARGET_BYTES: usize = 128 * 1024;

/// Refuse to allocate an unbounded buffer on the word of a frame header. No
/// real event approaches this; a frame claiming more is treated as corrupt.
const MAX_DECOMPRESSED_BYTES: usize = 256 * 1024 * 1024;

/// A trained dictionary and the id it is stored under in `session_event_dicts`.
///
/// Dictionaries are append-only and never deleted: each row records the exact
/// dictionary it was written with, so retraining is always safe and never
/// requires rewriting history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventDictionary {
    pub dict_id: i32,
    pub bytes: Vec<u8>,
}

/// How one event body is represented in the row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoredBody {
    /// `event_json` is set; `event_body` and `dict_id` are null.
    Readable(serde_json::Value),
    /// `event_body` is set; `dict_id` is null when the bytes are plain UTF-8
    /// JSON rather than zstd output.
    Compressed {
        bytes: Vec<u8>,
        dict_id: Option<i32>,
    },
}

impl StoredBody {
    /// The readable form, or `None` when this body is in the cold tier.
    ///
    /// A cold body is deliberately not decoded here: decoding needs the
    /// dictionary the row references, which is a database read the caller must
    /// perform. This keeps "I have the bytes" and "I can read them" distinct
    /// rather than silently returning an empty body.
    pub fn readable(&self) -> Option<&serde_json::Value> {
        match self {
            Self::Readable(value) => Some(value),
            Self::Compressed { .. } => None,
        }
    }

    pub fn is_compressed(&self) -> bool {
        matches!(self, Self::Compressed { .. })
    }
}

/// Encode a body for the cold tier.
///
/// `dictionary` is optional so the store stays usable before any dictionary has
/// been trained -- the very first events in an empty database have no corpus to
/// train from.
pub fn compress(json: &[u8], dictionary: Option<&EventDictionary>) -> Result<StoredBody> {
    let bytes = match dictionary {
        Some(dictionary) => {
            let mut compressor =
                zstd::bulk::Compressor::with_dictionary(COLD_COMPRESSION_LEVEL, &dictionary.bytes)
                    .context("failed to build a dictionary compressor")?;
            compressor
                .compress(json)
                .context("failed to compress an event body with a dictionary")?
        }
        None => zstd::bulk::compress(json, COLD_COMPRESSION_LEVEL)
            .context("failed to compress an event body")?,
    };
    Ok(StoredBody::Compressed {
        bytes,
        dict_id: dictionary.map(|dictionary| dictionary.dict_id),
    })
}

/// Decode a cold-tier body.
///
/// The dictionary must be the one named by the row's `dict_id`; zstd rejects a
/// mismatch rather than returning wrong bytes, and that error is surfaced
/// rather than papered over, because a body that cannot be decoded is a
/// corrupted journal entry and must not read as an empty event.
pub fn decompress(bytes: &[u8], dictionary: Option<&EventDictionary>) -> Result<Vec<u8>> {
    let capacity = decompressed_capacity(bytes)?;
    match dictionary {
        Some(dictionary) => {
            let mut decompressor = zstd::bulk::Decompressor::with_dictionary(&dictionary.bytes)
                .context("failed to build a dictionary decompressor")?;
            decompressor
                .decompress(bytes, capacity)
                .context("failed to decompress an event body with its dictionary")
        }
        None => zstd::bulk::decompress(bytes, capacity).context("failed to decompress an event body"),
    }
}

/// The exact decompressed size from the frame header, bounded.
///
/// zstd's bulk encoder always records the content size, so this is exact for
/// anything Borg wrote. A frame without one is rejected rather than guessed at:
/// guessing means either a needless huge allocation or a truncated body.
fn decompressed_capacity(bytes: &[u8]) -> Result<usize> {
    let Some(size) = zstd::zstd_safe::get_frame_content_size(bytes)
        .ok()
        .flatten()
    else {
        bail!("compressed event body has no recorded content size");
    };
    let size = usize::try_from(size).context("compressed event body is larger than this machine")?;
    if size > MAX_DECOMPRESSED_BYTES {
        bail!("compressed event body claims an implausible size of {size} bytes");
    }
    Ok(size)
}

/// Train a dictionary from real event bodies.
///
/// The caller supplies the corpus: dictionary quality depends entirely on the
/// samples resembling what will be written, so this must be trained from actual
/// journal events rather than synthetic ones.
pub fn train_dictionary(samples: &[Vec<u8>], target_bytes: usize) -> Result<Vec<u8>> {
    if samples.is_empty() {
        bail!("cannot train an event dictionary from an empty corpus");
    }
    zstd::dict::from_samples(samples, target_bytes)
        .context("failed to train an event body dictionary")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Bodies shaped like real session events: the same keys repeated across
    /// every record, which is the redundancy a trained dictionary exploits and
    /// per-record compression alone cannot see.
    fn corpus(count: usize) -> Vec<Vec<u8>> {
        (0..count)
            .map(|index| {
                serde_json::to_vec(&serde_json::json!({
                    "id": format!("0192f3c4-{index:04}-7000-8000-000000000000"),
                    "sequence": index,
                    "kind": {
                        "type": "tool_completed",
                        "tool": "Bash",
                        "actor": "assistant",
                        "output": format!("compiling borg-agent-runtime v0.8.3 ({index})"),
                    },
                    "created_at": "2026-09-18T14:00:00Z",
                }))
                .expect("serialise sample")
            })
            .collect()
    }

    #[test]
    fn a_body_round_trips_without_a_dictionary() {
        let body = corpus(1).remove(0);
        let StoredBody::Compressed { bytes, dict_id } =
            compress(&body, None).expect("compress") else {
            panic!("compress must produce a cold body");
        };
        assert_eq!(dict_id, None, "no dictionary means no dict_id to record");
        assert_eq!(decompress(&bytes, None).expect("decompress"), body);
    }

    #[test]
    fn a_body_round_trips_through_its_dictionary() {
        let samples = corpus(512);
        let dictionary = EventDictionary {
            dict_id: 1,
            bytes: train_dictionary(&samples, DICTIONARY_TARGET_BYTES).expect("train"),
        };
        let body = corpus(1).remove(0);
        let StoredBody::Compressed { bytes, dict_id } =
            compress(&body, Some(&dictionary)).expect("compress") else {
            panic!("compress must produce a cold body");
        };
        assert_eq!(
            dict_id,
            Some(1),
            "the row must record which dictionary decodes it"
        );
        assert_eq!(
            decompress(&bytes, Some(&dictionary)).expect("decompress"),
            body
        );
    }

    #[test]
    fn a_dictionary_beats_per_record_compression_on_event_shaped_bodies() {
        let samples = corpus(512);
        let dictionary = EventDictionary {
            dict_id: 1,
            bytes: train_dictionary(&samples, DICTIONARY_TARGET_BYTES).expect("train"),
        };
        // Held out from training, as the design's measurement was.
        let held_out = corpus(600)[512..].to_vec();
        let raw: usize = held_out.iter().map(Vec::len).sum();
        let mut plain = 0usize;
        let mut with_dictionary = 0usize;
        for body in &held_out {
            let StoredBody::Compressed { bytes, .. } = compress(body, None).expect("compress")
            else {
                panic!("cold body expected");
            };
            plain += bytes.len();
            let StoredBody::Compressed { bytes, .. } =
                compress(body, Some(&dictionary)).expect("compress") else {
                panic!("cold body expected");
            };
            with_dictionary += bytes.len();
        }
        assert!(
            with_dictionary < plain,
            "a trained dictionary must beat per-record zstd: {with_dictionary} vs {plain} from {raw} raw"
        );
    }

    #[test]
    fn decoding_with_the_wrong_dictionary_fails_instead_of_returning_wrong_bytes() {
        let dictionary = EventDictionary {
            dict_id: 1,
            bytes: train_dictionary(&corpus(512), DICTIONARY_TARGET_BYTES).expect("train"),
        };
        let other = EventDictionary {
            dict_id: 2,
            bytes: train_dictionary(
                &(0..512)
                    .map(|index| format!("an unrelated corpus entry number {index}").into_bytes())
                    .collect::<Vec<_>>(),
                DICTIONARY_TARGET_BYTES,
            )
            .expect("train"),
        };
        let body = corpus(1).remove(0);
        let StoredBody::Compressed { bytes, .. } =
            compress(&body, Some(&dictionary)).expect("compress") else {
            panic!("cold body expected");
        };
        // A silently wrong body would be worse than a loud failure: it would
        // read as a valid but incorrect journal entry.
        assert!(decompress(&bytes, Some(&other)).is_err());
    }

    #[test]
    fn a_readable_body_reports_itself_readable_and_a_cold_one_does_not() {
        let value = serde_json::json!({"kind": {"type": "message"}});
        let hot = StoredBody::Readable(value.clone());
        assert_eq!(hot.readable(), Some(&value));
        assert!(!hot.is_compressed());

        let cold = compress(b"{}", None).expect("compress");
        assert_eq!(cold.readable(), None);
        assert!(cold.is_compressed());
    }


    #[test]
    fn training_refuses_an_empty_corpus() {
        assert!(train_dictionary(&[], DICTIONARY_TARGET_BYTES).is_err());
    }

    #[test]
    fn a_body_that_is_not_a_zstd_frame_is_rejected() {
        assert!(decompress(b"not a zstd frame at all", None).is_err());
    }
}
