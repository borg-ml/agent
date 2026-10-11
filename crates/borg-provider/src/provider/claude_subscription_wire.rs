//! Borg-owned byte encoder for the Claude subscription attribution checksum.
//! Protocol 2.1.296: observed with fabricated requests in an isolated process.
//! Hash and transmit the same compact UTF-8 bytes; never serialize after stamping.

const SEED: u64 = 0x4d65_9218_e32a_3268;
const P1: u64 = 0x9e37_79b1_85eb_ca87;
const P2: u64 = 0xc2b2_ae3d_27d4_eb4f;
const P3: u64 = 0x1656_67b1_9e37_79f9;
const P4: u64 = 0x85eb_ca77_c2b2_ae63;
const P5: u64 = 0x27d4_eb2f_1656_67c5;

pub(super) fn stamp(body: &mut [u8]) -> Result<(), &'static str> {
    let system = find(body, b"\"system\":[").ok_or("missing subscription system block")?;
    let end = body.len().min(system.saturating_add(300));
    let slot = system
        + find(&body[system..end], b"cch=00000")
            .ok_or("missing subscription attribution placeholder")?
        + 4;
    let value = xxh64(&normalized(body), SEED) & 0xfffff;
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for i in 0..5 {
        body[slot + i] = HEX[((value >> ((4 - i) * 4)) & 15) as usize];
    }
    Ok(())
}

fn find(data: &[u8], needle: &[u8]) -> Option<usize> {
    data.windows(needle.len()).position(|part| part == needle)
}

// These are byte-pattern exclusions, not JSON canonicalization. In particular,
// nested model values and numeric max_tokens are excluded too. Do not parse and
// reserialize the body: that changes escaping, ordering and the checksum input.
fn normalized(body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(body.len());
    let mut cursor = 0;
    while cursor < body.len() {
        let skip = next_skip(body, cursor);
        let model = find(&body[cursor..], b"\"model\":\"").map(|p| cursor + p);
        if let Some(model) = model.filter(|m| skip.is_none_or(|(s, _)| *m < s)) {
            let value = model + 9;
            let Some(close) = body[value..].iter().position(|b| *b == b'"') else {
                out.extend_from_slice(&body[cursor..]);
                break;
            };
            out.extend_from_slice(&body[cursor..value]);
            cursor = value + close;
        } else if let Some((start, end)) = skip {
            out.extend_from_slice(&body[cursor..start]);
            cursor = end;
        } else {
            out.extend_from_slice(&body[cursor..]);
            break;
        }
    }
    out
}

fn next_skip(body: &[u8], cursor: usize) -> Option<(usize, usize)> {
    let mut best = None;
    for (key, kind) in [
        (b"\"fallbacks\":[".as_slice(), 0),
        (b"\"fallback_credit_token\":\"".as_slice(), 1),
        (b"\"max_tokens\":".as_slice(), 2),
    ] {
        let mut search = cursor;
        while let Some(at) = find(&body[search..], key) {
            let start = search + at;
            let value = start + key.len();
            let end = match kind {
                0 => array_end(body, value),
                1 => body[value..]
                    .iter()
                    .position(|b| *b == b'"')
                    .map(|n| value + n + 1),
                _ => {
                    let n = body[value..]
                        .iter()
                        .take_while(|b| b.is_ascii_digit())
                        .count();
                    (n > 0).then_some(value + n)
                }
            };
            if let Some(mut end) = end {
                let mut start = start;
                if body.get(end) == Some(&b',') {
                    end += 1;
                } else if start > cursor && body[start - 1] == b',' {
                    start -= 1;
                }
                if best.is_none_or(|(s, _)| start < s) {
                    best = Some((start, end));
                }
                break;
            }
            search = value;
            if search >= body.len() {
                break;
            }
        }
    }
    best
}

fn array_end(body: &[u8], mut pos: usize) -> Option<usize> {
    let mut depth = 1;
    let mut quoted = false;
    while pos < body.len() {
        match body[pos] {
            b'\\' if quoted => {
                pos = pos.saturating_add(2);
                continue;
            }
            b'"' => quoted = !quoted,
            b'[' if !quoted => depth += 1,
            b']' if !quoted => {
                depth -= 1;
                if depth == 0 {
                    return Some(pos + 1);
                }
            }
            _ => (),
        }
        pos += 1;
    }
    None
}

// Independently implemented standard xxHash64 arithmetic (all operations wrap).
// No upstream runtime or third-party code is loaded to calculate this value.
fn xxh64(data: &[u8], seed: u64) -> u64 {
    fn lane(data: &[u8], at: usize) -> u64 {
        u64::from_le_bytes(data[at..at + 8].try_into().expect("bounded lane"))
    }
    fn round(acc: u64, input: u64) -> u64 {
        acc.wrapping_add(input.wrapping_mul(P2))
            .rotate_left(31)
            .wrapping_mul(P1)
    }
    let mut i = 0;
    let mut h = if data.len() >= 32 {
        let mut v = [
            seed.wrapping_add(P1).wrapping_add(P2),
            seed.wrapping_add(P2),
            seed,
            seed.wrapping_sub(P1),
        ];
        while i + 32 <= data.len() {
            for (n, acc) in v.iter_mut().enumerate() {
                *acc = round(*acc, lane(data, i + n * 8));
            }
            i += 32;
        }
        let mut h = v[0]
            .rotate_left(1)
            .wrapping_add(v[1].rotate_left(7))
            .wrapping_add(v[2].rotate_left(12))
            .wrapping_add(v[3].rotate_left(18));
        for acc in v {
            h = (h ^ round(0, acc)).wrapping_mul(P1).wrapping_add(P4);
        }
        h
    } else {
        seed.wrapping_add(P5)
    };
    h = h.wrapping_add(data.len() as u64);
    while i + 8 <= data.len() {
        h = (h ^ round(0, lane(data, i)))
            .rotate_left(27)
            .wrapping_mul(P1)
            .wrapping_add(P4);
        i += 8;
    }
    if i + 4 <= data.len() {
        let word = u32::from_le_bytes(data[i..i + 4].try_into().expect("bounded word"));
        h = (h ^ u64::from(word).wrapping_mul(P1))
            .rotate_left(23)
            .wrapping_mul(P2)
            .wrapping_add(P3);
        i += 4;
    }
    for byte in &data[i..] {
        h = (h ^ u64::from(*byte).wrapping_mul(P5))
            .rotate_left(11)
            .wrapping_mul(P1);
    }
    h ^= h >> 33;
    h = h.wrapping_mul(P2);
    h ^= h >> 29;
    h = h.wrapping_mul(P3);
    h ^ (h >> 32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standard_hash_vectors() {
        assert_eq!(xxh64(b"", 0), 0xef46_db37_51d8_e999);
        assert_eq!(xxh64(b"abc", 0), 0x44bc_2cf5_ad77_0999);
    }

    #[test]
    fn excludes_only_observed_fields_without_reserializing() {
        assert_eq!(
            normalized(br#"{"model":"x","max_tokens":42,"x":1}"#),
            br#"{"model":"","x":1}"#
        );
        assert_eq!(normalized(br#"{"x":1,"max_tokens":42}"#), br#"{"x":1}"#);
        assert_eq!(
            normalized(br#"{"max_tokens":"42","model":{"a":1}}"#),
            br#"{"max_tokens":"42","model":{"a":1}}"#
        );
        assert_eq!(normalized(br#"{"x":1,"fallbacks":[{"model":"x","note":"[\"]"}],"fallback_credit_token":"fake"}"#), br#"{"x":1,}"#);
        assert_eq!(
            normalized(
                br#"{"input":{"model":"x","max_tokens":42},"note":"\"model\":\"unchanged\""}"#
            ),
            br#"{"input":{"model":""},"note":"\"model\":\"unchanged\""}"#
        );
    }

    #[test]
    fn refuses_missing_or_distant_placeholder() {
        assert!(stamp(&mut b"{}".to_vec()).is_err());
        let mut body = b"{\"system\":[".to_vec();
        body.extend(vec![b' '; 300]);
        body.extend(b"cch=00000");
        assert!(stamp(&mut body).is_err());
    }

    #[test]
    fn native_checksum_goldens() {
        let mut body = r##"{"model":"owned-fixture","max_tokens":1,"stream":false,"messages":[{"role":"user","content":"owned offline fixture sample 1"}],"system":[{"type":"text","text":"x-anthropic-billing-header: cc_version=2.1.296.a1b; cc_entrypoint=cli; cch=00000;"}]}"##.as_bytes().to_vec();
        let before = body.clone();
        stamp(&mut body).unwrap();
        let slot = find(&before, b"cch=00000").unwrap() + 4;
        assert_eq!(&body[slot..slot + 5], b"94113", "sample1");
        assert_eq!(&body[..slot], &before[..slot]);
        assert_eq!(&body[slot + 5..], &before[slot + 5..]);
        let mut body = r##"{"model":"owned-fixture","max_tokens":1,"stream":false,"messages":[{"role":"user","content":"owned offline fixture sample 2"}],"system":[{"type":"text","text":"x-anthropic-billing-header: cc_version=2.1.296.a1b; cc_entrypoint=cli; cch=00000;"}]}"##.as_bytes().to_vec();
        let before = body.clone();
        stamp(&mut body).unwrap();
        let slot = find(&before, b"cch=00000").unwrap() + 4;
        assert_eq!(&body[slot..slot + 5], b"ee5d7", "sample2");
        assert_eq!(&body[..slot], &before[..slot]);
        assert_eq!(&body[slot + 5..], &before[slot + 5..]);
        let mut body = r##"{"model":"owned-fixture","max_tokens":1,"stream":false,"messages":[{"role":"user","content":"owned offline fixture sample 3"}],"system":[{"type":"text","text":"x-anthropic-billing-header: cc_version=2.1.296.a1b; cc_entrypoint=cli; cch=00000;"}]}"##.as_bytes().to_vec();
        let before = body.clone();
        stamp(&mut body).unwrap();
        let slot = find(&before, b"cch=00000").unwrap() + 4;
        assert_eq!(&body[slot..slot + 5], b"cbb5d", "sample3");
        assert_eq!(&body[..slot], &before[..slot]);
        assert_eq!(&body[slot + 5..], &before[slot + 5..]);
        let mut body = r##"{"model":"owned-fixture","max_tokens":1,"stream":true,"system":[{"type":"text","text":"x-anthropic-billing-header: cc_version=2.1.296.a1b; cc_entrypoint=cli; cch=00000;"},{"type":"text","text":"owned system block"}],"messages":[{"role":"user","content":[{"type":"text","text":"owned offline fixture connector-shaped probe"}]}],"metadata":{"user_id":"{\"device_id\":\"owned-device\",\"account_uuid\":\"owned-account\",\"session_id\":\"owned-session\"}"}}"##.as_bytes().to_vec();
        let before = body.clone();
        stamp(&mut body).unwrap();
        let slot = find(&before, b"cch=00000").unwrap() + 4;
        assert_eq!(&body[slot..slot + 5], b"d77ba", "connector");
        assert_eq!(&body[..slot], &before[..slot]);
        assert_eq!(&body[slot + 5..], &before[slot + 5..]);
        let mut body = r##"{"model":"owned-fixture","max_tokens":1,"stream":false,"messages":[{"role":"user","content":"owned π 🧪 fixture"}],"system":[{"type":"text","text":"x-anthropic-billing-header: cc_version=2.1.296.a1b; cc_entrypoint=cli; cch=00000;"}]}"##.as_bytes().to_vec();
        let before = body.clone();
        stamp(&mut body).unwrap();
        let slot = find(&before, b"cch=00000").unwrap() + 4;
        assert_eq!(&body[slot..slot + 5], b"bdd0b", "unicode");
        assert_eq!(&body[..slot], &before[..slot]);
        assert_eq!(&body[slot + 5..], &before[slot + 5..]);
        let mut body = r##"{"model":"owned-fixture","max_tokens":1,"stream":false,"messages":[{"role":"user","content":"owned offline fixture sample 1"}],"system":[{"type":"text","text":"x-anthropic-billing-header: cc_version=2.1.296.a1b; cc_entrypoint=cli; cch=00000;"}],"tools":[{"name":"owned","input_schema":{"type":"object"},"input_examples":[{"model":"owned-submodel","max_tokens":42}]}]}"##.as_bytes().to_vec();
        let before = body.clone();
        stamp(&mut body).unwrap();
        let slot = find(&before, b"cch=00000").unwrap() + 4;
        assert_eq!(&body[slot..slot + 5], b"a87a3", "nested");
        assert_eq!(&body[..slot], &before[..slot]);
        assert_eq!(&body[slot + 5..], &before[slot + 5..]);
        let mut body = r##"{"model":"owned-fixture","stream":false,"messages":[{"role":"user","content":"owned offline fixture sample 1"}],"system":[{"type":"text","text":"x-anthropic-billing-header: cc_version=2.1.296.a1b; cc_entrypoint=cli; cch=00000;"}],"max_tokens":12345}"##.as_bytes().to_vec();
        let before = body.clone();
        stamp(&mut body).unwrap();
        let slot = find(&before, b"cch=00000").unwrap() + 4;
        assert_eq!(&body[slot..slot + 5], b"94113", "max-last");
        assert_eq!(&body[..slot], &before[..slot]);
        assert_eq!(&body[slot + 5..], &before[slot + 5..]);
        let mut body = r##"{"model":"owned-fixture","max_tokens":1,"stream":false,"messages":[{"role":"user","content":"owned offline fixture sample 1"}],"system":[{"type":"text","text":"x-anthropic-billing-header: cc_version=2.1.296.a1b; cc_entrypoint=cli; cch=00000;"}],"fallbacks":["owned-model",{"model":"owned-other","max_tokens":99,"note":"escaped \\\" [ ]"}]}"##.as_bytes().to_vec();
        let before = body.clone();
        stamp(&mut body).unwrap();
        let slot = find(&before, b"cch=00000").unwrap() + 4;
        assert_eq!(&body[slot..slot + 5], b"94113", "fallback-array");
        assert_eq!(&body[..slot], &before[..slot]);
        assert_eq!(&body[slot + 5..], &before[slot + 5..]);
        let mut body = r##"{"model":"owned-fixture","max_tokens":1,"stream":false,"messages":[{"role":"user","content":"owned offline fixture sample 1"}],"system":[{"type":"text","text":"x-anthropic-billing-header: cc_version=2.1.296.a1b; cc_entrypoint=cli; cch=00000;"}],"fallback_credit_token":"owned-fake-credit-token"}"##.as_bytes().to_vec();
        let before = body.clone();
        stamp(&mut body).unwrap();
        let slot = find(&before, b"cch=00000").unwrap() + 4;
        assert_eq!(&body[slot..slot + 5], b"94113", "fallback-token");
        assert_eq!(&body[..slot], &before[..slot]);
        assert_eq!(&body[slot + 5..], &before[slot + 5..]);
        let mut body = r##"{"model":"owned-fixture","max_tokens":1,"stream":false,"messages":[{"role":"user","content":"owned offline fixture sample 1"}],"system":[{"type":"text","text":"x-anthropic-billing-header: cc_version=2.1.296.a1b; cc_entrypoint=cli; cch=00000;"}],"fallbacks":["owned"],"fallback_credit_token":"owned-fake"}"##.as_bytes().to_vec();
        let before = body.clone();
        stamp(&mut body).unwrap();
        let slot = find(&before, b"cch=00000").unwrap() + 4;
        assert_eq!(&body[slot..slot + 5], b"11b96", "adjacent");
        assert_eq!(&body[..slot], &before[..slot]);
        assert_eq!(&body[slot + 5..], &before[slot + 5..]);
    }
}
