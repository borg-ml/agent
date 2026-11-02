//! How every frontend shows Claude Code's `ultrathink` keyword: colored only
//! where the session's model will honour it, as a rainbow swept by a crest.

use std::ops::Range;

use crate::CodingProvider;

pub const HINT: &str = "Deeper reasoning requested for this turn";

/// One crest pass across the word, then a rest.
pub const SHIMMER_PASS_MILLIS: u128 = 1_600;
pub const SHIMMER_INTERVAL_MILLIS: u128 = SHIMMER_PASS_MILLIS * 3 / 2;

const RAINBOW: [(u8, u8, u8); 7] = [
    (235, 95, 87),
    (245, 145, 70),
    (240, 200, 80),
    (120, 200, 120),
    (95, 165, 235),
    (125, 120, 225),
    (190, 115, 210),
];

/// Byte ranges of `ultrathink` in a draft that will raise the next turn: a
/// Claude session on a model that offers it. Elsewhere the word is plain text.
pub fn ranges(provider: CodingProvider, model: Option<&str>, text: &str) -> Vec<Range<usize>> {
    let model = model.unwrap_or(borg_provider::CLAUDE_MODEL_CATALOG.default_model);
    if provider == CodingProvider::Claude && borg_provider::ultrathink::available(model) {
        borg_provider::ultrathink::ranges(text)
    } else {
        Vec::new()
    }
}

/// The color of one letter of a `letters`-long keyword. `phase` is the time
/// into the current sweep interval; `None` draws the rainbow at rest.
pub fn color(letter: usize, letters: usize, phase: Option<u128>) -> (u8, u8, u8) {
    const PADDING: usize = 3;
    const HALF_WIDTH: f32 = 3.0;
    let (red, green, blue) = RAINBOW[letter % RAINBOW.len()];
    let lift = phase.map_or(0.0, |phase| {
        let period = letters + PADDING * 2;
        // Past one pass the crest is beyond the word: the rest between sweeps.
        let center = (phase * period as u128 / SHIMMER_PASS_MILLIS) as f32;
        let distance = ((letter + PADDING) as f32 - center).abs();
        if distance <= HALF_WIDTH {
            0.5 * (1.0 + (std::f32::consts::PI * distance / HALF_WIDTH).cos())
        } else {
            0.0
        }
    });
    let channel = |value: u8| (f32::from(value) + (255.0 - f32::from(value)) * lift * 0.8) as u8;
    (channel(red), channel(green), channel(blue))
}
