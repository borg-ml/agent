//! A large image as a downscaled overview plus tiles the model sees unscaled.
//!
//! Providers shrink any single image to about 1.2 megapixels (Anthropic above
//! a 1568 px edge or ~1.15 MP), so a 4K frame sent whole reaches the model at
//! a third of its resolution. Split into tiles within that limit, every pixel
//! arrives; the overview keeps the whole picture, and each tile states the
//! region of the original it covers.

use std::io::Cursor;

use image::{DynamicImage, ImageFormat, imageops::FilterType};

/// The largest single image a model sees without rescaling.
pub const MAX_EDGE: u32 = 1568;
pub const MAX_PIXELS: f64 = 1_150_000.0;
/// Tiles per image: a 4K frame fits in eight at native scale; larger images
/// are tiled at the highest scale eight tiles allow.
pub const MAX_TILES: u32 = 8;
/// Images needing less reduction than this are sent whole: a 1080p screenshot
/// (scale 0.745) loses little and would cost three images tiled. Tiling starts
/// around 2560x1440 (0.597); a 4K frame (0.408) keeps every pixel.
const TILE_BELOW_SCALE: f64 = 0.6;
const MAX_PNG_BYTES: usize = 1024 * 1024;

pub struct Piece {
    pub label: String,
    pub media_type: &'static str,
    pub bytes: Vec<u8>,
}

/// Scale at which the whole image fits one model image.
pub fn fit_scale(width: u32, height: u32) -> f64 {
    1f64.min(f64::from(MAX_EDGE) / f64::from(width.max(height)))
        .min((MAX_PIXELS / (f64::from(width) * f64::from(height))).sqrt())
}

/// Columns, rows and scale of the tile grid: the highest scale at which every
/// tile fits the limits, then the fewest tiles.
fn plan(width: u32, height: u32) -> (u32, u32, f64) {
    let (w, h) = (f64::from(width), f64::from(height));
    let mut best = (1, 1, fit_scale(width, height));
    for cols in 1..=MAX_TILES {
        for rows in 1..=MAX_TILES / cols {
            // Just under the bound, so rounding tile edges up cannot overshoot.
            let scale = 0.999
                * 1f64
                    .min(f64::from(MAX_EDGE) * f64::from(cols) / w)
                    .min(f64::from(MAX_EDGE) * f64::from(rows) / h)
                    .min((MAX_PIXELS * f64::from(cols * rows) / (w * h)).sqrt());
            let scale = if scale >= 0.999 { 1.0 } else { scale };
            if scale > best.2 + 1e-9 || (scale >= best.2 - 1e-9 && cols * rows < best.0 * best.1) {
                best = (cols, rows, scale);
            }
        }
    }
    best
}

/// Tile edges along one axis of `length` scaled pixels.
fn edges(length: u32, count: u32) -> impl Iterator<Item = (u32, u32)> {
    (0..count).map(move |i| (i * length / count, (i + 1) * length / count))
}

fn encode(image: &DynamicImage) -> Option<(&'static str, Vec<u8>)> {
    let mut png = Vec::new();
    image
        .write_to(&mut Cursor::new(&mut png), ImageFormat::Png)
        .ok()?;
    if png.len() <= MAX_PNG_BYTES {
        return Some(("image/png", png));
    }
    let mut jpeg = Vec::new();
    DynamicImage::ImageRgb8(image.to_rgb8())
        .write_with_encoder(image::codecs::jpeg::JpegEncoder::new_with_quality(
            &mut Cursor::new(&mut jpeg),
            85,
        ))
        .ok()?;
    Some(("image/jpeg", jpeg))
}

/// The number of image blocks `tile` will produce, without decoding pixels.
/// A malformed or nearly fitting image is charged as one block.
pub fn piece_count(bytes: &[u8]) -> usize {
    let Some((width, height)) = dimensions(bytes) else {
        return 1;
    };
    if width == 0 || height == 0 || fit_scale(width, height) >= TILE_BELOW_SCALE {
        return 1;
    }
    let (cols, rows, _) = plan(width, height);
    (1 + cols * rows) as usize
}

fn dimensions(bytes: &[u8]) -> Option<(u32, u32)> {
    image::ImageReader::new(Cursor::new(bytes))
        .with_guessed_format()
        .ok()?
        .into_dimensions()
        .ok()
}

/// The image as an overview followed by its tiles, or `None` when it should
/// be sent as one image (it nearly fits, or cannot be decoded).
pub fn tile(bytes: &[u8]) -> Option<Vec<Piece>> {
    let (width, height) = dimensions(bytes)?;
    if width == 0 || height == 0 || fit_scale(width, height) >= TILE_BELOW_SCALE {
        return None;
    }
    let image = image::load_from_memory(bytes).ok()?;
    let (cols, rows, scale) = plan(width, height);
    let scaled_width = ((f64::from(width) * scale).floor() as u32).max(cols);
    let scaled_height = ((f64::from(height) * scale).floor() as u32).max(rows);
    let working = if scale < 1.0 {
        image.resize_exact(scaled_width, scaled_height, FilterType::Triangle)
    } else {
        image.clone()
    };
    let overview_scale = fit_scale(width, height);
    let overview = image.resize_exact(
        ((f64::from(width) * overview_scale).round() as u32).max(1),
        ((f64::from(height) * overview_scale).round() as u32).max(1),
        FilterType::Triangle,
    );
    let count = cols * rows;
    let tile_scale = if scale < 1.0 {
        format!(" at scale {scale:.3}")
    } else {
        " at full resolution".into()
    };
    let (media_type, bytes) = encode(&overview)?;
    let mut pieces = vec![Piece {
        label: format!(
            "Overview of the whole {width}x{height} image at scale {overview_scale:.3}; \
             {count} tiles{tile_scale} follow, left to right, top to bottom."
        ),
        media_type,
        bytes,
    }];
    let original = |edge: u32, full: u32, scaled: u32| {
        (u64::from(edge) * u64::from(full) / u64::from(scaled.max(1))) as u32
    };
    for (row, (y0, y1)) in edges(scaled_height, rows).enumerate() {
        for (col, (x0, x1)) in edges(scaled_width, cols).enumerate() {
            let (media_type, bytes) = encode(&working.crop_imm(x0, y0, x1 - x0, y1 - y0))?;
            pieces.push(Piece {
                label: format!(
                    "Tile {}/{count} (row {}, column {}): x {}-{}, y {}-{} of the {width}x{height} image{tile_scale}.",
                    row as u32 * cols + col as u32 + 1,
                    row + 1,
                    col + 1,
                    original(x0, width, scaled_width),
                    original(x1, width, scaled_width),
                    original(y0, height, scaled_height),
                    original(y1, height, scaled_height),
                ),
                media_type,
                bytes,
            });
        }
    }
    Some(pieces)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A tile over the per-image limit is rescaled by the provider (losing the
    /// detail tiling exists for) or, past 2000 px in a long session, rejects
    /// every later request; a gap loses part of the image.
    #[test]
    fn tiles_cover_the_image_within_the_model_limit() {
        assert_eq!(piece_count(b"not an image"), 1);
        assert_eq!(
            plan(3840, 2160),
            (4, 2, 1.0),
            "4K is sent at full resolution"
        );
        for (width, height) in [
            (3840, 2160),
            (7680, 4320),
            (2000, 1125),
            (2560, 1440),
            (1080, 7000),
        ] {
            let (cols, rows, scale) = plan(width, height);
            assert!(cols * rows <= MAX_TILES);
            let scaled_width = ((f64::from(width) * scale).floor() as u32).max(cols);
            let scaled_height = ((f64::from(height) * scale).floor() as u32).max(rows);
            let mut covered = 0_u64;
            for (y0, y1) in edges(scaled_height, rows) {
                for (x0, x1) in edges(scaled_width, cols) {
                    let (tile_width, tile_height) = (x1 - x0, y1 - y0);
                    assert!(tile_width.max(tile_height) <= MAX_EDGE, "{width}x{height}");
                    assert!(
                        f64::from(tile_width) * f64::from(tile_height) <= MAX_PIXELS,
                        "{width}x{height}"
                    );
                    covered += u64::from(tile_width) * u64::from(tile_height);
                }
            }
            assert_eq!(covered, u64::from(scaled_width) * u64::from(scaled_height));
        }
        let png = |width, height| {
            let mut bytes = Vec::new();
            DynamicImage::new_rgb8(width, height)
                .write_to(&mut Cursor::new(&mut bytes), ImageFormat::Png)
                .unwrap();
            bytes
        };
        assert_eq!(piece_count(&png(3840, 2160)), 9);
        assert_eq!(piece_count(&png(1920, 1080)), 1);
        assert_eq!(
            tile(&png(3840, 2160)).map(|pieces| pieces.len()),
            Some(9),
            "overview + 8 tiles"
        );
        assert!(
            tile(&png(1920, 1080)).is_none(),
            "a 1080p screenshot is sent whole"
        );
    }
}
