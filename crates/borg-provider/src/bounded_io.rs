use std::fs;
use std::io::{self, Read as _};
use std::path::Path;

use anyhow::{Context, Result, bail};

pub(crate) fn read_file_bytes_with_limit(
    path: &Path,
    label: &str,
    max_bytes: u64,
) -> Result<Vec<u8>> {
    let metadata =
        fs::metadata(path).with_context(|| format!("failed to stat {label} {}", path.display()))?;
    ensure_bytes_within_limit(path, label, metadata.len(), max_bytes)?;

    let mut file = fs::File::open(path)
        .with_context(|| format!("failed to read {label} {}", path.display()))?;
    let mut bytes = Vec::new();
    file.by_ref()
        .take(max_bytes.saturating_add(1))
        .read_to_end(&mut bytes)
        .with_context(|| format!("failed to read {label} {}", path.display()))?;
    let read_bytes = u64::try_from(bytes.len()).context("bounded file byte count overflowed")?;
    ensure_bytes_within_limit(path, label, read_bytes, max_bytes)?;
    Ok(bytes)
}

pub(crate) fn read_open_file_bytes_with_limit(
    path: &Path,
    label: &str,
    mut file: fs::File,
    max_bytes: u64,
) -> Result<Vec<u8>> {
    if file
        .metadata()
        .is_ok_and(|metadata| metadata.len() > max_bytes)
    {
        bail!("{label} {} exceeds {max_bytes} byte limit", path.display());
    }

    let mut bytes = Vec::new();
    file.by_ref()
        .take(max_bytes.saturating_add(1))
        .read_to_end(&mut bytes)
        .with_context(|| format!("failed to read {label} {}", path.display()))?;
    let read_bytes = u64::try_from(bytes.len()).context("bounded file byte count overflowed")?;
    ensure_bytes_within_limit(path, label, read_bytes, max_bytes)?;
    Ok(bytes)
}

pub(crate) fn read_file_text_with_limit(
    path: &Path,
    label: &str,
    max_bytes: u64,
) -> Result<String> {
    let bytes = read_file_bytes_with_limit(path, label, max_bytes)?;
    String::from_utf8(bytes)
        .with_context(|| format!("{label} {} is not valid UTF-8", path.display()))
}

pub(crate) fn read_reader_bytes_with_limit(
    mut reader: impl io::Read,
    label: &str,
    max_bytes: usize,
) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    reader
        .by_ref()
        .take(
            u64::try_from(max_bytes)
                .unwrap_or(u64::MAX)
                .saturating_add(1),
        )
        .read_to_end(&mut bytes)?;
    if bytes.len() > max_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{label} exceeded {max_bytes} bytes"),
        ));
    }
    Ok(bytes)
}

pub(crate) fn read_reader_lossy_text_with_limit(
    reader: impl io::Read,
    label: &str,
    max_bytes: usize,
) -> io::Result<String> {
    let bytes = read_reader_bytes_with_limit(reader, label, max_bytes)?;
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

pub(crate) fn ensure_bytes_within_limit(
    path: &Path,
    label: &str,
    bytes: u64,
    max_bytes: u64,
) -> Result<()> {
    if bytes > max_bytes {
        bail!("{label} {} exceeds {max_bytes} byte limit", path.display());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        read_file_bytes_with_limit, read_file_text_with_limit, read_open_file_bytes_with_limit,
        read_reader_bytes_with_limit,
    };

    #[test]
    fn bounded_file_reader_allows_exact_limit() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("exact.txt");
        std::fs::write(&path, b"exact").expect("write exact file");

        let text = read_file_text_with_limit(&path, "bounded input", 5).expect("exact limit");

        assert_eq!(text, "exact");
    }

    #[test]
    fn bounded_file_reader_rejects_oversized_sparse_file() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("oversized.bin");
        let file = std::fs::File::create(&path).expect("create sparse file");
        file.set_len(6).expect("set sparse file length");

        let error = read_file_bytes_with_limit(&path, "bounded input", 5).unwrap_err();

        assert!(error.to_string().contains("exceeds"));
        assert!(error.to_string().contains("5"));
    }

    #[test]
    fn bounded_open_file_reader_rejects_oversized_sparse_file() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("oversized-open.bin");
        let file = std::fs::File::create(&path).expect("create sparse file");
        file.set_len(6).expect("set sparse file length");
        drop(file);
        let file = std::fs::File::open(&path).expect("open sparse file");

        let error = read_open_file_bytes_with_limit(&path, "bounded input", file, 5).unwrap_err();

        assert!(error.to_string().contains("exceeds"));
        assert!(error.to_string().contains("5"));
    }

    #[test]
    fn bounded_reader_rejects_oversized_stream() {
        let error =
            read_reader_bytes_with_limit(std::io::Cursor::new(b"abcde"), "stream", 4).unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("exceeded"));
    }
}
