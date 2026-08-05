use std::collections::BTreeMap;
use std::fmt;
use std::fs::File;
use std::io::{self, Read};
use std::path::Path;

const GGUF_MAGIC: u32 = 0x4655_4747;
const MIN_VERSION: u32 = 2;
const MAX_VERSION: u32 = 3;
const MAX_KEY_BYTES: u64 = 1 << 20;
const MAX_STRING_BYTES: u64 = 64 << 20;
const MAX_ARRAY_ITEMS: u64 = 10_000_000;
const MAX_ARRAY_DEPTH: usize = 8;
const GGUF_TYPE_UINT8: u32 = 0;
const GGUF_TYPE_INT8: u32 = 1;
const GGUF_TYPE_UINT16: u32 = 2;
const GGUF_TYPE_INT16: u32 = 3;
const GGUF_TYPE_UINT32: u32 = 4;
const GGUF_TYPE_INT32: u32 = 5;
const GGUF_TYPE_FLOAT32: u32 = 6;
const GGUF_TYPE_BOOL: u32 = 7;
const GGUF_TYPE_STRING: u32 = 8;
const GGUF_TYPE_ARRAY: u32 = 9;
const GGUF_TYPE_UINT64: u32 = 10;
const GGUF_TYPE_INT64: u32 = 11;
const GGUF_TYPE_FLOAT64: u32 = 12;

/// A typed GGUF metadata value. Unknown types are rejected rather than
/// guessed, which keeps malformed headers from becoming plausible listings.
#[derive(Debug, Clone, PartialEq)]
pub enum GgufValue {
    U8(u8),
    I8(i8),
    U16(u16),
    I16(i16),
    U32(u32),
    I32(i32),
    F32(f32),
    Bool(bool),
    String(String),
    Array {
        value_type: u32,
        values: Vec<GgufValue>,
    },
    U64(u64),
    I64(i64),
    F64(f64),
}

impl GgufValue {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value),
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Self::U8(value) => Some(u64::from(*value)),
            Self::U16(value) => Some(u64::from(*value)),
            Self::U32(value) => Some(u64::from(*value)),
            Self::U64(value) => Some(*value),
            Self::I8(value) if *value >= 0 => Some(*value as u64),
            Self::I16(value) if *value >= 0 => Some(*value as u64),
            Self::I32(value) if *value >= 0 => Some(*value as u64),
            Self::I64(value) if *value >= 0 => Some(*value as u64),
            _ => None,
        }
    }
}

/// Typed errors returned while reading a GGUF header. The parser reads only
/// the magic/version/counts and metadata block; tensor data is never touched.
#[derive(Debug)]
pub enum GgufError {
    Io(io::Error),
    Truncated { offset: u64, needed: usize },
    InvalidMagic { found: u32 },
    UnsupportedVersion { version: u32 },
    InvalidCount { field: &'static str, value: u64 },
    InvalidLength { field: &'static str, value: u64 },
    InvalidUtf8 { field: &'static str },
    InvalidBool { value: u8 },
    UnknownValueType { value_type: u32 },
    ExcessiveNesting,
}

impl fmt::Display for GgufError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "I/O error while reading GGUF: {error}"),
            Self::Truncated { offset, needed } => {
                write!(
                    formatter,
                    "truncated GGUF header at byte {offset} (need {needed} bytes)"
                )
            }
            Self::InvalidMagic { found } => {
                write!(formatter, "invalid GGUF magic 0x{found:08x}")
            }
            Self::UnsupportedVersion { version } => {
                write!(formatter, "unsupported GGUF version {version}")
            }
            Self::InvalidCount { field, value } => {
                write!(formatter, "invalid GGUF {field} count {value}")
            }
            Self::InvalidLength { field, value } => {
                write!(formatter, "invalid GGUF {field} length {value}")
            }
            Self::InvalidUtf8 { field } => write!(formatter, "invalid UTF-8 in GGUF {field}"),
            Self::InvalidBool { value } => write!(formatter, "invalid GGUF boolean value {value}"),
            Self::UnknownValueType { value_type } => {
                write!(formatter, "unknown GGUF metadata value type {value_type}")
            }
            Self::ExcessiveNesting => {
                formatter.write_str("GGUF metadata array nesting is too deep")
            }
        }
    }
}

impl std::error::Error for GgufError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for GgufError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct GgufHeader {
    pub version: u32,
    pub tensor_count: u64,
    pub kv_count: u64,
    pub metadata: BTreeMap<String, GgufValue>,
}

impl GgufHeader {
    pub fn get(&self, key: &str) -> Option<&GgufValue> {
        self.metadata.get(key)
    }

    pub fn string(&self, key: &str) -> Option<&str> {
        self.get(key).and_then(GgufValue::as_str)
    }

    pub fn positive_u64(&self, key: &str) -> Option<u64> {
        self.get(key)
            .and_then(GgufValue::as_u64)
            .filter(|value| *value > 0)
    }
}

/// Parse a GGUF file's header and metadata without reading its tensor data.
pub fn parse_gguf_file(path: &Path) -> Result<GgufHeader, GgufError> {
    let mut file = File::open(path)?;
    parse_gguf_reader(&mut file)
}

fn parse_gguf_reader(reader: &mut impl Read) -> Result<GgufHeader, GgufError> {
    let mut cursor = CursorReader::new(reader);
    let magic = cursor.read_u32()?;
    if magic != GGUF_MAGIC {
        return Err(GgufError::InvalidMagic { found: magic });
    }

    let version = cursor.read_u32()?;
    if !(MIN_VERSION..=MAX_VERSION).contains(&version) {
        return Err(GgufError::UnsupportedVersion { version });
    }

    let tensor_count = cursor.read_u64()?;
    if tensor_count > 10_000_000 {
        return Err(GgufError::InvalidCount {
            field: "tensor",
            value: tensor_count,
        });
    }
    let kv_count = cursor.read_u64()?;
    if kv_count > 1_000_000 {
        return Err(GgufError::InvalidCount {
            field: "metadata",
            value: kv_count,
        });
    }

    let mut metadata = BTreeMap::new();
    for _ in 0..kv_count {
        let key = cursor.read_string(MAX_KEY_BYTES, "key")?;
        let value_type = cursor.read_u32()?;
        let value = cursor.read_value(value_type, 0)?;
        metadata.insert(key, value);
    }

    Ok(GgufHeader {
        version,
        tensor_count,
        kv_count,
        metadata,
    })
}

struct CursorReader<'a, R> {
    reader: &'a mut R,
    offset: u64,
}

impl<'a, R: Read> CursorReader<'a, R> {
    fn new(reader: &'a mut R) -> Self {
        Self { reader, offset: 0 }
    }

    fn read_exact(&mut self, bytes: &mut [u8]) -> Result<(), GgufError> {
        match self.reader.read_exact(bytes) {
            Ok(()) => {
                self.offset = self.offset.saturating_add(bytes.len() as u64);
                Ok(())
            }
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => {
                Err(GgufError::Truncated {
                    offset: self.offset,
                    needed: bytes.len(),
                })
            }
            Err(error) => Err(GgufError::Io(error)),
        }
    }

    fn read_u8(&mut self) -> Result<u8, GgufError> {
        let mut bytes = [0; 1];
        self.read_exact(&mut bytes)?;
        Ok(bytes[0])
    }

    fn read_i8(&mut self) -> Result<i8, GgufError> {
        Ok(self.read_u8()? as i8)
    }

    fn read_u16(&mut self) -> Result<u16, GgufError> {
        let mut bytes = [0; 2];
        self.read_exact(&mut bytes)?;
        Ok(u16::from_le_bytes(bytes))
    }

    fn read_i16(&mut self) -> Result<i16, GgufError> {
        let mut bytes = [0; 2];
        self.read_exact(&mut bytes)?;
        Ok(i16::from_le_bytes(bytes))
    }

    fn read_u32(&mut self) -> Result<u32, GgufError> {
        let mut bytes = [0; 4];
        self.read_exact(&mut bytes)?;
        Ok(u32::from_le_bytes(bytes))
    }

    fn read_i32(&mut self) -> Result<i32, GgufError> {
        let mut bytes = [0; 4];
        self.read_exact(&mut bytes)?;
        Ok(i32::from_le_bytes(bytes))
    }

    fn read_u64(&mut self) -> Result<u64, GgufError> {
        let mut bytes = [0; 8];
        self.read_exact(&mut bytes)?;
        Ok(u64::from_le_bytes(bytes))
    }

    fn read_i64(&mut self) -> Result<i64, GgufError> {
        let mut bytes = [0; 8];
        self.read_exact(&mut bytes)?;
        Ok(i64::from_le_bytes(bytes))
    }

    fn read_f32(&mut self) -> Result<f32, GgufError> {
        Ok(f32::from_bits(self.read_u32()?))
    }

    fn read_f64(&mut self) -> Result<f64, GgufError> {
        Ok(f64::from_bits(self.read_u64()?))
    }

    fn read_string(&mut self, max_bytes: u64, field: &'static str) -> Result<String, GgufError> {
        let length = self.read_u64()?;
        if length > max_bytes {
            return Err(GgufError::InvalidLength {
                field,
                value: length,
            });
        }
        let length = usize::try_from(length).map_err(|_| GgufError::InvalidLength {
            field,
            value: u64::MAX,
        })?;
        let mut bytes = vec![0; length];
        self.read_exact(&mut bytes)?;
        String::from_utf8(bytes).map_err(|_| GgufError::InvalidUtf8 { field })
    }

    fn read_value(&mut self, value_type: u32, depth: usize) -> Result<GgufValue, GgufError> {
        match value_type {
            GGUF_TYPE_UINT8 => Ok(GgufValue::U8(self.read_u8()?)),
            GGUF_TYPE_INT8 => Ok(GgufValue::I8(self.read_i8()?)),
            GGUF_TYPE_UINT16 => Ok(GgufValue::U16(self.read_u16()?)),
            GGUF_TYPE_INT16 => Ok(GgufValue::I16(self.read_i16()?)),
            GGUF_TYPE_UINT32 => Ok(GgufValue::U32(self.read_u32()?)),
            GGUF_TYPE_INT32 => Ok(GgufValue::I32(self.read_i32()?)),
            GGUF_TYPE_FLOAT32 => Ok(GgufValue::F32(self.read_f32()?)),
            GGUF_TYPE_BOOL => {
                let value = self.read_u8()?;
                match value {
                    0 => Ok(GgufValue::Bool(false)),
                    1 => Ok(GgufValue::Bool(true)),
                    value => Err(GgufError::InvalidBool { value }),
                }
            }
            GGUF_TYPE_STRING => Ok(GgufValue::String(
                self.read_string(MAX_STRING_BYTES, "string")?,
            )),
            GGUF_TYPE_ARRAY => {
                if depth >= MAX_ARRAY_DEPTH {
                    return Err(GgufError::ExcessiveNesting);
                }
                let element_type = self.read_u32()?;
                let count = self.read_u64()?;
                if count > MAX_ARRAY_ITEMS {
                    return Err(GgufError::InvalidCount {
                        field: "array",
                        value: count,
                    });
                }
                let count = usize::try_from(count).map_err(|_| GgufError::InvalidCount {
                    field: "array",
                    value: u64::MAX,
                })?;
                let mut values = Vec::with_capacity(count);
                for _ in 0..count {
                    values.push(self.read_value(element_type, depth + 1)?);
                }
                Ok(GgufValue::Array {
                    value_type: element_type,
                    values,
                })
            }
            GGUF_TYPE_UINT64 => Ok(GgufValue::U64(self.read_u64()?)),
            GGUF_TYPE_INT64 => Ok(GgufValue::I64(self.read_i64()?)),
            GGUF_TYPE_FLOAT64 => Ok(GgufValue::F64(self.read_f64()?)),
            value_type => Err(GgufError::UnknownValueType { value_type }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn string_bytes(value: &str) -> Vec<u8> {
        let mut output = (value.len() as u64).to_le_bytes().to_vec();
        output.extend_from_slice(value.as_bytes());
        output
    }

    fn minimal_header() -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&GGUF_MAGIC.to_le_bytes());
        bytes.extend_from_slice(&3_u32.to_le_bytes());
        bytes.extend_from_slice(&2_u64.to_le_bytes());
        bytes.extend_from_slice(&4_u64.to_le_bytes());

        for (key, value_type, value) in [
            (
                "general.architecture",
                GGUF_TYPE_STRING,
                string_bytes("qwen35"),
            ),
            ("general.name", GGUF_TYPE_STRING, string_bytes("Test model")),
            (
                "qwen35.block_count",
                GGUF_TYPE_UINT32,
                42_u32.to_le_bytes().to_vec(),
            ),
            (
                "qwen35.expert_count",
                GGUF_TYPE_UINT32,
                8_u32.to_le_bytes().to_vec(),
            ),
        ] {
            bytes.extend_from_slice(&string_bytes(key));
            bytes.extend_from_slice(&value_type.to_le_bytes());
            bytes.extend(value);
        }
        bytes
    }

    #[test]
    fn parses_typed_metadata_and_stops_at_metadata() {
        let mut bytes = minimal_header();
        bytes.extend_from_slice(b"tensor data that must not be interpreted");
        let header = parse_gguf_reader(&mut Cursor::new(bytes)).expect("header");
        assert_eq!(header.version, 3);
        assert_eq!(header.tensor_count, 2);
        assert_eq!(header.string("general.architecture"), Some("qwen35"));
        assert_eq!(header.positive_u64("qwen35.block_count"), Some(42));
        assert_eq!(header.positive_u64("qwen35.expert_count"), Some(8));
    }

    #[test]
    fn rejects_bad_magic_and_truncation_with_typed_errors() {
        let mut bad_magic = minimal_header();
        bad_magic[..4].copy_from_slice(b"NOPE");
        assert!(matches!(
            parse_gguf_reader(&mut Cursor::new(bad_magic)),
            Err(GgufError::InvalidMagic { .. })
        ));

        let truncated = minimal_header();
        for length in 0..truncated.len() {
            let result = parse_gguf_reader(&mut Cursor::new(&truncated[..length]));
            if result.is_err() {
                assert!(matches!(result, Err(GgufError::Truncated { .. })));
            }
        }
    }

    #[test]
    fn parses_real_fixture_headers_when_available() {
        let fixtures = [
            (
                "/home/shulgin/twilight/target/qwen36-local-gate/Qwen3.6-27B-Q4_K_M.gguf",
                "Qwen3.6-27B",
            ),
            (
                "/home/shulgin/twilight/target/qwen36-local-gate/Qwen3.6-27B-MTP-Q4_K_M.gguf",
                "Qwen3.6-27B",
            ),
            (
                "/home/shulgin/twilight/target/ternary-bonsai27-gate/Ternary-Bonsai-27B-Q2_g64.gguf",
                "Bonsai-27B",
            ),
        ];
        for (path, name) in fixtures {
            let path = Path::new(path);
            if !path.is_file() {
                continue;
            }
            let header = parse_gguf_file(path).expect("real GGUF header");
            assert_eq!(header.string("general.name"), Some(name));
            assert_eq!(header.string("general.architecture"), Some("qwen35"));
            assert!(header.positive_u64("qwen35.block_count").is_some());
        }
    }
}
