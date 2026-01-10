use crate::{
    storage::errors::{StorageError, StorageResult},
    types::value::Value,
};

/// A total-orderable key usable by B+Tree.
/// NO NULLs allowed.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum IndexKey {
    Int(i64),
    Bool(bool),
    String(String),
}

impl TryFrom<&Value> for IndexKey {
    type Error = &'static str;

    fn try_from(v: &Value) -> Result<Self, Self::Error> {
        match v {
            Value::Int64(v) => Ok(IndexKey::Int(*v)),
            Value::Boolean(v) => Ok(IndexKey::Bool(*v)),
            Value::String(v) => Ok(IndexKey::String(v.clone())),
            Value::Null => Err("NULL cannot be indexed"),
            _ => Err("unsupported value type for index"),
        }
    }
}

impl IndexKey {
    pub fn serialize(&self, buf: &mut Vec<u8>) {
        match self {
            IndexKey::Int(v) => {
                buf.push(0);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            IndexKey::Bool(v) => {
                buf.push(1);
                buf.push(*v as u8);
            }
            IndexKey::String(s) => {
                buf.push(2);
                let bytes = s.as_bytes();
                buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(bytes);
            }
        }
    }

    pub fn deserialize(input: &mut &[u8], page_id: u64) -> StorageResult<IndexKey> {
        // Read tag
        let tag = *input.get(0).ok_or(StorageError::IndexCorrupted {
            page_id,
            reason: "unexpected EOF reading IndexKey tag".into(),
        })?;
        *input = &input[1..];

        match tag {
            // -------- Int64 --------
            0 => {
                let bytes = input.get(..8).ok_or(StorageError::IndexCorrupted {
                    page_id,
                    reason: "unexpected EOF reading i64 IndexKey".into(),
                })?;
                *input = &input[8..];

                Ok(IndexKey::Int(i64::from_le_bytes(
                    bytes.try_into().unwrap(), // length guaranteed
                )))
            }

            // -------- Bool --------
            1 => {
                let v = *input.get(0).ok_or(StorageError::IndexCorrupted {
                    page_id,
                    reason: "unexpected EOF reading bool IndexKey".into(),
                })?;
                *input = &input[1..];

                Ok(IndexKey::Bool(v != 0))
            }

            // -------- String --------
            2 => {
                let len_bytes = input.get(..4).ok_or(StorageError::IndexCorrupted {
                    page_id,
                    reason: "unexpected EOF reading string length".into(),
                })?;
                let len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;

                *input = &input[4..];

                let str_bytes = input.get(..len).ok_or(StorageError::IndexCorrupted {
                    page_id,
                    reason: "unexpected EOF reading string bytes".into(),
                })?;
                *input = &input[len..];

                let s = String::from_utf8(str_bytes.to_vec()).map_err(|_| {
                    StorageError::IndexCorrupted {
                        page_id,
                        reason: "invalid UTF-8 in IndexKey::String".into(),
                    }
                })?;

                Ok(IndexKey::String(s))
            }

            // -------- Invalid Tag --------
            _ => Err(StorageError::IndexCorrupted {
                page_id,
                reason: format!("invalid IndexKey tag {}", tag),
            }),
        }
    }
}
