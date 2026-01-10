//! Runtime values flowing through execution.
//!
//! This is NOT a SQL literal representation.
//! This is the canonical runtime value model.

use crate::types::datatype::DataType;

#[derive(Debug, Clone, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum Value {
    Int32(i32),
    Int64(i64),

    Float32(f32),
    Float64(f64),

    Boolean(bool),

    String(String),
    Blob(Vec<u8>),

    // Temporal (encoded, not formatted)
    Date(i32),
    Timestamp(i64),

    // Explicit NULL
    Null,
}

impl Value {
    /// Returns the logical type of the value.
    ///
    /// NOTE: This must stay trivial.
    pub fn data_type(&self) -> DataType {
        match self {
            Value::Int32(_) => DataType::Int32,
            Value::Int64(_) => DataType::Int64,
            Value::Float32(_) => DataType::Float32,
            Value::Float64(_) => DataType::Float64,
            Value::Boolean(_) => DataType::Boolean,
            Value::String(_) => DataType::Varchar { max_len: None },
            Value::Blob(_) => DataType::Blob,
            Value::Date(_) => DataType::Date,
            Value::Timestamp(_) => DataType::Timestamp,
            Value::Null => DataType::Null,
        }
    }

    /// Returns true if this value is NULL.
    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Int64(v) => write!(f, "{v}"),
            Value::Float64(v) => write!(f, "{v}"),
            Value::String(v) => write!(f, "{v}"),
            _ => write!(f, "<value>"),
        }
    }
}

impl Value {
    pub fn serialize(&self, buf: &mut Vec<u8>) {
        match self {
            Value::Int32(v) => {
                buf.push(0);
                buf.extend_from_slice(&v.to_le_bytes());
            }

            Value::Int64(v) => {
                buf.push(1);
                buf.extend_from_slice(&v.to_le_bytes());
            }

            Value::Float32(v) => {
                buf.push(2);
                buf.extend_from_slice(&v.to_le_bytes());
            }

            Value::Float64(v) => {
                buf.push(3);
                buf.extend_from_slice(&v.to_le_bytes());
            }

            Value::Boolean(v) => {
                buf.push(4);
                buf.push(*v as u8);
            }

            Value::String(s) => {
                buf.push(5);
                let bytes = s.as_bytes();
                buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(bytes);
            }

            Value::Blob(b) => {
                buf.push(6);
                buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
                buf.extend_from_slice(b);
            }

            Value::Date(d) => {
                buf.push(7);
                buf.extend_from_slice(&d.to_le_bytes());
            }

            Value::Timestamp(ts) => {
                buf.push(8);
                buf.extend_from_slice(&ts.to_le_bytes());
            }

            Value::Null => {
                buf.push(255);
            }
        }
    }
}

impl Value {
    pub fn deserialize(input: &mut &[u8]) -> Self {
        assert!(
            !input.is_empty(),
            "buffer underflow while deserializing Value"
        );

        let tag = input[0];
        *input = &input[1..];

        match tag {
            0 => {
                let (b, rest) = input.split_at(4);
                *input = rest;
                Value::Int32(i32::from_le_bytes(b.try_into().unwrap()))
            }

            1 => {
                let (b, rest) = input.split_at(8);
                *input = rest;
                Value::Int64(i64::from_le_bytes(b.try_into().unwrap()))
            }

            2 => {
                let (b, rest) = input.split_at(4);
                *input = rest;
                Value::Float32(f32::from_le_bytes(b.try_into().unwrap()))
            }

            3 => {
                let (b, rest) = input.split_at(8);
                *input = rest;
                Value::Float64(f64::from_le_bytes(b.try_into().unwrap()))
            }

            4 => {
                let v = input[0] != 0;
                *input = &input[1..];
                Value::Boolean(v)
            }

            5 => {
                let (len_bytes, rest) = input.split_at(4);
                let len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
                let (s, rest2) = rest.split_at(len);
                *input = rest2;
                Value::String(String::from_utf8(s.to_vec()).expect("invalid UTF-8 string"))
            }

            6 => {
                let (len_bytes, rest) = input.split_at(4);
                let len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
                let (b, rest2) = rest.split_at(len);
                *input = rest2;
                Value::Blob(b.to_vec())
            }

            7 => {
                let (b, rest) = input.split_at(4);
                *input = rest;
                Value::Date(i32::from_le_bytes(b.try_into().unwrap()))
            }

            8 => {
                let (b, rest) = input.split_at(8);
                *input = rest;
                Value::Timestamp(i64::from_le_bytes(b.try_into().unwrap()))
            }

            255 => Value::Null,

            _ => panic!("unknown Value tag {}", tag),
        }
    }
}
