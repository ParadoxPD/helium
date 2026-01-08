//! Runtime values flowing through execution.
//!
//! This is NOT a SQL literal representation.
//! This is the canonical runtime value model.

use crate::types::datatype::DataType;

#[derive(Debug, Clone, PartialEq)]
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
            Value::Bool(v) => write!(f, "{v}"),
            Value::String(v) => write!(f, "{v}"),
            _ => write!(f, "<value>"),
        }
    }
}
