//! Logical data types understood by the system.
//!
//! These represent *semantic* types, not physical layout.

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum DataType {
    // Integer types
    Int32,
    Int64,

    // Floating point
    Float32,
    Float64,

    // Boolean
    Boolean,

    // Strings
    Varchar { max_len: Option<u32> },

    // Temporal
    Date,      // days since epoch
    Timestamp, // microseconds since epoch

    // Binary
    Blob,

    // Special
    Null,
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Int32 => write!(f, "INT"),
            DataType::Int64 => write!(f, "BIGINT"),
            DataType::Float32 => write!(f, "FLOAT"),
            DataType::Float64 => write!(f, "DOUBLE"),
            DataType::Boolean => write!(f, "BOOLEAN"),
            DataType::Varchar { .. } => write!(f, "VARCHAR"),
            DataType::Date => write!(f, "DATE"),
            DataType::Timestamp => write!(f, "TIMESTAMP"),
            DataType::Blob => write!(f, "BLOB"),
            DataType::Null => write!(f, "NULL"),
        }
    }
}
