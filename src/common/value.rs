use std::fmt;

use crate::common::types::DataType;

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Int64(i64),
    Float64(f64),
    Bool(bool),
    String(String),
    Null,
}

impl Value {
    pub fn data_type(&self) -> Option<DataType> {
        match self {
            Value::Int64(_) => Some(DataType::Int64),
            Value::Float64(_) => Some(DataType::Float64),
            Value::Bool(_) => Some(DataType::Bool),
            Value::String(_) => Some(DataType::String),
            Value::Null => None,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Int64(v) => write!(f, "{v}"),
            Value::Float64(v) => write!(f, "{v}"),
            Value::Bool(v) => write!(f, "{v}"),
            Value::String(v) => write!(f, "\"{v}\""),
            Value::Null => write!(f, "NULL"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_datatype_mapping() {
        assert_eq!(Value::Int64(10).data_type(), Some(DataType::Int64));

        assert_eq!(Value::Bool(true).data_type(), Some(DataType::Bool));

        assert_eq!(Value::Null.data_type(), None);
    }

    #[test]
    fn null_detection() {
        assert!(Value::Null.is_null());
        assert!(!Value::Int64(1).is_null());
    }

    #[test]
    fn value_display() {
        let v = Value::String("hello".into());
        assert_eq!(format!("{v}"), "\"hello\"");
    }
}
