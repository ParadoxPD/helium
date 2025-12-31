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

impl Value {
    pub fn serialize(&self, buf: &mut Vec<u8>) {
        match self {
            Value::Null => {
                buf.push(0);
            }

            Value::Int64(v) => {
                buf.push(1);
                buf.extend_from_slice(&v.to_le_bytes());
            }

            Value::Float64(v) => {
                buf.push(2);
                buf.extend_from_slice(&v.to_le_bytes());
            }

            Value::Bool(v) => {
                buf.push(3);
                buf.push(if *v { 1 } else { 0 });
            }

            Value::String(s) => {
                buf.push(4);
                let bytes = s.as_bytes();
                let len = bytes.len() as u32;
                buf.extend_from_slice(&len.to_le_bytes());
                buf.extend_from_slice(bytes);
            }
        }
    }

    pub fn deserialize(buf: &mut &[u8]) -> Self {
        assert!(
            !buf.is_empty(),
            "buffer underflow while deserializing Value"
        );

        let tag = buf[0];
        *buf = &buf[1..];

        match tag {
            0 => Value::Null,

            1 => {
                let (num, rest) = buf.split_at(8);
                *buf = rest;
                Value::Int64(i64::from_le_bytes(num.try_into().unwrap()))
            }

            2 => {
                let (num, rest) = buf.split_at(8);
                *buf = rest;
                Value::Float64(f64::from_le_bytes(num.try_into().unwrap()))
            }

            3 => {
                let v = buf[0] != 0;
                *buf = &buf[1..];
                Value::Bool(v)
            }

            4 => {
                let (len_bytes, rest) = buf.split_at(4);
                let len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;

                let (str_bytes, rest2) = rest.split_at(len);
                *buf = rest2;

                let s =
                    String::from_utf8(str_bytes.to_vec()).expect("invalid UTF-8 string in Value");
                Value::String(s)
            }

            _ => panic!("unknown Value tag {}", tag),
        }
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

    #[test]
    fn value_roundtrip() {
        let values = vec![
            Value::Null,
            Value::Int64(42),
            Value::Float64(3.14),
            Value::Bool(true),
            Value::Bool(false),
            Value::String("hello".into()),
        ];

        for v in values {
            let mut buf = Vec::new();
            v.serialize(&mut buf);

            let mut slice = buf.as_slice();
            let v2 = Value::deserialize(&mut slice);

            assert_eq!(v, v2);
            assert!(slice.is_empty());
        }
    }
}
