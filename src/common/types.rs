use core::fmt;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum DataType {
    Int64,
    Float64,
    Bool,
    String,
    Null,
    Unknown,
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Int64 => write!(f, "Integer"),
            DataType::Float64 => write!(f, "Float"),
            DataType::Bool => write!(f, "Boolean"),
            DataType::String => write!(f, "String"),
            DataType::Null => write!(f, "Null"),
            DataType::Unknown => write!(f, "Unknown Type !!!!"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn datatype_equality() {
        assert_eq!(DataType::Int64, DataType::Int64);
        assert_ne!(DataType::Int64, DataType::Bool);
    }
}
