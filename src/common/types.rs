#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum DataType {
    Int64,
    Float64,
    Bool,
    String,
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
