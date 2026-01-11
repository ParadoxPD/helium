use crate::types::value::Value;

#[derive(Clone, Debug, PartialEq)]
pub enum IndexPredicate {
    Eq(Value),
    Range { low: Value, high: Value },
}
