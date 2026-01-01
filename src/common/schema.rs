use crate::common::types::DataType;

pub type Schema = Vec<Column>;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct Column {
    pub name: String,
    pub ty: DataType,
    pub nullable: bool,
}
