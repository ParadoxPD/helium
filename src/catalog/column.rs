use crate::catalog::ids::ColumnId;
use crate::types::datatype::DataType;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnMeta {
    pub id: ColumnId,
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}
