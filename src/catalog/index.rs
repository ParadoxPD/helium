use crate::catalog::ids::{ColumnId, IndexId, TableId};

#[derive(Debug)]
pub struct IndexMeta {
    pub id: IndexId,
    pub name: String,
    pub table_id: TableId,
    pub column_ids: Vec<ColumnId>,
    pub unique: bool,
}
