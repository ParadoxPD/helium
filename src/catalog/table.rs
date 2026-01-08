use crate::catalog::column::ColumnMeta;
use crate::catalog::ids::{ColumnId, TableId};

#[derive(Debug)]
pub struct TableMeta {
    pub id: TableId,
    pub name: String,
    pub columns: Vec<ColumnMeta>,
}

impl TableMeta {
    pub fn column_by_name(&self, name: &str) -> Option<&ColumnMeta> {
        self.columns.iter().find(|c| c.name == name)
    }

    pub fn column_by_id(&self, id: ColumnId) -> Option<&ColumnMeta> {
        self.columns.iter().find(|c| c.id == id)
    }
}
