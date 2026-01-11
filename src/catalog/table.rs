use crate::catalog::column::ColumnMeta;
use crate::catalog::ids::{ColumnId, IndexId, TableId};
use crate::storage::page::page_id::PageId;
use crate::types::schema::Schema;

#[derive(Debug)]
pub struct TableMeta {
    pub id: TableId,
    pub name: String,
    pub schema: Schema,
    pub root_page: Option<PageId>, // First page of heap
    pub index_ids: Vec<IndexId>,
}

impl TableMeta {
    pub fn column_by_name(&self, name: &str) -> Option<&ColumnMeta> {
        self.schema.columns.iter().find(|c| c.name == name)
    }

    pub fn column_by_id(&self, id: ColumnId) -> Option<&ColumnMeta> {
        self.schema.columns.iter().find(|c| c.id == id)
    }
}

