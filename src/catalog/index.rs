use std::sync::{Arc, Mutex};

use crate::{
    catalog::ids::{ColumnId, IndexId, TableId},
    storage::index::index::Index,
};

pub struct IndexEntry {
    pub meta: IndexMeta,
    pub index: Arc<Mutex<dyn Index>>,
}

#[derive(Debug)]
pub struct IndexMeta {
    pub id: IndexId,
    pub name: String,
    pub table_id: TableId,
    pub column_ids: Vec<ColumnId>,
    pub unique: bool,
}
