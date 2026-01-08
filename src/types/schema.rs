//! Schema definitions for tables and intermediate results.
//!
//! Schemas describe *shape*, not storage or ownership.

use crate::types::datatype::DataType;

pub type ColumnId = u16;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub id: ColumnId,
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    pub columns: Vec<Column>,
}

impl Schema {
    /// Number of columns in the schema.
    #[inline]
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Lookup by column id.
    pub fn column(&self, id: ColumnId) -> Option<&Column> {
        self.columns.iter().find(|c| c.id == id)
    }
}
