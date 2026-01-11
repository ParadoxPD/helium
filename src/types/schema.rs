//! Schema definitions for tables and intermediate results.
//!
//! Schemas describe *shape*, not storage or ownership.

use crate::catalog::{column::ColumnMeta, ids::ColumnId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    pub columns: Vec<ColumnMeta>,
}

impl Schema {
    pub fn new() -> Self {
        Self {
            columns: Vec::new(),
        }
    }
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
    pub fn column(&self, id: ColumnId) -> Option<&ColumnMeta> {
        self.columns.iter().find(|c| c.id == id)
    }

    pub fn push(&mut self, column: ColumnMeta) {
        self.columns.push(column);
    }
}
