use crate::types::schema::{Column, Schema};

impl Schema {
    pub fn has_column_named(&self, name: &str) -> bool {
        self.columns.iter().any(|c| c.name == name)
    }

    pub fn column_named(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }

    pub fn column_index_named(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }
}
