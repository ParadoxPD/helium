use std::collections::HashMap;

use crate::{
    binder::{bind_stmt::Binder, errors::BindError},
    catalog::{column::ColumnMeta, ids::ColumnId},
    frontend::sql::ast::FromItem,
    types::{datatype::DataType, schema::Schema},
};

impl Schema {
    pub fn has_column_named(&self, name: &str) -> bool {
        self.columns.iter().any(|c| c.name == name)
    }

    pub fn column_named(&self, name: &str) -> Option<&ColumnMeta> {
        self.columns.iter().find(|c| c.name == name)
    }

    pub fn column_index_named(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }
}

/// Column resolution scope.
/// Maps visible column names to ColumnId + DataType.
#[derive(Debug)]
pub struct ColumnScope {
    /// name -> (ColumnId, DataType)
    columns: HashMap<String, Vec<(ColumnId, DataType)>>,
}

impl ColumnScope {
    pub fn new() -> Self {
        Self {
            columns: HashMap::new(),
        }
    }

    pub fn add_column(
        &mut self,
        name: String,
        id: ColumnId,
        ty: DataType,
    ) -> Result<(), BindError> {
        self.columns.entry(name).or_default().push((id, ty));
        Ok(())
    }

    pub fn resolve(&self, name: &str) -> Result<(ColumnId, DataType), BindError> {
        match self.columns.get(name) {
            None => Err(BindError::UnknownColumn(name.to_string())),
            Some(cols) if cols.len() == 1 => Ok(cols[0].clone()),
            Some(_) => Err(BindError::AmbiguousColumn(name.to_string())),
        }
    }

    pub fn iter_columns(&self) -> impl Iterator<Item = (ColumnId, DataType)> + '_ {
        self.columns.values().flat_map(|v| v.iter().cloned())
    }
}

impl<'a> Binder<'a> {
    pub fn collect_tables(&mut self, from: &FromItem) -> Result<(), BindError> {
        match from {
            FromItem::Table { name, alias } => {
                let alias = alias.clone().unwrap_or_else(|| name.clone());
                self.catalog.insert(alias, name.clone());
            }
            FromItem::Join { left, right, .. } => {
                self.collect_tables(left)?;
                self.collect_tables(right)?;
            }
        }
        Ok(())
    }
}
