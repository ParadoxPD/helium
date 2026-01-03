use crate::common::types::DataType;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Schema {
    pub columns: Vec<Column>,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct Column {
    pub name: String,
    pub ty: DataType,
    pub nullable: bool,
}

impl Schema {
    pub fn new(column_names: Vec<String>) -> Self {
        let mut columns = Vec::new();
        for column_name in column_names {
            columns.push(Column {
                name: column_name,
                ty: DataType::Int64,
                nullable: true,
            })
        }
        Self { columns }
    }

    /// Check if a column exists by name
    pub fn has_column(&self, name: &str) -> bool {
        self.columns.iter().any(|c| c.name == name)
    }

    /// Borrowed lookup (preferred for binder)
    pub fn get_column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Index lookup (useful later for projection pruning, offsets, etc.)
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    /// Keep your existing owned lookup (still useful sometimes)
    pub fn lookup(&self, column_name: String) -> Option<Column> {
        self.columns.iter().find(|c| c.name == column_name).cloned()
    }
}
