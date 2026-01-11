use core::fmt;

use crate::storage::errors::StorageError;

#[derive(Debug)]
pub enum CatalogError {
    TableExists(String),
    TableNotFound(String),
    IndexExists(String),
    IndexNotFound(String),
    Storage(StorageError),
}

impl From<StorageError> for CatalogError {
    fn from(e: StorageError) -> Self {
        CatalogError::Storage(e)
    }
}
impl fmt::Display for CatalogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CatalogError::TableExists(_) => todo!(),
            CatalogError::TableNotFound(_) => todo!(),
            CatalogError::IndexExists(_) => todo!(),
            CatalogError::IndexNotFound(_) => todo!(),
            CatalogError::Storage(storage_error) => todo!(),
        }
    }
}
impl std::error::Error for CatalogError {}
