#[derive(Debug)]
pub enum CatalogError {
    TableExists(String),
    TableNotFound(String),
    IndexExists(String),
    IndexNotFound(String),
}
