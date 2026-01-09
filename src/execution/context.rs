use crate::catalog::catalog::Catalog;

pub struct ExecutionContext<'a> {
    pub catalog: &'a Catalog,
    // txn, memory, stats later
}
