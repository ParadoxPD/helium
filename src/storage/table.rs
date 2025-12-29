use crate::exec::operator::Row;

pub trait Table: Send + Sync {
    fn scan(&self) -> Box<dyn TableCursor>;
}

pub trait TableCursor {
    fn next(&mut self) -> Option<Row>;
}
