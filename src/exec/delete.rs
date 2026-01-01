use std::sync::Arc;

use crate::{
    exec::operator::{Operator, Row},
    storage::table::{HeapTable, Table},
};

pub struct DeleteExec {
    input: Box<dyn Operator>,
    table: Arc<HeapTable>,
}

impl Operator for DeleteExec {
    fn open(&mut self) {
        self.input.open();
    }

    fn next(&mut self) -> Option<Row> {
        let row = self.input.next()?;
        let rid = row;
        self.table.delete(rid);
        None
    }

    fn close(&mut self) {
        self.input.close();
    }
}
