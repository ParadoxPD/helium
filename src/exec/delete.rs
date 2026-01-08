use std::sync::Arc;

use crate::{
    exec::{
        evaluator::ExecError,
        operator::{Operator, Row},
    },
    storage::table::HeapTable,
};

pub struct DeleteExec {
    input: Box<dyn Operator>,
    table: Arc<HeapTable>,
}

impl Operator for DeleteExec {
    fn open(&mut self) -> Result<(), ExecError> {
        self.input.open()
    }

    fn next(&mut self) -> Result<Option<Row>, ExecError> {
        let row = match self.input.next()? {
            Some(r) => r,
            None => return Ok(None),
        };
        let rid = row.row_id;
        self.table.delete(rid);
        Ok(None)
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.input.close()
    }
}
