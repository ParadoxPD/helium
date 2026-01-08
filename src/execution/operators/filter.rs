use crate::exec::evaluator::{Evaluator, ExecError};
use crate::exec::operator::{Operator, Row};
use crate::ir::expr::Expr;

pub struct FilterExec {
    input: Box<dyn Operator>,
    predicate: Expr,
}

impl FilterExec {
    pub fn new(input: Box<dyn Operator>, predicate: Expr) -> Self {
        Self { input, predicate }
    }
}

impl Operator for FilterExec {
    fn open(&mut self) -> Result<(), ExecError> {
        println!("FilterExec.open()");
        self.input.open()
    }

    fn next(&mut self) -> Result<Option<Row>, ExecError> {
        while let Some(row) = self.input.next()? {
            let ev = Evaluator::new(&row);
            let passed = ev.eval_predicate(&self.predicate)?;
            if passed {
                return Ok(Some(row));
            } else {
                continue;
            }
        }

        Ok(None)
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.input.close()
    }
}
