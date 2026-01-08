use std::collections::HashMap;

use crate::common::value::Value;
use crate::exec::evaluator::{Evaluator, ExecError};
use crate::exec::operator::{Operator, Row};
use crate::ir::expr::Expr;

pub struct ProjectExec {
    input: Box<dyn Operator>,
    exprs: Vec<(Expr, String)>,
}

impl ProjectExec {
    pub fn new(input: Box<dyn Operator>, exprs: Vec<(Expr, String)>) -> Self {
        Self { input, exprs }
    }
}

impl Operator for ProjectExec {
    fn open(&mut self) -> Result<(), ExecError> {
        self.input.open()
    }

    fn next(&mut self) -> Result<Option<Row>, ExecError> {
        let row = match self.input.next()? {
            Some(r) => r,
            None => return Ok(None),
        };
        let ev = Evaluator::new(&row);

        let mut out = HashMap::new();

        for (expr, alias) in &self.exprs {
            let value = ev.eval_expr(expr)?.unwrap_or(Value::Null);
            out.insert(alias.clone(), value);
        }

        debug_assert!(
            out.keys().all(|k| !k.contains('.')),
            "Project output must be unqualified"
        );

        Ok(Some(Row {
            row_id: row.row_id,
            values: out,
        }))
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.input.close()
    }
}
