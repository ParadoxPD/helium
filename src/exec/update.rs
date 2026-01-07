use std::{collections::HashMap, sync::Arc};

use crate::{
    common::{schema::Column, value::Value},
    exec::{
        evaluator::Evaluator,
        operator::{Operator, Row},
    },
    ir::expr::Expr,
    storage::table::HeapTable,
};

pub struct UpdateExec {
    input: Box<dyn Operator>,
    table: Arc<HeapTable>,
    assignments: Vec<(Column, Expr)>,
}

impl Operator for UpdateExec {
    fn open(&mut self) {
        self.input.open();
    }

    fn next(&mut self) -> Option<Row> {
        let row = self.input.next()?; // execution Row (qualified keys)
        let ev = Evaluator::new(&row);

        // 1. Start from existing fully-qualified values
        let mut updated = row.values.clone();

        // 2. Apply assignments USING fully-qualified keys
        for (col, expr) in &self.assignments {
            let v = ev.eval_expr(expr)?.unwrap_or(Value::Null);

            let fq = format!("{}.{}", self.table.name, col.name);
            updated.insert(fq, v);
        }

        // 3. Convert HashMap -> Vec<Value> in schema order (fully-qualified ONLY)
        let mut values = Vec::with_capacity(self.table.schema().columns.len());

        for col in &self.table.schema().columns {
            let fq = format!("{}.{}", self.table.name, col.name);
            values.push(updated.get(&fq).cloned().unwrap_or(Value::Null));
        }

        // 4. Copy-on-write: insert new row, delete old
        let new_rid = self.table.insert(values);
        self.table.delete(row.row_id);

        // 5. Emit execution Row (projection rebuilds values)
        Some(Row {
            row_id: new_rid,
            values: HashMap::new(),
        })
    }
    fn close(&mut self) {
        self.input.close();
    }
}
