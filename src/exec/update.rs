use std::{collections::HashMap, sync::Arc};

use crate::{
    common::{schema::Column, value::Value},
    exec::{
        expr_eval::eval_value,
        operator::{Operator, Row},
    },
    ir::expr::Expr,
    storage::{page::StorageRow, table::HeapTable},
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
        let row = self.input.next()?; // execution Row

        // 1. Start from existing values
        let mut updated = row.values.clone();

        // 2. Apply assignments using eval_value
        for (col, expr) in &self.assignments {
            let v = eval_value(expr, &row);
            updated.insert(col.name.clone(), v);
        }

        // 3. Convert HashMap -> Vec<Value> in schema order
        let mut values = Vec::with_capacity(self.table.schema().columns.len());
        for col in &self.table.schema().columns {
            values.push(
                updated
                    .get(&format!("{}.{}", self.table.name, col.name))
                    .or_else(|| updated.get(&col.name))
                    .cloned()
                    .unwrap_or(Value::Null),
            );
        }

        // 4. Copy-on-write: insert new row, delete old
        let new_rid = self.table.insert(values);
        self.table.delete(row.row_id);

        // 5. Return execution Row with new RowId
        Some(Row {
            row_id: new_rid,
            values: HashMap::new(), // projection will rebuild this
        })
    }

    fn close(&mut self) {
        self.input.close();
    }
}
