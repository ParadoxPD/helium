use std::sync::Arc;

use crate::{
    exec::operator::{Operator, Row},
    frontend::sql::binder::Column,
    ir::expr::Expr,
    storage::{
        page::StorageRow,
        table::{HeapTable, Table},
    },
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
        let row = self.input.next()?;

        let mut values = row.values.clone();

        for (col, expr) in &self.assignments {
            let v = expr.eval(&row.values)?;
            values.insert(col.name.clone(), v);
        }

        let new_rid = self
            .table
            .insert(StorageRow::new(values.into_values().collect()));
        self.table.delete(row.row_id);
        let old_rid = row.row_id();
        let new_rid = self.table.insert(new_row.to_storage_row());
        self.table.delete(old_rid);

        Some(Row::from_row_id(new_rid))
    }

    fn close(&mut self) {
        self.input.close();
    }
}
