use crate::{
    exec::Catalog,
    ir::{
        expr::{BinaryOp, Expr},
        plan::{IndexScan, LogicalPlan},
    },
};

pub fn index_selection(plan: &LogicalPlan, catalog: &Catalog) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter(f) => {
            if let LogicalPlan::Scan(s) = &*f.input {
                if let Expr::Binary {
                    left,
                    op: BinaryOp::Eq,
                    right,
                } = &f.predicate
                {
                    if let Expr::BoundColumn { table, name } = &**left {
                        if let Expr::Literal(v) = &**right {
                            if catalog[table].get_index(name).is_some() {
                                return LogicalPlan::IndexScan(IndexScan {
                                    table: s.table.clone(),
                                    alias: s.alias.clone(),
                                    column: name.clone(),
                                    value: v.clone(),
                                });
                            }
                        }
                    }
                }
            }
        }
        _ => {}
    }

    plan.clone()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::common::value::Value;
    use crate::exec::Catalog;
    use crate::ir::expr::{BinaryOp, Expr};
    use crate::ir::plan::{Filter, IndexScan, LogicalPlan, Scan};
    use crate::optimizer::index_selection::index_selection;
    use crate::storage::in_memory::InMemoryTable;

    #[test]
    fn filter_on_indexed_column_becomes_indexscan() {
        let mut table =
            InMemoryTable::new("users".into(), vec!["id".into(), "age".into()], Vec::new());

        table.create_index("age", 4);

        // 2. Build catalog
        let mut catalog: Catalog = Catalog::new();
        catalog.insert("users".into(), Arc::new(table));

        let scan = LogicalPlan::Scan(Scan {
            table: "users".into(),
            alias: "users".into(),
            columns: vec!["id".into(), "age".into()],
        });

        let filter = LogicalPlan::Filter(Filter {
            predicate: Expr::Binary {
                left: Box::new(Expr::BoundColumn {
                    table: "users".into(),
                    name: "age".into(),
                }),
                op: BinaryOp::Eq,
                right: Box::new(Expr::Literal(Value::Int64(20))),
            },
            input: Box::new(scan),
        });

        let optimized = index_selection(&filter, &catalog);

        match optimized {
            LogicalPlan::IndexScan(IndexScan { column, .. }) => {
                assert_eq!(column, "age");
            }
            _ => panic!("expected IndexScan"),
        }
    }

    #[test]
    fn filter_without_index_remains_filter() {
        let table = InMemoryTable::new("users".into(), vec!["id".into(), "age".into()], Vec::new());

        let mut catalog: Catalog = Catalog::new();
        catalog.insert("users".into(), Arc::new(table));

        let scan = LogicalPlan::Scan(Scan {
            table: "users".into(),
            alias: "users".into(),
            columns: vec!["id".into(), "age".into()],
        });

        let filter = LogicalPlan::Filter(Filter {
            predicate: Expr::Binary {
                left: Box::new(Expr::BoundColumn {
                    table: "users".into(),
                    name: "age".into(),
                }),
                op: BinaryOp::Eq,
                right: Box::new(Expr::Literal(Value::Int64(20))),
            },
            input: Box::new(scan),
        });

        let optimized = index_selection(&filter, &catalog);

        matches!(optimized, LogicalPlan::Filter(_));
    }
}
