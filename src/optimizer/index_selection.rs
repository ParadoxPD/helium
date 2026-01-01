use crate::{
    exec::catalog::Catalog,
    ir::{
        expr::{BinaryOp, Expr},
        plan::{Filter, IndexPredicate, IndexScan, LogicalPlan, Project},
    },
};

pub fn index_selection(plan: &LogicalPlan, catalog: &Catalog) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter(filter) => {
            // First, recursively optimize the input
            let optimized_input = index_selection(&filter.input, catalog);

            // We only care about Filter(Scan)
            let LogicalPlan::Scan(scan) = &optimized_input else {
                return LogicalPlan::Filter(Filter {
                    predicate: filter.predicate.clone(),
                    input: Box::new(optimized_input),
                });
            };

            // Match: table.column = literal
            if let Expr::Binary {
                left,
                op: BinaryOp::Eq,
                right,
            } = &filter.predicate
            {
                if let (Expr::BoundColumn { table, name }, Expr::Literal(value)) =
                    (&**left, &**right)
                {
                    // Table name must match
                    if table == &scan.table {
                        // Check catalog for index
                        if catalog.get_index(table, name).is_some() {
                            return LogicalPlan::IndexScan(IndexScan {
                                table: table.clone(),
                                column: name.clone(),
                                predicate: IndexPredicate::Eq(value.clone()),
                            });
                        }
                    }
                }
            }

            // Default: keep filter
            LogicalPlan::Filter(Filter {
                predicate: filter.predicate.clone(),
                input: Box::new(optimized_input),
            })
        }

        // Recurse into other nodes
        LogicalPlan::Project(p) => LogicalPlan::Project(Project {
            exprs: p.exprs.clone(),
            input: Box::new(index_selection(&p.input, catalog)),
        }),

        _ => plan.clone(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use crate::buffer::buffer_pool::BufferPool;
    use crate::common::value::Value;
    use crate::exec::catalog::Catalog;
    use crate::ir::expr::{BinaryOp, Expr};
    use crate::ir::plan::{Filter, IndexScan, LogicalPlan, Scan};
    use crate::optimizer::index_selection::index_selection;
    use crate::storage::btree::DiskBPlusTree;
    use crate::storage::page_manager::FilePageManager;
    use crate::storage::table::HeapTable;

    #[test]
    fn filter_on_indexed_column_becomes_indexscan() {
        let path = format!("/tmp/db_{}.db", rand::random::<u64>());

        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open(&path).unwrap(),
        ))));

        // ---- heap table (no indexes!) ----
        let table = HeapTable::new(
            "users".into(),
            vec!["id".into(), "age".into()],
            4,
            bp.clone(),
        );

        // ---- disk index ----
        let index = Arc::new(Mutex::new(DiskBPlusTree::new(4, bp.clone())));

        // ---- catalog ----
        let mut catalog = Catalog::new();
        catalog.insert("users".into(), Arc::new(table));
        catalog.add_index("a".into(), "users".into(), "age".into(), index);

        // ---- logical plan ----
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
        let path = format!("/tmp/db_{}.db", rand::random::<u64>());

        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open(&path).unwrap(),
        ))));

        // ---- heap table WITHOUT index ----
        let table = HeapTable::new(
            "users".into(),
            vec!["id".into(), "age".into()],
            4,
            bp.clone(),
        );

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

        assert!(matches!(optimized, LogicalPlan::Filter(_)));
    }
}
