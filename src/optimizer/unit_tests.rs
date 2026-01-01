#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::value::Value;
    use crate::ir::expr::{BinaryOp, Expr};
    use crate::ir::plan::LogicalPlan;
    use crate::ir::pretty::pretty;

    #[test]
    fn folds_simple_arithmetic() {
        let expr = Expr::bin(
            Expr::lit(Value::Int64(10)),
            BinaryOp::Add,
            Expr::lit(Value::Int64(20)),
        );

        let folded = fold_expr(&expr);
        assert_eq!(folded, Expr::lit(Value::Int64(30)));
    }

    #[test]
    fn folds_nested_arithmetic() {
        let expr = Expr::bin(
            Expr::bin(
                Expr::lit(Value::Int64(2)),
                BinaryOp::Mul,
                Expr::lit(Value::Int64(5)),
            ),
            BinaryOp::Add,
            Expr::lit(Value::Int64(1)),
        );

        let folded = fold_expr(&expr);
        assert_eq!(folded, Expr::lit(Value::Int64(11)));
    }

    #[test]
    fn does_not_fold_column_expressions() {
        let expr = Expr::bin(
            Expr::bound_col("t", "age"),
            BinaryOp::Add,
            Expr::lit(Value::Int64(1)),
        );

        let folded = fold_expr(&expr);
        assert_eq!(folded, expr);
    }

    #[test]
    fn folds_boolean_logic() {
        let expr = Expr::bin(
            Expr::lit(Value::Bool(true)),
            BinaryOp::And,
            Expr::lit(Value::Bool(false)),
        );

        let folded = fold_expr(&expr);
        assert_eq!(folded, Expr::lit(Value::Bool(false)));
    }

    #[test]
    fn folds_filter_predicate_in_plan() {
        let plan = LogicalPlan::scan("users", "u").filter(Expr::bin(
            Expr::bin(
                Expr::lit(Value::Int64(10)),
                BinaryOp::Gt,
                Expr::lit(Value::Int64(5)),
            ),
            BinaryOp::And,
            Expr::lit(Value::Bool(true)),
        ));

        let optimized = constant_fold(&plan);

        let expected = r#"
Filter (true)
└─ Scan users
"#;

        assert_eq!(pretty(&optimized).trim(), expected.trim());
    }

    #[test]
    fn optimizer_is_idempotent() {
        let plan = LogicalPlan::scan("users", "u").filter(Expr::bin(
            Expr::lit(Value::Int64(1)),
            BinaryOp::Eq,
            Expr::lit(Value::Int64(1)),
        ));

        let once = constant_fold(&plan);
        let twice = constant_fold(&once);

        assert_eq!(pretty(&once), pretty(&twice));
    }
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
    use super::*;
    use crate::common::value::Value;
    use crate::ir::expr::{BinaryOp, Expr};
    use crate::ir::plan::LogicalPlan;
    use crate::ir::pretty::pretty;

    #[test]
    fn pushes_filter_below_project() {
        let plan = LogicalPlan::scan("users", "u")
            .project(vec![(Expr::bound_col("t", "name"), "name")])
            .filter(Expr::bin(
                Expr::bound_col("t", "age"),
                BinaryOp::Gt,
                Expr::lit(Value::Int64(18)),
            ));

        let optimized = predicate_pushdown(&plan);

        let expected = r#"
Project [name]
└─ Filter (age Gt 18)
   └─ Scan users
"#;

        assert_eq!(pretty(&optimized).trim(), expected.trim());
    }

    #[test]
    fn does_not_push_filter_below_limit() {
        let plan = LogicalPlan::scan("users", "u").limit(10).filter(Expr::bin(
            Expr::bound_col("t", "age"),
            BinaryOp::Gt,
            Expr::lit(Value::Int64(18)),
        ));

        let optimized = predicate_pushdown(&plan);

        // Filter must stay above Limit
        let expected = r#"
Filter (age Gt 18)
└─ Limit 10
   └─ Scan users
"#;

        assert_eq!(pretty(&optimized).trim(), expected.trim());
    }

    #[test]
    fn preserves_non_pushable_structure() {
        let plan = LogicalPlan::scan("users", "u");

        let optimized = predicate_pushdown(&plan);

        assert_eq!(pretty(&optimized), pretty(&plan));
    }

    #[test]
    fn optimizer_is_idempotent() {
        let plan = LogicalPlan::scan("users", "u")
            .project(vec![(Expr::bound_col("t", "name"), "name")])
            .filter(Expr::bin(
                Expr::bound_col("t", "age"),
                BinaryOp::Gt,
                Expr::lit(Value::Int64(18)),
            ));

        let once = predicate_pushdown(&plan);
        let twice = predicate_pushdown(&once);

        assert_eq!(pretty(&once), pretty(&twice));
    }
    use super::*;
    use crate::common::value::Value;
    use crate::ir::expr::{BinaryOp, Expr};
    use crate::ir::plan::LogicalPlan;
    use crate::ir::pretty::pretty;

    #[test]
    fn prunes_unused_project_fields() {
        let plan = LogicalPlan::scan("users", "u")
            .project(vec![
                (Expr::bound_col("u", "name"), "name"),
                (Expr::bound_col("u", "city"), "city"),
                (Expr::bound_col("u", "age"), "age"),
            ])
            .filter(Expr::bin(
                Expr::bound_col("u", "age"),
                BinaryOp::Gt,
                Expr::lit(Value::Int64(18)),
            ))
            .project(vec![
                (Expr::bound_col("u", "name"), "name"),
                (Expr::bound_col("u", "city"), "city"),
            ]);

        let optimized = projection_prune(&plan);

        let expected = r#"
Project [name, city]
└─ Filter (age Gt 18)
   └─ Scan users
"#;

        assert_eq!(pretty(&optimized).trim(), expected.trim());
    }

    #[test]
    fn keeps_columns_used_in_filter() {
        let plan = LogicalPlan::scan("users", "u")
            .project(vec![
                (Expr::bound_col("t", "name"), "name"),
                (Expr::bound_col("t", "age"), "age"),
            ])
            .filter(Expr::bin(
                Expr::bound_col("t", "age"),
                BinaryOp::Gt,
                Expr::lit(Value::Int64(30)),
            ))
            .project(vec![(Expr::bound_col("t", "name"), "name")]);

        let optimized = projection_prune(&plan);

        let output = pretty(&optimized);

        assert!(output.contains("Scan users"));
        assert!(output.contains("age"));
    }

    #[test]
    fn idempotent_projection_prune() {
        let plan = LogicalPlan::scan("users", "u").project(vec![
            (Expr::bound_col("t", "name"), "name"),
            (Expr::bound_col("t", "age"), "age"),
        ]);

        let once = projection_prune(&plan);
        let twice = projection_prune(&once);

        assert_eq!(pretty(&once), pretty(&twice));
    }
}
