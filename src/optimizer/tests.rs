#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use crate::buffer::buffer_pool::{BufferPool, BufferPoolHandle};
    use crate::common::types::DataType;
    use crate::common::value::Value;
    use crate::ir::expr::{BinaryOp, Expr};
    use crate::ir::plan::{Filter, IndexScan, LogicalPlan, Scan};
    use crate::ir::pretty::pretty;
    use crate::optimizer::constant_fold::fold_expr;
    use crate::optimizer::{
        constant_fold::constant_fold, index_selection::index_selection,
        predicate_pushdown::predicate_pushdown, projection_prune::projection_prune,
    };

    use crate::common::schema::{Column, Schema};
    use crate::exec::catalog::Catalog;
    use crate::storage::btree::node::{Index, IndexKey};
    use crate::storage::page::RowId;
    use crate::storage::page_manager::FilePageManager;

    struct DummyIndex;

    impl Index for DummyIndex {
        fn insert(&mut self, _key: IndexKey, _rid: RowId) {
            panic!("DummyIndex::insert called — optimizer-only index was executed");
        }

        fn delete(&mut self, _key: &IndexKey, _rid: RowId) {
            panic!("DummyIndex::delete called — optimizer-only index was executed");
        }

        fn get(&self, _key: &IndexKey) -> Vec<RowId> {
            todo!()
        }

        fn range(&self, _from: &IndexKey, _to: &IndexKey) -> Vec<RowId> {
            todo!()
        }
    }

    /// Create a dummy index for optimizer tests
    pub fn dummy_index() -> Arc<Mutex<dyn Index>> {
        Arc::new(Mutex::new(DummyIndex))
    }

    fn dummy_buffer_pool() -> BufferPoolHandle {
        let path = format!("/tmp/helium_test_{}.db", rand::random::<u64>());
        Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open(&path.into()).unwrap(),
        ))))
    }
    fn test_catalog_with_index(has_index: bool) -> Catalog {
        let mut catalog = Catalog::new();

        let schema = Schema {
            columns: vec![
                Column {
                    name: "id".into(),
                    ty: DataType::Int64,
                    nullable: true,
                },
                Column {
                    name: "age".into(),
                    ty: DataType::Int64,
                    nullable: true,
                },
            ],
        };

        // NOTE: buffer pool is irrelevant for optimizer tests
        let bp = dummy_buffer_pool();

        catalog.create_table("users".into(), schema, bp).unwrap();

        if has_index {
            // fake index entry – optimizer only checks metadata
            catalog.add_index(
                "idx_age".into(),
                "users".into(),
                "age".into(),
                dummy_index(),
            );
        }

        catalog
    }

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
            Expr::bound_col("users", "age"),
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
    fn constant_fold_is_idempotent() {
        let plan = LogicalPlan::scan("users", "u").filter(Expr::bin(
            Expr::lit(Value::Int64(1)),
            BinaryOp::Eq,
            Expr::lit(Value::Int64(1)),
        ));

        let once = constant_fold(&plan);
        let twice = constant_fold(&once);

        assert_eq!(pretty(&once), pretty(&twice));
    }

    #[test]
    fn filter_on_indexed_column_becomes_indexscan() {
        let catalog = test_catalog_with_index(true);

        let scan = LogicalPlan::Scan(Scan {
            table: "users".into(),
            alias: "users".into(),
            columns: vec!["id".into(), "age".into()],
        });

        let filter = LogicalPlan::Filter(Filter {
            predicate: Expr::bin(
                Expr::bound_col("users", "age"),
                BinaryOp::Eq,
                Expr::lit(Value::Int64(20)),
            ),
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
        let catalog = test_catalog_with_index(false);

        let scan = LogicalPlan::scan("users", "users");
        let filter = scan.filter(Expr::bin(
            Expr::bound_col("users", "age"),
            BinaryOp::Eq,
            Expr::lit(Value::Int64(20)),
        ));

        let optimized = index_selection(&filter, &catalog);

        assert!(matches!(optimized, LogicalPlan::Filter(_)));
    }

    #[test]
    fn pushes_filter_below_project() {
        let plan = LogicalPlan::scan("users", "u")
            .project(vec![(Expr::bound_col("u", "name"), "name")])
            .filter(Expr::bin(
                Expr::bound_col("u", "age"),
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
            Expr::bound_col("u", "age"),
            BinaryOp::Gt,
            Expr::lit(Value::Int64(18)),
        ));

        let optimized = predicate_pushdown(&plan);

        let expected = r#"
Filter (age Gt 18)
└─ Limit 10
   └─ Scan users
"#;

        assert_eq!(pretty(&optimized).trim(), expected.trim());
    }

    #[test]
    fn predicate_pushdown_is_idempotent() {
        let plan = LogicalPlan::scan("users", "u")
            .project(vec![(Expr::bound_col("u", "name"), "name")])
            .filter(Expr::bin(
                Expr::bound_col("u", "age"),
                BinaryOp::Gt,
                Expr::lit(Value::Int64(18)),
            ));

        let once = predicate_pushdown(&plan);
        let twice = predicate_pushdown(&once);

        assert_eq!(pretty(&once), pretty(&twice));
    }

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
                (Expr::bound_col("u", "name"), "name"),
                (Expr::bound_col("u", "age"), "age"),
            ])
            .filter(Expr::bin(
                Expr::bound_col("u", "age"),
                BinaryOp::Gt,
                Expr::lit(Value::Int64(30)),
            ))
            .project(vec![(Expr::bound_col("u", "name"), "name")]);

        let optimized = projection_prune(&plan);

        let output = pretty(&optimized);
        assert!(output.contains("age"));
    }

    #[test]
    fn projection_prune_is_idempotent() {
        let plan = LogicalPlan::scan("users", "u").project(vec![
            (Expr::bound_col("u", "name"), "name"),
            (Expr::bound_col("u", "age"), "age"),
        ]);

        let once = projection_prune(&plan);
        let twice = projection_prune(&once);

        assert_eq!(pretty(&once), pretty(&twice));
    }
}
