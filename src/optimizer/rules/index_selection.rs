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
