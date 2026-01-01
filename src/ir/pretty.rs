use crate::ir::plan::{Filter, Limit, LogicalPlan, Project, Scan};

/// Pretty-print a logical plan into a tree-like string.
pub fn pretty(plan: &LogicalPlan) -> String {
    let mut out = String::new();
    fmt_plan(plan, "", true, true, &mut out);
    out
}

fn fmt_plan(plan: &LogicalPlan, prefix: &str, is_last: bool, is_root: bool, out: &mut String) {
    if is_root {
        out.push_str(&node_label(plan));
        out.push('\n');
    } else {
        out.push_str(prefix);
        out.push_str(if is_last { "└─ " } else { "├─ " });
        out.push_str(&node_label(plan));
        out.push('\n');
    }

    let child_prefix = if is_root {
        String::new()
    } else if is_last {
        format!("{prefix}   ")
    } else {
        format!("{prefix}│  ")
    };

    match plan {
        LogicalPlan::Scan(_) => {}
        LogicalPlan::IndexScan(_) => {}
        LogicalPlan::Filter(Filter { input, .. }) => {
            fmt_plan(input, &child_prefix, true, false, out);
        }
        LogicalPlan::Project(Project { input, .. }) => {
            fmt_plan(input, &child_prefix, true, false, out);
        }
        LogicalPlan::Sort(sort) => {
            fmt_plan(&sort.input, &child_prefix, true, false, out);
        }
        LogicalPlan::Limit(Limit { input, .. }) => {
            fmt_plan(input, &child_prefix, true, false, out);
        }
        LogicalPlan::Join(join) => {
            fmt_plan(&join.left, &child_prefix, false, false, out);
            fmt_plan(&join.right, &child_prefix, true, false, out);
        }
    }
}

fn node_label(plan: &LogicalPlan) -> String {
    match plan {
        LogicalPlan::Scan(Scan { table, .. }) => {
            format!("Scan {table}")
        }
        LogicalPlan::IndexScan(i) => {
            format!("IndexScan(table={}, column={})", i.table, i.column)
        }

        LogicalPlan::Filter(Filter { predicate, .. }) => {
            format!("Filter ({predicate})")
        }
        LogicalPlan::Project(Project { exprs, .. }) => {
            let fields = exprs
                .iter()
                .map(|(_, alias)| alias.as_str())
                .collect::<Vec<_>>()
                .join(", ");

            format!("Project [{fields}]")
        }
        LogicalPlan::Sort(sort) => {
            let keys = sort
                .keys
                .iter()
                .map(|(e, asc)| {
                    if *asc {
                        format!("{e} ASC")
                    } else {
                        format!("{e} DESC")
                    }
                })
                .collect::<Vec<_>>()
                .join(", ");

            format!("Sort [{keys}]")
        }
        LogicalPlan::Limit(Limit { count, .. }) => {
            format!("Limit {count}")
        }
        LogicalPlan::Join(join) => {
            format!("Join ({})", join.on)
        }
    }
}
