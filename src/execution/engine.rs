use crate::api::errors::{MutationKind, MutationResult, QueryResult};
use crate::execution::context::ExecutionContext;
use crate::execution::errors::{ExecutionError, ExecutionResult, ExecutionResultType};
use crate::execution::executor::{ExecResult, Executor};
use crate::execution::operators::delete::DeleteExecutor;
use crate::execution::operators::filter::FilterExecutor;
use crate::execution::operators::index_scan::IndexScanExecutor;
use crate::execution::operators::insert::InsertExecutor;
use crate::execution::operators::join::JoinExecutor;
use crate::execution::operators::limit::LimitExecutor;
use crate::execution::operators::project::ProjectExecutor;
use crate::execution::operators::scan::ScanExecutor;
use crate::execution::operators::sort::SortExecutor;
use crate::execution::operators::update::UpdateExecutor;
use crate::ir::plan::LogicalPlan;

pub fn execute_plan(plan: LogicalPlan, ctx: &mut ExecutionContext) -> ExecutionResultType {
    match plan {
        LogicalPlan::Scan { .. }
        | LogicalPlan::Filter { .. }
        | LogicalPlan::Project { .. }
        | LogicalPlan::Sort { .. }
        | LogicalPlan::Limit { .. }
        | LogicalPlan::Join { .. }
        | LogicalPlan::IndexScan { .. } => execute_query(plan, ctx),

        LogicalPlan::Insert { .. } | LogicalPlan::Update { .. } | LogicalPlan::Delete { .. } => {
            execute_mutation(plan, ctx)
        }
    }
}

pub fn execute_query(plan: LogicalPlan, ctx: &mut ExecutionContext) -> ExecutionResultType {
    let schema = plan_output_schema(&plan, &ctx.catalog)?;

    let mut root = build_executor(plan, ctx)?;
    root.open(ctx)?;

    let mut rows = Vec::new();

    while let Some(row) = root.next(ctx)? {
        ctx.stats.rows_output += 1;
        rows.push(row);
    }

    let _ = root.close(ctx)?;

    Ok(ExecutionResult::Query(QueryResult {
        schema,
        rows,
        stats: ctx.stats.clone(),
    }))
}

pub fn execute_mutation(plan: LogicalPlan, ctx: &mut ExecutionContext) -> ExecutionResultType {
    let kind = match &plan {
        LogicalPlan::Insert { .. } => MutationKind::Insert,
        LogicalPlan::Update { .. } => MutationKind::Update,
        LogicalPlan::Delete { .. } => MutationKind::Delete,
        _ => unreachable!(),
    };

    let mut exec = build_executor(plan, ctx)?;
    exec.open(ctx)?;

    while exec.next(ctx)?.is_some() {}

    let per_table = exec.close(ctx)?;

    let rows_affected = per_table.iter().map(|t| t.rows_affected).sum();

    Ok(ExecutionResult::Mutation(MutationResult {
        kind,
        rows_affected,
        per_table,
        stats: ctx.stats.clone(),
    }))
}

pub fn build_executor(
    plan: LogicalPlan,
    ctx: &mut ExecutionContext,
) -> ExecResult<Box<dyn Executor>> {
    Ok(match plan {
        LogicalPlan::Scan { table_id } => Box::new(ScanExecutor::new(table_id)),

        LogicalPlan::IndexScan {
            table_id,
            index_id,
            predicate,
        } => {
            let _table = ctx
                .catalog
                .get_table_by_id(table_id)
                .ok_or(ExecutionError::TableNotFound { table_id })?;

            let index = ctx
                .catalog
                .get_index_by_id(index_id)
                .ok_or(ExecutionError::IndexNotFound { index_id })?;

            let heap = ctx.get_heap(table_id)?;

            Box::new(IndexScanExecutor::new(index.index.clone(), heap, predicate))
        }

        LogicalPlan::Filter { input, predicate } => {
            Box::new(FilterExecutor::new(build_executor(*input, ctx)?, predicate))
        }

        LogicalPlan::Project { input, exprs } => {
            Box::new(ProjectExecutor::new(build_executor(*input, ctx)?, exprs))
        }

        LogicalPlan::Sort { input, keys } => {
            Box::new(SortExecutor::new(build_executor(*input, ctx)?, keys))
        }

        LogicalPlan::Limit {
            input,
            limit,
            offset,
        } => Box::new(LimitExecutor::new(
            build_executor(*input, ctx)?,
            limit,
            offset,
        )),

        LogicalPlan::Join {
            left,
            right,
            on,
            join_type,
        } => Box::new(JoinExecutor::new(
            build_executor(*left, ctx)?,
            build_executor(*right, ctx)?,
            on,
            join_type,
        )),

        LogicalPlan::Insert { table_id, rows } => Box::new(InsertExecutor::new(table_id, rows)),

        LogicalPlan::Update {
            table_id,
            assignments,
            predicate,
        } => Box::new(UpdateExecutor::new(table_id, assignments, predicate)),

        LogicalPlan::Delete {
            table_id,
            predicate,
        } => Box::new(DeleteExecutor::new(table_id, predicate)),
    })
}

fn plan_output_schema(
    plan: &LogicalPlan,
    catalog: &crate::catalog::catalog::Catalog,
) -> ExecResult<crate::types::schema::Schema> {
    use crate::types::schema::Schema;

    match plan {
        LogicalPlan::Scan { table_id } => {
            let table =
                catalog
                    .get_table_by_id(*table_id)
                    .ok_or(ExecutionError::TableNotFound {
                        table_id: *table_id,
                    })?;
            Ok(table.schema.clone())
        }

        LogicalPlan::Project { input: _, exprs } => {
            let mut schema = Schema::new();
            for (idx, _expr) in exprs.iter().enumerate() {
                use crate::catalog::column::ColumnMeta;
                use crate::catalog::ids::ColumnId;
                use crate::types::datatype::DataType;

                schema.push(ColumnMeta {
                    id: ColumnId(idx as u32),
                    name: format!("col_{}", idx),
                    data_type: DataType::Int64,
                    nullable: true,
                });
            }
            Ok(schema)
        }

        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. } => plan_output_schema(input, catalog),

        _ => {
            let schema = Schema::new();
            Ok(schema)
        }
    }
}

