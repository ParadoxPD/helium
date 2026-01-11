use crate::api::errors::{DefinitionResult, MutationKind, MutationResult, QueryResult};
use crate::execution::context::ExecutionContext;
use crate::execution::errors::{
    ExecutionError, ExecutionResult, ExecutionResultType, TableMutationStats,
};
use crate::execution::executor::{ExecResult, Executor, Row};
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
use crate::storage::heap::heap_table::HeapTable;

pub fn execute_plan(plan: LogicalPlan, ctx: &mut ExecutionContext) -> ExecutionResultType {
    match plan {
        // -------------------------
        // QUERY
        // -------------------------
        LogicalPlan::Scan { .. }
        | LogicalPlan::Filter { .. }
        | LogicalPlan::Project { .. }
        | LogicalPlan::Sort { .. }
        | LogicalPlan::Limit { .. }
        | LogicalPlan::Join { .. }
        | LogicalPlan::IndexScan { .. } => execute_query(plan, ctx),

        // -------------------------
        // DML
        // -------------------------
        LogicalPlan::Insert { .. } | LogicalPlan::Update { .. } | LogicalPlan::Delete { .. } => {
            execute_mutation(plan, ctx)
        }

        // -------------------------
        // DDL (future)
        // -------------------------
        _ => Err(ExecutionError::InvalidPlan {
            reason: "unsupported plan type".into(),
        }),
    }
}

pub fn execute_query(plan: LogicalPlan, ctx: &mut ExecutionContext) -> ExecutionResultType {
    let schema = plan.output_schema(&ctx.catalog)?;

    let mut root = build_executor(plan, ctx)?;
    root.open(ctx)?;

    let mut rows = Vec::new();

    while let Some(row) = root.next(ctx)? {
        ctx.stats.rows_output += 1;
        rows.push(row);
    }

    // Query executors return empty mutation stats
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

    // Drain executor (mutations usually return no rows)
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

pub fn build_executor(plan: LogicalPlan, ctx: &ExecutionContext) -> ExecResult<Box<dyn Executor>> {
    let table_meta = ctx.catalog.get_table_by_id(table_id)?;
    let heap = HeapTable::open(table_meta.id, ctx.buffer_pool.clone());

    Ok(match plan {
        LogicalPlan::Scan { table_id } => Box::new(ScanExecutor::new(table_id)),

        LogicalPlan::IndexScan {
            table_id,
            index_id,
            predicate,
        } => {
            let table = ctx
                .catalog
                .get_table_by_id(table_id)
                .ok_or(ExecutionError::TableNotFound { table_id })?;

            let index = ctx
                .catalog
                .get_index_by_id(index_id)
                .ok_or(ExecutionError::IndexNotFound { index_id })?;

            Box::new(IndexScanExecutor::new(heap, index.index.clone(), predicate))
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

        // -------------------------
        // DML AS EXECUTORS
        // -------------------------
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
