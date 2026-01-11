use crate::{catalog::catalog::Catalog, ir::plan::LogicalPlan};

#[derive(Debug, Clone, Copy)]
pub struct Cost {
    pub cpu: u64,
    pub io: u64,
}

impl Cost {
    pub fn total(&self) -> u64 {
        self.cpu + self.io
    }
}

pub fn estimate_cost(plan: &LogicalPlan, catalog: &Catalog) -> Cost {
    match plan {
        LogicalPlan::Scan { table_id } => {
            let rows = catalog.table_stats(*table_id).row_count;
            Cost {
                cpu: rows,
                io: rows,
            }
        }

        LogicalPlan::IndexScan { .. } => Cost { cpu: 10, io: 5 },

        LogicalPlan::Filter { input, .. } => {
            let c = estimate_cost(input, catalog);
            Cost {
                cpu: c.cpu + 10,
                io: c.io,
            }
        }

        LogicalPlan::Join { left, right, .. } => {
            let l = estimate_cost(left, catalog);
            let r = estimate_cost(right, catalog);
            Cost {
                cpu: l.cpu * r.cpu,
                io: l.io + r.io,
            }
        }

        _ => Cost { cpu: 1, io: 1 },
    }
}
