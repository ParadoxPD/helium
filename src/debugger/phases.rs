// src/debugger/phases.rs

use std::collections::HashMap;
use std::time::Instant;

pub struct PhaseTracker {
    phases: Vec<Phase>,
    current: Option<usize>,
}

struct Phase {
    name: String,
    start: Instant,
    end: Option<Instant>,
    children: Vec<usize>,
}

impl PhaseTracker {
    pub fn new() -> Self {
        Self {
            phases: Vec::new(),
            current: None,
        }
    }

    pub fn begin(&mut self, name: impl Into<String>) {
        let idx = self.phases.len();
        let phase = Phase {
            name: name.into(),
            start: Instant::now(),
            end: None,
            children: Vec::new(),
        };

        if let Some(parent_idx) = self.current {
            self.phases[parent_idx].children.push(idx);
        }

        self.phases.push(phase);
        self.current = Some(idx);
    }

    pub fn end(&mut self) {
        if let Some(idx) = self.current {
            self.phases[idx].end = Some(Instant::now());

            // Find parent
            self.current = self.phases.iter().position(|p| p.children.contains(&idx));
        }
    }

    pub fn report(&self) -> String {
        let mut output = String::new();
        output.push_str("\n=== Query Execution Timeline ===\n");

        for (idx, phase) in self.phases.iter().enumerate() {
            let duration = phase
                .end
                .map(|end| end.duration_since(phase.start))
                .unwrap_or_default();

            let indent = self.get_depth(idx);
            output.push_str(&format!(
                "{}{}  {:.2}ms\n",
                "  ".repeat(indent),
                phase.name,
                duration.as_secs_f64() * 1000.0
            ));
        }

        output
    }

    fn get_depth(&self, idx: usize) -> usize {
        let mut depth = 0;
        for (i, phase) in self.phases.iter().enumerate() {
            if phase.children.contains(&idx) {
                depth = self.get_depth(i) + 1;
                break;
            }
        }
        depth
    }
}

thread_local! {
    static PHASE_TRACKER: std::cell::RefCell<PhaseTracker> = std::cell::RefCell::new(PhaseTracker::new());
}

pub fn begin_phase(name: impl Into<String>) {
    PHASE_TRACKER.with(|t| t.borrow_mut().begin(name));
}

pub fn end_phase() {
    PHASE_TRACKER.with(|t| t.borrow_mut().end());
}

pub fn get_report() -> String {
    PHASE_TRACKER.with(|t| t.borrow().report())
}

pub fn reset() {
    PHASE_TRACKER.with(|t| *t.borrow_mut() = PhaseTracker::new());
}

#[macro_export]
macro_rules! db_phase {
    ($name:expr, $body:block) => {{
        $crate::debugger::phases::begin_phase($name);
        let result = $body;
        $crate::debugger::phases::end_phase();
        result
    }};
}
