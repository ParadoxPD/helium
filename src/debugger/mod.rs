pub mod debugger;
mod macros;
pub mod phases;

pub use debugger::{Component, DebugLevel, get_debug_level, set_debug_level};
pub use phases::{begin_phase, end_phase, get_report, reset};
