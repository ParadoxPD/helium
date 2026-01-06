pub mod data;
pub mod harness;

use std::sync::Once;

use helium::debugger::{DebugLevel, set_debug_level};

#[allow(dead_code)]
static INIT: Once = Once::new();

#[allow(dead_code)]
pub fn init_debug_for_tests() {
    let level = std::env::var("RUST_LOG")
        .ok()
        .and_then(|s| match s.to_uppercase().as_str() {
            "OFF" => Some(DebugLevel::Off),
            "ERROR" => Some(DebugLevel::Error),
            "WARN" => Some(DebugLevel::Warn),
            "INFO" => Some(DebugLevel::Info),
            "DEBUG" => Some(DebugLevel::Debug),
            "TRACE" => Some(DebugLevel::Trace),
            _ => None,
        })
        .unwrap_or(DebugLevel::Trace);

    INIT.call_once(|| {
        set_debug_level(level);
    });
}
