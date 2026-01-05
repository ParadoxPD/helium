use std::sync::atomic::{AtomicU8, Ordering};

#[derive(Copy, Clone, PartialEq, PartialOrd)]
pub enum DebugLevel {
    Off = 0,
    Error = 1,
    Info = 2,
    Debug = 3,
    Trace = 4,
}

pub static DEBUG_LEVEL: AtomicU8 = AtomicU8::new(DebugLevel::Off as u8);

#[macro_export]
macro_rules! db_debug {
    ($lvl:expr, $($arg:tt)*) => {
        if ($lvl as u8)
            <= $crate::debugger::debugger::DEBUG_LEVEL
                .load(std::sync::atomic::Ordering::Relaxed)
        {
            eprintln!($($arg)*);
        }
    };
}

pub fn set_debug_level(level: DebugLevel) {
    DEBUG_LEVEL.store(level as u8, Ordering::Relaxed);
}
