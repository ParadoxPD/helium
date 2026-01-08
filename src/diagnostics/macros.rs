// src/debugger/macros.rs

#[macro_export]
macro_rules! db_log {
    ($level:expr, $component:expr, $($arg:tt)*) => {
        if $crate::debugger::debugger::should_log($level) {
            use $crate::debugger::debugger::color::*;
            let indent = $crate::debugger::debugger::format_indent();
            let level_str = match $level {
                $crate::debugger::debugger::DebugLevel::Error => format!("{}ERROR{}", RED, RESET),
                $crate::debugger::debugger::DebugLevel::Warn => format!("{}WARN{}", YELLOW, RESET),
                $crate::debugger::debugger::DebugLevel::Info => format!("{}INFO{}", GREEN, RESET),
                $crate::debugger::debugger::DebugLevel::Debug => format!("{}DEBUG{}", BLUE, RESET),
                $crate::debugger::debugger::DebugLevel::Trace => format!("{}TRACE{}", GRAY, RESET),
                _ => "".to_string(),
            };
            eprintln!("{}{} [{}] {}", indent, level_str, $component, format!($($arg)*));
        }
    };
}

#[macro_export]
macro_rules! db_error {
    ($component:expr, $($arg:tt)*) => {
        $crate::db_log!($crate::debugger::debugger::DebugLevel::Error, $component, $($arg)*);
    };
}

#[macro_export]
macro_rules! db_warn {
    ($component:expr, $($arg:tt)*) => {
        $crate::db_log!($crate::debugger::debugger::DebugLevel::Warn, $component, $($arg)*);
    };
}

#[macro_export]
macro_rules! db_info {
    ($component:expr, $($arg:tt)*) => {
        $crate::db_log!($crate::debugger::debugger::DebugLevel::Info, $component, $($arg)*)
    };
}

#[macro_export]
macro_rules! db_debug {
    ($component:expr, $($arg:tt)*) => {
        $crate::db_log!($crate::debugger::debugger::DebugLevel::Debug, $component, $($arg)*);
    };
}

#[macro_export]
macro_rules! db_trace {
    ($component:expr, $($arg:tt)*) => {
        $crate::db_log!($crate::debugger::debugger::DebugLevel::Trace, $component, $($arg)*);
    };
}

// Scoped debugging - automatically indents/dedents
#[macro_export]
macro_rules! db_scope {
    ($level:expr, $component:expr, $name:expr, $body:block) => {{
        if $crate::debugger::debugger::should_log($level) {
            $crate::db_log!($level, $component, "→ {}", $name);
            $crate::debugger::debugger::indent();
        }

        let result = $body;

        if $crate::debugger::debugger::should_log($level) {
            $crate::debugger::debugger::dedent();
            $crate::db_log!($level, $component, "← {}", $name);
        }

        result
    }};
}
