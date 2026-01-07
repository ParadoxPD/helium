// src/debugger/debugger.rs

use std::fmt;
use std::sync::atomic::{AtomicU8, Ordering};

static DEFAULT_DEBUG_LEVEL: u8 = 5;
static DEBUG_LEVEL: AtomicU8 = AtomicU8::new(DEFAULT_DEBUG_LEVEL);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum DebugLevel {
    Off = 0,   // No output
    Error = 1, // Only errors
    Warn = 2,  // Warnings + errors
    Info = 3,  // Important milestones (query start/end, major phases)
    Debug = 4, // Detailed info (plan structure, optimization passes)
    Trace = 5, // Everything (row-by-row, token-by-token)
}

impl DebugLevel {
    pub fn from_u8(level: u8) -> Self {
        match level {
            0 => DebugLevel::Off,
            1 => DebugLevel::Error,
            2 => DebugLevel::Warn,
            3 => DebugLevel::Info,
            4 => DebugLevel::Debug,
            5 => DebugLevel::Trace,
            _ => DebugLevel::Off,
        }
    }
}

pub fn set_debug_level(level: DebugLevel) {
    DEBUG_LEVEL.store(level as u8, Ordering::SeqCst);
}

pub fn get_debug_level() -> DebugLevel {
    DebugLevel::from_u8(DEBUG_LEVEL.load(Ordering::SeqCst))
}

pub fn should_log(level: DebugLevel) -> bool {
    get_debug_level() >= level
}

// Component categories for filtering
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Component {
    Lexer,
    Parser,
    Binder,
    Planner,
    Optimizer,
    Executor,
    Storage,
    Buffer,
    BTree,
    Transaction,
}

impl fmt::Display for Component {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Component::Lexer => write!(f, "LEXER"),
            Component::Parser => write!(f, "PARSER"),
            Component::Binder => write!(f, "BINDER"),
            Component::Planner => write!(f, "PLANNER"),
            Component::Optimizer => write!(f, "OPTIMIZER"),
            Component::Executor => write!(f, "EXECUTOR"),
            Component::Storage => write!(f, "STORAGE"),
            Component::Buffer => write!(f, "BUFFER"),
            Component::BTree => write!(f, "BTREE"),
            Component::Transaction => write!(f, "TXN"),
        }
    }
}

// Color support for terminal output
pub mod color {
    pub const RESET: &str = "\x1b[0m";
    pub const RED: &str = "\x1b[31m";
    pub const YELLOW: &str = "\x1b[33m";
    pub const GREEN: &str = "\x1b[32m";
    pub const BLUE: &str = "\x1b[34m";
    pub const MAGENTA: &str = "\x1b[35m";
    pub const CYAN: &str = "\x1b[36m";
    pub const GRAY: &str = "\x1b[90m";
    pub const BOLD: &str = "\x1b[1m";
}

// Context tracking for hierarchical output
thread_local! {
    static INDENT_LEVEL: std::cell::Cell<usize> = std::cell::Cell::new(0);
}

pub fn indent() {
    INDENT_LEVEL.with(|level| level.set(level.get() + 1));
}

pub fn dedent() {
    INDENT_LEVEL.with(|level| {
        let current = level.get();
        if current > 0 {
            level.set(current - 1);
        }
    });
}

pub fn get_indent() -> usize {
    INDENT_LEVEL.with(|level| level.get())
}

pub fn reset_indent() {
    INDENT_LEVEL.with(|level| level.set(0));
}

// Pretty printing utilities
pub fn format_indent() -> String {
    "  ".repeat(get_indent())
}
