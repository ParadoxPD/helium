#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindError {
    UnknownTable(String),
    UnknownColumn(String),
    AmbiguousColumn(String),
    DuplicateColumn(String),
    ColumnCountMismatch,
    Unsupported,
    EmptyTable,
    TypeMismatch {
        column: String,
        expected: String,
        found: String,
    },
    TypeMismatchBinary {
        op: String,
        left: DataType,
        right: DataType,
    },
}

impl fmt::Display for BindError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BindError::UnknownTable(t) => write!(f, "'{}' table does not exist", t),
            BindError::UnknownColumn(c) => write!(f, "'{}' column does not exist", c),
            BindError::AmbiguousColumn(c) => write!(f, "ambiguous column '{}'", c),
            BindError::ColumnCountMismatch => write!(f, "column count mismatch"),
            BindError::Unsupported => write!(f, "Unsupported Statement"),
            BindError::DuplicateColumn(c) => write!(f, "duplicate column '{}'", c),
            BindError::EmptyTable => write!(f, "table must have at least one column"),
            BindError::TypeMismatch {
                column,
                expected,
                found,
            } => write!(
                f,
                "Type mismatch on column : {}, expected : {}, got : {}",
                column, expected, found
            ),
            BindError::TypeMismatchBinary { op, left, right } => {
                write!(f, "{} {} {}", op, left, right)
            }
        }
    }
}

impl std::error::Error for BindError {}


