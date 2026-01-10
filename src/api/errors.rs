#[derive(Debug)]
pub enum QueryError {
    Parse(ParseError),
    Bind(BindError),
    Exec(anyhow::Error),
}

impl std::fmt::Display for QueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryError::Parse(e) => write!(f, "Parse error: {}", e),
            QueryError::Bind(e) => write!(f, "Bind error: {}", e),
            QueryError::Exec(e) => write!(f, "Execution error: {}", e),
        }
    }
}

impl std::error::Error for QueryError {}

impl From<ParseError> for QueryError {
    fn from(e: ParseError) -> Self {
        QueryError::Parse(e)
    }
}

impl From<BindError> for QueryError {
    fn from(e: BindError) -> Self {
        QueryError::Bind(e)
    }
}

impl From<anyhow::Error> for QueryError {
    fn from(e: anyhow::Error) -> Self {
        QueryError::Exec(e)
    }
}
