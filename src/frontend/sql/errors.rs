use std::fmt;

use crate::frontend::sql::{lexer::Token, parser::Position};

#[derive(Debug, Clone)]
pub enum ParseError {
    UnexpectedEOF {
        position: Position,
    },

    UnexpectedToken {
        token: Token,
        position: Position,
    },

    Expected {
        expected: String,
        found: Option<String>,
        position: Position,
    },

    Unsupported {
        message: String,
        position: Position,
    },

    InvalidLiteral {
        literal: String,
        position: Position,
    },

    SyntaxError {
        message: String,
        position: Position,
    },
}
impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::UnexpectedEOF { position } => {
                write!(f, "parse error at {}: unexpected end of input", position)
            }

            ParseError::UnexpectedToken { token, position } => {
                write!(
                    f,
                    "parse error at {}: unexpected token '{:?}'",
                    position, token
                )
            }

            ParseError::Expected {
                expected,
                found,
                position,
            } => match found {
                Some(fnd) => write!(
                    f,
                    "parse error at {}: expected {}, found {}",
                    position, expected, fnd
                ),
                None => write!(
                    f,
                    "parse error at {}: expected {}, found end of input",
                    position, expected
                ),
            },

            ParseError::Unsupported { message, position } => {
                write!(f, "parse error at {}: {}", position, message)
            }

            ParseError::InvalidLiteral { literal, position } => {
                write!(
                    f,
                    "parse error at {}: invalid literal '{}'",
                    position, literal
                )
            }

            ParseError::SyntaxError { message, position } => {
                write!(f, "syntax error at {}: {}", position, message)
            }
        }
    }
}
impl std::error::Error for ParseError {}
