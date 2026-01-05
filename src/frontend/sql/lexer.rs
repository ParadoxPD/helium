use crate::{db_debug, debugger::debugger::DebugLevel, frontend::sql::parser::Position};

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // keywords
    Select,
    From,
    Where,
    Order,
    By,
    Limit,
    And,
    Or,
    Not,
    Null,
    True,
    False,
    Join,
    On,
    Create,
    Drop,
    Table,
    Index,
    Insert,
    Into,
    Values,
    Update,
    Set,
    Delete,
    Explain,
    Analyze,
    Asc,
    Desc,

    // identifiers
    Ident(String),

    // literals
    Int(i64),
    String(String),

    // punctuation
    Dot,
    Comma,
    LParen,
    RParen,
    Semicolon,

    // operators
    Eq,
    NotEq,
    Lt,
    Le,
    Gt,
    Ge,
    Plus,
    Minus,
    Star,
    Slash,

    EOF,
}

pub struct Tokenizer<'a> {
    input: &'a str,
    chars: std::iter::Peekable<std::str::Chars<'a>>,
    line: usize,
    column: usize,
}

impl<'a> Tokenizer<'a> {
    pub fn new(input: &'a str) -> Self {
        Self {
            input,
            chars: input.chars().peekable(),
            line: 1,
            column: 1,
        }
    }

    pub fn next_token(&mut self) -> (Token, Position) {
        self.skip_whitespace();

        let start_pos = Position {
            line: self.line,
            column: self.column,
        };

        let c = match self.chars.next() {
            Some(c) => {
                self.advance_position(c);
                c
            }
            None => return (Token::EOF, start_pos),
        };

        let tok = match c {
            // ---------- punctuation ----------
            '.' => Token::Dot,
            ',' => Token::Comma,
            '(' => Token::LParen,
            ')' => Token::RParen,
            ';' => Token::Semicolon,

            // ---------- operators ----------
            '+' => Token::Plus,
            '-' => Token::Minus,
            '*' => Token::Star,
            '/' => Token::Slash,
            '=' => Token::Eq,

            '!' => {
                if self.consume('=') {
                    Token::NotEq
                } else {
                    Token::NotEq // or return error later
                }
            }

            '<' => {
                if self.consume('=') {
                    Token::Le
                } else {
                    Token::Lt
                }
            }

            '>' => {
                if self.consume('=') {
                    Token::Ge
                } else {
                    Token::Gt
                }
            }

            // ---------- string literal ----------
            '\'' => {
                let mut s = String::new();
                while let Some(&ch) = self.chars.peek() {
                    let ch = self.chars.next().unwrap();
                    self.advance_position(ch);
                    if ch == '\'' {
                        break;
                    }
                    s.push(ch);
                }
                Token::String(s)
            }
            '"' => {
                let mut s = String::new();
                while let Some(&ch) = self.chars.peek() {
                    self.chars.next();
                    if ch == '"' {
                        break;
                    }
                    s.push(ch);
                }
                Token::String(s)
            }

            // ---------- number ----------
            c if c.is_ascii_digit() => {
                let mut num = c.to_string();
                while let Some(&ch) = self.chars.peek() {
                    if ch.is_ascii_digit() {
                        num.push(ch);
                        self.chars.next();
                        self.advance_position(ch);
                    } else {
                        break;
                    }
                }
                Token::Int(num.parse().unwrap())
            }

            // ---------- identifier ----------
            // ---------- identifier / keyword ----------
            c if is_ident_start(c) => {
                let mut ident = c.to_string();
                while let Some(&ch) = self.chars.peek() {
                    if is_ident_continue(ch) {
                        ident.push(ch);
                        self.chars.next();
                        self.advance_position(ch);
                    } else {
                        break;
                    }
                }

                match ident.to_ascii_uppercase().as_str() {
                    "SELECT" => Token::Select,
                    "FROM" => Token::From,
                    "WHERE" => Token::Where,
                    "ORDER" => Token::Order,
                    "BY" => Token::By,
                    "LIMIT" => Token::Limit,
                    "AND" => Token::And,
                    "OR" => Token::Or,
                    "NOT" => Token::Not,
                    "NULL" => Token::Null,
                    "TRUE" => Token::True,
                    "FALSE" => Token::False,
                    "JOIN" => Token::Join,
                    "ON" => Token::On,
                    "ASC" => Token::Asc,
                    "DESC" => Token::Desc,
                    "CREATE" => Token::Create,
                    "DROP" => Token::Drop,
                    "TABLE" => Token::Table,
                    "INDEX" => Token::Index,
                    "INSERT" => Token::Insert,
                    "INTO" => Token::Into,
                    "VALUES" => Token::Values,
                    "UPDATE" => Token::Update,
                    "SET" => Token::Set,
                    "DELETE" => Token::Delete,
                    "EXPLAIN" => Token::Explain,
                    "ANALYZE" => Token::Analyze,
                    _ => Token::Ident(ident),
                }
            }
            _ => self.next_token().0,
        };

        db_debug!(
            DebugLevel::Trace,
            "[LEX] token = {:?} at {:?}",
            tok,
            start_pos
        );
        (tok, start_pos)
    }

    fn consume(&mut self, expected: char) -> bool {
        matches!(self.chars.peek(), Some(&c) if c == expected) && {
            self.chars.next();
            true
        }
    }
    fn advance_position(&mut self, c: char) {
        if c == '\n' {
            self.line += 1;
            self.column = 1;
        } else {
            self.column += 1;
        }
    }

    fn skip_whitespace(&mut self) {
        loop {
            match self.chars.peek() {
                Some(&c) if c.is_whitespace() => {
                    let c = self.chars.next().unwrap();
                    self.advance_position(c);
                }
                Some('-') => {
                    let mut temp = self.chars.clone();
                    temp.next();
                    if matches!(temp.peek(), Some('-')) {
                        self.chars.next();
                        self.advance_position('-');
                        self.chars.next();
                        self.advance_position('-');

                        while let Some(&ch) = self.chars.peek() {
                            let ch = self.chars.next().unwrap();
                            self.advance_position(ch);
                            if ch == '\n' {
                                break;
                            }
                        }
                    } else {
                        break;
                    }
                }
                _ => break,
            }
        }
    }
}

fn is_ident_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_'
}

fn is_ident_continue(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}
