#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // identifiers & keywords
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
}

impl<'a> Tokenizer<'a> {
    pub fn new(input: &'a str) -> Self {
        Self {
            input,
            chars: input.chars().peekable(),
        }
    }

    pub fn next_token(&mut self) -> Token {
        self.skip_whitespace();

        let c = match self.chars.next() {
            Some(c) => c,
            None => return Token::EOF,
        };

        match c {
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
                    self.chars.next();
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
                    } else {
                        break;
                    }
                }
                Token::Int(num.parse().unwrap())
            }

            // ---------- identifier ----------
            c if is_ident_start(c) => {
                let mut ident = c.to_string();
                while let Some(&ch) = self.chars.peek() {
                    if is_ident_continue(ch) {
                        ident.push(ch);
                        self.chars.next();
                    } else {
                        break;
                    }
                }
                Token::Ident(ident)
            }

            // ---------- everything else ----------
            _ => {
                // IMPORTANT: do NOT panic
                // skip unknown characters for now
                self.next_token()
            }
        }
    }

    fn consume(&mut self, expected: char) -> bool {
        matches!(self.chars.peek(), Some(&c) if c == expected) && {
            self.chars.next();
            true
        }
    }

    fn skip_whitespace(&mut self) {
        loop {
            match self.chars.peek() {
                Some(c) if c.is_whitespace() => {
                    self.chars.next();
                }
                Some('-') => {
                    // Look ahead for '--'
                    let mut temp = self.chars.clone();
                    temp.next(); // consume first '-'
                    if matches!(temp.peek(), Some('-')) {
                        // Skip until newline
                        self.chars.next(); // first -
                        self.chars.next(); // second -
                        while let Some(&ch) = self.chars.peek() {
                            self.chars.next();
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
