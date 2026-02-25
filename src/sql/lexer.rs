use crate::error::{PivotError, Result};
use crate::sql::token::Token;

pub struct Lexer {
    input: Vec<char>,
    pos: usize,
}

impl Lexer {
    pub fn new(input: &str) -> Self {
        Self { input: input.chars().collect(), pos: 0 }
    }

    pub fn tokenize(&mut self) -> Result<Vec<Token>> {
        let mut tokens = Vec::new();
        loop {
            let tok = self.next_token()?;
            let is_eof = tok == Token::Eof;
            tokens.push(tok);
            if is_eof { break; }
        }
        Ok(tokens)
    }

    fn peek(&self) -> Option<char> { self.input.get(self.pos).copied() }
    fn peek2(&self) -> Option<char> { self.input.get(self.pos + 1).copied() }
    fn advance(&mut self) -> Option<char> {
        let c = self.input.get(self.pos).copied();
        if c.is_some() { self.pos += 1; }
        c
    }

    fn skip_whitespace_and_comments(&mut self) {
        loop {
            // skip whitespace
            while self.peek().map(|c| c.is_whitespace()).unwrap_or(false) {
                self.advance();
            }
            // skip line comment
            if self.peek() == Some('-') && self.peek2() == Some('-') {
                while self.peek().map(|c| c != '\n').unwrap_or(false) {
                    self.advance();
                }
                continue;
            }
            // skip block comment
            if self.peek() == Some('/') && self.peek2() == Some('*') {
                self.advance(); self.advance();
                loop {
                    if self.pos + 1 >= self.input.len() { break; }
                    if self.input[self.pos] == '*' && self.input[self.pos + 1] == '/' {
                        self.advance(); self.advance(); break;
                    }
                    self.advance();
                }
                continue;
            }
            break;
        }
    }

    fn next_token(&mut self) -> Result<Token> {
        self.skip_whitespace_and_comments();
        match self.peek() {
            None => Ok(Token::Eof),
            Some(c) => match c {
                '0'..='9' => self.read_number(),
                '\'' => self.read_string_literal(),
                '"' => self.read_quoted_ident(),
                '`' => self.read_backtick_ident(),
                'a'..='z' | 'A'..='Z' | '_' => self.read_ident_or_keyword(),
                '+' => { self.advance(); Ok(Token::Plus) }
                '-' => { self.advance(); Ok(Token::Minus) }
                '*' => { self.advance(); Ok(Token::Star) }
                '/' => { self.advance(); Ok(Token::Slash) }
                '%' => { self.advance(); Ok(Token::Percent) }
                '=' => { self.advance(); Ok(Token::Eq) }
                '!' => {
                    self.advance();
                    if self.peek() == Some('=') { self.advance(); Ok(Token::NotEq) }
                    else { Err(PivotError::SqlError("Unexpected '!'".to_string())) }
                }
                '<' => {
                    self.advance();
                    match self.peek() {
                        Some('=') => { self.advance(); Ok(Token::LtEq) }
                        Some('>') => { self.advance(); Ok(Token::NotEq) }
                        _ => Ok(Token::Lt),
                    }
                }
                '>' => {
                    self.advance();
                    if self.peek() == Some('=') { self.advance(); Ok(Token::GtEq) }
                    else { Ok(Token::Gt) }
                }
                '|' => {
                    self.advance();
                    if self.peek() == Some('|') { self.advance(); Ok(Token::Concat) }
                    else { Err(PivotError::SqlError("Expected '||'".to_string())) }
                }
                '(' => { self.advance(); Ok(Token::LParen) }
                ')' => { self.advance(); Ok(Token::RParen) }
                ',' => { self.advance(); Ok(Token::Comma) }
                ';' => { self.advance(); Ok(Token::Semicolon) }
                '.' => { self.advance(); Ok(Token::Dot) }
                ':' => {
                    self.advance();
                    if self.peek() == Some(':') { self.advance(); Ok(Token::ColonColon) }
                    else { Ok(Token::Colon) }
                }
                other => {
                    self.advance();
                    Err(PivotError::SqlError(format!("Unexpected character: '{}'", other)))
                }
            }
        }
    }

    fn read_number(&mut self) -> Result<Token> {
        let start = self.pos;
        let mut has_dot = false;
        let mut has_e = false;
        while let Some(c) = self.peek() {
            match c {
                '0'..='9' => { self.advance(); }
                '.' if !has_dot && !has_e => {
                    if self.peek2().map(|c| c.is_ascii_digit()).unwrap_or(false) {
                        has_dot = true;
                        self.advance();
                    } else { break; }
                }
                'e' | 'E' if !has_e => {
                    has_e = true; has_dot = true;
                    self.advance();
                    if self.peek() == Some('+') || self.peek() == Some('-') { self.advance(); }
                }
                _ => break,
            }
        }
        let s: String = self.input[start..self.pos].iter().collect();
        if has_dot || has_e {
            s.parse::<f64>()
                .map(Token::Float)
                .map_err(|_| PivotError::SqlError(format!("Invalid float: {}", s)))
        } else {
            s.parse::<i64>()
                .map(Token::Integer)
                .map_err(|_| PivotError::SqlError(format!("Invalid integer: {}", s)))
        }
    }

    fn read_string_literal(&mut self) -> Result<Token> {
        self.advance(); // skip opening '
        let mut s = String::new();
        loop {
            match self.advance() {
                None => return Err(PivotError::SqlError("Unterminated string literal".to_string())),
                Some('\'') => {
                    if self.peek() == Some('\'') { self.advance(); s.push('\''); }
                    else { break; }
                }
                Some(c) => s.push(c),
            }
        }
        Ok(Token::StringLiteral(s))
    }

    fn read_quoted_ident(&mut self) -> Result<Token> {
        self.advance(); // skip "
        let mut s = String::new();
        loop {
            match self.advance() {
                None => return Err(PivotError::SqlError("Unterminated quoted identifier".to_string())),
                Some('"') => break,
                Some(c) => s.push(c),
            }
        }
        Ok(Token::Ident(s))
    }

    fn read_backtick_ident(&mut self) -> Result<Token> {
        self.advance(); // skip `
        let mut s = String::new();
        loop {
            match self.advance() {
                None => return Err(PivotError::SqlError("Unterminated backtick identifier".to_string())),
                Some('`') => break,
                Some(c) => s.push(c),
            }
        }
        Ok(Token::Ident(s))
    }

    fn read_ident_or_keyword(&mut self) -> Result<Token> {
        let start = self.pos;
        while self.peek().map(|c| c.is_alphanumeric() || c == '_').unwrap_or(false) {
            self.advance();
        }
        let word: String = self.input[start..self.pos].iter().collect();
        Ok(keyword_or_ident(&word))
    }
}

fn keyword_or_ident(s: &str) -> Token {
    match s.to_uppercase().as_str() {
        "SELECT" => Token::Select,
        "FROM" => Token::From,
        "WHERE" => Token::Where,
        "GROUP" => Token::Group,
        "BY" => Token::By,
        "HAVING" => Token::Having,
        "ORDER" => Token::Order,
        "LIMIT" => Token::Limit,
        "OFFSET" => Token::Offset,
        "INSERT" => Token::Insert,
        "INTO" => Token::Into,
        "VALUES" => Token::Values,
        "UPDATE" => Token::Update,
        "SET" => Token::Set,
        "DELETE" => Token::Delete,
        "CREATE" => Token::Create,
        "TABLE" => Token::Table,
        "DROP" => Token::Drop,
        "INDEX" => Token::Index,
        "ON" => Token::On,
        "AS" => Token::As,
        "JOIN" => Token::Join,
        "INNER" => Token::Inner,
        "LEFT" => Token::Left,
        "RIGHT" => Token::Right,
        "FULL" => Token::Full,
        "OUTER" => Token::Outer,
        "CROSS" => Token::Cross,
        "NATURAL" => Token::Natural,
        "UNION" => Token::Union,
        "INTERSECT" => Token::Intersect,
        "EXCEPT" => Token::Except,
        "ALL" => Token::All,
        "DISTINCT" => Token::Distinct,
        "AND" => Token::And,
        "OR" => Token::Or,
        "NOT" => Token::Not,
        "IS" => Token::Is,
        "IN" => Token::In,
        "LIKE" => Token::Like,
        "ILIKE" => Token::ILike,
        "BETWEEN" => Token::Between,
        "CASE" => Token::Case,
        "WHEN" => Token::When,
        "THEN" => Token::Then,
        "ELSE" => Token::Else,
        "END" => Token::End,
        "CAST" => Token::Cast,
        "WITH" => Token::With,
        "RECURSIVE" => Token::Recursive,
        "OVER" => Token::Over,
        "PARTITION" => Token::Partition,
        "ROWS" => Token::Rows,
        "RANGE" => Token::Range,
        "UNBOUNDED" => Token::Unbounded,
        "PRECEDING" => Token::Preceding,
        "FOLLOWING" => Token::Following,
        "CURRENT" => Token::Current,
        "ROW" => Token::Row,
        "ASC" => Token::Asc,
        "DESC" => Token::Desc,
        "NULLS" => Token::Nulls,
        "FIRST" => Token::First,
        "LAST" => Token::Last,
        "PRIMARY" => Token::Primary,
        "KEY" => Token::Key,
        "UNIQUE" => Token::Unique,
        "DEFAULT" => Token::Default,
        "CONSTRAINT" => Token::Constraint,
        "FOREIGN" => Token::Foreign,
        "REFERENCES" => Token::References,
        "CHECK" => Token::Check,
        "ALTER" => Token::Alter,
        "ADD" => Token::Add,
        "RENAME" => Token::Rename,
        "TO" => Token::To,
        "TRUNCATE" => Token::Truncate,
        "BEGIN" => Token::Begin,
        "COMMIT" => Token::Commit,
        "ROLLBACK" => Token::Rollback,
        "TRANSACTION" => Token::Transaction,
        "EXPLAIN" => Token::Explain,
        "IF" => Token::If,
        "EXISTS" => Token::Exists,
        "TEMPORARY" => Token::Temporary,
        "TEMP" => Token::Temp,
        "VIEW" => Token::View,
        "TRUE" => Token::True,
        "FALSE" => Token::False,
        "NULL" => Token::Null,
        "INTERVAL" => Token::Interval,
        "FILTER" => Token::Filter,
        "USING" => Token::Using,
        _ => Token::Ident(s.to_string()),
    }
}
