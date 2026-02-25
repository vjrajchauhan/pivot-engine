use crate::error::{PivotError, Result};
use crate::schema::DataType;
use crate::sql::ast::*;
use crate::sql::token::Token;

pub struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    pub fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    pub fn parse(&mut self) -> Result<Vec<Statement>> {
        let mut stmts = Vec::new();
        while !self.is_eof() {
            self.skip_semicolons();
            if self.is_eof() { break; }
            stmts.push(self.parse_statement()?);
            self.skip_semicolons();
        }
        Ok(stmts)
    }

    fn skip_semicolons(&mut self) {
        while self.peek() == &Token::Semicolon { self.advance(); }
    }

    fn peek(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or(&Token::Eof)
    }

    fn peek2(&self) -> &Token {
        self.tokens.get(self.pos + 1).unwrap_or(&Token::Eof)
    }

    fn advance(&mut self) -> &Token {
        let tok = self.tokens.get(self.pos).unwrap_or(&Token::Eof);
        if self.pos < self.tokens.len() { self.pos += 1; }
        tok
    }

    fn is_eof(&self) -> bool {
        matches!(self.peek(), Token::Eof)
    }

    fn expect(&mut self, expected: &Token) -> Result<()> {
        if self.peek() == expected {
            self.advance();
            Ok(())
        } else {
            Err(PivotError::SqlError(format!(
                "Expected {:?}, got {:?}", expected, self.peek()
            )))
        }
    }

    fn expect_ident(&mut self) -> Result<String> {
        match self.peek().clone() {
            Token::Ident(s) => { self.advance(); Ok(s) }
            Token::Table => { self.advance(); Ok("table".to_string()) }
            Token::Index => { self.advance(); Ok("index".to_string()) }
            Token::Filter => { self.advance(); Ok("filter".to_string()) }
            Token::Values => { self.advance(); Ok("value".to_string()) }
            other => Err(PivotError::SqlError(format!("Expected identifier, got {:?}", other))),
        }
    }

    fn try_consume(&mut self, tok: &Token) -> bool {
        if self.peek() == tok { self.advance(); true } else { false }
    }

    fn parse_statement(&mut self) -> Result<Statement> {
        let left = self.parse_primary_stmt()?;
        // Handle set operations (UNION, INTERSECT, EXCEPT)
        self.parse_set_op(left)
    }

    fn parse_primary_stmt(&mut self) -> Result<Statement> {
        match self.peek().clone() {
            Token::Select => Ok(Statement::Select(self.parse_select()?)),
            Token::With => self.parse_with(),
            Token::Insert => self.parse_insert(),
            Token::Update => self.parse_update(),
            Token::Delete => self.parse_delete(),
            Token::Create => self.parse_create(),
            Token::Drop => self.parse_drop(),
            Token::Begin => { self.advance(); self.try_consume(&Token::Transaction); Ok(Statement::Begin) }
            Token::Commit => { self.advance(); self.try_consume(&Token::Transaction); Ok(Statement::Commit) }
            Token::Rollback => { self.advance(); self.try_consume(&Token::Transaction); Ok(Statement::Rollback) }
            Token::Explain => {
                self.advance();
                let inner = self.parse_statement()?;
                Ok(Statement::Explain(Box::new(inner)))
            }
            Token::LParen => {
                self.advance();
                let stmt = self.parse_statement()?;
                self.expect(&Token::RParen)?;
                Ok(stmt)
            }
            other => Err(PivotError::SqlError(format!("Unexpected token: {:?}", other))),
        }
    }

    fn parse_set_op(&mut self, left: Statement) -> Result<Statement> {
        let (op, all) = match self.peek() {
            Token::Union => {
                self.advance();
                let all = self.try_consume(&Token::All);
                (SetOp::Union, all)
            }
            Token::Intersect => {
                self.advance();
                let all = self.try_consume(&Token::All);
                (SetOp::Intersect, all)
            }
            Token::Except => {
                self.advance();
                let all = self.try_consume(&Token::All);
                (SetOp::Except, all)
            }
            _ => return Ok(left),
        };
        let right = self.parse_primary_stmt()?;
        let set_op = Statement::SetOp(SetOpStatement {
            op, all, left: Box::new(left), right: Box::new(right),
        });
        // Handle chained set operations
        self.parse_set_op(set_op)
    }

    fn parse_with(&mut self) -> Result<Statement> {
        self.expect(&Token::With)?;
        self.try_consume(&Token::Recursive);
        let mut ctes = Vec::new();
        loop {
            let name = self.expect_ident()?;
            self.expect(&Token::As)?;
            self.expect(&Token::LParen)?;
            let query = self.parse_statement()?;
            self.expect(&Token::RParen)?;
            ctes.push(Cte { name, query: Box::new(query) });
            if !self.try_consume(&Token::Comma) { break; }
        }
        let body = self.parse_statement()?;
        Ok(Statement::With(WithStatement { ctes, body: Box::new(body) }))
    }

    fn parse_select(&mut self) -> Result<SelectStatement> {
        self.expect(&Token::Select)?;
        let distinct = self.try_consume(&Token::Distinct);
        if self.try_consume(&Token::All) {} // ALL is default

        // Parse SELECT items
        let columns = self.parse_select_items()?;

        // FROM
        let (from, joins) = if self.try_consume(&Token::From) {
            let table_ref = self.parse_table_ref()?;
            let joins = self.parse_joins()?;
            (Some(table_ref), joins)
        } else {
            (None, Vec::new())
        };

        // WHERE
        let where_clause = if self.try_consume(&Token::Where) {
            Some(self.parse_expr()?)
        } else { None };

        // GROUP BY
        let group_by = if self.peek() == &Token::Group && self.peek2() == &Token::By {
            self.advance(); self.advance();
            self.parse_expr_list()?
        } else { Vec::new() };

        // HAVING
        let having = if self.try_consume(&Token::Having) {
            Some(self.parse_expr()?)
        } else { None };

        // ORDER BY
        let order_by = if self.peek() == &Token::Order && self.peek2() == &Token::By {
            self.advance(); self.advance();
            self.parse_order_by_items()?
        } else { Vec::new() };

        // LIMIT
        let limit = if self.try_consume(&Token::Limit) {
            Some(self.parse_expr()?)
        } else { None };

        // OFFSET
        let offset = if self.try_consume(&Token::Offset) {
            Some(self.parse_expr()?)
        } else { None };

        Ok(SelectStatement {
            distinct, columns, from, joins, where_clause,
            group_by, having, order_by, limit, offset,
        })
    }

    fn parse_select_items(&mut self) -> Result<Vec<SelectItem>> {
        let mut items = Vec::new();
        loop {
            let item = self.parse_select_item()?;
            items.push(item);
            if !self.try_consume(&Token::Comma) { break; }
        }
        Ok(items)
    }

    fn parse_select_item(&mut self) -> Result<SelectItem> {
        if self.peek() == &Token::Star {
            self.advance();
            return Ok(SelectItem::Wildcard);
        }
        // Check for table.*
        if let Token::Ident(_) = self.peek() {
            if self.peek2() == &Token::Dot {
                // Could be table.* or table.col
                let save = self.pos;
                let name = self.expect_ident()?;
                self.advance(); // dot
                if self.peek() == &Token::Star {
                    self.advance();
                    return Ok(SelectItem::TableWildcard(name));
                }
                // Not table.* - restore and parse as expr
                self.pos = save;
            }
        }
        let expr = self.parse_expr()?;
        let alias = self.parse_alias();
        Ok(SelectItem::Expr { expr, alias })
    }

    fn parse_alias(&mut self) -> Option<String> {
        if self.try_consume(&Token::As) {
            // After AS, identifier is required
            match self.peek().clone() {
                Token::Ident(s) => { self.advance(); Some(s) }
                Token::StringLiteral(s) => { self.advance(); Some(s) }
                // Some keywords can be used as aliases
                tok => {
                    if let Some(s) = token_as_alias(&tok) {
                        self.advance(); Some(s)
                    } else {
                        None
                    }
                }
            }
        } else {
            // Optional alias without AS keyword
            match self.peek().clone() {
                Token::Ident(s) if !is_reserved_keyword(&s) => {
                    // Check it's not a keyword that could follow a select item
                    self.advance(); Some(s)
                }
                _ => None,
            }
        }
    }

    fn parse_table_ref(&mut self) -> Result<TableRef> {
        if self.peek() == &Token::LParen {
            self.advance();
            let query = self.parse_statement()?;
            self.expect(&Token::RParen)?;
            let alias = if self.try_consume(&Token::As) {
                self.expect_ident()?
            } else {
                self.expect_ident().unwrap_or_else(|_| "subq".to_string())
            };
            return Ok(TableRef::Subquery { query: Box::new(query), alias });
        }
        let name = self.expect_ident()?;
        let alias = self.parse_alias();
        Ok(TableRef::Table { name, alias })
    }

    fn parse_joins(&mut self) -> Result<Vec<Join>> {
        let mut joins = Vec::new();
        loop {
            let join_type = match self.peek() {
                Token::Join | Token::Inner => {
                    if self.peek() == &Token::Inner { self.advance(); }
                    self.expect(&Token::Join)?;
                    JoinType::Inner
                }
                Token::Left => {
                    self.advance();
                    self.try_consume(&Token::Outer);
                    self.expect(&Token::Join)?;
                    JoinType::Left
                }
                Token::Right => {
                    self.advance();
                    self.try_consume(&Token::Outer);
                    self.expect(&Token::Join)?;
                    JoinType::Right
                }
                Token::Full => {
                    self.advance();
                    self.try_consume(&Token::Outer);
                    self.expect(&Token::Join)?;
                    JoinType::Full
                }
                Token::Cross => {
                    self.advance();
                    self.expect(&Token::Join)?;
                    JoinType::Cross
                }
                _ => break,
            };
            let table = self.parse_table_ref()?;
            let condition = if self.try_consume(&Token::On) {
                JoinCondition::On(self.parse_expr()?)
            } else if self.try_consume(&Token::Using) {
                self.expect(&Token::LParen)?;
                let mut cols = Vec::new();
                loop {
                    cols.push(self.expect_ident()?);
                    if !self.try_consume(&Token::Comma) { break; }
                }
                self.expect(&Token::RParen)?;
                JoinCondition::Using(cols)
            } else {
                JoinCondition::None
            };
            joins.push(Join { join_type, table, condition });
        }
        Ok(joins)
    }

    fn parse_order_by_items(&mut self) -> Result<Vec<OrderByItem>> {
        let mut items = Vec::new();
        loop {
            let expr = self.parse_expr()?;
            let ascending = match self.peek() {
                Token::Asc => { self.advance(); true }
                Token::Desc => { self.advance(); false }
                _ => true,
            };
            let nulls_first = if self.peek() == &Token::Nulls {
                self.advance();
                match self.peek() {
                    Token::First => { self.advance(); Some(true) }
                    Token::Last => { self.advance(); Some(false) }
                    _ => None,
                }
            } else { None };
            items.push(OrderByItem { expr, ascending, nulls_first });
            if !self.try_consume(&Token::Comma) { break; }
        }
        Ok(items)
    }

    fn parse_insert(&mut self) -> Result<Statement> {
        self.expect(&Token::Insert)?;
        self.expect(&Token::Into)?;
        let table = self.expect_ident()?;
        let columns = if self.peek() == &Token::LParen
            && !matches!(self.tokens.get(self.pos + 1), Some(Token::Select)) {
            self.advance();
            let mut cols = Vec::new();
            loop {
                cols.push(self.expect_ident()?);
                if !self.try_consume(&Token::Comma) { break; }
            }
            self.expect(&Token::RParen)?;
            Some(cols)
        } else { None };

        let values = if self.try_consume(&Token::Values) {
            let mut all_rows = Vec::new();
            loop {
                self.expect(&Token::LParen)?;
                let row = self.parse_expr_list()?;
                self.expect(&Token::RParen)?;
                all_rows.push(row);
                if !self.try_consume(&Token::Comma) { break; }
            }
            InsertValues::Values(all_rows)
        } else {
            let stmt = self.parse_statement()?;
            InsertValues::Select(Box::new(stmt))
        };

        Ok(Statement::Insert(InsertStatement { table, columns, values }))
    }

    fn parse_update(&mut self) -> Result<Statement> {
        self.expect(&Token::Update)?;
        let table = self.expect_ident()?;
        let alias = self.parse_alias();
        self.expect(&Token::Set)?;
        let mut assignments = Vec::new();
        loop {
            let column = self.expect_ident()?;
            self.expect(&Token::Eq)?;
            let value = self.parse_expr()?;
            assignments.push(Assignment { column, value });
            if !self.try_consume(&Token::Comma) { break; }
        }
        let where_clause = if self.try_consume(&Token::Where) {
            Some(self.parse_expr()?)
        } else { None };
        Ok(Statement::Update(UpdateStatement { table, alias, assignments, where_clause }))
    }

    fn parse_delete(&mut self) -> Result<Statement> {
        self.expect(&Token::Delete)?;
        self.expect(&Token::From)?;
        let table = self.expect_ident()?;
        let where_clause = if self.try_consume(&Token::Where) {
            Some(self.parse_expr()?)
        } else { None };
        Ok(Statement::Delete(DeleteStatement { table, where_clause }))
    }

    fn parse_create(&mut self) -> Result<Statement> {
        self.expect(&Token::Create)?;
        self.try_consume(&Token::Temporary);
        self.try_consume(&Token::Temp);
        self.expect(&Token::Table)?;
        let if_not_exists = if self.peek() == &Token::If {
            self.advance();
            self.expect(&Token::Not)?;
            self.expect(&Token::Exists)?;
            true
        } else { false };
        let name = self.expect_ident()?;
        self.expect(&Token::LParen)?;
        let columns = self.parse_column_defs()?;
        self.expect(&Token::RParen)?;
        Ok(Statement::CreateTable(CreateTableStatement { name, if_not_exists, columns }))
    }

    fn parse_column_defs(&mut self) -> Result<Vec<ColumnDefAst>> {
        let mut cols = Vec::new();
        loop {
            // Skip table-level constraints
            match self.peek() {
                Token::Primary | Token::Unique | Token::Constraint | Token::Foreign | Token::Check => {
                    // Skip until next comma or closing paren
                    let mut depth = 0;
                    loop {
                        match self.peek() {
                            Token::LParen => { depth += 1; self.advance(); }
                            Token::RParen if depth == 0 => break,
                            Token::RParen => { depth -= 1; self.advance(); }
                            Token::Comma if depth == 0 => break,
                            Token::Eof => break,
                            _ => { self.advance(); }
                        }
                    }
                }
                Token::RParen | Token::Eof => break,
                _ => {
                    cols.push(self.parse_column_def()?);
                }
            }
            if !self.try_consume(&Token::Comma) { break; }
        }
        Ok(cols)
    }

    fn parse_column_def(&mut self) -> Result<ColumnDefAst> {
        let name = self.expect_ident()?;
        let data_type = self.parse_data_type()?;
        let mut nullable = true;
        let mut primary_key = false;
        let mut default = None;
        // Parse optional column constraints
        loop {
            match self.peek() {
                Token::Not => {
                    self.advance();
                    self.expect(&Token::Null)?;
                    nullable = false;
                }
                Token::Null => { self.advance(); nullable = true; }
                Token::Primary => {
                    self.advance();
                    self.expect(&Token::Key)?;
                    primary_key = true;
                    nullable = false;
                }
                Token::Unique => { self.advance(); }
                Token::Default => {
                    self.advance();
                    default = Some(self.parse_primary_expr()?);
                }
                Token::References => {
                    // REFERENCES table (col)
                    self.advance();
                    let _ = self.expect_ident();
                    if self.peek() == &Token::LParen {
                        self.advance();
                        while self.peek() != &Token::RParen && !self.is_eof() { self.advance(); }
                        self.advance(); // RParen
                    }
                }
                _ => break,
            }
        }
        Ok(ColumnDefAst { name, data_type, nullable, default, primary_key })
    }

    fn parse_data_type(&mut self) -> Result<DataType> {
        let name = match self.peek().clone() {
            Token::Ident(s) => { self.advance(); s.to_uppercase() }
            Token::Integer(_) => { self.advance(); "INTEGER".to_string() }
            Token::Not => {
                // Could be a keyword used as type name - try to handle gracefully
                return Ok(DataType::Utf8);
            }
            other => {
                // Try to extract keyword as type name
                let type_name = token_to_type_name(&other);
                if let Some(name) = type_name {
                    self.advance();
                    name
                } else {
                    return Err(PivotError::SqlError(format!(
                        "Expected data type, got {:?}", other
                    )));
                }
            }
        };
        self.parse_data_type_from_name(&name)
    }

    fn parse_data_type_from_name(&mut self, name: &str) -> Result<DataType> {
        match name {
            "INTEGER" | "INT" | "INT4" | "INT8" | "BIGINT" | "SMALLINT" | "TINYINT"
            | "HUGEINT" | "UBIGINT" | "UINT64" | "UINT32" | "UINT16" | "UINT8" => Ok(DataType::Int64),
            "FLOAT" | "REAL" | "FLOAT4" | "FLOAT8" | "DOUBLE" => Ok(DataType::Float64),
            "VARCHAR" | "TEXT" | "CHAR" | "STRING" | "BLOB" | "BPCHAR"
            | "CHARACTER" | "VARYING" => {
                // Consume optional length specifier
                if self.peek() == &Token::LParen {
                    self.advance();
                    while self.peek() != &Token::RParen && !self.is_eof() { self.advance(); }
                    self.advance();
                }
                Ok(DataType::Utf8)
            }
            "BOOLEAN" | "BOOL" | "BIT" => Ok(DataType::Boolean),
            "DATE" => Ok(DataType::Date),
            "TIMESTAMP" | "DATETIME" | "TIMESTAMPTZ" => {
                if self.peek() == &Token::LParen {
                    self.advance();
                    while self.peek() != &Token::RParen && !self.is_eof() { self.advance(); }
                    self.advance();
                }
                Ok(DataType::Timestamp)
            }
            "TIME" | "TIMETZ" => Ok(DataType::Time),
            "INTERVAL" => Ok(DataType::Interval),
            "DECIMAL" | "NUMERIC" => {
                let (precision, scale) = if self.peek() == &Token::LParen {
                    self.advance();
                    let p = match self.advance().clone() {
                        Token::Integer(n) => n as u8,
                        _ => 18,
                    };
                    let s = if self.try_consume(&Token::Comma) {
                        match self.advance().clone() {
                            Token::Integer(n) => n as u8,
                            _ => 2,
                        }
                    } else { 0 };
                    self.expect(&Token::RParen)?;
                    (p, s)
                } else { (18, 2) };
                Ok(DataType::Decimal { precision, scale })
            }
            _ => Ok(DataType::Utf8), // fallback
        }
    }

    fn parse_drop(&mut self) -> Result<Statement> {
        self.expect(&Token::Drop)?;
        self.expect(&Token::Table)?;
        let if_exists = if self.peek() == &Token::If {
            self.advance();
            self.expect(&Token::Exists)?;
            true
        } else { false };
        let name = self.expect_ident()?;
        Ok(Statement::DropTable(DropTableStatement { name, if_exists }))
    }

    // ─── Expression parsing ───────────────────────────────────────────────────

    pub fn parse_expr(&mut self) -> Result<Expr> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Expr> {
        let mut left = self.parse_and()?;
        while self.peek() == &Token::Or {
            self.advance();
            let right = self.parse_and()?;
            left = Expr::BinaryOp { left: Box::new(left), op: BinOp::Or, right: Box::new(right) };
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expr> {
        let mut left = self.parse_not()?;
        while self.peek() == &Token::And {
            self.advance();
            let right = self.parse_not()?;
            left = Expr::BinaryOp { left: Box::new(left), op: BinOp::And, right: Box::new(right) };
        }
        Ok(left)
    }

    fn parse_not(&mut self) -> Result<Expr> {
        if self.peek() == &Token::Not {
            self.advance();
            let expr = self.parse_not()?;
            Ok(Expr::UnaryOp { op: UnaryOp::Not, expr: Box::new(expr) })
        } else {
            self.parse_comparison()
        }
    }

    fn parse_comparison(&mut self) -> Result<Expr> {
        let left = self.parse_addition()?;

        match self.peek().clone() {
            Token::Eq => { self.advance(); let r = self.parse_addition()?; Ok(Expr::BinaryOp { left: Box::new(left), op: BinOp::Eq, right: Box::new(r) }) }
            Token::NotEq => { self.advance(); let r = self.parse_addition()?; Ok(Expr::BinaryOp { left: Box::new(left), op: BinOp::NotEq, right: Box::new(r) }) }
            Token::Lt => { self.advance(); let r = self.parse_addition()?; Ok(Expr::BinaryOp { left: Box::new(left), op: BinOp::Lt, right: Box::new(r) }) }
            Token::Gt => { self.advance(); let r = self.parse_addition()?; Ok(Expr::BinaryOp { left: Box::new(left), op: BinOp::Gt, right: Box::new(r) }) }
            Token::LtEq => { self.advance(); let r = self.parse_addition()?; Ok(Expr::BinaryOp { left: Box::new(left), op: BinOp::LtEq, right: Box::new(r) }) }
            Token::GtEq => { self.advance(); let r = self.parse_addition()?; Ok(Expr::BinaryOp { left: Box::new(left), op: BinOp::GtEq, right: Box::new(r) }) }
            Token::Is => {
                self.advance();
                let negated = self.try_consume(&Token::Not);
                self.expect(&Token::Null)?;
                Ok(Expr::IsNull { expr: Box::new(left), negated })
            }
            Token::Not => {
                self.advance();
                match self.peek().clone() {
                    Token::In => {
                        self.advance();
                        self.parse_in_expr(left, true)
                    }
                    Token::Like => {
                        self.advance();
                        let pattern = self.parse_addition()?;
                        Ok(Expr::Like { expr: Box::new(left), pattern: Box::new(pattern), negated: true, case_insensitive: false })
                    }
                    Token::ILike => {
                        self.advance();
                        let pattern = self.parse_addition()?;
                        Ok(Expr::Like { expr: Box::new(left), pattern: Box::new(pattern), negated: true, case_insensitive: true })
                    }
                    Token::Between => {
                        self.advance();
                        let low = self.parse_addition()?;
                        self.expect(&Token::And)?;
                        let high = self.parse_addition()?;
                        Ok(Expr::Between { expr: Box::new(left), low: Box::new(low), high: Box::new(high), negated: true })
                    }
                    other => Err(PivotError::SqlError(format!("Unexpected token after NOT: {:?}", other)))
                }
            }
            Token::In => {
                self.advance();
                self.parse_in_expr(left, false)
            }
            Token::Like => {
                self.advance();
                let pattern = self.parse_addition()?;
                Ok(Expr::Like { expr: Box::new(left), pattern: Box::new(pattern), negated: false, case_insensitive: false })
            }
            Token::ILike => {
                self.advance();
                let pattern = self.parse_addition()?;
                Ok(Expr::Like { expr: Box::new(left), pattern: Box::new(pattern), negated: false, case_insensitive: true })
            }
            Token::Between => {
                self.advance();
                let low = self.parse_addition()?;
                self.expect(&Token::And)?;
                let high = self.parse_addition()?;
                Ok(Expr::Between { expr: Box::new(left), low: Box::new(low), high: Box::new(high), negated: false })
            }
            _ => Ok(left),
        }
    }

    fn parse_in_expr(&mut self, left: Expr, negated: bool) -> Result<Expr> {
        self.expect(&Token::LParen)?;
        // Check if it's a subquery
        if self.peek() == &Token::Select || self.peek() == &Token::With {
            let query = self.parse_statement()?;
            self.expect(&Token::RParen)?;
            return Ok(Expr::InSubquery { expr: Box::new(left), query: Box::new(query), negated });
        }
        let list = self.parse_expr_list()?;
        self.expect(&Token::RParen)?;
        Ok(Expr::InList { expr: Box::new(left), list, negated })
    }

    fn parse_addition(&mut self) -> Result<Expr> {
        let mut left = self.parse_multiplication()?;
        loop {
            match self.peek() {
                Token::Plus => {
                    self.advance();
                    let right = self.parse_multiplication()?;
                    left = Expr::BinaryOp { left: Box::new(left), op: BinOp::Add, right: Box::new(right) };
                }
                Token::Minus => {
                    self.advance();
                    let right = self.parse_multiplication()?;
                    left = Expr::BinaryOp { left: Box::new(left), op: BinOp::Sub, right: Box::new(right) };
                }
                Token::Concat => {
                    self.advance();
                    let right = self.parse_multiplication()?;
                    left = Expr::BinaryOp { left: Box::new(left), op: BinOp::Concat, right: Box::new(right) };
                }
                _ => break,
            }
        }
        Ok(left)
    }

    fn parse_multiplication(&mut self) -> Result<Expr> {
        let mut left = self.parse_unary()?;
        loop {
            match self.peek() {
                Token::Star => {
                    self.advance();
                    let right = self.parse_unary()?;
                    left = Expr::BinaryOp { left: Box::new(left), op: BinOp::Mul, right: Box::new(right) };
                }
                Token::Slash => {
                    self.advance();
                    let right = self.parse_unary()?;
                    left = Expr::BinaryOp { left: Box::new(left), op: BinOp::Div, right: Box::new(right) };
                }
                Token::Percent => {
                    self.advance();
                    let right = self.parse_unary()?;
                    left = Expr::BinaryOp { left: Box::new(left), op: BinOp::Mod, right: Box::new(right) };
                }
                _ => break,
            }
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<Expr> {
        if self.peek() == &Token::Minus {
            self.advance();
            let expr = self.parse_postfix()?;
            return Ok(Expr::UnaryOp { op: UnaryOp::Neg, expr: Box::new(expr) });
        }
        if self.peek() == &Token::Plus {
            self.advance();
        }
        self.parse_postfix()
    }

    fn parse_postfix(&mut self) -> Result<Expr> {
        let mut expr = self.parse_primary_expr()?;
        loop {
            if self.peek() == &Token::ColonColon {
                self.advance();
                let dt = self.parse_data_type()?;
                expr = Expr::TypeCast { expr: Box::new(expr), data_type: dt };
            } else {
                break;
            }
        }
        Ok(expr)
    }

    fn parse_primary_expr(&mut self) -> Result<Expr> {
        match self.peek().clone() {
            Token::Integer(n) => { self.advance(); Ok(Expr::Literal(LiteralValue::Integer(n))) }
            Token::Float(f) => { self.advance(); Ok(Expr::Literal(LiteralValue::Float(f))) }
            Token::StringLiteral(s) => { self.advance(); Ok(Expr::Literal(LiteralValue::String(s))) }
            Token::True => { self.advance(); Ok(Expr::Literal(LiteralValue::Boolean(true))) }
            Token::False => { self.advance(); Ok(Expr::Literal(LiteralValue::Boolean(false))) }
            Token::Null => { self.advance(); Ok(Expr::Literal(LiteralValue::Null)) }
            Token::Star => { self.advance(); Ok(Expr::Wildcard) }

            Token::Interval => {
                self.advance();
                let val = match self.advance().clone() {
                    Token::StringLiteral(s) => s,
                    Token::Integer(n) => n.to_string(),
                    other => return Err(PivotError::SqlError(format!("Expected interval value, got {:?}", other))),
                };
                let unit = self.expect_ident()?;
                Ok(Expr::Literal(LiteralValue::Interval { value: val, unit }))
            }

            Token::Cast => {
                self.advance();
                self.expect(&Token::LParen)?;
                let expr = self.parse_expr()?;
                self.expect(&Token::As)?;
                let dt = self.parse_data_type()?;
                self.expect(&Token::RParen)?;
                Ok(Expr::Cast { expr: Box::new(expr), data_type: dt })
            }

            Token::Case => {
                self.advance();
                let operand = if self.peek() != &Token::When {
                    Some(Box::new(self.parse_expr()?))
                } else { None };
                let mut when_clauses = Vec::new();
                while self.peek() == &Token::When {
                    self.advance();
                    let cond = self.parse_expr()?;
                    self.expect(&Token::Then)?;
                    let result = self.parse_expr()?;
                    when_clauses.push((cond, result));
                }
                let else_clause = if self.try_consume(&Token::Else) {
                    Some(Box::new(self.parse_expr()?))
                } else { None };
                self.expect(&Token::End)?;
                Ok(Expr::Case { operand, when_clauses, else_clause })
            }

            Token::Exists => {
                self.advance();
                self.expect(&Token::LParen)?;
                let query = self.parse_statement()?;
                self.expect(&Token::RParen)?;
                Ok(Expr::Exists { query: Box::new(query), negated: false })
            }

            Token::Not => {
                self.advance();
                if self.peek() == &Token::Exists {
                    self.advance();
                    self.expect(&Token::LParen)?;
                    let query = self.parse_statement()?;
                    self.expect(&Token::RParen)?;
                    Ok(Expr::Exists { query: Box::new(query), negated: true })
                } else {
                    let expr = self.parse_primary_expr()?;
                    Ok(Expr::UnaryOp { op: UnaryOp::Not, expr: Box::new(expr) })
                }
            }

            Token::LParen => {
                self.advance();
                if self.peek() == &Token::Select || self.peek() == &Token::With {
                    let stmt = self.parse_statement()?;
                    self.expect(&Token::RParen)?;
                    return Ok(Expr::Subquery(Box::new(stmt)));
                }
                let expr = self.parse_expr()?;
                self.expect(&Token::RParen)?;
                Ok(expr)
            }

            Token::Ident(_) | Token::Row | Token::Current => {
                self.parse_ident_or_function()
            }

            // Type names that can appear as function-like casts
            other => {
                if let Some(type_name) = token_to_type_name(&other) {
                    // Could be a type cast like `DATE '2020-01-01'`
                    self.advance();
                    if let Token::StringLiteral(s) = self.peek().clone() {
                        self.advance();
                        return Ok(Expr::Literal(LiteralValue::String(s)));
                    }
                    // Otherwise treat as function name
                    self.parse_function_call(type_name)
                } else {
                    Err(PivotError::SqlError(format!("Unexpected token in expression: {:?}", other)))
                }
            }
        }
    }

    fn parse_ident_or_function(&mut self) -> Result<Expr> {
        let name = match self.peek().clone() {
            Token::Ident(s) => { self.advance(); s }
            Token::Row => { self.advance(); "row".to_string() }
            Token::Current => { self.advance(); "current".to_string() }
            other => return Err(PivotError::SqlError(format!("Expected identifier: {:?}", other))),
        };

        // table.column or schema.table.column
        if self.peek() == &Token::Dot {
            self.advance();
            if self.peek() == &Token::Star {
                self.advance();
                return Ok(Expr::Wildcard); // actually table.*
            }
            let col = match self.peek().clone() {
                Token::Ident(s) => { self.advance(); s }
                other => {
                    if let Some(s) = token_as_ident_name(&other) {
                        self.advance(); s
                    } else {
                        return Err(PivotError::SqlError(format!("Expected column name after '.': {:?}", other)));
                    }
                }
            };
            // Check for another dot (schema.table.col)
            if self.peek() == &Token::Dot {
                self.advance();
                let col2 = self.expect_ident()?;
                return Ok(Expr::Column(ColumnRef { table: Some(col), name: col2 }));
            }
            return Ok(Expr::Column(ColumnRef { table: Some(name), name: col }));
        }

        // Function call
        if self.peek() == &Token::LParen {
            return self.parse_function_call(name);
        }

        Ok(Expr::Column(ColumnRef { table: None, name }))
    }

    fn parse_function_call(&mut self, name: String) -> Result<Expr> {
        self.expect(&Token::LParen)?;
        let distinct = self.try_consume(&Token::Distinct);

        // COUNT(*) special case
        if name.to_uppercase() == "COUNT" && self.peek() == &Token::Star {
            self.advance();
            self.expect(&Token::RParen)?;
            let over = self.parse_over()?;
            return Ok(Expr::Function {
                name: "COUNT".to_string(),
                args: vec![Expr::Wildcard],
                distinct: false,
                over,
            });
        }

        let args = if self.peek() == &Token::RParen {
            Vec::new()
        } else {
            self.parse_expr_list()?
        };
        self.expect(&Token::RParen)?;

        // FILTER (WHERE ...) clause
        if self.peek() == &Token::Filter {
            self.advance();
            self.expect(&Token::LParen)?;
            self.expect(&Token::Where)?;
            let _filter_expr = self.parse_expr()?;
            self.expect(&Token::RParen)?;
        }

        let over = self.parse_over()?;

        Ok(Expr::Function { name: name.to_uppercase(), args, distinct, over })
    }

    fn parse_over(&mut self) -> Result<Option<WindowSpec>> {
        if self.peek() != &Token::Over { return Ok(None); }
        self.advance();

        if matches!(self.peek(), Token::Ident(_)) && !matches!(self.peek2(), Token::LParen) {
            // Named window reference - parse as empty spec for now
            if let Token::Ident(name) = self.peek().clone() {
                if !matches!(self.peek2(), Token::LParen) {
                    self.advance();
                    return Ok(Some(WindowSpec {
                        name: Some(name),
                        partition_by: Vec::new(),
                        order_by: Vec::new(),
                        frame: None,
                    }));
                }
            }
        }

        self.expect(&Token::LParen)?;
        let partition_by = if self.peek() == &Token::Partition && self.peek2() == &Token::By {
            self.advance(); self.advance();
            self.parse_expr_list()?
        } else { Vec::new() };

        let order_by = if self.peek() == &Token::Order && self.peek2() == &Token::By {
            self.advance(); self.advance();
            self.parse_order_by_items()?
        } else { Vec::new() };

        let frame = self.parse_window_frame()?;

        self.expect(&Token::RParen)?;
        Ok(Some(WindowSpec { name: None, partition_by, order_by, frame }))
    }

    fn parse_window_frame(&mut self) -> Result<Option<WindowFrame>> {
        let kind = match self.peek() {
            Token::Rows => { self.advance(); WindowFrameKind::Rows }
            Token::Range => { self.advance(); WindowFrameKind::Range }
            _ => return Ok(None),
        };
        let start = self.parse_window_frame_bound()?;
        let end = if self.peek() == &Token::Between {
            // Actually BETWEEN ... AND ...
            let low = start;
            self.expect(&Token::And)?;
            let high = self.parse_window_frame_bound()?;
            return Ok(Some(WindowFrame { kind, start: low, end: Some(high) }));
        } else { None };
        Ok(Some(WindowFrame { kind, start, end }))
    }

    fn parse_window_frame_bound(&mut self) -> Result<WindowFrameBound> {
        match self.peek().clone() {
            Token::Unbounded => {
                self.advance();
                match self.peek() {
                    Token::Preceding => { self.advance(); Ok(WindowFrameBound::UnboundedPreceding) }
                    Token::Following => { self.advance(); Ok(WindowFrameBound::UnboundedFollowing) }
                    _ => Ok(WindowFrameBound::UnboundedPreceding),
                }
            }
            Token::Current => {
                self.advance();
                self.expect(&Token::Row)?;
                Ok(WindowFrameBound::CurrentRow)
            }
            _ => {
                let n = self.parse_expr()?;
                match self.peek() {
                    Token::Preceding => { self.advance(); Ok(WindowFrameBound::Preceding(Box::new(n))) }
                    Token::Following => { self.advance(); Ok(WindowFrameBound::Following(Box::new(n))) }
                    _ => Ok(WindowFrameBound::Preceding(Box::new(n))),
                }
            }
        }
    }

    fn parse_expr_list(&mut self) -> Result<Vec<Expr>> {
        let mut exprs = Vec::new();
        exprs.push(self.parse_expr()?);
        while self.try_consume(&Token::Comma) {
            exprs.push(self.parse_expr()?);
        }
        Ok(exprs)
    }
}

// ─── Helper functions ────────────────────────────────────────────────────────

fn is_reserved_keyword(s: &str) -> bool {
    matches!(s.to_uppercase().as_str(),
        "SELECT" | "FROM" | "WHERE" | "GROUP" | "BY" | "HAVING" | "ORDER"
        | "LIMIT" | "OFFSET" | "JOIN" | "INNER" | "LEFT" | "RIGHT" | "FULL"
        | "OUTER" | "CROSS" | "ON" | "USING" | "UNION" | "INTERSECT" | "EXCEPT"
        | "INSERT" | "INTO" | "VALUES" | "UPDATE" | "SET" | "DELETE"
        | "CREATE" | "TABLE" | "DROP" | "WITH" | "AND" | "OR" | "NOT"
        | "IS" | "IN" | "LIKE" | "BETWEEN" | "CASE" | "WHEN" | "THEN"
        | "ELSE" | "END" | "DISTINCT" | "ALL"
    )
}

fn token_as_alias(tok: &Token) -> Option<String> {
    match tok {
        Token::Ident(s) => Some(s.clone()),
        _ => None,
    }
}

fn token_as_ident_name(tok: &Token) -> Option<String> {
    match tok {
        Token::Ident(s) => Some(s.clone()),
        Token::Filter => Some("filter".to_string()),
        Token::Values => Some("value".to_string()),
        Token::Row => Some("row".to_string()),
        _ => None,
    }
}

fn token_to_type_name(tok: &Token) -> Option<String> {
    match tok {
        Token::Ident(s) => Some(s.to_uppercase()),
        _ => None,
    }
}


