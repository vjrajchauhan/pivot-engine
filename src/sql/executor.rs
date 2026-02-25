use crate::column::ScalarValue;
use crate::error::{PivotError, Result};
use crate::schema::{ColumnDef, DataType, Schema};
use crate::sql::ast::*;
use crate::sql::catalog::Catalog;
use crate::sql::cast;
use crate::sql::functions_scalar;
use crate::sql::functions_datetime;
use crate::sql::lexer::Lexer;
use crate::sql::parser::Parser;
use std::collections::HashMap;

// ─── Public types ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<ScalarValue>>,
    pub affected_rows: usize,
    pub message: Option<String>,
}

impl QueryResult {
    pub fn empty() -> Self {
        Self { columns: Vec::new(), rows: Vec::new(), affected_rows: 0, message: None }
    }
    pub fn row_count(&self) -> usize { self.rows.len() }
    pub fn with_message(msg: String) -> Self {
        Self { columns: Vec::new(), rows: Vec::new(), affected_rows: 0, message: Some(msg) }
    }
    pub fn affected(n: usize) -> Self {
        Self { columns: Vec::new(), rows: Vec::new(), affected_rows: n, message: None }
    }
}

// ─── Internal row-set type ────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct Col {
    table: Option<String>,
    name: String,
    dtype: DataType,
}

impl Col {
    fn display_name(&self) -> String { self.name.clone() }
}

#[derive(Debug, Clone)]
struct RowSet {
    cols: Vec<Col>,
    rows: Vec<Vec<ScalarValue>>,
}

impl RowSet {
    fn new(cols: Vec<Col>) -> Self { Self { cols, rows: Vec::new() } }

    fn empty_single_row() -> Self {
        // A single empty row (for SELECT without FROM)
        let mut rs = Self::new(Vec::new());
        rs.rows.push(Vec::new());
        rs
    }

    fn col_names(&self) -> Vec<String> {
        self.cols.iter().map(|c| c.display_name()).collect()
    }

    // Find column index, handling table qualification
    fn find_col(&self, table: Option<&str>, name: &str) -> Option<usize> {
        if let Some(t) = table {
            // Qualified lookup
            if let Some(idx) = self.cols.iter().position(|c|
                c.table.as_deref().map(|s| s.eq_ignore_ascii_case(t)).unwrap_or(false)
                && c.name.eq_ignore_ascii_case(name)
            ) {
                return Some(idx);
            }
            // Also try name = "table.col"
            let qualified = format!("{}.{}", t, name);
            if let Some(idx) = self.cols.iter().position(|c|
                c.name.eq_ignore_ascii_case(&qualified)
            ) {
                return Some(idx);
            }
        }
        // Unqualified: find unique match
        let matches: Vec<usize> = self.cols.iter().enumerate()
            .filter(|(_, c)| c.name.eq_ignore_ascii_case(name))
            .map(|(i, _)| i)
            .collect();
        if matches.len() == 1 { Some(matches[0]) } else { matches.into_iter().next() }
    }

    fn into_query_result(self) -> QueryResult {
        QueryResult {
            columns: self.cols.iter().map(|c| c.display_name()).collect(),
            rows: self.rows,
            affected_rows: 0,
            message: None,
        }
    }
}

// ─── Execution context ────────────────────────────────────────────────────────

#[derive(Clone)]
struct ExecCtx {
    ctes: HashMap<String, RowSet>,
}

impl ExecCtx {
    fn new() -> Self { Self { ctes: HashMap::new() } }
}

// ─── SQL Engine ───────────────────────────────────────────────────────────────

pub struct SqlEngine {
    pub catalog: Catalog,
}

impl SqlEngine {
    pub fn new() -> Self {
        Self { catalog: Catalog::new() }
    }

    pub fn execute(&mut self, sql: &str) -> Result<QueryResult> {
        let tokens = Lexer::new(sql).tokenize()?;
        let stmts = Parser::new(tokens).parse()?;
        let mut last = QueryResult::empty();
        for stmt in stmts {
            last = self.exec_stmt(stmt)?;
        }
        Ok(last)
    }

    fn exec_stmt(&mut self, stmt: Statement) -> Result<QueryResult> {
        match stmt {
            Statement::Select(s) => {
                let ctx = ExecCtx::new();
                Ok(self.exec_select(&s, &ctx)?.into_query_result())
            }
            Statement::With(w) => {
                let mut ctx = ExecCtx::new();
                for cte in &w.ctes {
                    let rs = self.exec_stmt_ctx(&*cte.query, &ctx)?;
                    ctx.ctes.insert(cte.name.to_uppercase(), rs);
                }
                Ok(self.exec_stmt_ctx(&*w.body, &ctx)?.into_query_result())
            }
            Statement::SetOp(s) => {
                let ctx = ExecCtx::new();
                Ok(self.exec_set_op(&s, &ctx)?.into_query_result())
            }
            Statement::Insert(i) => self.exec_insert(i),
            Statement::Update(u) => self.exec_update(u),
            Statement::Delete(d) => self.exec_delete(d),
            Statement::CreateTable(c) => self.exec_create_table(c),
            Statement::DropTable(d) => self.exec_drop_table(d),
            Statement::Begin | Statement::Commit | Statement::Rollback => {
                Ok(QueryResult::with_message("OK".to_string()))
            }
            Statement::Explain(inner) => {
                Ok(QueryResult::with_message(format!("Plan: {:?}", inner)))
            }
        }
    }

    fn exec_stmt_ctx(&mut self, stmt: &Statement, ctx: &ExecCtx) -> Result<RowSet> {
        match stmt {
            Statement::Select(s) => self.exec_select(s, ctx),
            Statement::With(w) => {
                let mut new_ctx = ctx.clone();
                for cte in &w.ctes {
                    let rs = self.exec_stmt_ctx(&*cte.query, &new_ctx)?;
                    new_ctx.ctes.insert(cte.name.to_uppercase(), rs);
                }
                self.exec_stmt_ctx(&*w.body, &new_ctx)
            }
            Statement::SetOp(s) => self.exec_set_op(s, ctx),
            other => {
                let result = self.exec_stmt(other.clone())?;
                Ok(RowSet {
                    cols: result.columns.iter().map(|n| Col {
                        table: None, name: n.clone(), dtype: DataType::Utf8
                    }).collect(),
                    rows: result.rows,
                })
            }
        }
    }

    // ─── SELECT ───────────────────────────────────────────────────────────────

    fn exec_select(&mut self, stmt: &SelectStatement, ctx: &ExecCtx) -> Result<RowSet> {
        // 1. FROM
        let base = if let Some(table_ref) = &stmt.from {
            self.resolve_table_ref(table_ref, ctx)?
        } else {
            RowSet::empty_single_row()
        };

        // 2. JOINs
        let joined = self.apply_joins(base, &stmt.joins, ctx)?;

        // 3. WHERE
        let filtered = self.apply_where(joined, stmt.where_clause.as_ref())?;

        // 4. GROUP BY or direct projection
        let has_agg = select_items_have_aggregate(&stmt.columns);
        let projected = if !stmt.group_by.is_empty() || has_agg {
            // HAVING is evaluated inside exec_group_by with group context
            self.exec_group_by(filtered, stmt)?
        } else {
            let rs = self.project_select(filtered, &stmt.columns, stmt.distinct)?;
            // For non-aggregate queries, HAVING is unusual but apply it
            self.apply_having(rs, stmt.having.as_ref())?
        };

        // 5. Window functions
        let windowed = self.apply_window_funcs(projected, &stmt.columns)?;

        // 6. ORDER BY
        let sorted = self.apply_order_by(windowed, &stmt.order_by)?;

        // 7. LIMIT / OFFSET
        self.apply_limit_offset(sorted, stmt.limit.as_ref(), stmt.offset.as_ref())
    }

    // ─── FROM / table resolution ──────────────────────────────────────────────

    fn resolve_table_ref(&mut self, table_ref: &TableRef, ctx: &ExecCtx) -> Result<RowSet> {
        match table_ref {
            TableRef::Table { name, alias } => {
                let upper = name.to_uppercase();
                // Check CTEs first
                if let Some(rs) = ctx.ctes.get(&upper) {
                    let effective_alias = alias.as_ref().map(|a| a.as_str()).unwrap_or(name.as_str());
                    return Ok(tag_rowset(rs.clone(), effective_alias));
                }
                // Then catalog
                let store = self.catalog.get_table(&upper)
                    .ok_or_else(|| PivotError::SqlError(format!("Table '{}' not found", name)))?;
                let effective_alias = alias.as_ref().map(|a| a.as_str()).unwrap_or(name.as_str());
                let cols: Vec<Col> = store.schema().columns.iter().map(|c| Col {
                    table: Some(effective_alias.to_string()),
                    name: c.name.clone(),
                    dtype: c.data_type.clone(),
                }).collect();
                let mut rs = RowSet::new(cols);
                for row in 0..store.row_count() {
                    rs.rows.push(store.get_row(row)?);
                }
                Ok(rs)
            }
            TableRef::Subquery { query, alias } => {
                let mut rs = self.exec_stmt_ctx(query, ctx)?;
                // Tag with alias
                for col in &mut rs.cols {
                    col.table = Some(alias.clone());
                }
                Ok(rs)
            }
        }
    }

    // ─── JOINs ────────────────────────────────────────────────────────────────

    fn apply_joins(&mut self, base: RowSet, joins: &[Join], ctx: &ExecCtx) -> Result<RowSet> {
        let mut result = base;
        for join in joins {
            result = self.apply_join(result, join, ctx)?;
        }
        Ok(result)
    }

    fn apply_join(&mut self, left: RowSet, join: &Join, ctx: &ExecCtx) -> Result<RowSet> {
        let right = self.resolve_table_ref(&join.table, ctx)?;

        // Build combined schema
        let mut combined_cols: Vec<Col> = left.cols.clone();
        for rc in &right.cols {
            combined_cols.push(rc.clone());
        }
        let left_len = left.cols.len();
        let right_len = right.cols.len();

        let mut result = RowSet::new(combined_cols.clone());

        match &join.join_type {
            JoinType::Cross => {
                for lr in &left.rows {
                    for rr in &right.rows {
                        let mut combined = lr.clone();
                        combined.extend_from_slice(rr);
                        result.rows.push(combined);
                    }
                }
            }
            JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
                let is_left = matches!(join.join_type, JoinType::Left | JoinType::Full);
                let is_right = matches!(join.join_type, JoinType::Right | JoinType::Full);

                let mut right_matched = vec![false; right.rows.len()];

                for lr in &left.rows {
                    let mut found = false;
                    for (ri, rr) in right.rows.iter().enumerate() {
                        let mut combined = lr.clone();
                        combined.extend_from_slice(rr);
                        let matches = self.eval_join_condition(
                            &join.condition, &combined, &combined_cols
                        )?;
                        if matches {
                            result.rows.push(combined);
                            right_matched[ri] = true;
                            found = true;
                        }
                    }
                    if !found && is_left {
                        // Left row with nulls for right
                        let mut combined = lr.clone();
                        combined.extend(std::iter::repeat(ScalarValue::Null).take(right_len));
                        result.rows.push(combined);
                    }
                }

                if is_right {
                    for (ri, rr) in right.rows.iter().enumerate() {
                        if !right_matched[ri] {
                            let mut combined: Vec<ScalarValue> = std::iter::repeat(ScalarValue::Null)
                                .take(left_len).collect();
                            combined.extend_from_slice(rr);
                            result.rows.push(combined);
                        }
                    }
                }
            }
        }
        Ok(result)
    }

    fn eval_join_condition(
        &self,
        cond: &JoinCondition,
        row: &[ScalarValue],
        cols: &[Col],
    ) -> Result<bool> {
        match cond {
            JoinCondition::None => Ok(true),
            JoinCondition::On(expr) => {
                let v = eval_expr(expr, row, cols, None, &HashMap::new())?;
                Ok(is_truthy(&v))
            }
            JoinCondition::Using(col_names) => {
                for col_name in col_names {
                    // Find the two occurrences of this column
                    let matches: Vec<usize> = cols.iter().enumerate()
                        .filter(|(_, c)| c.name.eq_ignore_ascii_case(col_name))
                        .map(|(i, _)| i)
                        .collect();
                    if matches.len() >= 2 {
                        let v1 = row.get(matches[0]).cloned().unwrap_or(ScalarValue::Null);
                        let v2 = row.get(matches[1]).cloned().unwrap_or(ScalarValue::Null);
                        if !scalar_eq(&v1, &v2) { return Ok(false); }
                    }
                }
                Ok(true)
            }
        }
    }

    // ─── WHERE ────────────────────────────────────────────────────────────────

    fn apply_where(&self, rs: RowSet, where_clause: Option<&Expr>) -> Result<RowSet> {
        let expr = match where_clause {
            None => return Ok(rs),
            Some(e) => e,
        };
        let mut result = RowSet::new(rs.cols.clone());
        for row in &rs.rows {
            let v = eval_expr(expr, row, &rs.cols, None, &HashMap::new())?;
            if is_truthy(&v) {
                result.rows.push(row.clone());
            }
        }
        Ok(result)
    }

    // ─── GROUP BY ─────────────────────────────────────────────────────────────

    fn exec_group_by(&self, rs: RowSet, stmt: &SelectStatement) -> Result<RowSet> {
        // Determine output columns from SELECT items
        let mut out_cols: Vec<Col> = Vec::new();
        let mut out_exprs: Vec<(Expr, Option<String>)> = Vec::new();

        for item in &stmt.columns {
            match item {
                SelectItem::Wildcard => {
                    for col in &rs.cols {
                        out_cols.push(col.clone());
                        out_exprs.push((Expr::Column(ColumnRef {
                            table: col.table.clone(),
                            name: col.name.clone(),
                        }), None));
                    }
                }
                SelectItem::Expr { expr, alias } => {
                    let col_name = alias.clone().unwrap_or_else(|| expr_display_name(expr));
                    out_cols.push(Col { table: None, name: col_name.clone(), dtype: DataType::Utf8 });
                    out_exprs.push((expr.clone(), alias.clone()));
                }
                SelectItem::TableWildcard(_) => {}
            }
        }

        if stmt.group_by.is_empty() {
            // No GROUP BY but has aggregates: entire table is one group
            let group_rows: Vec<usize> = (0..rs.rows.len()).collect();
            let result_row = self.eval_agg_row(&out_exprs, &rs.rows, &group_rows, &rs.cols,
                                               &[ScalarValue::Null], &[])?;
            let mut result = RowSet::new(out_cols);
            result.rows.push(result_row);
            return Ok(result);
        }

        // Group rows by GROUP BY key
        let mut group_map: HashMap<Vec<String>, Vec<usize>> = HashMap::new();
        let mut group_order: Vec<Vec<String>> = Vec::new();

        for (row_idx, row) in rs.rows.iter().enumerate() {
            let key: Vec<String> = stmt.group_by.iter().map(|expr| {
                eval_expr(expr, row, &rs.cols, None, &HashMap::new())
                    .ok()
                    .map(|v| scalar_to_key(&v))
                    .unwrap_or_default()
            }).collect();

            let entry = group_map.entry(key.clone()).or_insert_with(Vec::new);
            if entry.is_empty() {
                group_order.push(key);
            }
            entry.push(row_idx);
        }

        let mut result = RowSet::new(out_cols);
        for key in &group_order {
            let indices = &group_map[key];
            // Evaluate HAVING using group context before projecting
            if let Some(ref having_expr) = stmt.having {
                let passes = self.eval_expr_agg(
                    having_expr, &rs.rows, indices, &rs.cols, &stmt.group_by
                )?;
                if !is_truthy(&passes) { continue; }
            }
            let result_row = self.eval_agg_row(
                &out_exprs, &rs.rows, indices, &rs.cols,
                &[], &stmt.group_by
            )?;
            result.rows.push(result_row);
        }
        Ok(result)
    }

    fn eval_agg_row(
        &self,
        out_exprs: &[(Expr, Option<String>)],
        all_rows: &[Vec<ScalarValue>],
        group_indices: &[usize],
        cols: &[Col],
        _key_vals: &[ScalarValue],
        group_exprs: &[Expr],
    ) -> Result<Vec<ScalarValue>> {
        let mut result = Vec::new();
        for (expr, _alias) in out_exprs {
            let val = self.eval_expr_agg(expr, all_rows, group_indices, cols, group_exprs)?;
            result.push(val);
        }
        Ok(result)
    }

    fn eval_expr_agg(
        &self,
        expr: &Expr,
        all_rows: &[Vec<ScalarValue>],
        group_indices: &[usize],
        cols: &[Col],
        group_exprs: &[Expr],
    ) -> Result<ScalarValue> {
        match expr {
            Expr::Function { name, args, distinct, over: None } => {
                let agg_name = name.to_uppercase();
                match agg_name.as_str() {
                    "COUNT" => {
                        if args.len() == 1 && matches!(&args[0], Expr::Wildcard) {
                            return Ok(ScalarValue::Int64(group_indices.len() as i64));
                        }
                        let mut n = 0i64;
                        for &idx in group_indices {
                            let v = eval_expr(&args[0], &all_rows[idx], cols, None, &HashMap::new())?;
                            if !matches!(v, ScalarValue::Null) {
                                if *distinct {
                                    // Simplified: count all
                                }
                                n += 1;
                            }
                        }
                        Ok(ScalarValue::Int64(n))
                    }
                    "SUM" => {
                        let mut total_f = 0.0f64;
                        let mut total_i = 0i64;
                        let mut is_float = false;
                        let mut has = false;
                        for &idx in group_indices {
                            match eval_expr(&args[0], &all_rows[idx], cols, None, &HashMap::new())? {
                                ScalarValue::Int64(i) => { total_i += i; has = true; }
                                ScalarValue::Float64(f) => { total_f += f; is_float = true; has = true; }
                                _ => {}
                            }
                        }
                        if !has { Ok(ScalarValue::Null) }
                        else if is_float { Ok(ScalarValue::Float64(total_f + total_i as f64)) }
                        else { Ok(ScalarValue::Int64(total_i)) }
                    }
                    "AVG" => {
                        let mut total = 0.0f64;
                        let mut n = 0i64;
                        for &idx in group_indices {
                            match eval_expr(&args[0], &all_rows[idx], cols, None, &HashMap::new())? {
                                ScalarValue::Int64(i) => { total += i as f64; n += 1; }
                                ScalarValue::Float64(f) => { total += f; n += 1; }
                                _ => {}
                            }
                        }
                        if n == 0 { Ok(ScalarValue::Null) }
                        else { Ok(ScalarValue::Float64(total / n as f64)) }
                    }
                    "MIN" => {
                        let mut best: Option<ScalarValue> = None;
                        for &idx in group_indices {
                            let v = eval_expr(&args[0], &all_rows[idx], cols, None, &HashMap::new())?;
                            if matches!(v, ScalarValue::Null) { continue; }
                            best = Some(match best {
                                None => v,
                                Some(cur) => if scalar_cmp(&v, &cur) == std::cmp::Ordering::Less { v } else { cur },
                            });
                        }
                        Ok(best.unwrap_or(ScalarValue::Null))
                    }
                    "MAX" => {
                        let mut best: Option<ScalarValue> = None;
                        for &idx in group_indices {
                            let v = eval_expr(&args[0], &all_rows[idx], cols, None, &HashMap::new())?;
                            if matches!(v, ScalarValue::Null) { continue; }
                            best = Some(match best {
                                None => v,
                                Some(cur) => if scalar_cmp(&v, &cur) == std::cmp::Ordering::Greater { v } else { cur },
                            });
                        }
                        Ok(best.unwrap_or(ScalarValue::Null))
                    }
                    "STRING_AGG" | "GROUP_CONCAT" | "LISTAGG" => {
                        let sep = if args.len() > 1 {
                            match eval_expr(&args[1], &all_rows.get(0).map(|r| r.as_slice()).unwrap_or(&[]),
                                           cols, None, &HashMap::new())? {
                                ScalarValue::Utf8(s) => s,
                                _ => ",".to_string(),
                            }
                        } else { ",".to_string() };
                        let mut parts: Vec<String> = Vec::new();
                        for &idx in group_indices {
                            let v = eval_expr(&args[0], &all_rows[idx], cols, None, &HashMap::new())?;
                            if !matches!(v, ScalarValue::Null) {
                                parts.push(format!("{}", v));
                            }
                        }
                        Ok(ScalarValue::Utf8(parts.join(&sep)))
                    }
                    "ARRAY_AGG" => {
                        let mut parts: Vec<String> = Vec::new();
                        for &idx in group_indices {
                            let v = eval_expr(&args[0], &all_rows[idx], cols, None, &HashMap::new())?;
                            parts.push(format!("{}", v));
                        }
                        Ok(ScalarValue::Utf8(format!("[{}]", parts.join(", "))))
                    }
                    "STDDEV" | "STDEV" | "STDDEV_SAMP" | "STDDEV_POP" => {
                        let mut vals: Vec<f64> = Vec::new();
                        for &idx in group_indices {
                            match eval_expr(&args[0], &all_rows[idx], cols, None, &HashMap::new())? {
                                ScalarValue::Int64(i) => vals.push(i as f64),
                                ScalarValue::Float64(f) => vals.push(f),
                                _ => {}
                            }
                        }
                        if vals.is_empty() { return Ok(ScalarValue::Null); }
                        let mean = vals.iter().sum::<f64>() / vals.len() as f64;
                        let var = vals.iter().map(|v| (v - mean).powi(2)).sum::<f64>()
                            / (if agg_name == "STDDEV_POP" { vals.len() } else { vals.len().max(2) - 1 }) as f64;
                        Ok(ScalarValue::Float64(var.sqrt()))
                    }
                    "VARIANCE" | "VAR_SAMP" | "VAR_POP" => {
                        let mut vals: Vec<f64> = Vec::new();
                        for &idx in group_indices {
                            match eval_expr(&args[0], &all_rows[idx], cols, None, &HashMap::new())? {
                                ScalarValue::Int64(i) => vals.push(i as f64),
                                ScalarValue::Float64(f) => vals.push(f),
                                _ => {}
                            }
                        }
                        if vals.is_empty() { return Ok(ScalarValue::Null); }
                        let mean = vals.iter().sum::<f64>() / vals.len() as f64;
                        let var = vals.iter().map(|v| (v - mean).powi(2)).sum::<f64>()
                            / (vals.len().max(2) - 1) as f64;
                        Ok(ScalarValue::Float64(var))
                    }
                    _ => {
                        // Not an aggregate - evaluate against first row of group
                        if let Some(&first_idx) = group_indices.first() {
                            eval_expr(expr, &all_rows[first_idx], cols, None, &HashMap::new())
                        } else {
                            Ok(ScalarValue::Null)
                        }
                    }
                }
            }
            Expr::BinaryOp { left, op, right } => {
                let l = self.eval_expr_agg(left, all_rows, group_indices, cols, group_exprs)?;
                let r = self.eval_expr_agg(right, all_rows, group_indices, cols, group_exprs)?;
                eval_binary_op(op, l, r)
            }
            Expr::UnaryOp { op, expr: inner } => {
                let v = self.eval_expr_agg(inner, all_rows, group_indices, cols, group_exprs)?;
                eval_unary_op(op, v)
            }
            Expr::Cast { expr: inner, data_type } => {
                let v = self.eval_expr_agg(inner, all_rows, group_indices, cols, group_exprs)?;
                Ok(cast::cast_value(v, data_type))
            }
            Expr::Case { operand, when_clauses, else_clause } => {
                // Use first row for case evaluation
                if let Some(&first_idx) = group_indices.first() {
                    eval_expr(expr, &all_rows[first_idx], cols, None, &HashMap::new())
                } else {
                    Ok(ScalarValue::Null)
                }
            }
            // For non-aggregate expressions, evaluate against first row in group
            _ => {
                if let Some(&first_idx) = group_indices.first() {
                    eval_expr(expr, &all_rows[first_idx], cols, None, &HashMap::new())
                } else {
                    Ok(ScalarValue::Null)
                }
            }
        }
    }

    // ─── HAVING ───────────────────────────────────────────────────────────────

    fn apply_having(&self, rs: RowSet, having: Option<&Expr>) -> Result<RowSet> {
        let expr = match having {
            None => return Ok(rs),
            Some(e) => e,
        };
        let mut result = RowSet::new(rs.cols.clone());
        for row in &rs.rows {
            let v = eval_expr(expr, row, &rs.cols, None, &HashMap::new())?;
            if is_truthy(&v) {
                result.rows.push(row.clone());
            }
        }
        Ok(result)
    }

    // ─── SELECT projection ────────────────────────────────────────────────────

    fn project_select(
        &self,
        rs: RowSet,
        items: &[SelectItem],
        distinct: bool,
    ) -> Result<RowSet> {
        // Determine output columns
        let mut out_cols: Vec<Col> = Vec::new();
        let mut out_item_indices: Vec<(usize, Option<String>)> = Vec::new(); // (item_idx, alias)

        for (item_idx, item) in items.iter().enumerate() {
            match item {
                SelectItem::Wildcard => {
                    for (ci, col) in rs.cols.iter().enumerate() {
                        out_cols.push(col.clone());
                        out_item_indices.push((item_idx * 1000 + ci, None)); // encode
                    }
                }
                SelectItem::TableWildcard(tname) => {
                    for (ci, col) in rs.cols.iter().enumerate() {
                        if col.table.as_deref().map(|t| t.eq_ignore_ascii_case(tname)).unwrap_or(false) {
                            out_cols.push(col.clone());
                            out_item_indices.push((item_idx * 1000 + ci, None));
                        }
                    }
                }
                SelectItem::Expr { expr, alias } => {
                    let name = alias.clone().unwrap_or_else(|| expr_display_name(expr));
                    out_cols.push(Col { table: None, name, dtype: DataType::Utf8 });
                    out_item_indices.push((item_idx, alias.clone()));
                }
            }
        }

        // Project each row
        let mut result_rows: Vec<Vec<ScalarValue>> = Vec::new();
        for row in &rs.rows {
            let mut out_row = Vec::new();
            let mut item_idx_iter = items.iter().enumerate().peekable();
            let mut col_out_idx = 0;

            for item in items {
                match item {
                    SelectItem::Wildcard => {
                        for val in row.iter() {
                            out_row.push(val.clone());
                        }
                    }
                    SelectItem::TableWildcard(tname) => {
                        for (ci, col) in rs.cols.iter().enumerate() {
                            if col.table.as_deref().map(|t| t.eq_ignore_ascii_case(tname)).unwrap_or(false) {
                                out_row.push(row[ci].clone());
                            }
                        }
                    }
                    SelectItem::Expr { expr, .. } => {
                        let v = eval_expr(expr, row, &rs.cols, None, &HashMap::new())?;
                        out_row.push(v);
                    }
                }
            }
            result_rows.push(out_row);
        }

        // Fix column count: re-derive from actual projection
        // We need to be careful about wildcards expanding to different counts
        let first_len = result_rows.first().map(|r| r.len()).unwrap_or(0);
        let _ = first_len;

        // Rebuild out_cols properly
        let mut proper_cols: Vec<Col> = Vec::new();
        let dummy_row: Vec<ScalarValue> = rs.cols.iter().map(|_| ScalarValue::Null).collect();
        let sample_row = rs.rows.first().unwrap_or(&dummy_row);

        for item in items {
            match item {
                SelectItem::Wildcard => {
                    for col in &rs.cols {
                        proper_cols.push(col.clone());
                    }
                }
                SelectItem::TableWildcard(tname) => {
                    for col in &rs.cols {
                        if col.table.as_deref().map(|t| t.eq_ignore_ascii_case(tname)).unwrap_or(false) {
                            proper_cols.push(col.clone());
                        }
                    }
                }
                SelectItem::Expr { expr, alias } => {
                    let name = alias.clone().unwrap_or_else(|| expr_display_name(expr));
                    proper_cols.push(Col { table: None, name, dtype: DataType::Utf8 });
                }
            }
        }

        let mut result = RowSet::new(proper_cols);
        result.rows = result_rows;

        if distinct {
            result = dedup_rowset(result);
        }
        Ok(result)
    }

    // ─── Window functions ─────────────────────────────────────────────────────

    fn apply_window_funcs(&self, mut rs: RowSet, items: &[SelectItem]) -> Result<RowSet> {
        // Find window function columns by index in the result
        let mut window_col_indices: Vec<(usize, Expr)> = Vec::new();
        let mut col_idx = 0;

        for item in items {
            match item {
                SelectItem::Wildcard => {
                    // Count wildcard expansion - we need the base RS column count
                    // but after projection, wildcard is already expanded
                    // We can skip these (they're just regular columns)
                    // Actually we need to know how many columns wildcard expanded to
                    // For safety, just skip
                }
                SelectItem::TableWildcard(_) => {}
                SelectItem::Expr { expr, .. } => {
                    if expr_has_window(expr) {
                        window_col_indices.push((col_idx, expr.clone()));
                    }
                    col_idx += 1;
                }
            }
        }

        if window_col_indices.is_empty() {
            return Ok(rs);
        }

        // We need to recompute window function values
        // The rs currently has placeholder values; we'll overwrite them
        // But first we need the "input" to window functions - which is rs itself
        let n_rows = rs.rows.len();

        for (col_idx, expr) in &window_col_indices {
            let values = self.compute_window_col(&rs, expr)?;
            for (row_idx, val) in values.into_iter().enumerate() {
                if row_idx < rs.rows.len() && *col_idx < rs.rows[row_idx].len() {
                    rs.rows[row_idx][*col_idx] = val;
                }
            }
        }
        Ok(rs)
    }

    fn compute_window_col(&self, rs: &RowSet, expr: &Expr) -> Result<Vec<ScalarValue>> {
        match expr {
            Expr::Function { name, args, over: Some(spec), .. } => {
                self.compute_window_func(name, args, spec, rs)
            }
            Expr::BinaryOp { left, op, right } => {
                let left_vals = self.compute_window_col(rs, left)?;
                let right_vals = self.compute_window_col(rs, right)?;
                left_vals.into_iter().zip(right_vals).map(|(l, r)| eval_binary_op(op, l, r)).collect()
            }
            _ => {
                // Not a window function - evaluate normally
                rs.rows.iter().map(|row| {
                    eval_expr(expr, row, &rs.cols, None, &HashMap::new())
                }).collect()
            }
        }
    }

    fn compute_window_func(
        &self,
        func_name: &str,
        args: &[Expr],
        spec: &WindowSpec,
        rs: &RowSet,
    ) -> Result<Vec<ScalarValue>> {
        let n = rs.rows.len();
        if n == 0 { return Ok(Vec::new()); }

        let fname = func_name.to_uppercase();

        // Get partition key for each row
        let partition_keys: Vec<Vec<String>> = rs.rows.iter().map(|row| {
            spec.partition_by.iter().map(|e| {
                eval_expr(e, row, &rs.cols, None, &HashMap::new())
                    .ok()
                    .map(|v| scalar_to_key(&v))
                    .unwrap_or_default()
            }).collect()
        }).collect();

        // Get ORDER BY sort key for each row
        let order_keys: Vec<Vec<ScalarValue>> = rs.rows.iter().map(|row| {
            spec.order_by.iter().map(|ob| {
                eval_expr(&ob.expr, row, &rs.cols, None, &HashMap::new())
                    .unwrap_or(ScalarValue::Null)
            }).collect()
        }).collect();

        let mut result = vec![ScalarValue::Null; n];

        // Group rows by partition
        let mut partitions: HashMap<Vec<String>, Vec<usize>> = HashMap::new();
        let mut part_order: Vec<Vec<String>> = Vec::new();
        for (i, key) in partition_keys.iter().enumerate() {
            let entry = partitions.entry(key.clone()).or_insert_with(Vec::new);
            if entry.is_empty() { part_order.push(key.clone()); }
            entry.push(i);
        }

        for part_key in &part_order {
            let part_indices = &partitions[part_key];

            // Sort partition by order_by
            let mut sorted: Vec<usize> = part_indices.clone();
            sorted.sort_by(|&a, &b| {
                for (i, ob) in spec.order_by.iter().enumerate() {
                    let va = order_keys[a].get(i).cloned().unwrap_or(ScalarValue::Null);
                    let vb = order_keys[b].get(i).cloned().unwrap_or(ScalarValue::Null);
                    let ord = scalar_cmp(&va, &vb);
                    if ord != std::cmp::Ordering::Equal {
                        return if ob.ascending { ord } else { ord.reverse() };
                    }
                }
                std::cmp::Ordering::Equal
            });

            match fname.as_str() {
                "ROW_NUMBER" => {
                    for (rank, &idx) in sorted.iter().enumerate() {
                        result[idx] = ScalarValue::Int64(rank as i64 + 1);
                    }
                }
                "RANK" => {
                    let mut rank = 1usize;
                    let mut prev_key: Option<Vec<String>> = None;
                    let mut prev_start = 1usize;
                    for (i, &idx) in sorted.iter().enumerate() {
                        let cur_key: Vec<String> = spec.order_by.iter().enumerate()
                            .map(|(ki, _)| order_keys[idx].get(ki)
                                .map(|v| scalar_to_key(v)).unwrap_or_default())
                            .collect();
                        if Some(&cur_key) != prev_key.as_ref() {
                            rank = i + 1;
                            prev_key = Some(cur_key);
                        }
                        result[idx] = ScalarValue::Int64(rank as i64);
                    }
                }
                "DENSE_RANK" => {
                    let mut rank = 0usize;
                    let mut prev_key: Option<Vec<String>> = None;
                    for &idx in &sorted {
                        let cur_key: Vec<String> = spec.order_by.iter().enumerate()
                            .map(|(ki, _)| order_keys[idx].get(ki)
                                .map(|v| scalar_to_key(v)).unwrap_or_default())
                            .collect();
                        if Some(&cur_key) != prev_key.as_ref() {
                            rank += 1;
                            prev_key = Some(cur_key);
                        }
                        result[idx] = ScalarValue::Int64(rank as i64);
                    }
                }
                "NTILE" => {
                    let n_buckets = if !args.is_empty() {
                        match eval_expr(&args[0], &rs.rows[sorted[0]], &rs.cols, None, &HashMap::new()) {
                            Ok(ScalarValue::Int64(n)) => n as usize,
                            _ => 1,
                        }
                    } else { 1 };
                    let part_size = sorted.len();
                    for (i, &idx) in sorted.iter().enumerate() {
                        let bucket = (i * n_buckets / part_size.max(1)) + 1;
                        result[idx] = ScalarValue::Int64(bucket as i64);
                    }
                }
                "PERCENT_RANK" => {
                    let n_part = sorted.len();
                    let mut rank = 0usize;
                    let mut prev_key: Option<Vec<String>> = None;
                    for (i, &idx) in sorted.iter().enumerate() {
                        let cur_key: Vec<String> = spec.order_by.iter().enumerate()
                            .map(|(ki, _)| order_keys[idx].get(ki)
                                .map(|v| scalar_to_key(v)).unwrap_or_default())
                            .collect();
                        if Some(&cur_key) != prev_key.as_ref() {
                            rank = i;
                            prev_key = Some(cur_key);
                        }
                        let pr = if n_part <= 1 { 0.0 } else { rank as f64 / (n_part - 1) as f64 };
                        result[idx] = ScalarValue::Float64(pr);
                    }
                }
                "CUME_DIST" => {
                    let n_part = sorted.len();
                    let mut pos_end = 0usize;
                    let mut i = 0;
                    while i < sorted.len() {
                        let cur_key: Vec<String> = spec.order_by.iter().enumerate()
                            .map(|(ki, _)| order_keys[sorted[i]].get(ki)
                                .map(|v| scalar_to_key(v)).unwrap_or_default())
                            .collect();
                        let mut j = i + 1;
                        while j < sorted.len() {
                            let next_key: Vec<String> = spec.order_by.iter().enumerate()
                                .map(|(ki, _)| order_keys[sorted[j]].get(ki)
                                    .map(|v| scalar_to_key(v)).unwrap_or_default())
                                .collect();
                            if next_key == cur_key { j += 1; } else { break; }
                        }
                        let cd = j as f64 / n_part as f64;
                        for k in i..j {
                            result[sorted[k]] = ScalarValue::Float64(cd);
                        }
                        i = j;
                    }
                }
                "LAG" | "LEAD" => {
                    let offset = if args.len() > 1 {
                        match eval_expr(&args[1], &rs.rows[sorted[0]], &rs.cols, None, &HashMap::new()) {
                            Ok(ScalarValue::Int64(n)) => n as usize,
                            _ => 1,
                        }
                    } else { 1 };
                    let default = if args.len() > 2 {
                        eval_expr(&args[2], &rs.rows[sorted[0]], &rs.cols, None, &HashMap::new())
                            .unwrap_or(ScalarValue::Null)
                    } else { ScalarValue::Null };

                    for (i, &idx) in sorted.iter().enumerate() {
                        let source_i = if fname == "LAG" {
                            if i >= offset { Some(sorted[i - offset]) } else { None }
                        } else {
                            if i + offset < sorted.len() { Some(sorted[i + offset]) } else { None }
                        };
                        result[idx] = if let Some(src_idx) = source_i {
                            if args.is_empty() { ScalarValue::Null }
                            else {
                                eval_expr(&args[0], &rs.rows[src_idx], &rs.cols, None, &HashMap::new())
                                    .unwrap_or(ScalarValue::Null)
                            }
                        } else { default.clone() };
                    }
                }
                "FIRST_VALUE" | "LAST_VALUE" => {
                    let target_idx = if fname == "FIRST_VALUE" { sorted[0] } else { *sorted.last().unwrap() };
                    let val = if args.is_empty() { ScalarValue::Null }
                        else { eval_expr(&args[0], &rs.rows[target_idx], &rs.cols, None, &HashMap::new())
                            .unwrap_or(ScalarValue::Null) };
                    for &idx in &sorted {
                        result[idx] = val.clone();
                    }
                }
                "NTH_VALUE" => {
                    let n_arg = if args.len() > 1 {
                        match eval_expr(&args[1], &rs.rows[sorted[0]], &rs.cols, None, &HashMap::new()) {
                            Ok(ScalarValue::Int64(n)) => n as usize,
                            _ => 1,
                        }
                    } else { 1 };
                    let target = if n_arg > 0 && n_arg <= sorted.len() { Some(sorted[n_arg - 1]) } else { None };
                    for &idx in &sorted {
                        result[idx] = if let Some(t) = target {
                            if args.is_empty() { ScalarValue::Null }
                            else { eval_expr(&args[0], &rs.rows[t], &rs.cols, None, &HashMap::new())
                                .unwrap_or(ScalarValue::Null) }
                        } else { ScalarValue::Null };
                    }
                }
                // Aggregate window functions (SUM, AVG, etc. over window)
                "SUM" | "AVG" | "COUNT" | "MIN" | "MAX" => {
                    for &idx in &sorted {
                        // Default: entire partition (no frame spec)
                        let part_row_indices: Vec<usize> = part_indices.clone();
                        let val = self.eval_expr_agg(
                            &Expr::Function {
                                name: func_name.to_string(),
                                args: args.to_vec(),
                                distinct: false,
                                over: None,
                            },
                            &rs.rows,
                            &part_row_indices,
                            &rs.cols,
                            &[],
                        )?;
                        result[idx] = val;
                    }
                }
                _ => {
                    // Unknown window function - return NULL
                    for &idx in &sorted {
                        result[idx] = ScalarValue::Null;
                    }
                }
            }
        }
        Ok(result)
    }

    // ─── ORDER BY ─────────────────────────────────────────────────────────────

    fn apply_order_by(&self, mut rs: RowSet, items: &[OrderByItem]) -> Result<RowSet> {
        if items.is_empty() { return Ok(rs); }
        rs.rows.sort_by(|a, b| {
            for item in items {
                let va = eval_expr(&item.expr, a, &rs.cols, None, &HashMap::new())
                    .unwrap_or(ScalarValue::Null);
                let vb = eval_expr(&item.expr, b, &rs.cols, None, &HashMap::new())
                    .unwrap_or(ScalarValue::Null);
                let ord = match (item.nulls_first, &va, &vb) {
                    (Some(true), ScalarValue::Null, ScalarValue::Null) => std::cmp::Ordering::Equal,
                    (Some(true), ScalarValue::Null, _) => std::cmp::Ordering::Less,
                    (Some(true), _, ScalarValue::Null) => std::cmp::Ordering::Greater,
                    (Some(false), ScalarValue::Null, ScalarValue::Null) => std::cmp::Ordering::Equal,
                    (Some(false), ScalarValue::Null, _) => std::cmp::Ordering::Greater,
                    (Some(false), _, ScalarValue::Null) => std::cmp::Ordering::Less,
                    _ => scalar_cmp(&va, &vb),
                };
                let ord = if item.ascending { ord } else { ord.reverse() };
                if ord != std::cmp::Ordering::Equal { return ord; }
            }
            std::cmp::Ordering::Equal
        });
        Ok(rs)
    }

    // ─── LIMIT / OFFSET ───────────────────────────────────────────────────────

    fn apply_limit_offset(
        &self,
        mut rs: RowSet,
        limit: Option<&Expr>,
        offset: Option<&Expr>,
    ) -> Result<RowSet> {
        let offset_val = if let Some(off_expr) = offset {
            match eval_expr(off_expr, &[], &[], None, &HashMap::new())? {
                ScalarValue::Int64(n) => n as usize,
                _ => 0,
            }
        } else { 0 };

        let limit_val = if let Some(lim_expr) = limit {
            match eval_expr(lim_expr, &[], &[], None, &HashMap::new())? {
                ScalarValue::Int64(n) => Some(n as usize),
                _ => None,
            }
        } else { None };

        if offset_val > 0 || limit_val.is_some() {
            let start = offset_val.min(rs.rows.len());
            let end = if let Some(lim) = limit_val {
                (start + lim).min(rs.rows.len())
            } else {
                rs.rows.len()
            };
            rs.rows = rs.rows[start..end].to_vec();
        }
        Ok(rs)
    }

    // ─── SET operations ───────────────────────────────────────────────────────

    fn exec_set_op(&mut self, stmt: &SetOpStatement, ctx: &ExecCtx) -> Result<RowSet> {
        let left = self.exec_stmt_ctx(&*stmt.left, ctx)?;
        let right = self.exec_stmt_ctx(&*stmt.right, ctx)?;

        let mut result = RowSet::new(left.cols.clone());
        match stmt.op {
            SetOp::Union => {
                result.rows.extend(left.rows.clone());
                if stmt.all {
                    result.rows.extend(right.rows);
                } else {
                    for row in right.rows {
                        if !result.rows.contains(&row) {
                            result.rows.push(row);
                        }
                    }
                    if !stmt.all {
                        result = dedup_rowset(result);
                    }
                }
            }
            SetOp::Intersect => {
                for row in &left.rows {
                    if right.rows.contains(row) {
                        result.rows.push(row.clone());
                    }
                }
                if !stmt.all { result = dedup_rowset(result); }
            }
            SetOp::Except => {
                for row in &left.rows {
                    if !right.rows.contains(row) {
                        result.rows.push(row.clone());
                    }
                }
            }
        }
        Ok(result)
    }

    // ─── INSERT ───────────────────────────────────────────────────────────────

    fn exec_insert(&mut self, stmt: InsertStatement) -> Result<QueryResult> {
        let table = self.catalog.get_table_mut(&stmt.table)
            .ok_or_else(|| PivotError::SqlError(format!("Table '{}' not found", stmt.table)))?;
        let schema = table.schema().clone();
        let col_indices: Vec<usize> = if let Some(ref cols) = stmt.columns {
            cols.iter().map(|c| schema.find_column_index(c)
                .ok_or_else(|| PivotError::ColumnNotFound(c.clone())))
                .collect::<Result<Vec<_>>>()?
        } else {
            (0..schema.column_count()).collect()
        };

        let mut affected = 0;
        match &stmt.values {
            InsertValues::Values(all_rows) => {
                for row_exprs in all_rows {
                    let mut values: Vec<ScalarValue> = (0..schema.column_count())
                        .map(|_| ScalarValue::Null).collect();
                    for (i, expr) in row_exprs.iter().enumerate() {
                        if let Some(&col_idx) = col_indices.get(i) {
                            let v = eval_expr(expr, &[], &[], None, &HashMap::new())?;
                            values[col_idx] = v;
                        }
                    }
                    let table = self.catalog.get_table_mut(&stmt.table).unwrap();
                    table.append_row(values)?;
                    affected += 1;
                }
            }
            InsertValues::Select(select_stmt) => {
                let ctx = ExecCtx::new();
                let rs = self.exec_stmt_ctx(&*select_stmt, &ctx)?;
                for row in &rs.rows {
                    let mut values: Vec<ScalarValue> = (0..schema.column_count())
                        .map(|_| ScalarValue::Null).collect();
                    for (i, val) in row.iter().enumerate() {
                        if let Some(&col_idx) = col_indices.get(i) {
                            values[col_idx] = val.clone();
                        }
                    }
                    let table = self.catalog.get_table_mut(&stmt.table).unwrap();
                    table.append_row(values)?;
                    affected += 1;
                }
            }
        }
        Ok(QueryResult::affected(affected))
    }

    // ─── UPDATE ───────────────────────────────────────────────────────────────

    fn exec_update(&mut self, stmt: UpdateStatement) -> Result<QueryResult> {
        let table = self.catalog.get_table(&stmt.table)
            .ok_or_else(|| PivotError::SqlError(format!("Table '{}' not found", stmt.table)))?;
        let schema = table.schema().clone();
        let row_count = table.row_count();

        let mut to_update: Vec<(usize, usize, ScalarValue)> = Vec::new();
        for row_idx in 0..row_count {
            let row = table.get_row(row_idx)?;
            let cols: Vec<Col> = schema.columns.iter().map(|c| Col {
                table: None, name: c.name.clone(), dtype: c.data_type.clone()
            }).collect();

            let should_update = if let Some(ref where_expr) = stmt.where_clause {
                let v = eval_expr(where_expr, &row, &cols, None, &HashMap::new())?;
                is_truthy(&v)
            } else { true };

            if should_update {
                for assign in &stmt.assignments {
                    let col_idx = schema.find_column_index(&assign.column)
                        .ok_or_else(|| PivotError::ColumnNotFound(assign.column.clone()))?;
                    let val = eval_expr(&assign.value, &row, &cols, None, &HashMap::new())?;
                    to_update.push((row_idx, col_idx, val));
                }
            }
        }

        let table = self.catalog.get_table_mut(&stmt.table).unwrap();
        let affected = to_update.len();
        for (row_idx, col_idx, val) in to_update {
            table.set_value(row_idx, col_idx, val)?;
        }
        Ok(QueryResult::affected(affected))
    }

    // ─── DELETE ───────────────────────────────────────────────────────────────

    fn exec_delete(&mut self, stmt: DeleteStatement) -> Result<QueryResult> {
        let table = self.catalog.get_table(&stmt.table)
            .ok_or_else(|| PivotError::SqlError(format!("Table '{}' not found", stmt.table)))?;
        let schema = table.schema().clone();
        let row_count = table.row_count();

        let cols: Vec<Col> = schema.columns.iter().map(|c| Col {
            table: None, name: c.name.clone(), dtype: c.data_type.clone()
        }).collect();

        let mut keep_rows: Vec<Vec<ScalarValue>> = Vec::new();
        let mut deleted = 0;

        for row_idx in 0..row_count {
            let row = table.get_row(row_idx)?;
            let delete = if let Some(ref where_expr) = stmt.where_clause {
                let v = eval_expr(where_expr, &row, &cols, None, &HashMap::new())?;
                is_truthy(&v)
            } else { true };

            if delete { deleted += 1; } else { keep_rows.push(row); }
        }

        // Rebuild table
        let new_store = {
            let mut s = crate::datastore::DataStore::new(schema);
            for row in keep_rows { s.append_row(row)?; }
            s
        };
        *self.catalog.get_table_mut(&stmt.table).unwrap() = new_store;
        Ok(QueryResult::affected(deleted))
    }

    // ─── CREATE TABLE ─────────────────────────────────────────────────────────

    fn exec_create_table(&mut self, stmt: CreateTableStatement) -> Result<QueryResult> {
        let schema = Schema::new(stmt.columns.iter().map(|c| ColumnDef {
            name: c.name.clone(),
            data_type: c.data_type.clone(),
            nullable: c.nullable,
        }).collect());

        if stmt.if_not_exists {
            self.catalog.create_table_if_not_exists(&stmt.name, schema);
        } else {
            if !self.catalog.create_table(&stmt.name, schema) {
                return Err(PivotError::SqlError(format!("Table '{}' already exists", stmt.name)));
            }
        }
        Ok(QueryResult::with_message(format!("Table '{}' created", stmt.name)))
    }

    // ─── DROP TABLE ───────────────────────────────────────────────────────────

    fn exec_drop_table(&mut self, stmt: DropTableStatement) -> Result<QueryResult> {
        if stmt.if_exists {
            self.catalog.drop_table(&stmt.name);
            Ok(QueryResult::with_message(format!("Table '{}' dropped", stmt.name)))
        } else if self.catalog.drop_table(&stmt.name) {
            Ok(QueryResult::with_message(format!("Table '{}' dropped", stmt.name)))
        } else {
            Err(PivotError::SqlError(format!("Table '{}' not found", stmt.name)))
        }
    }
}

// ─── Expression evaluation ────────────────────────────────────────────────────

fn eval_expr(
    expr: &Expr,
    row: &[ScalarValue],
    cols: &[Col],
    group_rows: Option<&Vec<Vec<ScalarValue>>>,
    ctes: &HashMap<String, RowSet>,
) -> Result<ScalarValue> {
    match expr {
        Expr::Literal(lit) => Ok(eval_literal(lit)),
        Expr::Column(col_ref) => {
            let idx = find_col_idx(cols, col_ref.table.as_deref(), &col_ref.name)
                .ok_or_else(|| PivotError::ColumnNotFound(
                    col_ref.table.as_ref()
                        .map(|t| format!("{}.{}", t, col_ref.name))
                        .unwrap_or_else(|| col_ref.name.clone())
                ))?;
            Ok(row.get(idx).cloned().unwrap_or(ScalarValue::Null))
        }
        Expr::Wildcard => Ok(ScalarValue::Null),
        Expr::BinaryOp { left, op, right } => {
            let l = eval_expr(left, row, cols, group_rows, ctes)?;
            let r = eval_expr(right, row, cols, group_rows, ctes)?;
            eval_binary_op(op, l, r)
        }
        Expr::UnaryOp { op, expr: inner } => {
            let v = eval_expr(inner, row, cols, group_rows, ctes)?;
            eval_unary_op(op, v)
        }
        Expr::Cast { expr: inner, data_type } => {
            let v = eval_expr(inner, row, cols, group_rows, ctes)?;
            Ok(cast::cast_value(v, data_type))
        }
        Expr::TypeCast { expr: inner, data_type } => {
            let v = eval_expr(inner, row, cols, group_rows, ctes)?;
            Ok(cast::cast_value(v, data_type))
        }
        Expr::TryCast { expr: inner, data_type } => {
            let v = eval_expr(inner, row, cols, group_rows, ctes)?;
            Ok(cast::try_cast_value(v, data_type))
        }
        Expr::IsNull { expr: inner, negated } => {
            let v = eval_expr(inner, row, cols, group_rows, ctes)?;
            let is_null = matches!(v, ScalarValue::Null);
            Ok(ScalarValue::Boolean(if *negated { !is_null } else { is_null }))
        }
        Expr::InList { expr: inner, list, negated } => {
            let v = eval_expr(inner, row, cols, group_rows, ctes)?;
            let mut found = false;
            for item in list {
                let iv = eval_expr(item, row, cols, group_rows, ctes)?;
                if scalar_eq(&v, &iv) { found = true; break; }
            }
            Ok(ScalarValue::Boolean(if *negated { !found } else { found }))
        }
        Expr::Between { expr: inner, low, high, negated } => {
            let v = eval_expr(inner, row, cols, group_rows, ctes)?;
            let l = eval_expr(low, row, cols, group_rows, ctes)?;
            let h = eval_expr(high, row, cols, group_rows, ctes)?;
            let in_range = scalar_cmp(&v, &l) != std::cmp::Ordering::Less
                && scalar_cmp(&v, &h) != std::cmp::Ordering::Greater;
            Ok(ScalarValue::Boolean(if *negated { !in_range } else { in_range }))
        }
        Expr::Like { expr: inner, pattern, negated, case_insensitive } => {
            let v = eval_expr(inner, row, cols, group_rows, ctes)?;
            let p = eval_expr(pattern, row, cols, group_rows, ctes)?;
            let result = match (&v, &p) {
                (ScalarValue::Utf8(s), ScalarValue::Utf8(pat)) => {
                    like_match(s, pat, *case_insensitive)
                }
                _ => false,
            };
            Ok(ScalarValue::Boolean(if *negated { !result } else { result }))
        }
        Expr::Case { operand, when_clauses, else_clause } => {
            let base = if let Some(op) = operand {
                Some(eval_expr(op, row, cols, group_rows, ctes)?)
            } else { None };
            for (cond, then_expr) in when_clauses {
                let matches = if let Some(ref bv) = base {
                    let cv = eval_expr(cond, row, cols, group_rows, ctes)?;
                    scalar_eq(bv, &cv)
                } else {
                    let cv = eval_expr(cond, row, cols, group_rows, ctes)?;
                    is_truthy(&cv)
                };
                if matches {
                    return eval_expr(then_expr, row, cols, group_rows, ctes);
                }
            }
            if let Some(else_e) = else_clause {
                eval_expr(else_e, row, cols, group_rows, ctes)
            } else {
                Ok(ScalarValue::Null)
            }
        }
        Expr::Function { name, args, distinct, over: None } => {
            eval_scalar_function(name, args, row, cols, group_rows, ctes)
        }
        Expr::Function { name, args, over: Some(spec), .. } => {
            // Window functions evaluated by compute_window_func, not here
            // Return NULL as placeholder (will be replaced later)
            Ok(ScalarValue::Null)
        }
        Expr::Subquery(stmt) => {
            // Scalar subquery - we can't execute it without the catalog
            // Return NULL as placeholder
            Ok(ScalarValue::Null)
        }
        Expr::Exists { .. } => Ok(ScalarValue::Boolean(false)),
        Expr::InSubquery { .. } => Ok(ScalarValue::Boolean(false)),
        _ => Ok(ScalarValue::Null),
    }
}

fn eval_literal(lit: &LiteralValue) -> ScalarValue {
    match lit {
        LiteralValue::Integer(n) => ScalarValue::Int64(*n),
        LiteralValue::Float(f) => ScalarValue::Float64(*f),
        LiteralValue::String(s) => ScalarValue::Utf8(s.clone()),
        LiteralValue::Boolean(b) => ScalarValue::Boolean(*b),
        LiteralValue::Null => ScalarValue::Null,
        LiteralValue::Interval { value, unit } => {
            // Parse interval - simplified
            let n: i64 = value.parse().unwrap_or(0);
            use crate::column::IntervalValue;
            let iv = match unit.to_uppercase().as_str() {
                "YEAR" | "YEARS" => IntervalValue::new(n as i32, 0, 0, 0),
                "MONTH" | "MONTHS" => IntervalValue::new(0, n as i32, 0, 0),
                "DAY" | "DAYS" => IntervalValue::new(0, 0, n as i32, 0),
                "HOUR" | "HOURS" => IntervalValue::new(0, 0, 0, n * 3_600_000_000),
                "MINUTE" | "MINUTES" => IntervalValue::new(0, 0, 0, n * 60_000_000),
                "SECOND" | "SECONDS" => IntervalValue::new(0, 0, 0, n * 1_000_000),
                _ => IntervalValue::zero(),
            };
            ScalarValue::Interval(iv)
        }
    }
}

fn eval_binary_op(op: &BinOp, l: ScalarValue, r: ScalarValue) -> Result<ScalarValue> {
    // NULL propagation
    if matches!(l, ScalarValue::Null) || matches!(r, ScalarValue::Null) {
        match op {
            BinOp::And => {
                if matches!(l, ScalarValue::Boolean(false)) || matches!(r, ScalarValue::Boolean(false)) {
                    return Ok(ScalarValue::Boolean(false));
                }
                return Ok(ScalarValue::Null);
            }
            BinOp::Or => {
                if matches!(l, ScalarValue::Boolean(true)) || matches!(r, ScalarValue::Boolean(true)) {
                    return Ok(ScalarValue::Boolean(true));
                }
                return Ok(ScalarValue::Null);
            }
            _ => return Ok(ScalarValue::Null),
        }
    }

    Ok(match op {
        BinOp::Add => numeric_op(&l, &r, |a, b| a + b, |a, b| a + b),
        BinOp::Sub => numeric_op(&l, &r, |a, b| a - b, |a, b| a - b),
        BinOp::Mul => numeric_op(&l, &r, |a, b| a * b, |a, b| a * b),
        BinOp::Div => {
            match (&l, &r) {
                (ScalarValue::Int64(a), ScalarValue::Int64(b)) => {
                    if *b == 0 { return Err(PivotError::SqlError("Division by zero".to_string())); }
                    ScalarValue::Float64(*a as f64 / *b as f64)
                }
                _ => numeric_op(&l, &r, |a, b| a / b, |a, b| a / b),
            }
        }
        BinOp::Mod => numeric_op(&l, &r, |a, b| a % b, |a, b| a % b),
        BinOp::Eq => ScalarValue::Boolean(scalar_eq(&l, &r)),
        BinOp::NotEq => ScalarValue::Boolean(!scalar_eq(&l, &r)),
        BinOp::Lt => ScalarValue::Boolean(scalar_cmp(&l, &r) == std::cmp::Ordering::Less),
        BinOp::LtEq => ScalarValue::Boolean(scalar_cmp(&l, &r) != std::cmp::Ordering::Greater),
        BinOp::Gt => ScalarValue::Boolean(scalar_cmp(&l, &r) == std::cmp::Ordering::Greater),
        BinOp::GtEq => ScalarValue::Boolean(scalar_cmp(&l, &r) != std::cmp::Ordering::Less),
        BinOp::And => ScalarValue::Boolean(is_truthy(&l) && is_truthy(&r)),
        BinOp::Or => ScalarValue::Boolean(is_truthy(&l) || is_truthy(&r)),
        BinOp::Concat => {
            let ls = scalar_to_string(&l);
            let rs = scalar_to_string(&r);
            ScalarValue::Utf8(ls + &rs)
        }
    })
}

fn eval_unary_op(op: &UnaryOp, v: ScalarValue) -> Result<ScalarValue> {
    match op {
        UnaryOp::Neg => match v {
            ScalarValue::Int64(i) => Ok(ScalarValue::Int64(-i)),
            ScalarValue::Float64(f) => Ok(ScalarValue::Float64(-f)),
            _ => Ok(ScalarValue::Null),
        },
        UnaryOp::Not => match v {
            ScalarValue::Boolean(b) => Ok(ScalarValue::Boolean(!b)),
            ScalarValue::Null => Ok(ScalarValue::Null),
            _ => Ok(ScalarValue::Boolean(!is_truthy(&v))),
        },
    }
}

fn eval_scalar_function(
    name: &str,
    args: &[Expr],
    row: &[ScalarValue],
    cols: &[Col],
    group_rows: Option<&Vec<Vec<ScalarValue>>>,
    ctes: &HashMap<String, RowSet>,
) -> Result<ScalarValue> {
    let fname = name.to_uppercase();

    // Evaluate arguments lazily where needed
    let eval_arg = |i: usize| -> Result<ScalarValue> {
        args.get(i)
            .map(|e| eval_expr(e, row, cols, group_rows, ctes))
            .unwrap_or(Ok(ScalarValue::Null))
    };

    match fname.as_str() {
        "COALESCE" | "IFNULL" | "NVL" => {
            for arg in args {
                let v = eval_expr(arg, row, cols, group_rows, ctes)?;
                if !matches!(v, ScalarValue::Null) { return Ok(v); }
            }
            Ok(ScalarValue::Null)
        }
        "NULLIF" => {
            let a = eval_arg(0)?;
            let b = eval_arg(1)?;
            Ok(if scalar_eq(&a, &b) { ScalarValue::Null } else { a })
        }
        "IF" | "IIF" => {
            let cond = eval_arg(0)?;
            if is_truthy(&cond) { eval_arg(1) } else { eval_arg(2) }
        }
        "GREATEST" => {
            let mut best: Option<ScalarValue> = None;
            for arg in args {
                let v = eval_expr(arg, row, cols, group_rows, ctes)?;
                if matches!(v, ScalarValue::Null) { continue; }
                best = Some(match best {
                    None => v,
                    Some(cur) => if scalar_cmp(&v, &cur) == std::cmp::Ordering::Greater { v } else { cur },
                });
            }
            Ok(best.unwrap_or(ScalarValue::Null))
        }
        "LEAST" => {
            let mut best: Option<ScalarValue> = None;
            for arg in args {
                let v = eval_expr(arg, row, cols, group_rows, ctes)?;
                if matches!(v, ScalarValue::Null) { continue; }
                best = Some(match best {
                    None => v,
                    Some(cur) => if scalar_cmp(&v, &cur) == std::cmp::Ordering::Less { v } else { cur },
                });
            }
            Ok(best.unwrap_or(ScalarValue::Null))
        }
        _ => {
            // Evaluate all args
            let evaled: Vec<ScalarValue> = args.iter()
                .map(|a| eval_expr(a, row, cols, group_rows, ctes))
                .collect::<Result<Vec<_>>>()?;

            // Try scalar functions
            if let Some(v) = functions_scalar::call(&fname, &evaled) {
                return Ok(v);
            }
            // Try datetime functions
            if let Some(v) = functions_datetime::call(&fname, &evaled) {
                return Ok(v);
            }

            // Unknown function - return NULL rather than error for robustness
            Ok(ScalarValue::Null)
        }
    }
}

// ─── Helper functions ─────────────────────────────────────────────────────────

fn find_col_idx(cols: &[Col], table: Option<&str>, name: &str) -> Option<usize> {
    if let Some(t) = table {
        if let Some(idx) = cols.iter().position(|c|
            c.table.as_deref().map(|s| s.eq_ignore_ascii_case(t)).unwrap_or(false)
            && c.name.eq_ignore_ascii_case(name)
        ) {
            return Some(idx);
        }
        // Try "table.col" format in name
        let qualified = format!("{}.{}", t, name);
        if let Some(idx) = cols.iter().position(|c| c.name.eq_ignore_ascii_case(&qualified)) {
            return Some(idx);
        }
    }
    let matches: Vec<usize> = cols.iter().enumerate()
        .filter(|(_, c)| c.name.eq_ignore_ascii_case(name))
        .map(|(i, _)| i)
        .collect();
    if matches.len() >= 1 { Some(matches[0]) } else { None }
}

fn is_truthy(v: &ScalarValue) -> bool {
    match v {
        ScalarValue::Boolean(b) => *b,
        ScalarValue::Null => false,
        ScalarValue::Int64(i) => *i != 0,
        ScalarValue::Float64(f) => *f != 0.0,
        ScalarValue::Utf8(s) => !s.is_empty(),
        _ => true,
    }
}

fn scalar_eq(a: &ScalarValue, b: &ScalarValue) -> bool {
    match (a, b) {
        (ScalarValue::Null, ScalarValue::Null) => false, // NULL != NULL in SQL
        (ScalarValue::Null, _) | (_, ScalarValue::Null) => false,
        (ScalarValue::Int64(x), ScalarValue::Int64(y)) => x == y,
        (ScalarValue::Float64(x), ScalarValue::Float64(y)) => x == y,
        (ScalarValue::Int64(x), ScalarValue::Float64(y)) => (*x as f64) == *y,
        (ScalarValue::Float64(x), ScalarValue::Int64(y)) => *x == (*y as f64),
        (ScalarValue::Utf8(x), ScalarValue::Utf8(y)) => x == y,
        (ScalarValue::Boolean(x), ScalarValue::Boolean(y)) => x == y,
        (ScalarValue::Date(x), ScalarValue::Date(y)) => x == y,
        (ScalarValue::Timestamp(x), ScalarValue::Timestamp(y)) => x == y,
        (ScalarValue::Time(x), ScalarValue::Time(y)) => x == y,
        _ => false,
    }
}

fn scalar_cmp(a: &ScalarValue, b: &ScalarValue) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (ScalarValue::Null, ScalarValue::Null) => Ordering::Equal,
        (ScalarValue::Null, _) => Ordering::Greater, // NULLs last
        (_, ScalarValue::Null) => Ordering::Less,
        (ScalarValue::Int64(x), ScalarValue::Int64(y)) => x.cmp(y),
        (ScalarValue::Float64(x), ScalarValue::Float64(y)) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
        (ScalarValue::Int64(x), ScalarValue::Float64(y)) => (*x as f64).partial_cmp(y).unwrap_or(Ordering::Equal),
        (ScalarValue::Float64(x), ScalarValue::Int64(y)) => x.partial_cmp(&(*y as f64)).unwrap_or(Ordering::Equal),
        (ScalarValue::Utf8(x), ScalarValue::Utf8(y)) => x.cmp(y),
        (ScalarValue::Boolean(x), ScalarValue::Boolean(y)) => x.cmp(y),
        (ScalarValue::Date(x), ScalarValue::Date(y)) => x.cmp(y),
        (ScalarValue::Timestamp(x), ScalarValue::Timestamp(y)) => x.cmp(y),
        (ScalarValue::Time(x), ScalarValue::Time(y)) => x.cmp(y),
        _ => Ordering::Equal,
    }
}

fn scalar_to_key(v: &ScalarValue) -> String {
    match v {
        ScalarValue::Null => "\x00NULL\x00".to_string(),
        other => format!("{}", other),
    }
}

fn scalar_to_string(v: &ScalarValue) -> String {
    match v {
        ScalarValue::Null => String::new(),
        other => format!("{}", other),
    }
}

fn numeric_op(
    l: &ScalarValue, r: &ScalarValue,
    int_op: impl Fn(i64, i64) -> i64,
    float_op: impl Fn(f64, f64) -> f64,
) -> ScalarValue {
    match (l, r) {
        (ScalarValue::Int64(a), ScalarValue::Int64(b)) => ScalarValue::Int64(int_op(*a, *b)),
        (ScalarValue::Float64(a), ScalarValue::Float64(b)) => ScalarValue::Float64(float_op(*a, *b)),
        (ScalarValue::Int64(a), ScalarValue::Float64(b)) => ScalarValue::Float64(float_op(*a as f64, *b)),
        (ScalarValue::Float64(a), ScalarValue::Int64(b)) => ScalarValue::Float64(float_op(*a, *b as f64)),
        _ => ScalarValue::Null,
    }
}

fn like_match(text: &str, pattern: &str, case_insensitive: bool) -> bool {
    let t: Vec<char> = if case_insensitive { text.to_lowercase().chars().collect() }
                       else { text.chars().collect() };
    let p: Vec<char> = if case_insensitive { pattern.to_lowercase().chars().collect() }
                       else { pattern.chars().collect() };
    like_match_chars(&t, &p)
}

fn like_match_chars(text: &[char], pattern: &[char]) -> bool {
    match (text, pattern) {
        (_, []) => text.is_empty(),
        (_, ['%', rest @ ..]) => {
            // % matches any sequence
            for i in 0..=text.len() {
                if like_match_chars(&text[i..], rest) { return true; }
            }
            false
        }
        ([], [_, ..]) => false,
        ([t, rest_t @ ..], ['_', rest_p @ ..]) => like_match_chars(rest_t, rest_p),
        ([t, rest_t @ ..], [p, rest_p @ ..]) => {
            t == p && like_match_chars(rest_t, rest_p)
        }
    }
}

fn expr_display_name(expr: &Expr) -> String {
    match expr {
        Expr::Column(col_ref) => col_ref.name.clone(),
        Expr::Function { name, .. } => name.to_lowercase(),
        Expr::Literal(lit) => match lit {
            LiteralValue::Integer(n) => n.to_string(),
            LiteralValue::Float(f) => f.to_string(),
            LiteralValue::String(s) => s.clone(),
            LiteralValue::Boolean(b) => b.to_string(),
            LiteralValue::Null => "NULL".to_string(),
            LiteralValue::Interval { value, unit } => format!("{} {}", value, unit),
        },
        Expr::Cast { data_type, .. } => format!("cast({})", data_type),
        Expr::BinaryOp { left, op, right } => {
            let op_str = match op {
                BinOp::Add => "+", BinOp::Sub => "-", BinOp::Mul => "*",
                BinOp::Div => "/", BinOp::Mod => "%",
                _ => "op",
            };
            format!("{}{}{}", expr_display_name(left), op_str, expr_display_name(right))
        }
        _ => "expr".to_string(),
    }
}

fn select_items_have_aggregate(items: &[SelectItem]) -> bool {
    items.iter().any(|item| match item {
        SelectItem::Expr { expr, .. } => expr_has_aggregate(expr),
        _ => false,
    })
}

fn expr_has_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Function { name, over: None, .. } => {
            matches!(name.to_uppercase().as_str(),
                "COUNT" | "SUM" | "AVG" | "MIN" | "MAX"
                | "STRING_AGG" | "GROUP_CONCAT" | "LISTAGG"
                | "ARRAY_AGG" | "STDDEV" | "STDEV" | "STDDEV_SAMP" | "STDDEV_POP"
                | "VARIANCE" | "VAR_SAMP" | "VAR_POP"
            )
        }
        Expr::BinaryOp { left, right, .. } => expr_has_aggregate(left) || expr_has_aggregate(right),
        Expr::UnaryOp { expr: inner, .. } => expr_has_aggregate(inner),
        Expr::Cast { expr: inner, .. } => expr_has_aggregate(inner),
        Expr::Case { operand, when_clauses, else_clause } => {
            operand.as_ref().map(|e| expr_has_aggregate(e)).unwrap_or(false)
                || when_clauses.iter().any(|(c, t)| expr_has_aggregate(c) || expr_has_aggregate(t))
                || else_clause.as_ref().map(|e| expr_has_aggregate(e)).unwrap_or(false)
        }
        _ => false,
    }
}

fn expr_has_window(expr: &Expr) -> bool {
    match expr {
        Expr::Function { over: Some(_), .. } => true,
        Expr::BinaryOp { left, right, .. } => expr_has_window(left) || expr_has_window(right),
        Expr::UnaryOp { expr: inner, .. } => expr_has_window(inner),
        _ => false,
    }
}

fn tag_rowset(mut rs: RowSet, alias: &str) -> RowSet {
    for col in &mut rs.cols {
        col.table = Some(alias.to_string());
    }
    rs
}

fn dedup_rowset(mut rs: RowSet) -> RowSet {
    let mut seen: Vec<Vec<ScalarValue>> = Vec::new();
    rs.rows.retain(|row| {
        let key: Vec<String> = row.iter().map(|v| format!("{:?}", v)).collect();
        if seen.iter().any(|s| {
            s.iter().zip(row.iter()).all(|(a, b)| format!("{:?}", a) == format!("{:?}", b))
        }) {
            false
        } else {
            seen.push(row.clone());
            true
        }
    });
    rs
}
