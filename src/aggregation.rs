use crate::column::ScalarValue;
use crate::datastore::DataStore;
use crate::error::Result;
use std::cmp::Ordering;

pub fn sum(store: &DataStore, col_name: &str) -> Result<ScalarValue> {
    let mut total_int = 0i64;
    let mut total_float = 0.0f64;
    let mut is_float = false;
    let mut has_value = false;
    for row in 0..store.row_count() {
        match store.get_value(row, col_name)? {
            ScalarValue::Int64(i) => { total_int += i; has_value = true; }
            ScalarValue::Float64(f) => { total_float += f; is_float = true; has_value = true; }
            _ => {}
        }
    }
    if !has_value { return Ok(ScalarValue::Null); }
    if is_float { Ok(ScalarValue::Float64(total_float + total_int as f64)) }
    else { Ok(ScalarValue::Int64(total_int)) }
}

pub fn count(store: &DataStore, col_name: &str) -> Result<ScalarValue> {
    let mut n = 0i64;
    for row in 0..store.row_count() {
        if !matches!(store.get_value(row, col_name)?, ScalarValue::Null) { n += 1; }
    }
    Ok(ScalarValue::Int64(n))
}

pub fn avg(store: &DataStore, col_name: &str) -> Result<ScalarValue> {
    let mut total = 0.0f64;
    let mut n = 0i64;
    for row in 0..store.row_count() {
        match store.get_value(row, col_name)? {
            ScalarValue::Int64(i) => { total += i as f64; n += 1; }
            ScalarValue::Float64(f) => { total += f; n += 1; }
            _ => {}
        }
    }
    if n == 0 { Ok(ScalarValue::Null) } else { Ok(ScalarValue::Float64(total / n as f64)) }
}

pub fn min(store: &DataStore, col_name: &str) -> Result<ScalarValue> {
    let mut result: Option<ScalarValue> = None;
    for row in 0..store.row_count() {
        let v = store.get_value(row, col_name)?;
        if matches!(v, ScalarValue::Null) { continue; }
        result = Some(match &result {
            None => v,
            Some(cur) => if cmp_scalar(&v, cur) == Ordering::Less { v } else { cur.clone() },
        });
    }
    Ok(result.unwrap_or(ScalarValue::Null))
}

pub fn max(store: &DataStore, col_name: &str) -> Result<ScalarValue> {
    let mut result: Option<ScalarValue> = None;
    for row in 0..store.row_count() {
        let v = store.get_value(row, col_name)?;
        if matches!(v, ScalarValue::Null) { continue; }
        result = Some(match &result {
            None => v,
            Some(cur) => if cmp_scalar(&v, cur) == Ordering::Greater { v } else { cur.clone() },
        });
    }
    Ok(result.unwrap_or(ScalarValue::Null))
}

pub fn cmp_scalar(a: &ScalarValue, b: &ScalarValue) -> Ordering {
    match (a, b) {
        (ScalarValue::Int64(x), ScalarValue::Int64(y)) => x.cmp(y),
        (ScalarValue::Float64(x), ScalarValue::Float64(y)) =>
            x.partial_cmp(y).unwrap_or(Ordering::Equal),
        (ScalarValue::Utf8(x), ScalarValue::Utf8(y)) => x.cmp(y),
        (ScalarValue::Boolean(x), ScalarValue::Boolean(y)) => x.cmp(y),
        _ => Ordering::Equal,
    }
}
