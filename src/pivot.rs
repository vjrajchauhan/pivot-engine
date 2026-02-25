use crate::column::ScalarValue;
use crate::datastore::DataStore;
use crate::error::Result;
use crate::schema::{ColumnDef, DataType, Schema};
use std::collections::HashMap;

pub fn pivot_table(
    store: &DataStore,
    row_keys: &[&str],
    col_key: &str,
    value_col: &str,
) -> Result<DataStore> {
    let mut pivot_vals: Vec<String> = Vec::new();
    for row in 0..store.row_count() {
        let v = format!("{}", store.get_value(row, col_key)?);
        if !pivot_vals.contains(&v) {
            pivot_vals.push(v);
        }
    }
    let mut schema_cols: Vec<ColumnDef> = row_keys.iter()
        .map(|k| ColumnDef::new(k, DataType::Utf8, true))
        .collect();
    for pv in &pivot_vals {
        schema_cols.push(ColumnDef::new(pv, DataType::Float64, true));
    }
    let mut result = DataStore::new(Schema::new(schema_cols));

    let mut groups: HashMap<Vec<String>, HashMap<String, ScalarValue>> = HashMap::new();
    let mut key_order: Vec<Vec<String>> = Vec::new();
    for row in 0..store.row_count() {
        let key: Vec<String> = row_keys.iter()
            .map(|k| format!("{}", store.get_value(row, k).unwrap_or(ScalarValue::Null)))
            .collect();
        let pv = format!("{}", store.get_value(row, col_key)?);
        let val = store.get_value(row, value_col)?;
        let entry = groups.entry(key.clone()).or_insert_with(|| {
            key_order.push(key);
            HashMap::new()
        });
        entry.insert(pv, val);
    }
    for key in &key_order {
        let vals = &groups[key];
        let mut row_vals: Vec<ScalarValue> = key.iter()
            .map(|k| ScalarValue::Utf8(k.clone()))
            .collect();
        for pv in &pivot_vals {
            row_vals.push(vals.get(pv).cloned().unwrap_or(ScalarValue::Null));
        }
        result.append_row(row_vals)?;
    }
    Ok(result)
}

pub fn unpivot_table(
    store: &DataStore,
    id_cols: &[&str],
    value_cols: &[&str],
    name_col: &str,
    value_col_name: &str,
) -> Result<DataStore> {
    let extra_cols = vec![
        ColumnDef::new(name_col, DataType::Utf8, false),
        ColumnDef::new(value_col_name, DataType::Utf8, true),
    ];
    let mut id_schema_cols: Vec<ColumnDef> = id_cols.iter()
        .map(|k| ColumnDef::new(k, DataType::Utf8, true))
        .collect();
    id_schema_cols.extend(extra_cols);
    let mut result = DataStore::new(Schema::new(id_schema_cols));
    for row in 0..store.row_count() {
        for vc in value_cols {
            let val = store.get_value(row, vc)?;
            if matches!(val, ScalarValue::Null) { continue; }
            let mut row_vals: Vec<ScalarValue> = id_cols.iter()
                .map(|k| store.get_value(row, k).unwrap_or(ScalarValue::Null))
                .collect();
            row_vals.push(ScalarValue::Utf8(vc.to_string()));
            row_vals.push(ScalarValue::Utf8(format!("{}", val)));
            result.append_row(row_vals)?;
        }
    }
    Ok(result)
}
