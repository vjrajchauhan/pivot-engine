use crate::column::ScalarValue;
use crate::datastore::DataStore;
use crate::error::Result;
use std::collections::HashMap;

pub struct GroupResult {
    pub key: Vec<ScalarValue>,
    pub row_indices: Vec<usize>,
}

pub fn group_by(store: &DataStore, col_names: &[&str]) -> Result<Vec<GroupResult>> {
    let mut map: HashMap<Vec<String>, Vec<usize>> = HashMap::new();
    let mut key_order: Vec<Vec<String>> = Vec::new();
    for row in 0..store.row_count() {
        let key_strs: Vec<String> = col_names.iter().map(|name| {
            store.get_value(row, name).ok()
                .map(|v| format!("{}", v))
                .unwrap_or_default()
        }).collect();
        let entry = map.entry(key_strs.clone()).or_insert_with(Vec::new);
        if entry.is_empty() {
            key_order.push(key_strs);
        }
        entry.push(row);
    }
    let mut results = Vec::new();
    for key_strs in &key_order {
        let indices = map.remove(key_strs).unwrap_or_default();
        let key: Vec<ScalarValue> = key_strs.iter()
            .map(|s| ScalarValue::Utf8(s.clone())).collect();
        results.push(GroupResult { key, row_indices: indices });
    }
    Ok(results)
}
