use crate::column::ScalarValue;
use crate::datastore::DataStore;
use crate::error::Result;

pub fn filter_by<F>(store: &DataStore, predicate: F) -> Result<DataStore>
where
    F: Fn(usize, &DataStore) -> bool,
{
    let mut result = DataStore::new(store.schema().clone());
    for row in 0..store.row_count() {
        if predicate(row, store) {
            let values = store.get_row(row)?;
            result.append_row(values)?;
        }
    }
    Ok(result)
}

pub fn filter_eq(store: &DataStore, col_name: &str, value: &ScalarValue) -> Result<DataStore> {
    filter_by(store, |row, s| {
        s.get_value(row, col_name)
            .ok()
            .map(|v| values_eq(&v, value))
            .unwrap_or(false)
    })
}

fn values_eq(a: &ScalarValue, b: &ScalarValue) -> bool {
    match (a, b) {
        (ScalarValue::Int64(x), ScalarValue::Int64(y)) => x == y,
        (ScalarValue::Float64(x), ScalarValue::Float64(y)) => x == y,
        (ScalarValue::Utf8(x), ScalarValue::Utf8(y)) => x == y,
        (ScalarValue::Boolean(x), ScalarValue::Boolean(y)) => x == y,
        (ScalarValue::Null, ScalarValue::Null) => true,
        _ => false,
    }
}
