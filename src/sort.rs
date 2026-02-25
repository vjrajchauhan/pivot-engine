use crate::column::ScalarValue;
use crate::datastore::DataStore;
use crate::error::Result;

pub fn sort_by(store: &DataStore, col_names: &[&str], ascending: &[bool]) -> Result<DataStore> {
    let mut indices: Vec<usize> = (0..store.row_count()).collect();
    indices.sort_by(|&a, &b| {
        for (i, col) in col_names.iter().enumerate() {
            let asc = ascending.get(i).copied().unwrap_or(true);
            let va = store.get_value(a, col).unwrap_or(ScalarValue::Null);
            let vb = store.get_value(b, col).unwrap_or(ScalarValue::Null);
            let ord = compare_scalar(&va, &vb);
            if ord != std::cmp::Ordering::Equal {
                return if asc { ord } else { ord.reverse() };
            }
        }
        std::cmp::Ordering::Equal
    });
    let mut result = DataStore::new(store.schema().clone());
    for idx in indices {
        let row = store.get_row(idx)?;
        result.append_row(row)?;
    }
    Ok(result)
}

pub fn compare_scalar(a: &ScalarValue, b: &ScalarValue) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (ScalarValue::Null, ScalarValue::Null) => Ordering::Equal,
        (ScalarValue::Null, _) => Ordering::Greater,
        (_, ScalarValue::Null) => Ordering::Less,
        (ScalarValue::Int64(x), ScalarValue::Int64(y)) => x.cmp(y),
        (ScalarValue::Float64(x), ScalarValue::Float64(y)) =>
            x.partial_cmp(y).unwrap_or(Ordering::Equal),
        (ScalarValue::Int64(x), ScalarValue::Float64(y)) =>
            (*x as f64).partial_cmp(y).unwrap_or(Ordering::Equal),
        (ScalarValue::Float64(x), ScalarValue::Int64(y)) =>
            x.partial_cmp(&(*y as f64)).unwrap_or(Ordering::Equal),
        (ScalarValue::Utf8(x), ScalarValue::Utf8(y)) => x.cmp(y),
        (ScalarValue::Boolean(x), ScalarValue::Boolean(y)) => x.cmp(y),
        (ScalarValue::Date(x), ScalarValue::Date(y)) => x.cmp(y),
        (ScalarValue::Timestamp(x), ScalarValue::Timestamp(y)) => x.cmp(y),
        (ScalarValue::Time(x), ScalarValue::Time(y)) => x.cmp(y),
        _ => Ordering::Equal,
    }
}
