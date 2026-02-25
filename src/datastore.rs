use crate::bitmap::NullBitmask;
use crate::column::{ScalarValue, date_string_to_epoch_days,
    timestamp_string_to_epoch_micros, time_string_to_micros};
use crate::error::{PivotError, Result};
use crate::schema::{ColumnDef, DataType, Schema};

#[derive(Debug, Clone)]
pub struct ColumnStorage {
    pub booleans: Vec<bool>,
    pub int64s: Vec<i64>,
    pub float64s: Vec<f64>,
    pub utf8s: Vec<String>,
    pub nullmask: NullBitmask,
}

impl ColumnStorage {
    fn new() -> Self {
        Self {
            booleans: Vec::new(),
            int64s: Vec::new(),
            float64s: Vec::new(),
            utf8s: Vec::new(),
            nullmask: NullBitmask::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DataStore {
    schema: Schema,
    columns: Vec<ColumnStorage>,
    row_count: usize,
}

impl DataStore {
    pub fn new(schema: Schema) -> Self {
        let col_count = schema.column_count();
        let mut columns = Vec::with_capacity(col_count);
        for _ in 0..col_count {
            columns.push(ColumnStorage::new());
        }
        Self { schema, columns, row_count: 0 }
    }

    pub fn schema(&self) -> &Schema { &self.schema }
    pub fn row_count(&self) -> usize { self.row_count }

    pub fn append_row(&mut self, values: Vec<ScalarValue>) -> Result<()> {
        if values.len() != self.schema.column_count() {
            return Err(PivotError::SchemaError(format!(
                "Expected {} values, got {}", self.schema.column_count(), values.len()
            )));
        }
        for (i, val) in values.iter().enumerate() {
            let col_def = &self.schema.columns[i];
            if matches!(val, ScalarValue::Null) && !col_def.nullable {
                return Err(PivotError::NullError(format!(
                    "Column '{}' is NOT NULL", col_def.name
                )));
            }
        }
        for (i, val) in values.into_iter().enumerate() {
            let col_def = &self.schema.columns[i];
            let coerced = self.coerce_value(val, &col_def.data_type.clone())?;
            self.push_to_column(i, coerced);
        }
        self.row_count += 1;
        Ok(())
    }

    fn coerce_value(&self, val: ScalarValue, target: &DataType) -> Result<ScalarValue> {
        match (&val, target) {
            (ScalarValue::Null, _) => Ok(ScalarValue::Null),
            (ScalarValue::Int64(i), DataType::Float64) => Ok(ScalarValue::Float64(*i as f64)),
            (ScalarValue::Float64(_), DataType::Float64) => Ok(val),
            (ScalarValue::Int64(_), DataType::Int64) => Ok(val),
            (ScalarValue::Utf8(s), DataType::Date) => {
                date_string_to_epoch_days(s)
                    .map(ScalarValue::Date)
                    .ok_or_else(|| PivotError::TypeError(format!("Cannot parse date: {}", s)))
            }
            (ScalarValue::Utf8(s), DataType::Timestamp) => {
                timestamp_string_to_epoch_micros(s)
                    .map(ScalarValue::Timestamp)
                    .ok_or_else(|| PivotError::TypeError(format!("Cannot parse timestamp: {}", s)))
            }
            (ScalarValue::Utf8(s), DataType::Time) => {
                time_string_to_micros(s)
                    .map(ScalarValue::Time)
                    .ok_or_else(|| PivotError::TypeError(format!("Cannot parse time: {}", s)))
            }
            (ScalarValue::Int64(i), DataType::Date) => Ok(ScalarValue::Date(*i)),
            (ScalarValue::Int64(i), DataType::Timestamp) => Ok(ScalarValue::Timestamp(*i)),
            (ScalarValue::Int64(i), DataType::Time) => Ok(ScalarValue::Time(*i)),
            (ScalarValue::Float64(v), DataType::Int64) => Ok(ScalarValue::Int64(*v as i64)),
            (ScalarValue::Int64(i), DataType::Decimal { .. }) => Ok(ScalarValue::Float64(*i as f64)),
            (ScalarValue::Float64(_), DataType::Decimal { .. }) => Ok(val),
            _ => Ok(val),
        }
    }

    fn push_to_column(&mut self, col_idx: usize, val: ScalarValue) {
        let col = &mut self.columns[col_idx];
        let data_type = &self.schema.columns[col_idx].data_type.clone();
        match val {
            ScalarValue::Null => {
                match data_type {
                    DataType::Boolean => col.booleans.push(false),
                    DataType::Int64 | DataType::Date
                    | DataType::Timestamp | DataType::Time => col.int64s.push(0),
                    DataType::Float64 | DataType::Decimal { .. } => col.float64s.push(0.0),
                    DataType::Utf8 | DataType::Interval => col.utf8s.push(String::new()),
                }
                col.nullmask.push(false);
            }
            ScalarValue::Boolean(b) => {
                col.booleans.push(b);
                col.nullmask.push(true);
            }
            ScalarValue::Int64(i) => {
                col.int64s.push(i);
                col.nullmask.push(true);
            }
            ScalarValue::Float64(f) => {
                col.float64s.push(f);
                col.nullmask.push(true);
            }
            ScalarValue::Utf8(s) => {
                col.utf8s.push(s);
                col.nullmask.push(true);
            }
            ScalarValue::Date(d) | ScalarValue::Timestamp(d) | ScalarValue::Time(d) => {
                col.int64s.push(d);
                col.nullmask.push(true);
            }
            ScalarValue::Interval(iv) => {
                col.utf8s.push(format!("{}:{}:{}:{}", iv.years, iv.months, iv.days, iv.micros));
                col.nullmask.push(true);
            }
        }
    }

    pub fn get_value_by_index(&self, row: usize, col_idx: usize) -> Result<ScalarValue> {
        if row >= self.row_count {
            return Err(PivotError::IndexOutOfBounds(
                format!("Row {} out of bounds ({})", row, self.row_count)));
        }
        if col_idx >= self.schema.column_count() {
            return Err(PivotError::IndexOutOfBounds(
                format!("Column {} out of bounds", col_idx)));
        }
        let col = &self.columns[col_idx];
        if !col.nullmask.get(row) {
            return Ok(ScalarValue::Null);
        }
        let data_type = &self.schema.columns[col_idx].data_type;
        Ok(match data_type {
            DataType::Boolean => ScalarValue::Boolean(*col.booleans.get(row).unwrap_or(&false)),
            DataType::Int64 => ScalarValue::Int64(*col.int64s.get(row).unwrap_or(&0)),
            DataType::Float64 | DataType::Decimal { .. } =>
                ScalarValue::Float64(*col.float64s.get(row).unwrap_or(&0.0)),
            DataType::Utf8 => ScalarValue::Utf8(col.utf8s.get(row).cloned().unwrap_or_default()),
            DataType::Date => ScalarValue::Date(*col.int64s.get(row).unwrap_or(&0)),
            DataType::Timestamp => ScalarValue::Timestamp(*col.int64s.get(row).unwrap_or(&0)),
            DataType::Time => ScalarValue::Time(*col.int64s.get(row).unwrap_or(&0)),
            DataType::Interval => {
                let s = col.utf8s.get(row).cloned().unwrap_or_default();
                let parts: Vec<&str> = s.splitn(4, ':').collect();
                if parts.len() == 4 {
                    use crate::column::IntervalValue;
                    ScalarValue::Interval(IntervalValue::new(
                        parts[0].parse().unwrap_or(0),
                        parts[1].parse().unwrap_or(0),
                        parts[2].parse().unwrap_or(0),
                        parts[3].parse().unwrap_or(0),
                    ))
                } else {
                    ScalarValue::Null
                }
            }
        })
    }

    pub fn get_value(&self, row: usize, col_name: &str) -> Result<ScalarValue> {
        let idx = self.schema.find_column_index(col_name)
            .ok_or_else(|| PivotError::ColumnNotFound(col_name.to_string()))?;
        self.get_value_by_index(row, idx)
    }

    pub fn get_row(&self, row: usize) -> Result<Vec<ScalarValue>> {
        (0..self.schema.column_count())
            .map(|i| self.get_value_by_index(row, i))
            .collect()
    }

    pub fn set_value(&mut self, row: usize, col_idx: usize, val: ScalarValue) -> Result<()> {
        if row >= self.row_count {
            return Err(PivotError::IndexOutOfBounds(format!("Row {} out of bounds", row)));
        }
        let data_type = self.schema.columns[col_idx].data_type.clone();
        let coerced = self.coerce_value(val, &data_type)?;
        let col = &mut self.columns[col_idx];
        match coerced {
            ScalarValue::Null => { col.nullmask.set(row, false); }
            ScalarValue::Boolean(b) => {
                if row < col.booleans.len() { col.booleans[row] = b; } else { col.booleans.push(b); }
                col.nullmask.set(row, true);
            }
            ScalarValue::Int64(i) => {
                if row < col.int64s.len() { col.int64s[row] = i; } else { col.int64s.push(i); }
                col.nullmask.set(row, true);
            }
            ScalarValue::Float64(f) => {
                if row < col.float64s.len() { col.float64s[row] = f; } else { col.float64s.push(f); }
                col.nullmask.set(row, true);
            }
            ScalarValue::Utf8(s) => {
                if row < col.utf8s.len() { col.utf8s[row] = s; } else { col.utf8s.push(s); }
                col.nullmask.set(row, true);
            }
            ScalarValue::Date(d) | ScalarValue::Timestamp(d) | ScalarValue::Time(d) => {
                if row < col.int64s.len() { col.int64s[row] = d; } else { col.int64s.push(d); }
                col.nullmask.set(row, true);
            }
            ScalarValue::Interval(iv) => {
                let s = format!("{}:{}:{}:{}", iv.years, iv.months, iv.days, iv.micros);
                if row < col.utf8s.len() { col.utf8s[row] = s; } else { col.utf8s.push(s); }
                col.nullmask.set(row, true);
            }
        }
        Ok(())
    }

    pub fn add_column(&mut self, def: ColumnDef) -> Result<()> {
        if self.schema.has_column(&def.name) {
            return Err(PivotError::SchemaError(
                format!("Column '{}' already exists", def.name)));
        }
        let mut storage = ColumnStorage::new();
        for _ in 0..self.row_count {
            match &def.data_type {
                DataType::Boolean => storage.booleans.push(false),
                DataType::Int64 | DataType::Date
                | DataType::Timestamp | DataType::Time => storage.int64s.push(0),
                DataType::Float64 | DataType::Decimal { .. } => storage.float64s.push(0.0),
                DataType::Utf8 | DataType::Interval => storage.utf8s.push(String::new()),
            }
            storage.nullmask.push(false);
        }
        self.schema.columns.push(def);
        self.columns.push(storage);
        Ok(())
    }

    pub fn drop_column(&mut self, name: &str) -> Result<()> {
        let idx = self.schema.find_column_index(name)
            .ok_or_else(|| PivotError::ColumnNotFound(name.to_string()))?;
        self.schema.columns.remove(idx);
        self.columns.remove(idx);
        Ok(())
    }

    pub fn rename_column(&mut self, old_name: &str, new_name: &str) -> Result<()> {
        let idx = self.schema.find_column_index(old_name)
            .ok_or_else(|| PivotError::ColumnNotFound(old_name.to_string()))?;
        self.schema.columns[idx].name = new_name.to_string();
        Ok(())
    }
}
