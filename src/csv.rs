use crate::column::ScalarValue;
use crate::datastore::DataStore;
use crate::error::{PivotError, Result};
use crate::schema::{ColumnDef, DataType, Schema};

pub struct CsvReader {
    pub delimiter: char,
    pub has_header: bool,
}

impl CsvReader {
    pub fn new() -> Self { Self { delimiter: ',', has_header: true } }
    pub fn with_delimiter(mut self, delimiter: char) -> Self { self.delimiter = delimiter; self }
    pub fn with_header(mut self, has_header: bool) -> Self { self.has_header = has_header; self }

    pub fn read_str(&self, data: &str) -> Result<DataStore> {
        let mut lines = data.lines();
        let headers: Vec<String> = if self.has_header {
            if let Some(line) = lines.next() {
                self.split_line(line)
            } else {
                return Err(PivotError::IoError("Empty CSV".to_string()));
            }
        } else {
            Vec::new()
        };

        let mut all_rows: Vec<Vec<String>> = Vec::new();
        for line in lines {
            if line.trim().is_empty() { continue; }
            all_rows.push(self.split_line(line));
        }

        let col_count = if !headers.is_empty() {
            headers.len()
        } else if !all_rows.is_empty() {
            all_rows[0].len()
        } else {
            0
        };

        let col_names: Vec<String> = if !headers.is_empty() {
            headers
        } else {
            (0..col_count).map(|i| format!("col{}", i)).collect()
        };

        let schema = Schema::new(col_names.iter()
            .map(|name| ColumnDef::new(name, DataType::Utf8, true))
            .collect());
        let mut store = DataStore::new(schema);
        for row in all_rows {
            let values: Vec<ScalarValue> = row.into_iter()
                .map(|s| if s.is_empty() { ScalarValue::Null } else { ScalarValue::Utf8(s) })
                .collect();
            let mut padded = values;
            padded.resize(col_count, ScalarValue::Null);
            store.append_row(padded)?;
        }
        Ok(store)
    }

    fn split_line(&self, line: &str) -> Vec<String> {
        let mut result = Vec::new();
        let mut current = String::new();
        let mut in_quotes = false;
        let mut chars = line.chars().peekable();
        while let Some(c) = chars.next() {
            if c == '"' {
                if in_quotes {
                    if chars.peek() == Some(&'"') {
                        chars.next();
                        current.push('"');
                    } else {
                        in_quotes = false;
                    }
                } else {
                    in_quotes = true;
                }
            } else if c == self.delimiter && !in_quotes {
                result.push(current.clone());
                current.clear();
            } else {
                current.push(c);
            }
        }
        result.push(current);
        result
    }
}

pub struct CsvWriter {
    pub delimiter: char,
    pub write_header: bool,
}

impl CsvWriter {
    pub fn new() -> Self { Self { delimiter: ',', write_header: true } }

    pub fn write_str(&self, store: &DataStore) -> Result<String> {
        let mut out = String::new();
        if self.write_header {
            let headers: Vec<String> = store.schema().column_names();
            out.push_str(&headers.join(&self.delimiter.to_string()));
            out.push('\n');
        }
        for row in 0..store.row_count() {
            let values: Vec<String> = (0..store.schema().column_count())
                .map(|col| {
                    store.get_value_by_index(row, col)
                        .ok()
                        .map(|v| match v {
                            ScalarValue::Null => String::new(),
                            other => format!("{}", other),
                        })
                        .unwrap_or_default()
                })
                .collect();
            out.push_str(&values.join(&self.delimiter.to_string()));
            out.push('\n');
        }
        Ok(out)
    }
}
