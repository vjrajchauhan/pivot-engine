use crate::error::{PivotError, Result};

#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    Boolean,
    Int64,
    Float64,
    Utf8,
    Date,
    Timestamp,
    Time,
    Interval,
    Decimal { precision: u8, scale: u8 },
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Boolean => write!(f, "BOOLEAN"),
            DataType::Int64 => write!(f, "INTEGER"),
            DataType::Float64 => write!(f, "DOUBLE"),
            DataType::Utf8 => write!(f, "VARCHAR"),
            DataType::Date => write!(f, "DATE"),
            DataType::Timestamp => write!(f, "TIMESTAMP"),
            DataType::Time => write!(f, "TIME"),
            DataType::Interval => write!(f, "INTERVAL"),
            DataType::Decimal { precision, scale } => write!(f, "DECIMAL({},{})", precision, scale),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl ColumnDef {
    pub fn new(name: &str, data_type: DataType, nullable: bool) -> Self {
        Self { name: name.to_string(), data_type, nullable }
    }
}

#[derive(Debug, Clone)]
pub struct Schema {
    pub columns: Vec<ColumnDef>,
}

impl Schema {
    pub fn new(columns: Vec<ColumnDef>) -> Self { Self { columns } }
    pub fn column_count(&self) -> usize { self.columns.len() }
    pub fn find_column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name.eq_ignore_ascii_case(name))
    }
    pub fn find_column(&self, name: &str) -> Option<&ColumnDef> {
        self.columns.iter().find(|c| c.name.eq_ignore_ascii_case(name))
    }
    pub fn has_column(&self, name: &str) -> bool { self.find_column_index(name).is_some() }
    pub fn column_names(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.name.clone()).collect()
    }
    pub fn validate_row_count(&self, count: usize) -> Result<()> {
        if count != self.columns.len() {
            Err(PivotError::SchemaError(format!(
                "Expected {} columns, got {}", self.columns.len(), count
            )))
        } else {
            Ok(())
        }
    }
}
