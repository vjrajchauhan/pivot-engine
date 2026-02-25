use std::fmt;

#[derive(Debug)]
pub enum PivotError {
    SqlError(String),
    SchemaError(String),
    ColumnNotFound(String),
    NullError(String),
    IndexOutOfBounds(String),
    IoError(String),
    TypeError(String),
}

impl fmt::Display for PivotError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PivotError::SqlError(msg) => write!(f, "SQL Error: {}", msg),
            PivotError::SchemaError(msg) => write!(f, "Schema Error: {}", msg),
            PivotError::ColumnNotFound(name) => write!(f, "Column not found: {}", name),
            PivotError::NullError(msg) => write!(f, "Null Error: {}", msg),
            PivotError::IndexOutOfBounds(msg) => write!(f, "Index out of bounds: {}", msg),
            PivotError::IoError(msg) => write!(f, "IO Error: {}", msg),
            PivotError::TypeError(msg) => write!(f, "Type Error: {}", msg),
        }
    }
}

impl std::error::Error for PivotError {}
pub type Result<T> = std::result::Result<T, PivotError>;
