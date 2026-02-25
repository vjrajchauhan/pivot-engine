use crate::column::ScalarValue;
use crate::schema::DataType;

/// Cast a ScalarValue to a target DataType, returning Null on failure.
pub fn cast_value(v: ScalarValue, target: &DataType) -> ScalarValue {
    try_cast_value(v, target)
}

/// Try to cast; returns Null if conversion is not possible.
pub fn try_cast_value(v: ScalarValue, target: &DataType) -> ScalarValue {
    if matches!(v, ScalarValue::Null) { return ScalarValue::Null; }
    match target {
        DataType::Int64 => to_int64(v),
        DataType::Float64 | DataType::Decimal { .. } => to_float64(v),
        DataType::Utf8 => to_utf8(v),
        DataType::Boolean => to_boolean(v),
        DataType::Date => to_date(v),
        DataType::Timestamp => to_timestamp(v),
        DataType::Time => to_time(v),
        DataType::Interval => ScalarValue::Null,
    }
}

fn to_int64(v: ScalarValue) -> ScalarValue {
    match v {
        ScalarValue::Int64(i) => ScalarValue::Int64(i),
        ScalarValue::Float64(f) => ScalarValue::Int64(f as i64),
        ScalarValue::Boolean(b) => ScalarValue::Int64(if b { 1 } else { 0 }),
        ScalarValue::Utf8(s) => {
            if let Ok(i) = s.trim().parse::<i64>() { ScalarValue::Int64(i) }
            else if let Ok(f) = s.trim().parse::<f64>() { ScalarValue::Int64(f as i64) }
            else { ScalarValue::Null }
        }
        ScalarValue::Date(d) => ScalarValue::Int64(d),
        ScalarValue::Timestamp(t) => ScalarValue::Int64(t),
        _ => ScalarValue::Null,
    }
}

fn to_float64(v: ScalarValue) -> ScalarValue {
    match v {
        ScalarValue::Int64(i) => ScalarValue::Float64(i as f64),
        ScalarValue::Float64(f) => ScalarValue::Float64(f),
        ScalarValue::Boolean(b) => ScalarValue::Float64(if b { 1.0 } else { 0.0 }),
        ScalarValue::Utf8(s) => {
            if let Ok(f) = s.trim().parse::<f64>() { ScalarValue::Float64(f) }
            else { ScalarValue::Null }
        }
        _ => ScalarValue::Null,
    }
}

fn to_utf8(v: ScalarValue) -> ScalarValue {
    ScalarValue::Utf8(format!("{}", v))
}

fn to_boolean(v: ScalarValue) -> ScalarValue {
    match v {
        ScalarValue::Boolean(b) => ScalarValue::Boolean(b),
        ScalarValue::Int64(i) => ScalarValue::Boolean(i != 0),
        ScalarValue::Float64(f) => ScalarValue::Boolean(f != 0.0),
        ScalarValue::Utf8(s) => match s.to_lowercase().as_str() {
            "true" | "1" | "yes" | "t" | "on" => ScalarValue::Boolean(true),
            "false" | "0" | "no" | "f" | "off" => ScalarValue::Boolean(false),
            _ => ScalarValue::Null,
        },
        _ => ScalarValue::Null,
    }
}

fn to_date(v: ScalarValue) -> ScalarValue {
    match v {
        ScalarValue::Date(d) => ScalarValue::Date(d),
        ScalarValue::Timestamp(t) => ScalarValue::Date(t / 86_400_000_000),
        ScalarValue::Int64(i) => ScalarValue::Date(i),
        ScalarValue::Utf8(s) => {
            use crate::column::date_string_to_epoch_days;
            date_string_to_epoch_days(&s)
                .map(ScalarValue::Date)
                .unwrap_or(ScalarValue::Null)
        }
        _ => ScalarValue::Null,
    }
}

fn to_timestamp(v: ScalarValue) -> ScalarValue {
    match v {
        ScalarValue::Timestamp(t) => ScalarValue::Timestamp(t),
        ScalarValue::Date(d) => ScalarValue::Timestamp(d * 86_400_000_000),
        ScalarValue::Int64(i) => ScalarValue::Timestamp(i),
        ScalarValue::Utf8(s) => {
            use crate::column::timestamp_string_to_epoch_micros;
            timestamp_string_to_epoch_micros(&s)
                .map(ScalarValue::Timestamp)
                .unwrap_or(ScalarValue::Null)
        }
        _ => ScalarValue::Null,
    }
}

fn to_time(v: ScalarValue) -> ScalarValue {
    match v {
        ScalarValue::Time(t) => ScalarValue::Time(t),
        ScalarValue::Int64(i) => ScalarValue::Time(i),
        ScalarValue::Utf8(s) => {
            use crate::column::time_string_to_micros;
            time_string_to_micros(&s)
                .map(ScalarValue::Time)
                .unwrap_or(ScalarValue::Null)
        }
        _ => ScalarValue::Null,
    }
}
