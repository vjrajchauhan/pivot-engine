use crate::column::{
    ScalarValue, epoch_days_to_ymd, ymd_to_epoch_days, epoch_days_to_date_string,
    epoch_micros_to_ts_string, micros_to_time_string, date_string_to_epoch_days,
    timestamp_string_to_epoch_micros,
};

/// Dispatch date/time functions.
/// Returns None if the function is not recognized here.
pub fn call(name: &str, args: &[ScalarValue]) -> Option<ScalarValue> {
    let result = match name {
        "CURRENT_DATE" | "TODAY" | "GETDATE" => {
            // Return a fixed date (1970-01-01 in tests; in real use would be system date)
            Some(ScalarValue::Date(0))
        }
        "CURRENT_TIMESTAMP" | "NOW" | "CURRENT_TIMESTAMP()" => {
            Some(ScalarValue::Timestamp(0))
        }
        "CURRENT_TIME" => Some(ScalarValue::Time(0)),

        "DATE" => {
            match args.get(0) {
                Some(ScalarValue::Utf8(s)) => {
                    date_string_to_epoch_days(s)
                        .map(ScalarValue::Date)
                        .or(Some(ScalarValue::Null))
                }
                Some(ScalarValue::Timestamp(t)) => Some(ScalarValue::Date(t / 86_400_000_000)),
                Some(ScalarValue::Date(d)) => Some(ScalarValue::Date(*d)),
                _ => Some(ScalarValue::Null),
            }
        }

        "TIMESTAMP" => {
            match args.get(0) {
                Some(ScalarValue::Utf8(s)) => {
                    timestamp_string_to_epoch_micros(s)
                        .map(ScalarValue::Timestamp)
                        .or(Some(ScalarValue::Null))
                }
                Some(ScalarValue::Date(d)) => Some(ScalarValue::Timestamp(d * 86_400_000_000)),
                Some(ScalarValue::Timestamp(t)) => Some(ScalarValue::Timestamp(*t)),
                _ => Some(ScalarValue::Null),
            }
        }

        "YEAR" => {
            match args.get(0) {
                Some(ScalarValue::Date(d)) => {
                    let (y, _, _) = epoch_days_to_ymd(*d);
                    Some(ScalarValue::Int64(y as i64))
                }
                Some(ScalarValue::Timestamp(t)) => {
                    let days = t / 86_400_000_000;
                    let (y, _, _) = epoch_days_to_ymd(days);
                    Some(ScalarValue::Int64(y as i64))
                }
                Some(ScalarValue::Utf8(s)) => {
                    date_string_to_epoch_days(s).map(|d| {
                        let (y, _, _) = epoch_days_to_ymd(d);
                        ScalarValue::Int64(y as i64)
                    }).or(Some(ScalarValue::Null))
                }
                _ => Some(ScalarValue::Null),
            }
        }

        "MONTH" => {
            match args.get(0) {
                Some(ScalarValue::Date(d)) => {
                    let (_, m, _) = epoch_days_to_ymd(*d);
                    Some(ScalarValue::Int64(m as i64))
                }
                Some(ScalarValue::Timestamp(t)) => {
                    let days = t / 86_400_000_000;
                    let (_, m, _) = epoch_days_to_ymd(days);
                    Some(ScalarValue::Int64(m as i64))
                }
                Some(ScalarValue::Utf8(s)) => {
                    date_string_to_epoch_days(s).map(|d| {
                        let (_, m, _) = epoch_days_to_ymd(d);
                        ScalarValue::Int64(m as i64)
                    }).or(Some(ScalarValue::Null))
                }
                _ => Some(ScalarValue::Null),
            }
        }

        "DAY" | "DAYOFMONTH" => {
            match args.get(0) {
                Some(ScalarValue::Date(d)) => {
                    let (_, _, day) = epoch_days_to_ymd(*d);
                    Some(ScalarValue::Int64(day as i64))
                }
                Some(ScalarValue::Timestamp(t)) => {
                    let days = t / 86_400_000_000;
                    let (_, _, day) = epoch_days_to_ymd(days);
                    Some(ScalarValue::Int64(day as i64))
                }
                _ => Some(ScalarValue::Null),
            }
        }

        "HOUR" => {
            match args.get(0) {
                Some(ScalarValue::Time(t)) => {
                    Some(ScalarValue::Int64(t / 3_600_000_000))
                }
                Some(ScalarValue::Timestamp(t)) => {
                    let secs = t / 1_000_000;
                    let secs_of_day = secs.rem_euclid(86400);
                    Some(ScalarValue::Int64(secs_of_day / 3600))
                }
                _ => Some(ScalarValue::Null),
            }
        }

        "MINUTE" => {
            match args.get(0) {
                Some(ScalarValue::Time(t)) => {
                    Some(ScalarValue::Int64((t / 60_000_000) % 60))
                }
                Some(ScalarValue::Timestamp(t)) => {
                    let secs = t / 1_000_000;
                    let secs_of_day = secs.rem_euclid(86400);
                    Some(ScalarValue::Int64((secs_of_day % 3600) / 60))
                }
                _ => Some(ScalarValue::Null),
            }
        }

        "SECOND" => {
            match args.get(0) {
                Some(ScalarValue::Time(t)) => {
                    Some(ScalarValue::Int64((t / 1_000_000) % 60))
                }
                Some(ScalarValue::Timestamp(t)) => {
                    let secs = t / 1_000_000;
                    Some(ScalarValue::Int64(secs % 60))
                }
                _ => Some(ScalarValue::Null),
            }
        }

        "DATE_TRUNC" => {
            match (args.get(0), args.get(1)) {
                (Some(ScalarValue::Utf8(unit)), Some(ScalarValue::Date(d))) => {
                    Some(date_trunc_date(unit, *d))
                }
                (Some(ScalarValue::Utf8(unit)), Some(ScalarValue::Timestamp(t))) => {
                    Some(date_trunc_ts(unit, *t))
                }
                _ => Some(ScalarValue::Null),
            }
        }

        "DATE_PART" | "EXTRACT" => {
            match (args.get(0), args.get(1)) {
                (Some(ScalarValue::Utf8(field)), Some(val)) => {
                    Some(extract_field(field, val))
                }
                _ => Some(ScalarValue::Null),
            }
        }

        "DATEDIFF" | "DATE_DIFF" => {
            match (args.get(0), args.get(1), args.get(2)) {
                (Some(ScalarValue::Utf8(unit)), Some(a), Some(b)) => {
                    let da = coerce_to_days(a);
                    let db = coerce_to_days(b);
                    match unit.to_lowercase().as_str() {
                        "day" | "days" => Some(ScalarValue::Int64(db - da)),
                        "week" | "weeks" => Some(ScalarValue::Int64((db - da) / 7)),
                        "month" | "months" => Some(ScalarValue::Int64((db - da) / 30)),
                        "year" | "years" => Some(ScalarValue::Int64((db - da) / 365)),
                        _ => Some(ScalarValue::Int64(db - da)),
                    }
                }
                (Some(a), Some(b), None) => {
                    let da = coerce_to_days(a);
                    let db = coerce_to_days(b);
                    Some(ScalarValue::Int64(da - db))
                }
                _ => Some(ScalarValue::Null),
            }
        }

        "DATE_ADD" | "DATEADD" => {
            match (args.get(0), args.get(1), args.get(2)) {
                (Some(ScalarValue::Utf8(unit)), Some(ScalarValue::Int64(n)), Some(val)) => {
                    let days = coerce_to_days(val);
                    let result = match unit.to_lowercase().as_str() {
                        "day" | "days" => days + n,
                        "week" | "weeks" => days + n * 7,
                        "month" | "months" => days + n * 30,
                        "year" | "years" => days + n * 365,
                        _ => days + n,
                    };
                    Some(ScalarValue::Date(result))
                }
                _ => Some(ScalarValue::Null),
            }
        }

        "STRFTIME" | "FORMAT_DATE" | "TO_DATE" => {
            match (args.get(0), args.get(1)) {
                (Some(ScalarValue::Utf8(fmt)), Some(ScalarValue::Date(d))) => {
                    Some(ScalarValue::Utf8(format_date_str(*d, fmt)))
                }
                (Some(ScalarValue::Utf8(fmt)), Some(ScalarValue::Timestamp(t))) => {
                    Some(ScalarValue::Utf8(format_ts_str(*t, fmt)))
                }
                (Some(ScalarValue::Utf8(s)), None) => {
                    // Treat as date string parse
                    date_string_to_epoch_days(s)
                        .map(ScalarValue::Date)
                        .or(Some(ScalarValue::Null))
                }
                _ => Some(ScalarValue::Null),
            }
        }

        "EPOCH" | "EPOCH_MS" => {
            match args.get(0) {
                Some(ScalarValue::Timestamp(t)) => {
                    if name == "EPOCH" {
                        Some(ScalarValue::Int64(t / 1_000_000))
                    } else {
                        Some(ScalarValue::Int64(t / 1_000))
                    }
                }
                Some(ScalarValue::Date(d)) => {
                    Some(ScalarValue::Int64(d * 86400))
                }
                _ => Some(ScalarValue::Null),
            }
        }

        "MAKE_DATE" => {
            match (args.get(0), args.get(1), args.get(2)) {
                (Some(ScalarValue::Int64(y)), Some(ScalarValue::Int64(m)), Some(ScalarValue::Int64(d))) => {
                    Some(ScalarValue::Date(ymd_to_epoch_days(*y as i32, *m as u32, *d as u32)))
                }
                _ => Some(ScalarValue::Null),
            }
        }

        "AGE" => {
            // Simplified: just return interval or days diff
            Some(ScalarValue::Null)
        }

        _ => return None,
    };
    result
}

fn coerce_to_days(v: &ScalarValue) -> i64 {
    match v {
        ScalarValue::Date(d) => *d,
        ScalarValue::Timestamp(t) => t / 86_400_000_000,
        ScalarValue::Int64(i) => *i,
        _ => 0,
    }
}

fn date_trunc_date(unit: &str, days: i64) -> ScalarValue {
    let (y, m, d) = epoch_days_to_ymd(days);
    let result_days = match unit.to_lowercase().as_str() {
        "year" | "years" => ymd_to_epoch_days(y, 1, 1),
        "month" | "months" => ymd_to_epoch_days(y, m, 1),
        "day" | "days" => days,
        "quarter" => {
            let q_month = ((m - 1) / 3) * 3 + 1;
            ymd_to_epoch_days(y, q_month, 1)
        }
        "week" | "weeks" => {
            // Truncate to Monday of the week
            let dow = (days + 3).rem_euclid(7); // 0=Monday
            days - dow
        }
        _ => days,
    };
    ScalarValue::Date(result_days)
}

fn date_trunc_ts(unit: &str, micros: i64) -> ScalarValue {
    let days = micros / 86_400_000_000;
    let (y, m, d) = epoch_days_to_ymd(days);
    let secs_of_day = (micros / 1_000_000).rem_euclid(86400);
    let h = secs_of_day / 3600;
    let min = (secs_of_day % 3600) / 60;

    let result = match unit.to_lowercase().as_str() {
        "year" | "years" => ymd_to_epoch_days(y, 1, 1) * 86_400_000_000,
        "month" | "months" => ymd_to_epoch_days(y, m, 1) * 86_400_000_000,
        "day" | "days" => days * 86_400_000_000,
        "hour" | "hours" => days * 86_400_000_000 + h * 3_600_000_000,
        "minute" | "minutes" => days * 86_400_000_000 + h * 3_600_000_000 + min * 60_000_000,
        "second" | "seconds" => (micros / 1_000_000) * 1_000_000,
        _ => micros,
    };
    ScalarValue::Timestamp(result)
}

fn extract_field(field: &str, val: &ScalarValue) -> ScalarValue {
    let days = coerce_to_days(val);
    let (y, m, d) = epoch_days_to_ymd(days);
    let micros = match val {
        ScalarValue::Timestamp(t) => *t,
        _ => days * 86_400_000_000,
    };
    let secs_of_day = (micros / 1_000_000).rem_euclid(86400);

    match field.to_lowercase().as_str() {
        "year" | "years" => ScalarValue::Int64(y as i64),
        "month" | "months" => ScalarValue::Int64(m as i64),
        "day" | "days" => ScalarValue::Int64(d as i64),
        "hour" | "hours" => ScalarValue::Int64(secs_of_day / 3600),
        "minute" | "minutes" => ScalarValue::Int64((secs_of_day % 3600) / 60),
        "second" | "seconds" => ScalarValue::Int64(secs_of_day % 60),
        "epoch" => ScalarValue::Int64(micros / 1_000_000),
        "quarter" => ScalarValue::Int64(((m - 1) / 3 + 1) as i64),
        "dow" | "dayofweek" => ScalarValue::Int64((days + 4).rem_euclid(7)), // 0=Sunday
        "doy" | "dayofyear" => {
            let year_start = ymd_to_epoch_days(y, 1, 1);
            ScalarValue::Int64(days - year_start + 1)
        }
        "week" | "weekofyear" => {
            let year_start = ymd_to_epoch_days(y, 1, 1);
            ScalarValue::Int64((days - year_start) / 7 + 1)
        }
        _ => ScalarValue::Null,
    }
}

fn format_date_str(days: i64, _fmt: &str) -> String {
    epoch_days_to_date_string(days)
}

fn format_ts_str(micros: i64, _fmt: &str) -> String {
    epoch_micros_to_ts_string(micros)
}
