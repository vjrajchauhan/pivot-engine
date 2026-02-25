use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub struct IntervalValue {
    pub years: i32,
    pub months: i32,
    pub days: i32,
    pub micros: i64,
}

impl IntervalValue {
    pub fn new(years: i32, months: i32, days: i32, micros: i64) -> Self {
        Self { years, months, days, micros }
    }
    pub fn zero() -> Self { Self { years: 0, months: 0, days: 0, micros: 0 } }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValue {
    Boolean(bool),
    Int64(i64),
    Float64(f64),
    Utf8(String),
    Date(i64),       // days since epoch
    Timestamp(i64),  // microseconds since epoch
    Time(i64),       // microseconds since midnight
    Interval(IntervalValue),
    Null,
}

impl fmt::Display for ScalarValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScalarValue::Boolean(b) => write!(f, "{}", b),
            ScalarValue::Int64(i) => write!(f, "{}", i),
            ScalarValue::Float64(v) => {
                if v.fract() == 0.0 && v.abs() < 1e15 {
                    write!(f, "{:.1}", v)
                } else {
                    write!(f, "{}", v)
                }
            }
            ScalarValue::Utf8(s) => write!(f, "{}", s),
            ScalarValue::Date(d) => write!(f, "{}", epoch_days_to_date_string(*d)),
            ScalarValue::Timestamp(ts) => write!(f, "{}", epoch_micros_to_ts_string(*ts)),
            ScalarValue::Time(t) => write!(f, "{}", micros_to_time_string(*t)),
            ScalarValue::Interval(iv) => {
                write!(f, "{} years {} months {} days {} micros",
                    iv.years, iv.months, iv.days, iv.micros)
            }
            ScalarValue::Null => write!(f, "NULL"),
        }
    }
}

pub fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

pub fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => if is_leap_year(year) { 29 } else { 28 },
        _ => 30,
    }
}

pub fn epoch_days_to_ymd(days: i64) -> (i32, u32, u32) {
    let mut remaining = days;
    let mut year = 1970i32;
    loop {
        let days_in_year = if is_leap_year(year) { 366 } else { 365 };
        if remaining >= 0 && remaining < days_in_year {
            break;
        }
        if remaining < 0 {
            year -= 1;
            remaining += if is_leap_year(year) { 366 } else { 365 };
        } else {
            remaining -= days_in_year;
            year += 1;
        }
    }
    let mut month = 1u32;
    loop {
        let dim = days_in_month(year, month) as i64;
        if remaining < dim { break; }
        remaining -= dim;
        month += 1;
    }
    (year, month, remaining as u32 + 1)
}

pub fn ymd_to_epoch_days(year: i32, month: u32, day: u32) -> i64 {
    let mut days = 0i64;
    if year >= 1970 {
        for y in 1970..year {
            days += if is_leap_year(y) { 366 } else { 365 };
        }
    } else {
        for y in year..1970 {
            days -= if is_leap_year(y) { 366 } else { 365 };
        }
    }
    for m in 1..month {
        days += days_in_month(year, m) as i64;
    }
    days += day as i64 - 1;
    days
}

pub fn epoch_days_to_date_string(days: i64) -> String {
    let (y, m, d) = epoch_days_to_ymd(days);
    format!("{:04}-{:02}-{:02}", y, m, d)
}

pub fn epoch_micros_to_ts_string(micros: i64) -> String {
    let total_secs = micros / 1_000_000;
    let us = (micros % 1_000_000).abs();
    let days = total_secs.div_euclid(86400);
    let secs_of_day = total_secs.rem_euclid(86400);
    let (y, m, d) = epoch_days_to_ymd(days);
    let h = secs_of_day / 3600;
    let min = (secs_of_day % 3600) / 60;
    let s = secs_of_day % 60;
    if us == 0 {
        format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}", y, m, d, h, min, s)
    } else {
        format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}", y, m, d, h, min, s, us)
    }
}

pub fn micros_to_time_string(micros: i64) -> String {
    let total_secs = micros / 1_000_000;
    let us = (micros % 1_000_000).abs();
    let h = total_secs / 3600;
    let min = (total_secs % 3600) / 60;
    let s = total_secs % 60;
    if us == 0 {
        format!("{:02}:{:02}:{:02}", h, min, s)
    } else {
        format!("{:02}:{:02}:{:02}.{:06}", h, min, s, us)
    }
}

pub fn date_string_to_epoch_days(s: &str) -> Option<i64> {
    let parts: Vec<&str> = s.splitn(3, '-').collect();
    if parts.len() < 3 { return None; }
    let year: i32 = parts[0].parse().ok()?;
    let month: u32 = parts[1].parse().ok()?;
    let day: u32 = parts[2].parse().ok()?;
    if month < 1 || month > 12 || day < 1 || day > 31 { return None; }
    Some(ymd_to_epoch_days(year, month, day))
}

pub fn timestamp_string_to_epoch_micros(s: &str) -> Option<i64> {
    let s = s.trim();
    let (date_part, time_part) = if let Some(pos) = s.find(' ') {
        (&s[..pos], &s[pos+1..])
    } else if let Some(pos) = s.find('T') {
        (&s[..pos], &s[pos+1..])
    } else {
        (s, "00:00:00")
    };
    let days = date_string_to_epoch_days(date_part)?;
    let time_micros = time_string_to_micros(time_part)?;
    Some(days * 86_400_000_000 + time_micros)
}

pub fn time_string_to_micros(s: &str) -> Option<i64> {
    let parts: Vec<&str> = s.splitn(3, ':').collect();
    if parts.len() < 2 { return None; }
    let h: i64 = parts[0].parse().ok()?;
    let m: i64 = parts[1].parse().ok()?;
    let (secs, micros) = if parts.len() == 3 {
        let sp: Vec<&str> = parts[2].splitn(2, '.').collect();
        let s: i64 = sp[0].parse().ok()?;
        let us = if sp.len() == 2 {
            let us_str = sp[1];
            let padded = format!("{:0<6}", &us_str[..us_str.len().min(6)]);
            padded.parse::<i64>().ok()?
        } else { 0 };
        (s, us)
    } else { (0, 0) };
    Some((h * 3600 + m * 60 + secs) * 1_000_000 + micros)
}
