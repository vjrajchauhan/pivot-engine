use crate::column::ScalarValue;

/// Dispatch scalar (non-aggregate, non-datetime) functions.
/// Returns None if the function name is not recognized here.
pub fn call(name: &str, args: &[ScalarValue]) -> Option<ScalarValue> {
    let result = match name {
        // String functions
        "UPPER" => args.get(0).map(|v| match v {
            ScalarValue::Utf8(s) => ScalarValue::Utf8(s.to_uppercase()),
            _ => ScalarValue::Null,
        }),
        "LOWER" => args.get(0).map(|v| match v {
            ScalarValue::Utf8(s) => ScalarValue::Utf8(s.to_lowercase()),
            _ => ScalarValue::Null,
        }),
        "LENGTH" | "LEN" | "CHAR_LENGTH" | "CHARACTER_LENGTH" => {
            args.get(0).map(|v| match v {
                ScalarValue::Utf8(s) => ScalarValue::Int64(s.chars().count() as i64),
                _ => ScalarValue::Null,
            })
        }
        "OCTET_LENGTH" | "BYTE_LENGTH" => {
            args.get(0).map(|v| match v {
                ScalarValue::Utf8(s) => ScalarValue::Int64(s.len() as i64),
                _ => ScalarValue::Null,
            })
        }
        "TRIM" => args.get(0).map(|v| match v {
            ScalarValue::Utf8(s) => ScalarValue::Utf8(s.trim().to_string()),
            _ => ScalarValue::Null,
        }),
        "LTRIM" => args.get(0).map(|v| match v {
            ScalarValue::Utf8(s) => ScalarValue::Utf8(s.trim_start().to_string()),
            _ => ScalarValue::Null,
        }),
        "RTRIM" => args.get(0).map(|v| match v {
            ScalarValue::Utf8(s) => ScalarValue::Utf8(s.trim_end().to_string()),
            _ => ScalarValue::Null,
        }),
        "REVERSE" => args.get(0).map(|v| match v {
            ScalarValue::Utf8(s) => ScalarValue::Utf8(s.chars().rev().collect()),
            _ => ScalarValue::Null,
        }),
        "SUBSTR" | "SUBSTRING" => {
            match (args.get(0), args.get(1)) {
                (Some(ScalarValue::Utf8(s)), Some(ScalarValue::Int64(start))) => {
                    let s_chars: Vec<char> = s.chars().collect();
                    let idx = (*start - 1).max(0) as usize;
                    let len = if let Some(ScalarValue::Int64(l)) = args.get(2) {
                        *l as usize
                    } else {
                        s_chars.len()
                    };
                    let end = (idx + len).min(s_chars.len());
                    let result: String = s_chars[idx.min(s_chars.len())..end].iter().collect();
                    Some(ScalarValue::Utf8(result))
                }
                _ => Some(ScalarValue::Null),
            }
        }
        "LEFT" => {
            match (args.get(0), args.get(1)) {
                (Some(ScalarValue::Utf8(s)), Some(ScalarValue::Int64(n))) => {
                    let chars: Vec<char> = s.chars().collect();
                    let n = (*n).max(0) as usize;
                    Some(ScalarValue::Utf8(chars[..n.min(chars.len())].iter().collect()))
                }
                _ => Some(ScalarValue::Null),
            }
        }
        "RIGHT" => {
            match (args.get(0), args.get(1)) {
                (Some(ScalarValue::Utf8(s)), Some(ScalarValue::Int64(n))) => {
                    let chars: Vec<char> = s.chars().collect();
                    let n = (*n).max(0) as usize;
                    let start = chars.len().saturating_sub(n);
                    Some(ScalarValue::Utf8(chars[start..].iter().collect()))
                }
                _ => Some(ScalarValue::Null),
            }
        }
        "REPEAT" => {
            match (args.get(0), args.get(1)) {
                (Some(ScalarValue::Utf8(s)), Some(ScalarValue::Int64(n))) => {
                    Some(ScalarValue::Utf8(s.repeat(*n as usize)))
                }
                _ => Some(ScalarValue::Null),
            }
        }
        "REPLACE" => {
            match (args.get(0), args.get(1), args.get(2)) {
                (Some(ScalarValue::Utf8(s)), Some(ScalarValue::Utf8(from)), Some(ScalarValue::Utf8(to))) => {
                    Some(ScalarValue::Utf8(s.replace(from.as_str(), to.as_str())))
                }
                _ => Some(ScalarValue::Null),
            }
        }
        "CONCAT" => {
            let mut result = String::new();
            for arg in args {
                match arg {
                    ScalarValue::Null => {}
                    ScalarValue::Utf8(s) => result.push_str(s),
                    other => result.push_str(&format!("{}", other)),
                }
            }
            Some(ScalarValue::Utf8(result))
        }
        "CONCAT_WS" => {
            let sep = match args.get(0) {
                Some(ScalarValue::Utf8(s)) => s.clone(),
                _ => ",".to_string(),
            };
            let parts: Vec<String> = args[1..].iter().filter_map(|v| match v {
                ScalarValue::Null => None,
                ScalarValue::Utf8(s) => Some(s.clone()),
                other => Some(format!("{}", other)),
            }).collect();
            Some(ScalarValue::Utf8(parts.join(&sep)))
        }
        "SPLIT_PART" => {
            match (args.get(0), args.get(1), args.get(2)) {
                (Some(ScalarValue::Utf8(s)), Some(ScalarValue::Utf8(delim)), Some(ScalarValue::Int64(n))) => {
                    let parts: Vec<&str> = s.split(delim.as_str()).collect();
                    let idx = (*n - 1).max(0) as usize;
                    Some(ScalarValue::Utf8(parts.get(idx).unwrap_or(&"").to_string()))
                }
                _ => Some(ScalarValue::Null),
            }
        }
        "STARTS_WITH" => {
            match (args.get(0), args.get(1)) {
                (Some(ScalarValue::Utf8(s)), Some(ScalarValue::Utf8(prefix))) => {
                    Some(ScalarValue::Boolean(s.starts_with(prefix.as_str())))
                }
                _ => Some(ScalarValue::Null),
            }
        }
        "ENDS_WITH" => {
            match (args.get(0), args.get(1)) {
                (Some(ScalarValue::Utf8(s)), Some(ScalarValue::Utf8(suffix))) => {
                    Some(ScalarValue::Boolean(s.ends_with(suffix.as_str())))
                }
                _ => Some(ScalarValue::Null),
            }
        }
        "CONTAINS" => {
            match (args.get(0), args.get(1)) {
                (Some(ScalarValue::Utf8(s)), Some(ScalarValue::Utf8(needle))) => {
                    Some(ScalarValue::Boolean(s.contains(needle.as_str())))
                }
                _ => Some(ScalarValue::Null),
            }
        }
        "POSITION" => {
            // POSITION(needle IN haystack) - simplified as POSITION(needle, haystack)
            match (args.get(0), args.get(1)) {
                (Some(ScalarValue::Utf8(needle)), Some(ScalarValue::Utf8(hay))) => {
                    let pos = hay.find(needle.as_str())
                        .map(|i| i as i64 + 1)
                        .unwrap_or(0);
                    Some(ScalarValue::Int64(pos))
                }
                _ => Some(ScalarValue::Null),
            }
        }
        "LPAD" => {
            match (args.get(0), args.get(1)) {
                (Some(ScalarValue::Utf8(s)), Some(ScalarValue::Int64(n))) => {
                    let pad_char = match args.get(2) {
                        Some(ScalarValue::Utf8(p)) => p.chars().next().unwrap_or(' '),
                        _ => ' ',
                    };
                    let n = *n as usize;
                    let len = s.chars().count();
                    if len >= n {
                        Some(ScalarValue::Utf8(s.chars().take(n).collect()))
                    } else {
                        let pad: String = std::iter::repeat(pad_char).take(n - len).collect();
                        Some(ScalarValue::Utf8(pad + s))
                    }
                }
                _ => Some(ScalarValue::Null),
            }
        }
        "RPAD" => {
            match (args.get(0), args.get(1)) {
                (Some(ScalarValue::Utf8(s)), Some(ScalarValue::Int64(n))) => {
                    let pad_char = match args.get(2) {
                        Some(ScalarValue::Utf8(p)) => p.chars().next().unwrap_or(' '),
                        _ => ' ',
                    };
                    let n = *n as usize;
                    let len = s.chars().count();
                    if len >= n {
                        Some(ScalarValue::Utf8(s.chars().take(n).collect()))
                    } else {
                        let mut result = s.clone();
                        for _ in 0..(n - len) { result.push(pad_char); }
                        Some(ScalarValue::Utf8(result))
                    }
                }
                _ => Some(ScalarValue::Null),
            }
        }
        "ASCII" => {
            args.get(0).map(|v| match v {
                ScalarValue::Utf8(s) => {
                    ScalarValue::Int64(s.chars().next().map(|c| c as i64).unwrap_or(0))
                }
                _ => ScalarValue::Null,
            })
        }
        "CHR" | "CHAR" => {
            args.get(0).map(|v| match v {
                ScalarValue::Int64(n) => {
                    char::from_u32(*n as u32)
                        .map(|c| ScalarValue::Utf8(c.to_string()))
                        .unwrap_or(ScalarValue::Null)
                }
                _ => ScalarValue::Null,
            })
        }

        // Math functions
        "ABS" => args.get(0).map(|v| match v {
            ScalarValue::Int64(i) => ScalarValue::Int64(i.abs()),
            ScalarValue::Float64(f) => ScalarValue::Float64(f.abs()),
            _ => ScalarValue::Null,
        }),
        "CEIL" | "CEILING" => args.get(0).map(|v| match v {
            ScalarValue::Float64(f) => ScalarValue::Float64(f.ceil()),
            ScalarValue::Int64(i) => ScalarValue::Int64(*i),
            _ => ScalarValue::Null,
        }),
        "FLOOR" => args.get(0).map(|v| match v {
            ScalarValue::Float64(f) => ScalarValue::Float64(f.floor()),
            ScalarValue::Int64(i) => ScalarValue::Int64(*i),
            _ => ScalarValue::Null,
        }),
        "ROUND" => {
            match (args.get(0), args.get(1)) {
                (Some(ScalarValue::Float64(f)), Some(ScalarValue::Int64(n))) => {
                    let factor = 10f64.powi(*n as i32);
                    Some(ScalarValue::Float64((f * factor).round() / factor))
                }
                (Some(ScalarValue::Float64(f)), None) => {
                    Some(ScalarValue::Float64(f.round()))
                }
                (Some(ScalarValue::Int64(i)), _) => Some(ScalarValue::Int64(*i)),
                _ => Some(ScalarValue::Null),
            }
        }
        "TRUNC" | "TRUNCATE" => args.get(0).map(|v| match v {
            ScalarValue::Float64(f) => ScalarValue::Float64(f.trunc()),
            ScalarValue::Int64(i) => ScalarValue::Int64(*i),
            _ => ScalarValue::Null,
        }),
        "SQRT" => args.get(0).map(|v| match v {
            ScalarValue::Float64(f) => ScalarValue::Float64(f.sqrt()),
            ScalarValue::Int64(i) => ScalarValue::Float64((*i as f64).sqrt()),
            _ => ScalarValue::Null,
        }),
        "POWER" | "POW" => {
            match (args.get(0), args.get(1)) {
                (Some(ScalarValue::Float64(a)), Some(ScalarValue::Float64(b))) => {
                    Some(ScalarValue::Float64(a.powf(*b)))
                }
                (Some(ScalarValue::Int64(a)), Some(ScalarValue::Int64(b))) => {
                    Some(ScalarValue::Float64((*a as f64).powf(*b as f64)))
                }
                (Some(ScalarValue::Float64(a)), Some(ScalarValue::Int64(b))) => {
                    Some(ScalarValue::Float64(a.powf(*b as f64)))
                }
                (Some(ScalarValue::Int64(a)), Some(ScalarValue::Float64(b))) => {
                    Some(ScalarValue::Float64((*a as f64).powf(*b)))
                }
                _ => Some(ScalarValue::Null),
            }
        }
        "LOG" | "LOG10" => args.get(0).map(|v| match v {
            ScalarValue::Float64(f) => ScalarValue::Float64(f.log10()),
            ScalarValue::Int64(i) => ScalarValue::Float64((*i as f64).log10()),
            _ => ScalarValue::Null,
        }),
        "LOG2" => args.get(0).map(|v| match v {
            ScalarValue::Float64(f) => ScalarValue::Float64(f.log2()),
            ScalarValue::Int64(i) => ScalarValue::Float64((*i as f64).log2()),
            _ => ScalarValue::Null,
        }),
        "LN" => args.get(0).map(|v| match v {
            ScalarValue::Float64(f) => ScalarValue::Float64(f.ln()),
            ScalarValue::Int64(i) => ScalarValue::Float64((*i as f64).ln()),
            _ => ScalarValue::Null,
        }),
        "EXP" => args.get(0).map(|v| match v {
            ScalarValue::Float64(f) => ScalarValue::Float64(f.exp()),
            ScalarValue::Int64(i) => ScalarValue::Float64((*i as f64).exp()),
            _ => ScalarValue::Null,
        }),
        "MOD" => {
            match (args.get(0), args.get(1)) {
                (Some(ScalarValue::Int64(a)), Some(ScalarValue::Int64(b))) => {
                    Some(if *b == 0 { ScalarValue::Null } else { ScalarValue::Int64(a % b) })
                }
                (Some(ScalarValue::Float64(a)), Some(ScalarValue::Float64(b))) => {
                    Some(ScalarValue::Float64(a % b))
                }
                _ => Some(ScalarValue::Null),
            }
        }
        "SIGN" => args.get(0).map(|v| match v {
            ScalarValue::Int64(i) => ScalarValue::Int64(i.signum()),
            ScalarValue::Float64(f) => ScalarValue::Float64(f.signum()),
            _ => ScalarValue::Null,
        }),
        "PI" => Some(ScalarValue::Float64(std::f64::consts::PI)),
        "E" => Some(ScalarValue::Float64(std::f64::consts::E)),
        "SIN" => args.get(0).map(|v| match v {
            ScalarValue::Float64(f) => ScalarValue::Float64(f.sin()),
            ScalarValue::Int64(i) => ScalarValue::Float64((*i as f64).sin()),
            _ => ScalarValue::Null,
        }),
        "COS" => args.get(0).map(|v| match v {
            ScalarValue::Float64(f) => ScalarValue::Float64(f.cos()),
            ScalarValue::Int64(i) => ScalarValue::Float64((*i as f64).cos()),
            _ => ScalarValue::Null,
        }),
        "TAN" => args.get(0).map(|v| match v {
            ScalarValue::Float64(f) => ScalarValue::Float64(f.tan()),
            ScalarValue::Int64(i) => ScalarValue::Float64((*i as f64).tan()),
            _ => ScalarValue::Null,
        }),

        // Type conversion
        "TO_VARCHAR" | "TO_STRING" => {
            args.get(0).map(|v| ScalarValue::Utf8(format!("{}", v)))
        }
        "TO_NUMBER" | "TO_NUMERIC" | "TO_DOUBLE" => {
            args.get(0).map(|v| match v {
                ScalarValue::Int64(i) => ScalarValue::Float64(*i as f64),
                ScalarValue::Float64(f) => ScalarValue::Float64(*f),
                ScalarValue::Utf8(s) => s.parse::<f64>()
                    .map(ScalarValue::Float64)
                    .unwrap_or(ScalarValue::Null),
                _ => ScalarValue::Null,
            })
        }
        "TO_INTEGER" | "TO_INT" => {
            args.get(0).map(|v| match v {
                ScalarValue::Int64(i) => ScalarValue::Int64(*i),
                ScalarValue::Float64(f) => ScalarValue::Int64(*f as i64),
                ScalarValue::Boolean(b) => ScalarValue::Int64(if *b { 1 } else { 0 }),
                ScalarValue::Utf8(s) => s.parse::<i64>()
                    .map(ScalarValue::Int64)
                    .unwrap_or(ScalarValue::Null),
                _ => ScalarValue::Null,
            })
        }

        // NULL-related
        "ISNULL" | "IS_NULL" => {
            args.get(0).map(|v| ScalarValue::Boolean(matches!(v, ScalarValue::Null)))
        }
        "ISNAN" => args.get(0).map(|v| match v {
            ScalarValue::Float64(f) => ScalarValue::Boolean(f.is_nan()),
            _ => ScalarValue::Boolean(false),
        }),

        // Array / list (simplified)
        "ARRAY_LENGTH" | "ARRAY_SIZE" | "LEN" => {
            args.get(0).map(|v| match v {
                ScalarValue::Utf8(s) => ScalarValue::Int64(s.split(',').count() as i64),
                _ => ScalarValue::Null,
            })
        }

        _ => return None,
    };
    Some(result.unwrap_or(ScalarValue::Null))
}
