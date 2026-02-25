use crate::sql::{SqlEngine, QueryResult};
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};

pub struct PivotEngineHandle {
    engine: SqlEngine,
}

pub struct PivotResultHandle {
    result: QueryResult,
}

#[no_mangle]
pub extern "C" fn pivot_engine_new() -> *mut PivotEngineHandle {
    let handle = Box::new(PivotEngineHandle { engine: SqlEngine::new() });
    Box::into_raw(handle)
}

#[no_mangle]
pub extern "C" fn pivot_engine_free(handle: *mut PivotEngineHandle) {
    if !handle.is_null() {
        unsafe { drop(Box::from_raw(handle)); }
    }
}

#[no_mangle]
pub extern "C" fn pivot_engine_execute(
    handle: *mut PivotEngineHandle,
    sql: *const c_char,
) -> *mut PivotResultHandle {
    if handle.is_null() || sql.is_null() { return std::ptr::null_mut(); }
    let sql_str = unsafe {
        match CStr::from_ptr(sql).to_str() {
            Ok(s) => s,
            Err(_) => return std::ptr::null_mut(),
        }
    };
    let engine = unsafe { &mut (*handle).engine };
    match engine.execute(sql_str) {
        Ok(result) => Box::into_raw(Box::new(PivotResultHandle { result })),
        Err(_) => std::ptr::null_mut(),
    }
}

#[no_mangle]
pub extern "C" fn pivot_result_row_count(handle: *const PivotResultHandle) -> c_int {
    if handle.is_null() { return 0; }
    unsafe { (*handle).result.rows.len() as c_int }
}

#[no_mangle]
pub extern "C" fn pivot_result_column_count(handle: *const PivotResultHandle) -> c_int {
    if handle.is_null() { return 0; }
    unsafe { (*handle).result.columns.len() as c_int }
}

#[no_mangle]
pub extern "C" fn pivot_result_column_name(
    handle: *const PivotResultHandle,
    col: c_int,
) -> *const c_char {
    if handle.is_null() { return std::ptr::null(); }
    let result = unsafe { &(*handle).result };
    let idx = col as usize;
    if idx >= result.columns.len() { return std::ptr::null(); }
    match CString::new(result.columns[idx].as_str()) {
        Ok(s) => s.into_raw(),
        Err(_) => std::ptr::null(),
    }
}

#[no_mangle]
pub extern "C" fn pivot_result_value(
    handle: *const PivotResultHandle,
    row: c_int,
    col: c_int,
) -> *const c_char {
    if handle.is_null() { return std::ptr::null(); }
    let result = unsafe { &(*handle).result };
    let r = row as usize;
    let c = col as usize;
    if r >= result.rows.len() { return std::ptr::null(); }
    if c >= result.rows[r].len() { return std::ptr::null(); }
    let s = format!("{}", result.rows[r][c]);
    match CString::new(s) {
        Ok(cs) => cs.into_raw(),
        Err(_) => std::ptr::null(),
    }
}

#[no_mangle]
pub extern "C" fn pivot_result_free(handle: *mut PivotResultHandle) {
    if !handle.is_null() {
        unsafe { drop(Box::from_raw(handle)); }
    }
}
