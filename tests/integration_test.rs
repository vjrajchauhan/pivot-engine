use pivot_engine::sql::SqlEngine;

fn make_engine_with_employees() -> SqlEngine {
    let mut engine = SqlEngine::new();
    engine.execute("CREATE TABLE employees (id INTEGER, name VARCHAR, dept VARCHAR, salary DOUBLE)").unwrap();
    engine.execute("INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 90000.0)").unwrap();
    engine.execute("INSERT INTO employees VALUES (2, 'Bob', 'Engineering', 80000.0)").unwrap();
    engine.execute("INSERT INTO employees VALUES (3, 'Carol', 'Marketing', 70000.0)").unwrap();
    engine.execute("INSERT INTO employees VALUES (4, 'Dave', 'Marketing', 75000.0)").unwrap();
    engine.execute("INSERT INTO employees VALUES (5, 'Eve', 'Engineering', 95000.0)").unwrap();
    engine
}

#[test]
fn test_basic_select() {
    let mut engine = make_engine_with_employees();
    let result = engine.execute("SELECT * FROM employees").unwrap();
    assert_eq!(result.row_count(), 5);
}

#[test]
fn test_select_with_where() {
    let mut engine = make_engine_with_employees();
    let result = engine.execute("SELECT * FROM employees WHERE dept = 'Engineering'").unwrap();
    assert_eq!(result.row_count(), 3);
}

#[test]
fn test_group_by_count() {
    let mut engine = make_engine_with_employees();
    let result = engine.execute(
        "SELECT dept, COUNT(*) as cnt FROM employees GROUP BY dept ORDER BY dept"
    ).unwrap();
    assert_eq!(result.row_count(), 2);
}

#[test]
fn test_group_by_sum() {
    let mut engine = make_engine_with_employees();
    let result = engine.execute(
        "SELECT dept, SUM(salary) as total FROM employees GROUP BY dept ORDER BY dept"
    ).unwrap();
    assert_eq!(result.row_count(), 2);
}

#[test]
fn test_order_by() {
    let mut engine = make_engine_with_employees();
    let result = engine.execute("SELECT name, salary FROM employees ORDER BY salary DESC").unwrap();
    assert_eq!(result.row_count(), 5);
    // Eve has highest salary
    assert_eq!(result.rows[0][0], pivot_engine::column::ScalarValue::Utf8("Eve".to_string()));
}

#[test]
fn test_limit_offset() {
    let mut engine = make_engine_with_employees();
    let result = engine.execute("SELECT * FROM employees ORDER BY id LIMIT 2").unwrap();
    assert_eq!(result.row_count(), 2);

    let result = engine.execute("SELECT * FROM employees ORDER BY id LIMIT 2 OFFSET 2").unwrap();
    assert_eq!(result.row_count(), 2);
}

#[test]
fn test_create_insert_select() {
    let mut engine = SqlEngine::new();
    engine.execute("CREATE TABLE items (id INTEGER, name VARCHAR, price DOUBLE)").unwrap();
    engine.execute("INSERT INTO items VALUES (1, 'Widget', 9.99)").unwrap();
    engine.execute("INSERT INTO items VALUES (2, 'Gadget', 19.99)").unwrap();
    let result = engine.execute("SELECT * FROM items").unwrap();
    assert_eq!(result.row_count(), 2);
}

#[test]
fn test_inner_join() {
    let mut engine = SqlEngine::new();
    engine.execute("CREATE TABLE departments (id INTEGER, name VARCHAR)").unwrap();
    engine.execute("INSERT INTO departments VALUES (1, 'Engineering')").unwrap();
    engine.execute("INSERT INTO departments VALUES (2, 'Marketing')").unwrap();
    engine.execute("CREATE TABLE emps (id INTEGER, name VARCHAR, dept_id INTEGER)").unwrap();
    engine.execute("INSERT INTO emps VALUES (1, 'Alice', 1)").unwrap();
    engine.execute("INSERT INTO emps VALUES (2, 'Bob', 2)").unwrap();
    engine.execute("INSERT INTO emps VALUES (3, 'Carol', 1)").unwrap();

    let result = engine.execute(
        "SELECT emps.name, departments.name FROM emps JOIN departments ON emps.dept_id = departments.id ORDER BY emps.id"
    ).unwrap();
    assert_eq!(result.row_count(), 3);
}

#[test]
fn test_left_join() {
    let mut engine = SqlEngine::new();
    engine.execute("CREATE TABLE departments (id INTEGER, name VARCHAR)").unwrap();
    engine.execute("INSERT INTO departments VALUES (1, 'Engineering')").unwrap();
    engine.execute("CREATE TABLE emps (id INTEGER, name VARCHAR, dept_id INTEGER)").unwrap();
    engine.execute("INSERT INTO emps VALUES (1, 'Alice', 1)").unwrap();
    engine.execute("INSERT INTO emps VALUES (2, 'Bob', 99)").unwrap(); // no matching dept

    let result = engine.execute(
        "SELECT emps.name, departments.name FROM emps LEFT JOIN departments ON emps.dept_id = departments.id"
    ).unwrap();
    assert_eq!(result.row_count(), 2); // Both rows, Bob's dept is NULL
}

#[test]
fn test_cte() {
    let mut engine = make_engine_with_employees();
    let result = engine.execute(
        "WITH high_earners AS (SELECT * FROM employees WHERE salary > 80000)
         SELECT COUNT(*) as cnt FROM high_earners"
    ).unwrap();
    assert_eq!(result.row_count(), 1);
    // Alice (90000), Eve (95000) - 2 high earners
    assert_eq!(result.rows[0][0], pivot_engine::column::ScalarValue::Int64(2));
}

#[test]
fn test_window_rank() {
    let mut engine = make_engine_with_employees();
    let result = engine.execute(
        "SELECT name, salary, RANK() OVER (PARTITION BY dept ORDER BY salary DESC) as rnk FROM employees"
    ).unwrap();
    assert_eq!(result.row_count(), 5);
}

#[test]
fn test_window_row_number() {
    let mut engine = make_engine_with_employees();
    let result = engine.execute(
        "SELECT name, ROW_NUMBER() OVER (ORDER BY salary DESC) as rn FROM employees"
    ).unwrap();
    assert_eq!(result.row_count(), 5);
}

#[test]
fn test_transactions() {
    let mut engine = make_engine_with_employees();
    engine.execute("BEGIN").unwrap();
    engine.execute("INSERT INTO employees VALUES (6, 'Frank', 'HR', 65000.0)").unwrap();
    engine.execute("COMMIT").unwrap();
    let result = engine.execute("SELECT COUNT(*) FROM employees").unwrap();
    assert_eq!(result.rows[0][0], pivot_engine::column::ScalarValue::Int64(6));
}

#[test]
fn test_update() {
    let mut engine = make_engine_with_employees();
    engine.execute("UPDATE employees SET salary = 100000.0 WHERE name = 'Alice'").unwrap();
    let result = engine.execute("SELECT salary FROM employees WHERE name = 'Alice'").unwrap();
    assert_eq!(result.row_count(), 1);
}

#[test]
fn test_delete() {
    let mut engine = make_engine_with_employees();
    engine.execute("DELETE FROM employees WHERE dept = 'Marketing'").unwrap();
    let result = engine.execute("SELECT COUNT(*) FROM employees").unwrap();
    assert_eq!(result.rows[0][0], pivot_engine::column::ScalarValue::Int64(3));
}

#[test]
fn test_having() {
    let mut engine = make_engine_with_employees();
    let result = engine.execute(
        "SELECT dept, COUNT(*) as cnt FROM employees GROUP BY dept HAVING COUNT(*) > 2"
    ).unwrap();
    assert_eq!(result.row_count(), 1); // Only Engineering has 3
}

#[test]
fn test_select_expressions() {
    let mut engine = make_engine_with_employees();
    let result = engine.execute("SELECT 1 + 1 as two").unwrap();
    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], pivot_engine::column::ScalarValue::Int64(2));
}

#[test]
fn test_distinct() {
    let mut engine = make_engine_with_employees();
    let result = engine.execute("SELECT DISTINCT dept FROM employees ORDER BY dept").unwrap();
    assert_eq!(result.row_count(), 2);
}

#[test]
fn test_drop_table() {
    let mut engine = make_engine_with_employees();
    engine.execute("DROP TABLE employees").unwrap();
    let err = engine.execute("SELECT * FROM employees");
    assert!(err.is_err());
}

#[test]
fn test_subquery_in_from() {
    let mut engine = make_engine_with_employees();
    let result = engine.execute(
        "SELECT dept, total FROM (SELECT dept, SUM(salary) as total FROM employees GROUP BY dept) sub ORDER BY dept"
    ).unwrap();
    assert_eq!(result.row_count(), 2);
}

#[test]
fn test_case_when() {
    let mut engine = make_engine_with_employees();
    let result = engine.execute(
        "SELECT name, CASE WHEN salary > 85000 THEN 'High' ELSE 'Low' END as tier FROM employees ORDER BY id"
    ).unwrap();
    assert_eq!(result.row_count(), 5);
    assert_eq!(result.rows[0][1], pivot_engine::column::ScalarValue::Utf8("High".to_string())); // Alice
    assert_eq!(result.rows[1][1], pivot_engine::column::ScalarValue::Utf8("Low".to_string()));  // Bob
}

#[test]
fn test_string_functions() {
    let mut engine = make_engine_with_employees();
    let result = engine.execute("SELECT UPPER(name), LENGTH(name) FROM employees ORDER BY id LIMIT 1").unwrap();
    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], pivot_engine::column::ScalarValue::Utf8("ALICE".to_string()));
    assert_eq!(result.rows[0][1], pivot_engine::column::ScalarValue::Int64(5));
}

#[test]
fn test_aggregate_functions() {
    let mut engine = make_engine_with_employees();
    let result = engine.execute(
        "SELECT COUNT(*), SUM(salary), AVG(salary), MIN(salary), MAX(salary) FROM employees"
    ).unwrap();
    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], pivot_engine::column::ScalarValue::Int64(5));
}

#[test]
fn test_in_clause() {
    let mut engine = make_engine_with_employees();
    let result = engine.execute(
        "SELECT name FROM employees WHERE name IN ('Alice', 'Eve') ORDER BY name"
    ).unwrap();
    assert_eq!(result.row_count(), 2);
}

#[test]
fn test_like_clause() {
    let mut engine = make_engine_with_employees();
    let result = engine.execute(
        "SELECT name FROM employees WHERE name LIKE 'A%'"
    ).unwrap();
    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], pivot_engine::column::ScalarValue::Utf8("Alice".to_string()));
}

#[test]
fn test_between() {
    let mut engine = make_engine_with_employees();
    let result = engine.execute(
        "SELECT name FROM employees WHERE salary BETWEEN 80000 AND 90000 ORDER BY salary"
    ).unwrap();
    assert_eq!(result.row_count(), 2); // Bob (80000), Alice (90000)
}

#[test]
fn test_is_null() {
    let mut engine = SqlEngine::new();
    engine.execute("CREATE TABLE nullable (id INTEGER, val VARCHAR)").unwrap();
    engine.execute("INSERT INTO nullable VALUES (1, 'hello')").unwrap();
    engine.execute("INSERT INTO nullable VALUES (2, NULL)").unwrap();
    let result = engine.execute("SELECT id FROM nullable WHERE val IS NULL").unwrap();
    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], pivot_engine::column::ScalarValue::Int64(2));
}

#[test]
fn test_coalesce() {
    let mut engine = SqlEngine::new();
    engine.execute("CREATE TABLE nullable (id INTEGER, val VARCHAR)").unwrap();
    engine.execute("INSERT INTO nullable VALUES (1, NULL)").unwrap();
    let result = engine.execute("SELECT COALESCE(val, 'default') FROM nullable").unwrap();
    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], pivot_engine::column::ScalarValue::Utf8("default".to_string()));
}

#[test]
fn test_union() {
    let mut engine = SqlEngine::new();
    engine.execute("CREATE TABLE t1 (val INTEGER)").unwrap();
    engine.execute("INSERT INTO t1 VALUES (1)").unwrap();
    engine.execute("INSERT INTO t1 VALUES (2)").unwrap();
    engine.execute("CREATE TABLE t2 (val INTEGER)").unwrap();
    engine.execute("INSERT INTO t2 VALUES (3)").unwrap();
    engine.execute("INSERT INTO t2 VALUES (4)").unwrap();
    let result = engine.execute("SELECT val FROM t1 UNION ALL SELECT val FROM t2 ORDER BY val").unwrap();
    assert_eq!(result.row_count(), 4);
}
