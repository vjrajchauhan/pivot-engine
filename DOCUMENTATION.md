# PivotEngine Documentation

## 1. Architecture Overview

```
┌──────────────────────────────────────────────────────────────┐
│                       PivotEngine                            │
│                                                              │
│  ┌─────────────┐   ┌──────────────────────────────────────┐ │
│  │  Core Engine│   │            SQL Engine                │ │
│  │             │   │                                      │ │
│  │  Schema     │   │  Lexer → Parser → AST → Executor     │ │
│  │  DataStore  │   │  Catalog (tables + views)            │ │
│  │  NullBitmask│   │  Functions (scalar + datetime)       │ │
│  │  ScalarValue│   │  Cast / Type coercion                │ │
│  └─────────────┘   └──────────────────────────────────────┘ │
│                                                              │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │  Operations: Grouping | Aggregation | Pivot | Filter    │ │
│  │             Sort | CSV I/O | FFI bindings               │ │
│  └─────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────┘
```

## 2. Core Engine

### 2.1 Schema & DataTypes

```rust
use pivot_engine::schema::{Schema, ColumnDef, DataType};

let schema = Schema::new(vec![
    ColumnDef::new("id",     DataType::Int64,   false),
    ColumnDef::new("name",   DataType::Utf8,    true),
    ColumnDef::new("salary", DataType::Float64, true),
    ColumnDef::new("hired",  DataType::Date,    true),
]);
```

**Supported DataTypes:**
| Type | SQL | Description |
|------|-----|-------------|
| `Boolean` | `BOOLEAN` | true/false |
| `Int64` | `INTEGER`, `BIGINT` | 64-bit integer |
| `Float64` | `DOUBLE`, `FLOAT` | 64-bit float |
| `Utf8` | `VARCHAR`, `TEXT` | UTF-8 string |
| `Date` | `DATE` | Days since epoch (1970-01-01) |
| `Timestamp` | `TIMESTAMP` | Microseconds since epoch |
| `Time` | `TIME` | Microseconds since midnight |
| `Interval` | `INTERVAL` | years/months/days/micros |
| `Decimal{p,s}` | `DECIMAL(p,s)` | Stored as Float64 |

### 2.2 DataStore

Columnar in-memory storage:

```rust
use pivot_engine::datastore::DataStore;
use pivot_engine::column::ScalarValue;

let mut store = DataStore::new(schema);
store.append_row(vec![
    ScalarValue::Int64(1),
    ScalarValue::Utf8("Alice".to_string()),
    ScalarValue::Float64(90000.0),
    ScalarValue::Utf8("2020-01-15".to_string()),
])?;

let val = store.get_value(0, "name")?;  // ScalarValue::Utf8("Alice")
let row = store.get_row(0)?;            // Vec<ScalarValue>
```

**DataStore methods:**
- `new(schema)` – Create empty store
- `append_row(values)` – Add a row (enforces NOT NULL + type coercion)
- `get_value(row, col_name)` – Get value by row and column name
- `get_value_by_index(row, col_idx)` – Get value by indices
- `get_row(row)` – Get entire row as `Vec<ScalarValue>`
- `set_value(row, col_idx, val)` – Update a cell
- `add_column(def)` – Add new column (NULLs for existing rows)
- `drop_column(name)` – Remove a column
- `rename_column(old, new)` – Rename a column
- `row_count()` – Number of rows
- `schema()` – Reference to schema

### 2.3 NullBitmask

Tracks NULL/valid status per row for each column:

```rust
use pivot_engine::bitmap::NullBitmask;
let mut mask = NullBitmask::new();
mask.push(true);   // valid
mask.push(false);  // null
assert!(mask.get(0));
assert!(!mask.get(1));
assert_eq!(mask.count_valid(), 1);
assert_eq!(mask.count_null(), 1);
```

### 2.4 ScalarValue

The universal value type for all operations:

```rust
use pivot_engine::column::ScalarValue;

let v = ScalarValue::Int64(42);
let f = ScalarValue::Float64(3.14);
let s = ScalarValue::Utf8("hello".to_string());
let n = ScalarValue::Null;
let d = ScalarValue::Date(18628);          // days since epoch
let ts = ScalarValue::Timestamp(1609459200000000); // microseconds since epoch
```

## 3. SQL Engine

### 3.1 SqlEngine

The main entry point for SQL execution:

```rust
use pivot_engine::sql::SqlEngine;

let mut engine = SqlEngine::new();

// DDL
engine.execute("CREATE TABLE t (id INTEGER, name VARCHAR)")?;

// DML
engine.execute("INSERT INTO t VALUES (1, 'Alice')")?;

// DQL
let result = engine.execute("SELECT * FROM t WHERE id = 1")?;
println!("Rows: {}", result.row_count());
println!("Cols: {}", result.columns.len());
```

**QueryResult:**
```rust
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<ScalarValue>>,
}
impl QueryResult {
    pub fn row_count(&self) -> usize;
    pub fn column_count(&self) -> usize;
    pub fn get(&self, row: usize, col: usize) -> &ScalarValue;
}
```

### 3.2 Catalog

Stores tables and views:

```rust
use pivot_engine::sql::catalog::Catalog;
use pivot_engine::datastore::DataStore;

let mut catalog = Catalog::new();
catalog.register("employees", store);
let table = catalog.get("employees")?;
```

## 4. SQL Reference

### 4.1 DDL

```sql
-- Create table
CREATE TABLE employees (
    id      INTEGER NOT NULL,
    name    VARCHAR,
    dept    VARCHAR,
    salary  DOUBLE DEFAULT 50000.0,
    PRIMARY KEY (id)
);

-- Create table if not exists
CREATE TABLE IF NOT EXISTS t (id INTEGER);

-- Create table from query
CREATE TABLE summary AS SELECT dept, AVG(salary) FROM employees GROUP BY dept;

-- Alter table
ALTER TABLE employees ADD COLUMN bonus DOUBLE;
ALTER TABLE employees DROP COLUMN bonus;
ALTER TABLE employees RENAME COLUMN dept TO department;
ALTER TABLE employees RENAME TO staff;

-- Drop table
DROP TABLE employees;
DROP TABLE IF EXISTS employees;
```

### 4.2 DML

```sql
-- Insert single row
INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 90000.0);

-- Insert with column list
INSERT INTO employees (id, name, salary) VALUES (2, 'Bob', 80000.0);

-- Insert from query
INSERT INTO summary SELECT dept, AVG(salary) FROM employees GROUP BY dept;

-- Update
UPDATE employees SET salary = salary * 1.1 WHERE dept = 'Engineering';

-- Delete
DELETE FROM employees WHERE id = 5;
```

### 4.3 DQL

```sql
-- Basic select
SELECT id, name, salary FROM employees;
SELECT * FROM employees;
SELECT DISTINCT dept FROM employees;

-- Aliases
SELECT name AS employee_name, salary * 12 AS annual FROM employees;

-- WHERE conditions
SELECT * FROM employees WHERE salary > 80000 AND dept = 'Engineering';
SELECT * FROM employees WHERE dept IN ('Engineering', 'Marketing');
SELECT * FROM employees WHERE salary BETWEEN 70000 AND 90000;
SELECT * FROM employees WHERE name LIKE 'A%';
SELECT * FROM employees WHERE bonus IS NULL;

-- ORDER BY
SELECT * FROM employees ORDER BY salary DESC, name ASC;
SELECT * FROM employees ORDER BY 3 DESC;  -- by column index

-- LIMIT / OFFSET
SELECT * FROM employees ORDER BY salary DESC LIMIT 3;
SELECT * FROM employees ORDER BY salary DESC LIMIT 3 OFFSET 1;

-- GROUP BY
SELECT dept, COUNT(*), AVG(salary), MIN(salary), MAX(salary)
FROM employees GROUP BY dept;

-- HAVING
SELECT dept, AVG(salary) as avg_sal
FROM employees GROUP BY dept HAVING avg_sal > 75000;
```

### 4.4 CTEs

```sql
-- Simple CTE
WITH high_earners AS (
    SELECT * FROM employees WHERE salary > 80000
)
SELECT * FROM high_earners ORDER BY salary DESC;

-- Multiple CTEs
WITH
  eng AS (SELECT * FROM employees WHERE dept = 'Engineering'),
  mkt AS (SELECT * FROM employees WHERE dept = 'Marketing')
SELECT 'Engineering' as dept, COUNT(*) FROM eng
UNION ALL
SELECT 'Marketing', COUNT(*) FROM mkt;

-- Recursive CTE (number series)
WITH RECURSIVE nums(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM nums WHERE n < 10
)
SELECT * FROM nums;
```

### 4.5 JOINs

```sql
-- INNER JOIN
SELECT e.name, d.budget
FROM employees e
JOIN departments d ON e.dept = d.name;

-- LEFT JOIN
SELECT e.name, d.budget
FROM employees e
LEFT JOIN departments d ON e.dept = d.name;

-- RIGHT JOIN
SELECT e.name, d.budget
FROM employees e
RIGHT JOIN departments d ON e.dept = d.name;

-- FULL OUTER JOIN
SELECT e.name, d.budget
FROM employees e
FULL OUTER JOIN departments d ON e.dept = d.name;

-- CROSS JOIN
SELECT e.name, p.project
FROM employees e CROSS JOIN projects p;

-- NATURAL JOIN
SELECT * FROM employees NATURAL JOIN departments;

-- JOIN USING
SELECT * FROM employees JOIN departments USING (dept_id);
```

### 4.6 Aggregates

```sql
SELECT
    dept,
    COUNT(*)             AS headcount,
    COUNT(DISTINCT name) AS unique_names,
    SUM(salary)          AS total,
    AVG(salary)          AS average,
    MIN(salary)          AS minimum,
    MAX(salary)          AS maximum,
    STDDEV(salary)       AS stddev,
    STDDEV_POP(salary)   AS stddev_pop,
    STDDEV_SAMP(salary)  AS stddev_samp,
    VARIANCE(salary)     AS variance,
    MEDIAN(salary)       AS median,
    MODE(salary)         AS mode,
    STRING_AGG(name, ', ') AS names
FROM employees
GROUP BY dept;
```

### 4.7 Window Functions

```sql
-- Ranking
SELECT name, dept, salary,
    ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn,
    RANK()        OVER (PARTITION BY dept ORDER BY salary DESC) AS rnk,
    DENSE_RANK()  OVER (PARTITION BY dept ORDER BY salary DESC) AS drnk,
    NTILE(4)      OVER (ORDER BY salary)                        AS quartile
FROM employees;

-- Value functions
SELECT name, salary,
    LAG(salary, 1, 0)  OVER (ORDER BY id) AS prev_salary,
    LEAD(salary, 1, 0) OVER (ORDER BY id) AS next_salary,
    FIRST_VALUE(salary) OVER (PARTITION BY dept ORDER BY salary) AS dept_min,
    LAST_VALUE(salary)  OVER (PARTITION BY dept ORDER BY salary) AS dept_max
FROM employees;

-- Aggregate as window
SELECT name, salary,
    SUM(salary) OVER (PARTITION BY dept ORDER BY id) AS running_total,
    AVG(salary) OVER (PARTITION BY dept) AS dept_avg
FROM employees;
```

### 4.8 Window Frames

```sql
-- Running total (default with ORDER BY)
SELECT name, salary,
    SUM(salary) OVER (ORDER BY id
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
FROM employees;

-- Moving average (3-row window)
SELECT name, salary,
    AVG(salary) OVER (ORDER BY id
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg
FROM employees;

-- Centered window
SELECT name, salary,
    AVG(salary) OVER (ORDER BY id
        ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS centered_avg
FROM employees;

-- Full partition
SELECT name, salary,
    MAX(salary) OVER (PARTITION BY dept
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS dept_max
FROM employees;
```

### 4.9 Set Operations

```sql
-- UNION (distinct)
SELECT name FROM employees WHERE dept = 'Engineering'
UNION
SELECT name FROM contractors WHERE dept = 'Engineering';

-- UNION ALL (with duplicates)
SELECT dept FROM employees
UNION ALL
SELECT dept FROM contractors;

-- INTERSECT
SELECT name FROM employees
INTERSECT
SELECT name FROM managers;

-- EXCEPT
SELECT name FROM employees
EXCEPT
SELECT name FROM managers;
```

### 4.10 QUALIFY (B1)

```sql
-- Top earner per department
SELECT name, dept, salary
FROM employees
QUALIFY ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) = 1;

-- Top 2 per department
SELECT name, dept, salary
FROM employees
QUALIFY DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary DESC) <= 2;
```

### 4.11 GROUPING SETS / ROLLUP / CUBE (B2)

```sql
-- ROLLUP
SELECT dept, EXTRACT(YEAR FROM hired) AS yr, SUM(salary)
FROM employees
GROUP BY ROLLUP(dept, yr);
-- Equivalent to GROUPING SETS: (dept,yr), (dept), ()

-- CUBE
SELECT dept, location, COUNT(*)
FROM employees
GROUP BY CUBE(dept, location);
-- Generates all 4 subsets of {dept, location}

-- GROUPING SETS
SELECT dept, yr, SUM(salary)
FROM employees
GROUP BY GROUPING SETS((dept, yr), (dept), ());
```

### 4.12 MERGE (B3)

```sql
MERGE INTO target t
USING source s ON t.id = s.id
WHEN MATCHED AND s.action = 'update' THEN
    UPDATE SET t.salary = s.salary
WHEN MATCHED AND s.action = 'delete' THEN
    DELETE
WHEN NOT MATCHED THEN
    INSERT (id, name, salary) VALUES (s.id, s.name, s.salary);
```

### 4.13 Views (B11)

```sql
-- Create view
CREATE VIEW high_earners AS
    SELECT * FROM employees WHERE salary > 80000;

-- Create or replace
CREATE OR REPLACE VIEW summary AS
    SELECT dept, COUNT(*) AS cnt, AVG(salary) AS avg_sal
    FROM employees GROUP BY dept;

-- Create if not exists
CREATE VIEW IF NOT EXISTS v AS SELECT 1 AS n;

-- Query view
SELECT * FROM high_earners ORDER BY salary DESC;

-- Drop view
DROP VIEW high_earners;
DROP VIEW IF EXISTS high_earners;
```

### 4.14 PIVOT / UNPIVOT (B12)

```sql
-- PIVOT
SELECT * FROM sales
PIVOT (SUM(amount) FOR quarter IN ('Q1', 'Q2', 'Q3', 'Q4'));

-- UNPIVOT
SELECT * FROM quarterly_sales
UNPIVOT (amount FOR quarter IN (q1, q2, q3, q4));
```

### 4.15 SHOW / DESCRIBE / EXPLAIN (B15)

```sql
-- List tables and views
SHOW TABLES;

-- Describe table structure
DESCRIBE employees;

-- Explain query plan
EXPLAIN SELECT * FROM employees WHERE salary > 80000;
```

### 4.16 Transactions (B14)

```sql
-- Basic transaction
BEGIN;
UPDATE employees SET salary = salary * 1.1;
COMMIT;

-- Rollback
BEGIN TRANSACTION;
DELETE FROM employees WHERE dept = 'Marketing';
ROLLBACK;

-- Savepoints
BEGIN;
INSERT INTO employees VALUES (10, 'Frank', 'HR', 60000.0);
SAVEPOINT sp1;
INSERT INTO employees VALUES (11, 'Grace', 'HR', 65000.0);
ROLLBACK TO SAVEPOINT sp1;  -- Frank is still there, Grace is gone
COMMIT;

-- Release savepoint
BEGIN;
SAVEPOINT sp1;
-- ... work ...
RELEASE SAVEPOINT sp1;
COMMIT;
```

### 4.17 Type Casting

```sql
-- CAST
SELECT CAST(salary AS VARCHAR) FROM employees;
SELECT CAST('42' AS INTEGER);
SELECT CAST('2024-01-01' AS DATE);

-- TRY_CAST (returns NULL on failure)
SELECT TRY_CAST('not_a_number' AS INTEGER);  -- NULL

-- Implicit coercion
SELECT salary + 0.0 FROM employees;  -- Int64 → Float64
```

### 4.18 Null-Handling Functions

```sql
SELECT COALESCE(bonus, 0) FROM employees;          -- first non-null
SELECT NULLIF(dept, 'Unknown') FROM employees;      -- null if equal
SELECT IFNULL(bonus, 0.0) FROM employees;           -- alias for COALESCE(x, y)
SELECT IIF(salary > 80000, 'Senior', 'Junior') FROM employees;
```

### 4.19 Scalar Functions

#### String Functions
```sql
SELECT LOWER(name), UPPER(name), LENGTH(name) FROM employees;
SELECT TRIM('  hello  '), LTRIM('  hi'), RTRIM('hi  ');
SELECT SUBSTRING(name, 1, 3) FROM employees;
SELECT REPLACE(name, 'A', 'a') FROM employees;
SELECT CONCAT(name, ' - ', dept) FROM employees;
SELECT CONCAT_WS(', ', name, dept, CAST(salary AS VARCHAR)) FROM employees;
SELECT LEFT(name, 3), RIGHT(name, 3) FROM employees;
SELECT REVERSE(name), REPEAT('ab', 3) FROM employees;
SELECT LPAD('42', 5, '0'), RPAD('hi', 5, '.') FROM employees;
SELECT POSITION('li' IN name), STARTS_WITH(name, 'A') FROM employees;
SELECT SPLIT_PART('a,b,c', ',', 2);  -- 'b'
```

#### Math Functions
```sql
SELECT ABS(-5), SIGN(-3), ROUND(3.14159, 2) FROM employees;
SELECT CEIL(1.1), FLOOR(1.9) FROM employees;
SELECT POWER(2, 10), SQRT(16), EXP(1), LN(2.718) FROM employees;
SELECT LOG(100), LOG2(8), LOG(10, 1000) FROM employees;
SELECT GREATEST(1, 5, 3), LEAST(1, 5, 3) FROM employees;
SELECT PI(), SIN(0), COS(0), TAN(0) FROM employees;
SELECT DEGREES(3.14159), RADIANS(180) FROM employees;
SELECT TYPEOF(salary) FROM employees;  -- 'DOUBLE'
```

### 4.20 DateTime Functions

```sql
SELECT NOW(), CURRENT_DATE, CURRENT_TIME FROM t;
SELECT EXTRACT(YEAR FROM hired) AS yr,
       EXTRACT(MONTH FROM hired) AS mo,
       EXTRACT(DAY FROM hired) AS dy,
       EXTRACT(DOW FROM hired) AS weekday,
       EXTRACT(QUARTER FROM hired) AS q
FROM employees;

SELECT DATE_TRUNC('month', hired) FROM employees;
SELECT DATE_ADD(hired, 30) FROM employees;         -- add 30 days
SELECT DATE_SUB(hired, INTERVAL '1' YEAR) FROM employees;
SELECT DATE_DIFF('day', hired, NOW()) AS tenure FROM employees;

SELECT MAKE_DATE(2024, 1, 15);
SELECT TO_TIMESTAMP(1609459200);
SELECT DAYNAME(hired), MONTHNAME(hired) FROM employees;
SELECT LAST_DAY(hired) FROM employees;
SELECT EPOCH(hired), EPOCH_MS(hired) FROM employees;
SELECT AGE(hired) FROM employees;
```

## 5. Core APIs (Non-SQL)

### 5.1 Grouping

```rust
use pivot_engine::grouping::group_by;

let groups = group_by(&store, &["dept"])?;
for group in &groups {
    println!("Group: {:?}, {} rows", group.key, group.row_indices.len());
}
```

### 5.2 Aggregation

```rust
use pivot_engine::aggregation::{sum, count, avg, min, max};

let total = sum(&store, "salary")?;
let count_n = count(&store, "id")?;
let average = avg(&store, "salary")?;
let minimum = min(&store, "salary")?;
let maximum = max(&store, "salary")?;
```

### 5.3 Pivot (non-SQL)

```rust
use pivot_engine::pivot::{pivot_table, unpivot_table};

// Pivot: rows=dept, cols=quarter, values=sales
let pivoted = pivot_table(&store, &["dept"], "quarter", "sales")?;

// Unpivot
let unpivoted = unpivot_table(&store, &["id"], &["q1","q2","q3"], "quarter", "sales")?;
```

### 5.4 Filtering

```rust
use pivot_engine::filter::{filter_by, filter_eq};
use pivot_engine::column::ScalarValue;

// Filter by predicate
let filtered = filter_by(&store, |row, s| {
    matches!(s.get_value(row, "dept"), Ok(ScalarValue::Utf8(d)) if d == "Engineering")
})?;

// Filter by equality
let result = filter_eq(&store, "dept", &ScalarValue::Utf8("Engineering".to_string()))?;
```

### 5.5 Sorting

```rust
use pivot_engine::sort::sort_by;

// Sort by salary DESC, name ASC
let sorted = sort_by(&store, &["salary", "name"], &[false, true])?;
```

## 6. CSV Import/Export

```rust
use pivot_engine::csv::{CsvReader, CsvWriter};

// Import
let reader = CsvReader::new().with_delimiter(',').with_header(true);
let store = reader.read_str("id,name,salary\n1,Alice,90000\n2,Bob,80000")?;

// Export
let writer = CsvWriter::new();
let csv_string = writer.write_str(&store)?;
println!("{}", csv_string);
```

## 7. FFI Bindings

C-compatible API for embedding in non-Rust projects:

```c
#include <stdint.h>

void* pivot_engine_new();
void  pivot_engine_free(void* engine);
void* pivot_engine_execute(void* engine, const char* sql);
int   pivot_result_row_count(const void* result);
int   pivot_result_column_count(const void* result);
const char* pivot_result_column_name(const void* result, int col);
const char* pivot_result_value(const void* result, int row, int col);
void  pivot_result_free(void* result);
```

```c
// Example usage in C
void* engine = pivot_engine_new();
pivot_engine_execute(engine, "CREATE TABLE t (id INTEGER, name VARCHAR)");
pivot_engine_execute(engine, "INSERT INTO t VALUES (1, 'Alice')");
void* result = pivot_engine_execute(engine, "SELECT * FROM t");
int rows = pivot_result_row_count(result);
int cols = pivot_result_column_count(result);
for (int r = 0; r < rows; r++) {
    for (int c = 0; c < cols; c++) {
        printf("%s\t", pivot_result_value(result, r, c));
    }
    printf("\n");
}
pivot_result_free(result);
pivot_engine_free(engine);
```

## 8. Complete Analytics Example

```rust
use pivot_engine::sql::SqlEngine;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut engine = SqlEngine::new();

    // Setup
    engine.execute("
        CREATE TABLE sales (
            id       INTEGER NOT NULL,
            rep      VARCHAR,
            region   VARCHAR,
            product  VARCHAR,
            amount   DOUBLE,
            sale_dt  DATE
        )
    ")?;

    // Load data
    let rows = vec![
        "(1, 'Alice', 'North', 'Widget', 1200.0, '2024-01-15')",
        "(2, 'Bob',   'South', 'Gadget', 850.0,  '2024-01-20')",
        "(3, 'Alice', 'North', 'Gadget', 950.0,  '2024-02-01')",
        "(4, 'Carol', 'East',  'Widget', 1100.0, '2024-02-10')",
        "(5, 'Bob',   'South', 'Widget', 1300.0, '2024-02-15')",
    ];
    for r in rows {
        engine.execute(&format!("INSERT INTO sales VALUES {}", r))?;
    }

    // Regional summary with ROLLUP
    let summary = engine.execute("
        SELECT region, product, SUM(amount) AS total
        FROM sales
        GROUP BY ROLLUP(region, product)
        ORDER BY region NULLS LAST, product NULLS LAST
    ")?;

    // Running total per rep
    let running = engine.execute("
        SELECT rep, sale_dt, amount,
            SUM(amount) OVER (
                PARTITION BY rep ORDER BY sale_dt
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS running_total
        FROM sales
        ORDER BY rep, sale_dt
    ")?;

    // Top seller per region
    let top = engine.execute("
        SELECT rep, region, amount
        FROM sales
        QUALIFY ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) = 1
    ")?;

    // PIVOT by product
    let pivoted = engine.execute("
        SELECT * FROM sales
        PIVOT (SUM(amount) FOR product IN ('Widget', 'Gadget'))
    ")?;

    Ok(())
}
```

## 9. Feature Matrix

| Feature | Status |
|---------|--------|
| Basic SELECT/INSERT/UPDATE/DELETE | ✅ Implemented |
| CREATE/DROP/ALTER TABLE | ✅ Implemented |
| WHERE (all operators) | ✅ Implemented |
| GROUP BY + HAVING | ✅ Implemented |
| ORDER BY + LIMIT/OFFSET | ✅ Implemented |
| DISTINCT | ✅ Implemented |
| INNER/LEFT/RIGHT/FULL/CROSS JOIN | ✅ Implemented |
| NATURAL JOIN / JOIN USING | ✅ Implemented |
| Subqueries (scalar, IN, EXISTS) | ✅ Implemented |
| Correlated subqueries | ✅ Implemented |
| CTEs (WITH clause) | ✅ Implemented |
| Recursive CTEs | ✅ Implemented |
| Window Functions (RANK, ROW_NUMBER, etc.) | ✅ Implemented |
| Window Frames (ROWS BETWEEN) | ✅ Implemented |
| QUALIFY | ✅ Implemented |
| GROUPING SETS / ROLLUP / CUBE | ✅ Implemented |
| MERGE INTO | ✅ Implemented |
| Views (CREATE/DROP VIEW) | ✅ Implemented |
| PIVOT / UNPIVOT | ✅ Implemented |
| Transactions (BEGIN/COMMIT/ROLLBACK) | ✅ Implemented |
| Savepoints | ✅ Implemented |
| SHOW TABLES / DESCRIBE / EXPLAIN | ✅ Implemented |
| Type casting (CAST/TRY_CAST) | ✅ Implemented |
| Null functions (COALESCE, NULLIF, etc.) | ✅ Implemented |
| CASE expressions | ✅ Implemented |
| BETWEEN / IN / LIKE / IS NULL | ✅ Implemented |
| SET ops (UNION/INTERSECT/EXCEPT) | ✅ Implemented |
| Scalar string functions (30+) | ✅ Implemented |
| Scalar math functions (20+) | ✅ Implemented |
| DateTime functions (20+) | ✅ Implemented |
| Statistics (STDDEV, VARIANCE, MEDIAN, MODE) | ✅ Implemented |
| STRING_AGG / GROUP_CONCAT | ✅ Implemented |
| DECIMAL type | ✅ Implemented |
| Constraints (PK, UNIQUE, NOT NULL, DEFAULT, CHECK) | ✅ Implemented |
| Hash Join optimization | ✅ Implemented |
| CSV Import/Export | ✅ Implemented |
| FFI (C-compatible API) | ✅ Implemented |
| Non-SQL APIs (grouping, filter, sort, etc.) | ✅ Implemented |
| Parallel execution | ❌ Not yet |
| Disk-based storage | ❌ Not yet |
| Indexes | ❌ Not yet |
| Query optimizer | ❌ Not yet |
| Distributed queries | ❌ Not yet |
