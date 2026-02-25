use crate::schema::DataType;

#[derive(Debug, Clone)]
pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    CreateTable(CreateTableStatement),
    DropTable(DropTableStatement),
    With(WithStatement),
    Begin,
    Commit,
    Rollback,
    Explain(Box<Statement>),
    SetOp(SetOpStatement),
}

#[derive(Debug, Clone)]
pub struct WithStatement {
    pub ctes: Vec<Cte>,
    pub body: Box<Statement>,
}

#[derive(Debug, Clone)]
pub struct Cte {
    pub name: String,
    pub query: Box<Statement>,
}

#[derive(Debug, Clone)]
pub struct SelectStatement {
    pub distinct: bool,
    pub columns: Vec<SelectItem>,
    pub from: Option<TableRef>,
    pub joins: Vec<Join>,
    pub where_clause: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<Expr>,
    pub offset: Option<Expr>,
}

#[derive(Debug, Clone)]
pub enum SelectItem {
    Wildcard,
    TableWildcard(String),
    Expr { expr: Expr, alias: Option<String> },
}

#[derive(Debug, Clone)]
pub enum TableRef {
    Table { name: String, alias: Option<String> },
    Subquery { query: Box<Statement>, alias: String },
}

#[derive(Debug, Clone)]
pub struct Join {
    pub join_type: JoinType,
    pub table: TableRef,
    pub condition: JoinCondition,
}

#[derive(Debug, Clone)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Debug, Clone)]
pub enum JoinCondition {
    On(Expr),
    Using(Vec<String>),
    None,
}

#[derive(Debug, Clone)]
pub struct OrderByItem {
    pub expr: Expr,
    pub ascending: bool,
    pub nulls_first: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct WindowSpec {
    pub name: Option<String>,
    pub partition_by: Vec<Expr>,
    pub order_by: Vec<OrderByItem>,
    pub frame: Option<WindowFrame>,
}

#[derive(Debug, Clone)]
pub struct WindowFrame {
    pub kind: WindowFrameKind,
    pub start: WindowFrameBound,
    pub end: Option<WindowFrameBound>,
}

#[derive(Debug, Clone)]
pub enum WindowFrameKind { Rows, Range }

#[derive(Debug, Clone)]
pub enum WindowFrameBound {
    UnboundedPreceding,
    Preceding(Box<Expr>),
    CurrentRow,
    Following(Box<Expr>),
    UnboundedFollowing,
}

#[derive(Debug, Clone)]
pub struct InsertStatement {
    pub table: String,
    pub columns: Option<Vec<String>>,
    pub values: InsertValues,
}

#[derive(Debug, Clone)]
pub enum InsertValues {
    Values(Vec<Vec<Expr>>),
    Select(Box<Statement>),
}

#[derive(Debug, Clone)]
pub struct UpdateStatement {
    pub table: String,
    pub alias: Option<String>,
    pub assignments: Vec<Assignment>,
    pub where_clause: Option<Expr>,
}

#[derive(Debug, Clone)]
pub struct Assignment {
    pub column: String,
    pub value: Expr,
}

#[derive(Debug, Clone)]
pub struct DeleteStatement {
    pub table: String,
    pub where_clause: Option<Expr>,
}

#[derive(Debug, Clone)]
pub struct CreateTableStatement {
    pub name: String,
    pub if_not_exists: bool,
    pub columns: Vec<ColumnDefAst>,
}

#[derive(Debug, Clone)]
pub struct ColumnDefAst {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub default: Option<Expr>,
    pub primary_key: bool,
}

#[derive(Debug, Clone)]
pub struct DropTableStatement {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub struct SetOpStatement {
    pub op: SetOp,
    pub all: bool,
    pub left: Box<Statement>,
    pub right: Box<Statement>,
}

#[derive(Debug, Clone)]
pub enum SetOp { Union, Intersect, Except }

#[derive(Debug, Clone)]
pub enum Expr {
    Literal(LiteralValue),
    Column(ColumnRef),
    BinaryOp { left: Box<Expr>, op: BinOp, right: Box<Expr> },
    UnaryOp { op: UnaryOp, expr: Box<Expr> },
    Function { name: String, args: Vec<Expr>, distinct: bool, over: Option<WindowSpec> },
    Cast { expr: Box<Expr>, data_type: DataType },
    TryCast { expr: Box<Expr>, data_type: DataType },
    Case {
        operand: Option<Box<Expr>>,
        when_clauses: Vec<(Expr, Expr)>,
        else_clause: Option<Box<Expr>>,
    },
    IsNull { expr: Box<Expr>, negated: bool },
    InList { expr: Box<Expr>, list: Vec<Expr>, negated: bool },
    InSubquery { expr: Box<Expr>, query: Box<Statement>, negated: bool },
    Between { expr: Box<Expr>, low: Box<Expr>, high: Box<Expr>, negated: bool },
    Like { expr: Box<Expr>, pattern: Box<Expr>, negated: bool, case_insensitive: bool },
    Subquery(Box<Statement>),
    Exists { query: Box<Statement>, negated: bool },
    Wildcard,
    TypeCast { expr: Box<Expr>, data_type: DataType },
}

#[derive(Debug, Clone)]
pub struct ColumnRef {
    pub table: Option<String>,
    pub name: String,
}

#[derive(Debug, Clone)]
pub enum LiteralValue {
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
    Null,
    Interval { value: String, unit: String },
}

#[derive(Debug, Clone, PartialEq)]
pub enum BinOp {
    Add, Sub, Mul, Div, Mod,
    Eq, NotEq, Lt, LtEq, Gt, GtEq,
    And, Or,
    Concat,
}

#[derive(Debug, Clone)]
pub enum UnaryOp { Neg, Not }
