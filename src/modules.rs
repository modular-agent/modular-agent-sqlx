use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};

use im::{Vector, hashmap};
use modular_agent_core::{
    AsModule, Error, ModularAgent, Module, ModuleContext, ModuleData, ModuleOutput, ModuleSpec,
    Result, Value, async_trait, modular_agent,
};
use sqlx::any::{AnyArguments, AnyRow, AnyValueRef, install_default_drivers};
use sqlx::{Any, AnyPool, Arguments, Column, Decode, Row, TypeInfo, ValueRef};

static DB_MAP: OnceLock<Mutex<BTreeMap<String, AnyPool>>> = OnceLock::new();
static DRIVERS_INSTALLED: OnceLock<()> = OnceLock::new();

static CATEGORY: &str = "DB/SQLx";

static PORT_ARRAY: &str = "array";
static PORT_VALUE: &str = "value";
static PORT_TABLE: &str = "table";

static CONFIG_DB: &str = "db";
static CONFIG_SCRIPT: &str = "script";

#[modular_agent(
    title = "SQLx Script",
    category = CATEGORY,
    inputs = [PORT_VALUE],
    outputs = [PORT_TABLE],
    string_config(name = CONFIG_DB),
    text_config(name = CONFIG_SCRIPT)
)]
struct SqlxScriptModule {
    data: ModuleData,
}

#[async_trait]
impl AsModule for SqlxScriptModule {
    fn new(ma: ModularAgent, id: String, spec: ModuleSpec) -> Result<Self> {
        Ok(Self {
            data: ModuleData::new(ma, id, spec),
        })
    }

    async fn process(&mut self, ctx: ModuleContext, _port: String, value: Value) -> Result<()> {
        let config = self.configs()?;
        let script = config.get_string(CONFIG_SCRIPT)?;
        if script.is_empty() {
            return Ok(());
        }
        let pool = get_pool(&config.get_string_or_default(CONFIG_DB)).await?;

        let params = build_sqlx_params(&value)?;
        let value = run_sqlx_statement(&pool, &script, params).await?;

        self.output(ctx, PORT_TABLE, value).await
    }
}

async fn get_pool(db: &str) -> Result<AnyPool> {
    // Install database drivers on first use
    DRIVERS_INSTALLED.get_or_init(install_default_drivers);

    let db_map = DB_MAP.get_or_init(|| Mutex::new(BTreeMap::new()));
    if let Some(pool) = db_map.lock().unwrap().get(db).cloned() {
        return Ok(pool);
    }

    let url = normalize_db_url(db);
    let pool = AnyPool::connect(&url)
        .await
        .map_err(|e| Error::IoError(format!("SQLx Error creating pool: {}", e)))?;

    let mut map_guard = db_map.lock().unwrap();
    let entry = map_guard
        .entry(db.to_string())
        .or_insert_with(|| pool.clone());
    Ok(entry.clone())
}

/// Normalize database URL to sqlx format.
/// - `mysql:...` -> `mysql://...`
/// - `postgres:...` -> `postgres://...`
/// - `sqlite:...` -> `sqlite:...`
/// - (default) path or empty -> `sqlite:path` or `sqlite::memory:`
fn normalize_db_url(db: &str) -> String {
    if db.is_empty() {
        return "sqlite::memory:".to_string();
    }

    if db.starts_with("mysql:") {
        let rest = db.strip_prefix("mysql:").unwrap();
        if rest.starts_with("//") {
            return db.to_string();
        }
        return format!("mysql://{}", rest);
    }

    if db.starts_with("postgres:") || db.starts_with("postgresql:") {
        let rest = if let Some(r) = db.strip_prefix("postgres:") {
            r
        } else {
            db.strip_prefix("postgresql:").unwrap()
        };
        if rest.starts_with("//") {
            return db.to_string();
        }
        return format!("postgres://{}", rest);
    }

    if db.starts_with("sqlite:") {
        return db.to_string();
    }

    // Default: treat as SQLite file path
    format!("sqlite:{}?mode=rwc", db)
}

fn build_sqlx_params(value: &Value) -> Result<AnyArguments<'static>> {
    let mut args = AnyArguments::default();

    if let Some(arr) = value.as_array() {
        for item in arr.iter() {
            add_value_param(&mut args, item)?;
        }
        return Ok(args);
    }
    add_value_param(&mut args, value)?;

    Ok(args)
}

fn add_value_param(args: &mut AnyArguments<'static>, value: &Value) -> Result<()> {
    let bind_result = match value {
        Value::Unit => args.add(Option::<i64>::None),
        Value::Boolean(b) => args.add(*b),
        Value::Integer(i) => args.add(*i),
        Value::Number(n) => args.add(*n),
        Value::String(s) => args.add(s.as_ref().clone()),
        Value::Array(_) | Value::Object(_) | Value::Tensor(_) => {
            let json = serde_json::to_string(&value.to_json()).unwrap_or_default();
            args.add(json)
        }
        Value::Message(_) | Value::Error(_) => {
            let json = serde_json::to_string(&value.to_json()).unwrap_or_default();
            args.add(json)
        }
        #[cfg(feature = "image")]
        Value::Image(_) => {
            let json = serde_json::to_string(&value.to_json()).unwrap_or_default();
            args.add(json)
        }
    };

    bind_result.map_err(|e| Error::IoError(format!("SQLx Error binding param: {}", e)))
}

async fn run_sqlx_statement(
    pool: &AnyPool,
    script: &str,
    params: AnyArguments<'static>,
) -> Result<Value> {
    if script_returns_rows(script) {
        // Use fetch_all for SELECT-like queries
        let rows: Vec<AnyRow> = sqlx::query_with(script, params)
            .fetch_all(pool)
            .await
            .map_err(|e| Error::IoError(format!("SQLx Error: {}", e)))?;

        let headers: Vec<String> = if let Some(first_row) = rows.first() {
            first_row
                .columns()
                .iter()
                .map(|c| c.name().to_string())
                .collect()
        } else {
            Vec::new()
        };

        let headers_value = Value::array(headers.into_iter().map(Value::string).collect());
        let mut row_values: Vector<Value> = Vector::new();
        for row in &rows {
            row_values.push_back(sqlx_row_to_value(row)?);
        }

        Ok(Value::object(hashmap! {
            "headers".into() => headers_value,
            "rows".into() => Value::array(row_values),
        }))
    } else {
        // Use execute for INSERT/UPDATE/DELETE
        let result = sqlx::query_with(script, params)
            .execute(pool)
            .await
            .map_err(|e| Error::IoError(format!("SQLx Error: {}", e)))?;

        Ok(rows_affected_to_table(result.rows_affected()))
    }
}

fn rows_affected_to_table(rows_affected: u64) -> Value {
    let rows_affected = i64::try_from(rows_affected).unwrap_or(i64::MAX);
    let headers = Value::array(Vector::unit(Value::string("rows_affected")));
    let row = Value::array(Vector::unit(Value::integer(rows_affected)));
    let rows = Value::array(Vector::unit(row));
    Value::object(hashmap! {
        "headers".into() => headers,
        "rows".into() => rows,
    })
}

fn script_returns_rows(script: &str) -> bool {
    let keyword = first_keyword(script);
    matches!(
        keyword.as_deref(),
        Some("select")
            | Some("with")
            | Some("pragma")
            | Some("show")
            | Some("describe")
            | Some("explain")
    )
}

fn first_keyword(script: &str) -> Option<String> {
    let mut rest = script;
    loop {
        let trimmed = rest.trim_start();
        if trimmed.starts_with("--") {
            rest = trimmed.split_once('\n')?.1;
            continue;
        }
        if trimmed.starts_with("/*") {
            let end = trimmed.find("*/")?;
            rest = &trimmed[end + 2..];
            continue;
        }
        return trimmed
            .split_whitespace()
            .next()
            .map(|word| word.to_ascii_lowercase());
    }
}

fn sqlx_row_to_value(row: &AnyRow) -> Result<Value> {
    let mut cells: Vector<Value> = Vector::new();
    for col_idx in 0..row.len() {
        let cell = row
            .try_get_raw(col_idx)
            .map(sqlx_value_ref_to_value)
            .map_err(|e| Error::IoError(format!("SQLx Error: {}", e)))?;
        cells.push_back(cell);
    }
    Ok(Value::array(cells))
}

fn sqlx_value_ref_to_value(value: AnyValueRef<'_>) -> Value {
    if value.is_null() {
        return Value::unit();
    }

    // Store type name as owned String before moving value
    let type_name = value.type_info().name().to_string();

    // Try to decode based on common type names across databases
    match type_name.to_uppercase().as_str() {
        // Boolean types
        "BOOL" | "BOOLEAN" => {
            if let Ok(v) = <bool as Decode<Any>>::decode(value) {
                Value::boolean(v)
            } else {
                Value::string(type_name)
            }
        }
        // Integer types (SQLite, MySQL, PostgreSQL)
        "INTEGER" | "INT" | "INT4" | "INT8" | "BIGINT" | "SMALLINT" | "TINYINT" | "MEDIUMINT" => {
            if let Ok(v) = <i64 as Decode<Any>>::decode(value) {
                Value::integer(v)
            } else {
                Value::string(type_name)
            }
        }
        // Float types
        "REAL" | "FLOAT" | "FLOAT4" | "FLOAT8" | "DOUBLE" | "DOUBLE PRECISION" | "NUMERIC"
        | "DECIMAL" => {
            if let Ok(v) = <f64 as Decode<Any>>::decode(value) {
                Value::number(v)
            } else {
                Value::string(type_name)
            }
        }
        // Text types
        "TEXT" | "VARCHAR" | "CHAR" | "BPCHAR" | "NAME" | "CITEXT" | "LONGTEXT" | "MEDIUMTEXT"
        | "TINYTEXT" => {
            if let Ok(v) = <String as Decode<Any>>::decode(value) {
                Value::string(v)
            } else {
                Value::string(type_name)
            }
        }
        // Blob types
        "BLOB" | "BYTEA" | "BINARY" | "VARBINARY" | "LONGBLOB" | "MEDIUMBLOB" | "TINYBLOB" => {
            if let Ok(v) = <Vec<u8> as Decode<Any>>::decode(value) {
                let arr: Vector<Value> = v.iter().map(|b: &u8| Value::integer(*b as i64)).collect();
                Value::array(arr)
            } else {
                Value::string(type_name)
            }
        }
        _ => {
            // Fallback: try to decode as string
            if let Ok(v) = <String as Decode<Any>>::decode(value) {
                Value::string(v)
            } else {
                Value::string(type_name)
            }
        }
    }
}

#[modular_agent(
    title = "Rows",
    category = CATEGORY,
    inputs = [PORT_TABLE],
    outputs = [PORT_ARRAY],
)]
struct RowsModule {
    data: ModuleData,
}

#[async_trait]
impl AsModule for RowsModule {
    fn new(ma: ModularAgent, id: String, spec: ModuleSpec) -> Result<Self> {
        Ok(Self {
            data: ModuleData::new(ma, id, spec),
        })
    }

    async fn process(&mut self, ctx: ModuleContext, _port: String, value: Value) -> Result<()> {
        let rows = value
            .get_array("rows")
            .ok_or_else(|| Error::InvalidValue("Missing 'rows' field".to_string()))?;
        self.output(ctx, PORT_ARRAY, Value::array(rows.clone()))
            .await
    }
}

#[modular_agent(
    title = "Row",
    category = CATEGORY,
    inputs = [PORT_TABLE],
    outputs = [PORT_ARRAY],
    integer_config(name = "index"),
)]
struct RowModule {
    data: ModuleData,
}

#[async_trait]
impl AsModule for RowModule {
    fn new(ma: ModularAgent, id: String, spec: ModuleSpec) -> Result<Self> {
        Ok(Self {
            data: ModuleData::new(ma, id, spec),
        })
    }

    async fn process(&mut self, ctx: ModuleContext, _port: String, value: Value) -> Result<()> {
        let index = self.configs()?.get_integer("index")? as usize;
        let row = value
            .get_array("rows")
            .ok_or_else(|| Error::InvalidValue("Missing 'rows' field".to_string()))?
            .get(index)
            .ok_or_else(|| Error::InvalidValue(format!("Row index {} out of bounds", index)))?;
        self.output(ctx, PORT_ARRAY, row.clone()).await
    }
}

#[modular_agent(
    title = "Select",
    category = CATEGORY,
    inputs = [PORT_TABLE],
    outputs = [PORT_ARRAY],
    string_config(name = "cols"),
)]
struct SelectModule {
    data: ModuleData,
}

#[async_trait]
impl AsModule for SelectModule {
    fn new(ma: ModularAgent, id: String, spec: ModuleSpec) -> Result<Self> {
        Ok(Self {
            data: ModuleData::new(ma, id, spec),
        })
    }

    async fn process(&mut self, ctx: ModuleContext, _port: String, value: Value) -> Result<()> {
        let cols = self
            .configs()?
            .get_string("cols")?
            .split(',')
            .map(|s| s.trim().to_string())
            .collect::<Vec<String>>();
        let headers = value
            .get_array("headers")
            .ok_or_else(|| Error::InvalidValue("Missing 'headers' field".to_string()))?;
        let col_indices: Vec<usize> = cols
            .iter()
            .map(|col| {
                headers
                    .iter()
                    .position(|h| h.as_str().map_or(false, |hs| hs == col))
                    .ok_or_else(|| Error::InvalidValue(format!("Column '{}' not found", col)))
            })
            .collect::<Result<Vec<usize>>>()?;

        let arr = value
            .get_array("rows")
            .ok_or_else(|| Error::InvalidValue("Missing 'rows' field".to_string()))?
            .iter()
            .map(|row| {
                let row_array = row
                    .as_array()
                    .ok_or_else(|| Error::InvalidValue("Row is not an array".to_string()))?;
                let selected_cells: im::Vector<Value> = col_indices
                    .iter()
                    .map(|&i| row_array.get(i).cloned().unwrap_or_else(|| Value::unit()))
                    .collect();
                Ok(Value::array(selected_cells))
            })
            .collect::<Result<im::Vector<Value>>>()?;

        if arr.len() == 1 {
            self.output(ctx, PORT_ARRAY, arr[0].clone()).await
        } else {
            self.output(ctx, PORT_ARRAY, Value::array(arr)).await
        }
    }
}
