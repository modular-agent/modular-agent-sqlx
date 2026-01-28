use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};

use im::{Vector, hashmap};
use modular_agent_core::{
    Agent, AgentContext, AgentData, AgentError, AgentOutput, AgentSpec, AgentValue, AsAgent,
    ModularAgent, async_trait, modular_agent,
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
struct SqlxScriptAgent {
    data: AgentData,
}

#[async_trait]
impl AsAgent for SqlxScriptAgent {
    fn new(ma: ModularAgent, id: String, spec: AgentSpec) -> Result<Self, AgentError> {
        Ok(Self {
            data: AgentData::new(ma, id, spec),
        })
    }

    async fn process(
        &mut self,
        ctx: AgentContext,
        _port: String,
        value: AgentValue,
    ) -> Result<(), AgentError> {
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

async fn get_pool(db: &str) -> Result<AnyPool, AgentError> {
    // Install database drivers on first use
    DRIVERS_INSTALLED.get_or_init(install_default_drivers);

    let db_map = DB_MAP.get_or_init(|| Mutex::new(BTreeMap::new()));
    if let Some(pool) = db_map.lock().unwrap().get(db).cloned() {
        return Ok(pool);
    }

    let url = normalize_db_url(db);
    let pool = AnyPool::connect(&url)
        .await
        .map_err(|e| AgentError::IoError(format!("SQLx Error creating pool: {}", e)))?;

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

fn build_sqlx_params(value: &AgentValue) -> Result<AnyArguments<'static>, AgentError> {
    let mut args = AnyArguments::default();

    if let Some(arr) = value.as_array() {
        for item in arr.iter() {
            add_agent_value_param(&mut args, item)?;
        }
        return Ok(args);
    }
    add_agent_value_param(&mut args, value)?;

    Ok(args)
}

fn add_agent_value_param(
    args: &mut AnyArguments<'static>,
    value: &AgentValue,
) -> Result<(), AgentError> {
    let bind_result = match value {
        AgentValue::Unit => args.add(Option::<i64>::None),
        AgentValue::Boolean(b) => args.add(*b),
        AgentValue::Integer(i) => args.add(*i),
        AgentValue::Number(n) => args.add(*n),
        AgentValue::String(s) => args.add(s.as_ref().clone()),
        AgentValue::Array(_) | AgentValue::Object(_) | AgentValue::Tensor(_) => {
            let json = serde_json::to_string(&value.to_json()).unwrap_or_default();
            args.add(json)
        }
        AgentValue::Message(_) | AgentValue::Error(_) => {
            let json = serde_json::to_string(&value.to_json()).unwrap_or_default();
            args.add(json)
        }
        #[cfg(feature = "image")]
        AgentValue::Image(_) => {
            let json = serde_json::to_string(&value.to_json()).unwrap_or_default();
            args.add(json)
        }
    };

    bind_result.map_err(|e| AgentError::IoError(format!("SQLx Error binding param: {}", e)))
}

async fn run_sqlx_statement(
    pool: &AnyPool,
    script: &str,
    params: AnyArguments<'static>,
) -> Result<AgentValue, AgentError> {
    if script_returns_rows(script) {
        // Use fetch_all for SELECT-like queries
        let rows: Vec<AnyRow> = sqlx::query_with(script, params)
            .fetch_all(pool)
            .await
            .map_err(|e| AgentError::IoError(format!("SQLx Error: {}", e)))?;

        let headers: Vec<String> = if let Some(first_row) = rows.first() {
            first_row
                .columns()
                .iter()
                .map(|c| c.name().to_string())
                .collect()
        } else {
            Vec::new()
        };

        let headers_value =
            AgentValue::array(headers.into_iter().map(AgentValue::string).collect());
        let mut row_values: Vector<AgentValue> = Vector::new();
        for row in &rows {
            row_values.push_back(sqlx_row_to_agent_value(row)?);
        }

        Ok(AgentValue::object(hashmap! {
            "headers".into() => headers_value,
            "rows".into() => AgentValue::array(row_values),
        }))
    } else {
        // Use execute for INSERT/UPDATE/DELETE
        let result = sqlx::query_with(script, params)
            .execute(pool)
            .await
            .map_err(|e| AgentError::IoError(format!("SQLx Error: {}", e)))?;

        Ok(rows_affected_to_table(result.rows_affected()))
    }
}

fn rows_affected_to_table(rows_affected: u64) -> AgentValue {
    let rows_affected = i64::try_from(rows_affected).unwrap_or(i64::MAX);
    let headers = AgentValue::array(Vector::unit(AgentValue::string("rows_affected")));
    let row = AgentValue::array(Vector::unit(AgentValue::integer(rows_affected)));
    let rows = AgentValue::array(Vector::unit(row));
    AgentValue::object(hashmap! {
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

fn sqlx_row_to_agent_value(row: &AnyRow) -> Result<AgentValue, AgentError> {
    let mut cells: Vector<AgentValue> = Vector::new();
    for col_idx in 0..row.len() {
        let cell = row
            .try_get_raw(col_idx)
            .map(sqlx_value_ref_to_agent_value)
            .map_err(|e| AgentError::IoError(format!("SQLx Error: {}", e)))?;
        cells.push_back(cell);
    }
    Ok(AgentValue::array(cells))
}

fn sqlx_value_ref_to_agent_value(value: AnyValueRef<'_>) -> AgentValue {
    if value.is_null() {
        return AgentValue::unit();
    }

    // Store type name as owned String before moving value
    let type_name = value.type_info().name().to_string();

    // Try to decode based on common type names across databases
    match type_name.to_uppercase().as_str() {
        // Boolean types
        "BOOL" | "BOOLEAN" => {
            if let Ok(v) = <bool as Decode<Any>>::decode(value) {
                AgentValue::boolean(v)
            } else {
                AgentValue::string(type_name)
            }
        }
        // Integer types (SQLite, MySQL, PostgreSQL)
        "INTEGER" | "INT" | "INT4" | "INT8" | "BIGINT" | "SMALLINT" | "TINYINT" | "MEDIUMINT" => {
            if let Ok(v) = <i64 as Decode<Any>>::decode(value) {
                AgentValue::integer(v)
            } else {
                AgentValue::string(type_name)
            }
        }
        // Float types
        "REAL" | "FLOAT" | "FLOAT4" | "FLOAT8" | "DOUBLE" | "DOUBLE PRECISION" | "NUMERIC"
        | "DECIMAL" => {
            if let Ok(v) = <f64 as Decode<Any>>::decode(value) {
                AgentValue::number(v)
            } else {
                AgentValue::string(type_name)
            }
        }
        // Text types
        "TEXT" | "VARCHAR" | "CHAR" | "BPCHAR" | "NAME" | "CITEXT" | "LONGTEXT" | "MEDIUMTEXT"
        | "TINYTEXT" => {
            if let Ok(v) = <String as Decode<Any>>::decode(value) {
                AgentValue::string(v)
            } else {
                AgentValue::string(type_name)
            }
        }
        // Blob types
        "BLOB" | "BYTEA" | "BINARY" | "VARBINARY" | "LONGBLOB" | "MEDIUMBLOB" | "TINYBLOB" => {
            if let Ok(v) = <Vec<u8> as Decode<Any>>::decode(value) {
                let arr: Vector<AgentValue> = v
                    .iter()
                    .map(|b: &u8| AgentValue::integer(*b as i64))
                    .collect();
                AgentValue::array(arr)
            } else {
                AgentValue::string(type_name)
            }
        }
        _ => {
            // Fallback: try to decode as string
            if let Ok(v) = <String as Decode<Any>>::decode(value) {
                AgentValue::string(v)
            } else {
                AgentValue::string(type_name)
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
struct RowsAgent {
    data: AgentData,
}

#[async_trait]
impl AsAgent for RowsAgent {
    fn new(ma: ModularAgent, id: String, spec: AgentSpec) -> Result<Self, AgentError> {
        Ok(Self {
            data: AgentData::new(ma, id, spec),
        })
    }

    async fn process(
        &mut self,
        ctx: AgentContext,
        _port: String,
        value: AgentValue,
    ) -> Result<(), AgentError> {
        let rows = value
            .get_array("rows")
            .ok_or_else(|| AgentError::InvalidValue("Missing 'rows' field".to_string()))?;
        self.output(ctx, PORT_ARRAY, AgentValue::array(rows.clone()))
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
struct RowAgent {
    data: AgentData,
}

#[async_trait]
impl AsAgent for RowAgent {
    fn new(ma: ModularAgent, id: String, spec: AgentSpec) -> Result<Self, AgentError> {
        Ok(Self {
            data: AgentData::new(ma, id, spec),
        })
    }

    async fn process(
        &mut self,
        ctx: AgentContext,
        _port: String,
        value: AgentValue,
    ) -> Result<(), AgentError> {
        let index = self.configs()?.get_integer("index")? as usize;
        let row = value
            .get_array("rows")
            .ok_or_else(|| AgentError::InvalidValue("Missing 'rows' field".to_string()))?
            .get(index)
            .ok_or_else(|| {
                AgentError::InvalidValue(format!("Row index {} out of bounds", index))
            })?;
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
struct SelectAgent {
    data: AgentData,
}

#[async_trait]
impl AsAgent for SelectAgent {
    fn new(ma: ModularAgent, id: String, spec: AgentSpec) -> Result<Self, AgentError> {
        Ok(Self {
            data: AgentData::new(ma, id, spec),
        })
    }

    async fn process(
        &mut self,
        ctx: AgentContext,
        _port: String,
        value: AgentValue,
    ) -> Result<(), AgentError> {
        let cols = self
            .configs()?
            .get_string("cols")?
            .split(',')
            .map(|s| s.trim().to_string())
            .collect::<Vec<String>>();
        let headers = value
            .get_array("headers")
            .ok_or_else(|| AgentError::InvalidValue("Missing 'headers' field".to_string()))?;
        let col_indices: Vec<usize> = cols
            .iter()
            .map(|col| {
                headers
                    .iter()
                    .position(|h| h.as_str().map_or(false, |hs| hs == col))
                    .ok_or_else(|| AgentError::InvalidValue(format!("Column '{}' not found", col)))
            })
            .collect::<Result<Vec<usize>, AgentError>>()?;

        let arr = value
            .get_array("rows")
            .ok_or_else(|| AgentError::InvalidValue("Missing 'rows' field".to_string()))?
            .iter()
            .map(|row| {
                let row_array = row
                    .as_array()
                    .ok_or_else(|| AgentError::InvalidValue("Row is not an array".to_string()))?;
                let selected_cells: im::Vector<AgentValue> = col_indices
                    .iter()
                    .map(|&i| {
                        row_array
                            .get(i)
                            .cloned()
                            .unwrap_or_else(|| AgentValue::unit())
                    })
                    .collect();
                Ok(AgentValue::array(selected_cells))
            })
            .collect::<Result<im::Vector<AgentValue>, AgentError>>()?;

        if arr.len() == 1 {
            self.output(ctx, PORT_ARRAY, arr[0].clone()).await
        } else {
            self.output(ctx, PORT_ARRAY, AgentValue::array(arr)).await
        }
    }
}
