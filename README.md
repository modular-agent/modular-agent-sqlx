# SQLx Agents for Modular Agent Kit

SQLx-based database agents for Modular Agent Kit. Supports SQLite, MySQL, and PostgreSQL.

## Database Connection

The `db` configuration specifies the database connection. The format depends on the database type:

| Format                                  | Database              |
|-----------------------------------------|-----------------------|
| (empty)                                 | SQLite in-memory      |
| `path/to/db.sqlite`                     | SQLite file (default) |
| `sqlite:path/to/db.sqlite`              | SQLite file           |
| `mysql:user:password@host/database`     | MySQL                 |
| `postgres:user:password@host/database`  | PostgreSQL            |

When no prefix is specified, the path is treated as a SQLite file path.

### Examples

```yaml
# SQLite in-memory (default)
db: ""

# SQLite file
db: "data.db"
db: "sqlite:data.db"

# MySQL
db: "mysql:root:password@localhost/mydb"
db: "mysql://root:password@localhost/mydb"

# PostgreSQL
db: "postgres:user:password@localhost/mydb"
db: "postgres://user:password@localhost/mydb"
```

## Agents

### SQLx Script

Executes SQL statements against the database.

- **Inputs**: `value` - Parameters for the SQL statement
- **Outputs**: `table` - Query results with `headers` and `rows`
- **Config**:
  - `db` - Database connection string
  - `script` - SQL statement to execute

### Rows

Extracts the `rows` array from a table result.

- **Inputs**: `table`
- **Outputs**: `array`

### Row

Extracts a single row by index from a table result.

- **Inputs**: `table`
- **Outputs**: `array`
- **Config**:
  - `index` - Row index (0-based)

### Select

Selects specific columns from a table result.

- **Inputs**: `table`
- **Outputs**: `array`
- **Config**:
  - `cols` - Comma-separated column names
