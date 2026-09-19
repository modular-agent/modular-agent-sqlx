use modular_agent_core::{
    AsModule, ModularAgent, ModuleContext, ModuleData, ModuleOutput, ModuleSpec, Result, Value,
    async_trait, modular_agent,
};

static CATEGORY: &str = "DB/SQLx";

static PORT_TABLE: &str = "table";

static CONFIG_TABLE: &str = "table";

// SQLx Display Table
#[modular_agent(
    kind = "Display",
    title = "Display Table",
    category = CATEGORY,
    inputs = [PORT_TABLE],
    custom_config(
        name = CONFIG_TABLE,
        readonly,
        type_="html",
        default="",
        hide_title,
    ),
)]
struct DisplayTableModule {
    data: ModuleData,
}

#[async_trait]
impl AsModule for DisplayTableModule {
    fn new(ma: ModularAgent, id: String, spec: ModuleSpec) -> Result<Self> {
        Ok(Self {
            data: ModuleData::new(ma, id, spec),
        })
    }

    async fn process(&mut self, _ctx: ModuleContext, _port: String, value: Value) -> Result<()> {
        let headers = value.get_array("headers");
        let rows = value.get_array("rows");

        let table_html = generate_html_table(headers, rows);

        self.emit_config_updated(CONFIG_TABLE, Value::string(table_html));
        Ok(())
    }
}

fn escape_html(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    for ch in text.chars() {
        match ch {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&#39;"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

fn cozo_cell_to_text(value: &Value) -> String {
    match value {
        Value::Unit => "null".to_string(),
        Value::Boolean(b) => b.to_string(),
        Value::Integer(i) => i.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.to_string(),
        Value::Array(arr) => {
            let rendered: Vec<String> = arr.iter().map(cozo_cell_to_text).collect();
            format!("[{}]", rendered.join(", "))
        }
        Value::Object(_) => serde_json::to_string(&value.to_json()).unwrap_or_default(),
        Value::Tensor(t) => {
            // show only the first and last several elements of the tensor, if large.
            let size = t.len();
            let elements_to_show = 5;
            let mut rendered: Vec<String> = Vec::new();
            if size <= 2 * elements_to_show {
                for v in t.iter() {
                    rendered.push(v.to_string());
                }
                format!("[{}]", rendered.join(", "))
            } else {
                for v in t.iter().take(elements_to_show) {
                    rendered.push(v.to_string());
                }
                rendered.push("...".to_string());
                for v in t
                    .iter()
                    .rev()
                    .take(elements_to_show)
                    .collect::<Vec<_>>()
                    .iter()
                    .rev()
                {
                    rendered.push(v.to_string());
                }
                format!("[{}, size = {}]", rendered.join(", "), size)
            }
        }
        _ => serde_json::to_string(&value.to_json()).unwrap_or_default(),
    }
}

fn generate_html_table(
    headers: Option<&im::Vector<Value>>,
    rows: Option<&im::Vector<Value>>,
) -> String {
    let mut html = String::new();
    html.push_str("<table border=\"1\" style=\"border-collapse:collapse;\">\n");
    if let Some(headers) = headers {
        html.push_str("<thead>\n<tr>\n");
        for header in headers.iter() {
            let header_text = escape_html(header.as_str().unwrap_or_default());
            html.push_str(&format!("<th>{}</th>\n", header_text));
        }
        html.push_str("</tr>\n</thead>\n");
    }
    if let Some(rows) = rows {
        html.push_str("<tbody>\n");
        for row in rows.iter() {
            html.push_str("<tr>\n");
            if let Some(cells) = row.as_array() {
                for cell in cells.iter() {
                    let cell_text = escape_html(&cozo_cell_to_text(cell));
                    html.push_str(&format!(
                        "<td><pre style=\"margin:0;white-space:pre-wrap;\">{}</pre></td>\n",
                        cell_text
                    ));
                }
            }
            html.push_str("</tr>\n");
        }
        html.push_str("</tbody>\n");
    }
    html.push_str("</table>\n");
    html
}
