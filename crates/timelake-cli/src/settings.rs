//! `timelake settings`: the settings registry, and a table's values.

use serde_json::{Value, json};
use timelake::Table;
use timelake::settings::{DefaultValue, SETTINGS, Setting};

/// One setting as JSON (what `--json` prints and the website's settings page renders), with the
/// table's value if it has one.
fn setting_json(s: &Setting, value: Option<&str>) -> Value {
    let default = match s.default {
        DefaultValue::Value(v) => json!({ "value": v }),
        DefaultValue::Unset(v) => json!({ "unset": v }),
        DefaultValue::Derived(v) => json!({ "derived": v }),
    };
    let mut out = json!({
        "name": s.name,
        "property": s.property(),
        "sql": s.sql_name(),
        "kind": s.kind.to_string(),
        "default": default,
        "levels": s.levels.iter().map(|l| l.as_str()).collect::<Vec<_>>(),
        "since": s.since,
        "description": s.description,
    });
    if let Some(v) = value {
        out["value"] = json!(v);
    }
    out
}

/// Prints every setting with its default and levels or, for a table, the value it has.
pub(crate) fn print(table: Option<&Table>, as_json: bool) {
    let props = table.map(|t| t.metadata().properties());
    let value = |s: &Setting| props.and_then(|p| p.get(&s.property())).map(String::as_str);
    if as_json {
        let all: Vec<Value> = SETTINGS.iter().map(|s| setting_json(s, value(s))).collect();
        println!("{}", serde_json::to_string_pretty(&Value::Array(all)).unwrap_or_default());
        return;
    }
    let header = if table.is_some() { "VALUE" } else { "DEFAULT" };
    let rows: Vec<[String; 4]> = SETTINGS
        .iter()
        .map(|s| {
            let shown = value(s).map_or_else(|| default_text(s, table.is_some()), str::to_string);
            let levels = if s.levels.is_empty() {
                "managed".to_string()
            } else {
                s.levels.iter().map(|l| l.as_str()).collect::<Vec<_>>().join(",")
            };
            [s.name.to_string(), shown, levels, first_sentence(s.description).to_string()]
        })
        .collect();
    let width = |i: usize| rows.iter().map(|r| r[i].chars().count()).max().unwrap_or(0);
    let (w0, w1, w2) = (width(0).max(7), width(1).max(header.len()), width(2).max(6));
    println!("{:w0$}  {header:w1$}  {:w2$}  DESCRIPTION", "SETTING", "LEVELS");
    for [name, shown, levels, description] in rows {
        println!("{name:w0$}  {shown:w1$}  {levels:w2$}  {description}");
    }
}

/// A setting's default; for a table, marked as the default it falls back to.
fn default_text(s: &Setting, for_table: bool) -> String {
    if for_table { format!("({})", s.default) } else { s.default.to_string() }
}

/// Up to the first full stop followed by a capital letter (so `e.g. 90d` doesn't end it).
fn first_sentence(text: &str) -> &str {
    text.match_indices(". ")
        .find(|(i, _)| text[i + 2..].starts_with(|c: char| c.is_uppercase()))
        .map_or(text, |(i, _)| &text[..=i])
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The website's copy of the registry is current. To update it:
    /// `cargo run -p timelake-cli -- settings --json > dashboard/lib/settings.json && bun run format`.
    #[test]
    fn the_website_lists_every_setting() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../dashboard/lib/settings.json");
        let site: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        assert!(
            site == Value::Array(SETTINGS.iter().map(|s| setting_json(s, None)).collect()),
            "dashboard/lib/settings.json is out of date: \
             cargo run -p timelake-cli -- settings --json > dashboard/lib/settings.json && bun run format"
        );
    }

    #[test]
    fn descriptions_shorten_to_their_first_sentence() {
        assert_eq!(first_sentence("One. Two."), "One.");
        assert_eq!(first_sentence("Only one, e.g. 90d."), "Only one, e.g. 90d.");
    }
}
