//! One compact view of the session's capabilities, shared by every way a model
//! finds them: the budgeted list in the system prompt, `borg tools --search`,
//! `borg.tools(...)` from code, and the hint a malformed call returns.

use serde_json::{Value, json};

/// `name(field: type, optional?: type) — first sentence of the description`.
pub(crate) fn signature(spec: &Value) -> String {
    render(spec, false)
}

/// The prompt's form: at most four optional fields, long enums as `str`, and
/// no description, so every capability fits; search gives the full form.
fn brief_signature(spec: &Value) -> String {
    render(spec, true)
}

const BRIEF_OPTIONAL_FIELDS: usize = 4;
const BRIEF_ENUM_VALUES: usize = 4;

fn render(spec: &Value, brief: bool) -> String {
    let name = spec.get("name").and_then(Value::as_str).unwrap_or("?");
    let schema = spec.get("inputSchema").or_else(|| spec.get("input_schema"));
    let required: Vec<&str> = schema
        .and_then(|schema| schema.get("required"))
        .and_then(Value::as_array)
        .map(|fields| fields.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    let mut fields: Vec<(&String, &Value)> = schema
        .and_then(|schema| schema.get("properties"))
        .and_then(Value::as_object)
        .map(|properties| {
            properties
                .iter()
                .filter(|(field, _)| field.as_str() != "action")
                .collect()
        })
        .unwrap_or_default();
    // Required fields first, in the order the schema requires them.
    fields.sort_by_key(|(field, _)| {
        required
            .iter()
            .position(|name| name == field)
            .unwrap_or(usize::MAX)
    });
    let mut rendered = Vec::new();
    let mut optional_shown = 0;
    for (field, property) in fields {
        let optional = !required.contains(&field.as_str());
        if brief && optional {
            if optional_shown == BRIEF_OPTIONAL_FIELDS {
                rendered.push("…".to_string());
                break;
            }
            optional_shown += 1;
        }
        let long_enum = property
            .get("enum")
            .and_then(Value::as_array)
            .is_some_and(|values| values.len() > BRIEF_ENUM_VALUES);
        let kind = if brief && long_enum {
            "str".to_string()
        } else {
            type_name(property)
        };
        rendered.push(format!(
            "{field}{}: {kind}",
            if optional { "?" } else { "" }
        ));
    }
    let fields = rendered.join(", ");
    let summary = first_sentence(
        spec.get("description")
            .and_then(Value::as_str)
            .unwrap_or(""),
    );
    if brief || summary.is_empty() {
        format!("{name}({fields})")
    } else {
        format!("{name}({fields}) — {summary}")
    }
}

fn type_name(property: &Value) -> String {
    if let Some(values) = property.get("enum").and_then(Value::as_array) {
        return values
            .iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join("|");
    }
    match property.get("type") {
        Some(Value::String(kind)) if kind == "array" => format!(
            "{}[]",
            property
                .get("items")
                .map_or_else(|| "any".to_string(), type_name)
        ),
        Some(Value::String(kind)) if kind == "integer" => "int".to_string(),
        Some(Value::String(kind)) if kind == "boolean" => "bool".to_string(),
        Some(Value::String(kind)) if kind == "string" => "str".to_string(),
        Some(Value::String(kind)) => kind.clone(),
        Some(Value::Array(kinds)) => kinds
            .iter()
            .filter_map(Value::as_str)
            .filter(|kind| *kind != "null")
            .collect::<Vec<_>>()
            .join("|"),
        _ => "any".to_string(),
    }
}

fn first_sentence(text: &str) -> &str {
    let text = text.trim();
    let end = text
        .char_indices()
        .find(|(index, character)| *character == '.' && text[index + 1..].starts_with([' ', '\n']))
        .map_or(text.len(), |(index, _)| index + 1);
    &text[..end.min(200)]
}

/// Capabilities ranked for `query`: name matches first, then description and
/// field names. An empty query lists everything by name.
pub(crate) fn search(specs: &[Value], query: &str, limit: usize) -> Vec<Value> {
    ranked(specs, query)
        .into_iter()
        .take(limit)
        .map(|(_, name, spec)| json!({ "name": name, "signature": signature(spec) }))
        .collect()
}

/// A name-segment match scores this much; weaker matches come only from the
/// description or field names.
const NAME_MATCH: u32 = 8;

fn ranked<'a>(specs: &'a [Value], query: &str) -> Vec<(u32, &'a str, &'a Value)> {
    let terms: Vec<String> = query
        .split(|character: char| !character.is_alphanumeric())
        .filter(|term| !term.is_empty())
        .map(str::to_ascii_lowercase)
        .collect();
    let mut ranked: Vec<(u32, &str, &Value)> = specs
        .iter()
        .filter_map(|spec| {
            let name = spec.get("name")?.as_str()?;
            let score = terms.iter().map(|term| score(spec, name, term)).sum();
            (terms.is_empty() || score > 0).then_some((score, name, spec))
        })
        .collect();
    ranked.sort_by(|left, right| right.0.cmp(&left.0).then(left.1.cmp(right.1)));
    ranked
}

fn score(spec: &Value, name: &str, term: &str) -> u32 {
    let singular = term.strip_suffix('s').filter(|stem| stem.len() > 2);
    let matches =
        |text: &str| text.contains(term) || singular.is_some_and(|stem| text.contains(stem));
    let name = name.to_ascii_lowercase();
    let description = spec
        .get("description")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_ascii_lowercase();
    let fields = spec
        .pointer("/inputSchema/properties")
        .and_then(Value::as_object)
        .map(|properties| properties.keys().cloned().collect::<Vec<_>>().join(" "))
        .unwrap_or_default();
    let mut score = 0;
    if name == term || name.split('_').any(|segment| segment == term) {
        score += 20;
    } else if matches(&name) {
        score += NAME_MATCH;
    }
    if matches(&description) {
        score += 4;
    }
    if matches(&fields) {
        score += 2;
    }
    score
}

/// Signatures for the system prompt, as many as fit `budget` characters, and
/// a line saying whether that is every capability or how to find the rest.
/// The shortest signatures are taken first, so a few very large capabilities
/// cost a search instead of crowding out many small ones.
pub(crate) fn prompt_listing(specs: &[Value], budget: usize) -> String {
    let mut signatures: Vec<String> = specs.iter().map(brief_signature).collect();
    signatures.sort_by_key(String::len);
    let mut listed = Vec::new();
    let mut used = 0;
    for signature in &signatures {
        if used + signature.len() > budget {
            break;
        }
        used += signature.len() + 1;
        listed.push(signature.as_str());
    }
    listed.sort_unstable();
    let coverage = if listed.len() == signatures.len() {
        format!(
            "All {} Borg capabilities (descriptions and full fields: `borg tools --search QUERY` or `borg.tools(\"query\")`)",
            signatures.len()
        )
    } else {
        format!(
            "{} of {} Borg capabilities (find the rest with `borg tools --search QUERY` or `borg.tools(\"query\")`)",
            listed.len(),
            signatures.len()
        )
    };
    format!("{coverage}:\n{}", listed.join("\n"))
}

/// A malformed call's error with the shape the capability expects, so the
/// next attempt can be right.
pub(crate) fn corrective_error(specs: &[Value], name: &str, error: &str) -> String {
    if let Some(spec) = specs
        .iter()
        .find(|spec| spec.get("name").and_then(Value::as_str) == Some(name))
    {
        return format!("{error}\nExpected: {}", signature(spec));
    }
    // Only names that share a word with the one asked for are likely meant.
    let suggestions: Vec<String> = ranked(specs, name)
        .into_iter()
        .filter(|(score, _, _)| *score >= NAME_MATCH)
        .take(3)
        .map(|(_, _, spec)| signature(spec))
        .collect();
    if suggestions.is_empty() {
        format!("{error}\nList capabilities with `borg tools --search QUERY`.")
    } else {
        format!("{error}\nDid you mean:\n{}", suggestions.join("\n"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn specs() -> Vec<Value> {
        vec![
            json!({
                "name": "send_message",
                "description": "Send a message to another agent. Long detail.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "action": {"type": "string"},
                        "target": {"type": "string"},
                        "message": {"type": "string"},
                        "wake": {"type": "boolean"}
                    },
                    "required": ["target", "message"]
                }
            }),
            json!({"name": "get_plan", "description": "Read the plan.", "inputSchema": {"type": "object"}}),
        ]
    }

    #[test]
    fn signatures_name_required_fields_first_and_mark_optional_ones() {
        assert_eq!(
            signature(&specs()[0]),
            "send_message(target: str, message: str, wake?: bool) — Send a message to another agent."
        );
    }

    #[test]
    fn search_ranks_a_name_match_above_a_description_match() {
        let found = search(&specs(), "message", 10);
        assert_eq!(found[0]["name"], "send_message");
        assert_eq!(search(&specs(), "plans", 10)[0]["name"], "get_plan");
    }

    #[test]
    fn a_malformed_call_is_told_the_expected_shape_or_the_likely_name() {
        let wrong_field = corrective_error(&specs(), "send_message", "missing field `target`");
        assert!(wrong_field.ends_with("Expected: send_message(target: str, message: str, wake?: bool) — Send a message to another agent."));
        let wrong_name = corrective_error(&specs(), "send_messages", "unknown tool");
        assert!(wrong_name.contains("Did you mean:\nsend_message("));
    }
}
