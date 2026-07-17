use std::collections::HashMap;
use std::env;

pub fn load_emoji_map() -> HashMap<String, String> {
    let mut map = HashMap::new();

    if let Ok(raw) = env::var("DISCORD_EMOJI_MAP") {
        for item in raw.split(',').map(str::trim).filter(|v| !v.is_empty()) {
            let item = item.trim_start_matches('.').trim();
            if item.is_empty() {
                continue;
            }

            let Some((name, id_raw)) = item.split_once('=') else {
                if let Some((name, id)) = parse_emoji_tag(item) {
                    let normalized_name = normalize_emoji_name(&name);
                    if !normalized_name.is_empty() {
                        map.insert(normalized_name, id);
                    }
                }
                continue;
            };

            let name = normalize_emoji_name(name);
            if name.is_empty() {
                continue;
            }

            if let Some(id) = parse_emoji_id(id_raw) {
                map.insert(name, id);
            }
        }
    }

    for (key, value) in env::vars() {
        let key = normalize_emoji_name(&key);
        if key.is_empty() || map.contains_key(&key) {
            continue;
        }

        if let Some(id) = parse_emoji_id(&value) {
            map.insert(key, id);
        }
    }

    map
}

pub fn expand_custom_emojis(input: &str, emoji_map: &HashMap<String, String>) -> String {
    if emoji_map.is_empty() {
        return input.to_string();
    }

    let mut output = String::with_capacity(input.len());
    let mut i = 0;

    while i < input.len() {
        let tail = &input[i..];
        let Some(start_rel) = tail.find(':') else {
            output.push_str(tail);
            break;
        };

        let start = i + start_rel;
        output.push_str(&input[i..start]);

        let name_start = start + 1;
        let Some(end_rel) = input[name_start..].find(':') else {
            output.push(':');
            i = name_start;
            continue;
        };
        let end = name_start + end_rel;
        let name = &input[name_start..end];

        if !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            let normalized_name = normalize_emoji_name(name);
            if let Some(id) = emoji_map.get(&normalized_name) {
                output.push_str(&format!("<:{normalized_name}:{id}>"));
            } else {
                output.push(':');
                output.push_str(name);
                output.push(':');
            }
            i = end + 1;
        } else {
            output.push(':');
            i = name_start;
        }
    }

    output
}

pub fn emoji_tag(name: &str, emoji_map: &HashMap<String, String>) -> String {
    let normalized_name = normalize_emoji_name(name);
    if let Some(id) = emoji_map.get(&normalized_name) {
        format!("<:{normalized_name}:{id}>")
    } else {
        format!(":{normalized_name}:")
    }
}

pub fn landscape_emoji_name(landscape: &str) -> Option<&'static str> {
    let key = landscape
        .trim()
        .to_ascii_lowercase()
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect::<String>();

    match key.as_str() {
        "rainbow" => Some("rainbow"),
        "lavaflats" => Some("lavaflats"),
        "nicelands" => Some("nicelands"),
        "icylands" => Some("icylands"),
        "uselessswamp" => Some("useless_swamp"),
        "sandylands" => Some("sandylands"),
        "cornfield" => Some("cornfield"),
        "blueplains" => Some("blue_plains"),
        _ => None,
    }
}

fn normalize_emoji_name(name: &str) -> String {
    let normalized = name.trim().to_ascii_lowercase();
    if normalized.is_empty() || !normalized.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return String::new();
    }

    normalized
}

fn parse_emoji_id(raw: &str) -> Option<String> {
    let value = raw.trim().trim_matches('"');
    if value.is_empty() {
        return None;
    }

    if value.chars().all(|c| c.is_ascii_digit()) {
        return Some(value.to_string());
    }

    if let Some((_, id)) = parse_emoji_tag(value) {
        return Some(id);
    }

    None
}

fn parse_emoji_tag(raw: &str) -> Option<(String, String)> {
    let value = raw.trim().trim_matches('"');
    if !(value.starts_with('<') && value.ends_with('>')) {
        return None;
    }

    let inner = &value[1..value.len() - 1];
    let mut parts = inner.split(':');
    let head = parts.next()?;
    if !head.is_empty() && head != "a" {
        return None;
    }

    let name = parts.next()?.trim();
    let id = parts.next()?.trim();
    if parts.next().is_some() {
        return None;
    }

    if name.is_empty() || !id.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }

    Some((name.to_string(), id.to_string()))
}