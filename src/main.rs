mod db;

use std::collections::HashMap;
use std::env;
use std::path::{Path, PathBuf};
use dotenvy::dotenv;
use sqlx::SqlitePool;
use serenity::async_trait;
use serenity::builder::{CreateAttachment, CreateEmbed, CreateEmbedFooter, CreateMessage};
use serenity::model::channel::Message;
use serenity::model::gateway::Ready;
use serenity::model::Timestamp;
use serenity::prelude::*;

struct Handler {
    pool: SqlitePool,
    emoji_map: HashMap<String, String>,
}

const MULTI_RESULTS_LIMIT: usize = 10;
const MULTI_RESULTS_QUERY_LIMIT: i64 = 11;
const MIN_PARTIAL_QUERY_ALNUM_LEN: usize = 4;

#[async_trait]
impl EventHandler for Handler {
    async fn message(&self, ctx: Context, msg: Message) {
        let Some(raw_query) = msg.content.strip_prefix("!ethan") else {
            return;
        };

        let card_name = raw_query.trim();
        if card_name.is_empty() {
            let ethan_emoji = emoji_tag("ethan_allfire", &self.emoji_map);
            let embed = CreateEmbed::new()
                .colour(0xe5b61b)
                .description(format!("Hello, I am Ethan! {ethan_emoji}\nTry: !ethan <card name>"))
                .timestamp(Timestamp::now());
            let builder = CreateMessage::new().embed(embed);
            let _ = msg.channel_id.send_message(&ctx.http, builder).await;
            return;
        }

        match db::find_card_by_name(&self.pool, card_name).await {
            Ok(Some(card)) => {
                send_card_embed(&ctx, &msg, card, &self.emoji_map).await;
            }
            Ok(None) => {
                let query_alnum_len = card_name.chars().filter(|c| c.is_alphanumeric()).count();
                if query_alnum_len < MIN_PARTIAL_QUERY_ALNUM_LEN {
                    let embed = CreateEmbed::new()
                        .colour(0xe63a24)
                        .title("Search Too Broad")
                        .description(format!(
                            "Search `{card_name}` is too broad. Please type at least 4 letters or a more specific card name."
                        ))
                        .timestamp(Timestamp::now());
                    let builder = CreateMessage::new().embed(embed);
                    let _ = msg.channel_id.send_message(&ctx.http, builder).await;
                    return;
                }

                match db::search_cards_by_partial_name(
                    &self.pool,
                    card_name,
                    MULTI_RESULTS_QUERY_LIMIT,
                )
                .await
                {
                    Ok(cards) if cards.is_empty() => {
                        let embed = CreateEmbed::new()
                            .colour(0xe63a24)
                            .title("Card Not Found")
                            .description(format!("Card '{card_name}' not found in the database."))
                            .timestamp(Timestamp::now());
                        let builder = CreateMessage::new().embed(embed);
                        let _ = msg.channel_id.send_message(&ctx.http, builder).await;
                    }
                    Ok(cards) => {
                        let truncated = cards.len() > MULTI_RESULTS_LIMIT;
                        let visible_count = cards.len().min(MULTI_RESULTS_LIMIT);
                        let listed = cards.into_iter().take(MULTI_RESULTS_LIMIT);
                        let has_single = visible_count == 1;

                        let mut embed = CreateEmbed::new()
                            .colour(0xe5b61b)
                            .title(if has_single {
                                "Possible Match"
                            } else {
                                "Multiple Results"
                            })
                            .description(format!(
                                "Found similar cards for `{card_name}`. Please type the full card name:"
                            ))
                            .timestamp(Timestamp::now());

                        for (index, card) in listed.enumerate() {
                            embed = embed.field(format!("{}.", index + 1), card.name, false);
                        }

                        if truncated {
                            embed = embed.field(
                                "Tip",
                                format!(
                                    "Showing first {MULTI_RESULTS_LIMIT} results. Add more words (for example, set/version) to narrow it down."
                                ),
                                false,
                            );
                        }

                        let builder = CreateMessage::new().embed(embed);
                        let _ = msg.channel_id.send_message(&ctx.http, builder).await;
                    }
                    Err(why) => {
                        println!("DB read error: {why:?}");
                        let embed = CreateEmbed::new()
                            .colour(0xe63a24)
                            .title("Database Error")
                            .description("Error reading from the database.")
                            .timestamp(Timestamp::now());
                        let builder = CreateMessage::new().embed(embed);
                        let _ = msg.channel_id.send_message(&ctx.http, builder).await;
                    }
                }
            }
            Err(why) => {
                println!("DB read error: {why:?}");
                let embed = CreateEmbed::new()
                    .colour(0xe63a24)
                    .title("Database Error")
                    .description("Error reading from the database.")
                    .timestamp(Timestamp::now());
                let builder = CreateMessage::new().embed(embed);
                let _ = msg.channel_id.send_message(&ctx.http, builder).await;
            }
        }
    }

    async fn ready(&self, _: Context, ready: Ready) {
        println!("{} is connected!", ready.user.name);
    }
}

async fn send_card_embed(
    ctx: &Context,
    msg: &Message,
    card: db::StoredCard,
    emoji_map: &HashMap<String, String>,
) {
    let mut embed = CreateEmbed::new()
        .colour(0xe5b61b)
        .title(card.name.clone())
        .timestamp(Timestamp::now());

    if let Some(value) = card.cost {
        embed = embed.field("Cost", value.to_string(), true);
    }
    if let Some(value) = card.card_type.filter(|v| !v.trim().is_empty()) {
        embed = embed.field("Card Type", value, true);
    }
    if let Some(value) = card.landscape.filter(|v| !v.trim().is_empty()) {
        let value = if let Some(emoji_name) = landscape_emoji_name(&value) {
            format!("{} {value}", emoji_tag(emoji_name, emoji_map))
        } else {
            value
        };
        embed = embed.field("Landscape", value, true);
    }
    if let Some(value) = card.ability.filter(|v| !v.trim().is_empty()) {
        let value = expand_custom_emojis(&value, emoji_map);
        embed = embed.field("Ability", value, false);
    }
    if let Some(value) = card.card_set.filter(|v| !v.trim().is_empty()) {
        embed = embed.field("Set", value, true);
    }
    if let Some(value) = card.attack {
        embed = embed.field("Attack", value.to_string(), true);
    }
    if let Some(value) = card.defense {
        embed = embed.field("Defense", value.to_string(), true);
    }

    let footer = CreateEmbedFooter::new(format!("Card ID: {}", card.id));
    embed = embed.footer(footer);

    let mut builder = CreateMessage::new();

    if let Some(image_path) = card.image_path.filter(|v| !v.trim().is_empty()) {
        let image_path = if Path::new(&image_path).is_absolute()
            || image_path.contains('/')
            || image_path.contains('\\')
        {
            image_path
        } else {
            project_root()
                .join("assets")
                .join("cards")
                .join(image_path)
                .to_string_lossy()
                .into_owned()
        };

        let file_name = Path::new(&image_path)
            .file_name()
            .and_then(|v| v.to_str())
            .unwrap_or("ethan_allfire.jpg");

        if let Ok(attachment) = CreateAttachment::path(&image_path).await {
            embed = embed.image(format!("attachment://{file_name}"));
            builder = builder.add_file(attachment);
        }
    }

    builder = builder.embed(embed);

    if let Err(why) = msg.channel_id.send_message(&ctx.http, builder).await {
        println!("Error sending message: {why:?}");
    }
}

#[tokio::main]
async fn main() {
    dotenv().ok();

    let project_root = project_root();
    let db_path = project_root.join("ethan.db");
    let csv_path = project_root.join("cards_template.csv");

    let pool = db::connect_and_init(db_path.to_string_lossy().as_ref())
        .await
        .expect("Failed to initialize SQLite database");

    let cards_count = db::count_cards(&pool).await.unwrap_or(0);
    if cards_count <= 1 {
        match db::import_cards_from_csv(&pool, csv_path.to_string_lossy().as_ref()).await {
            Ok(inserted) => println!("Imported {inserted} cards from {}", csv_path.display()),
            Err(why) => println!("CSV import skipped: {why}"),
        }
    }

    // Configure the client with your Discord bot token in the environment.
    let token = env::var("DISCORD_TOKEN").expect("Expected a token in the environment");
    let emoji_map = load_emoji_map();
    let intents = GatewayIntents::GUILD_MESSAGES
        | GatewayIntents::DIRECT_MESSAGES
        | GatewayIntents::MESSAGE_CONTENT;
    let handler = Handler { pool, emoji_map };
    let mut client =
        Client::builder(&token, intents).event_handler(handler).await.expect("Err creating client");

    if let Err(why) = client.start().await {
        println!("Client error: {why:?}");
    }
}

fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

// Make sure to upload custom emojis to your Discord bot and give permission to send custom emojis on server
fn load_emoji_map() -> HashMap<String, String> {
    let mut map = HashMap::new();

    if let Ok(raw) = env::var("DISCORD_EMOJI_MAP") {
        // Format: name=id,name2=id2...
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
        if key.is_empty() {
            continue;
        }

        if map.contains_key(&key) {
            continue;
        }

        if let Some(id) = parse_emoji_id(&value) {
            map.insert(key, id);
        }
    }

    map
}

fn expand_custom_emojis(input: &str, emoji_map: &HashMap<String, String>) -> String {
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

fn emoji_tag(name: &str, emoji_map: &HashMap<String, String>) -> String {
    let normalized_name = normalize_emoji_name(name);
    if let Some(id) = emoji_map.get(&normalized_name) {
        format!("<:{normalized_name}:{id}>")
    } else {
        format!(":{normalized_name}:")
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
    if head != "" && head != "a" {
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

fn landscape_emoji_name(landscape: &str) -> Option<&'static str> {
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