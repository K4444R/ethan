use std::collections::HashMap;
use std::path::Path;

use crate::{db, emoji, project_root};
use serenity::all::ReactionType;
use serenity::async_trait;
use serenity::builder::{CreateAttachment, CreateEmbed, CreateEmbedFooter, CreateMessage};
use serenity::model::channel::Channel;
use serenity::model::channel::Message;
use serenity::model::gateway::Ready;
use serenity::model::Timestamp;
use serenity::prelude::*;
use sqlx::SqlitePool;
use log::{info, warn, error};

pub struct Handler {
    pub pool: SqlitePool,
    pub emoji_map: HashMap<String, String>,
}

const MULTI_RESULTS_LIMIT: usize = 10;
const MULTI_RESULTS_QUERY_LIMIT: i64 = 11;
const MIN_PARTIAL_QUERY_ALNUM_LEN: usize = 3;

#[async_trait]
impl EventHandler for Handler {
    async fn message(&self, ctx: Context, msg: Message) {
        let Some(raw_query) = msg.content.strip_prefix("!ethan") else {
            return;
        };

        let guild_label = msg
            .guild_id
            .map(|guild_id| guild_id.to_string())
            .unwrap_or_else(|| "DM".to_string());
        let channel_label = describe_channel(&ctx, msg.channel_id).await;
        info!(
            "Request received: user={} ({}) channel={} ({}) guild={} message={} ({})",
            msg.author.name,
            msg.author.id,
            channel_label,
            msg.channel_id,
            guild_label,
            msg.link(),
            msg.id
        );
        
        let ethan_emoji = emoji::emoji_tag("ethan_allfire", &self.emoji_map);
        if let Ok(ethan_reaction) = ReactionType::try_from(ethan_emoji) {
            let _ = msg.react(&ctx.http, ethan_reaction).await;
        }

        let (use_image_response, card_name) = parse_query(raw_query);
        info!(
            "Request parsed: mode={} query='{}'",
            if use_image_response { "image" } else { "embed" },
            card_name
        );
        if card_name.is_empty() {
            info!("Intro request from user={} channel={} ({})", msg.author.id, channel_label, msg.channel_id);
            send_intro_message(&ctx, &msg, &self.emoji_map).await;
            return;
        }

        info!("Exact card lookup started for query='{}'", card_name);
        match db::find_card_by_name(&self.pool, card_name).await {
            Ok(Some(card)) => {
                info!("Exact card lookup succeeded: card='{}'", card.name);
                if use_image_response {
                    info!("Sending image response for card='{}'", card.name);
                    send_card_image(&ctx, &msg, &self.pool, card).await;
                } else {
                    info!("Sending embed response for card='{}'", card.name);
                    send_card_embed(&ctx, &msg, &self.pool, card, &self.emoji_map).await;
                }
            }
            Ok(None) => {
                info!("Exact card lookup returned no result for query='{}'", card_name);
                let query_alnum_len = card_name.chars().filter(|c| c.is_alphanumeric()).count();
                if query_alnum_len < MIN_PARTIAL_QUERY_ALNUM_LEN {
                    warn!("Search too broad for query='{}'", card_name);
                    send_status_embed(
                        &ctx,
                        &msg,
                        0xe63a24,
                        "Search Too Broad",
                        format!(
                            "Search `{card_name}` is too broad. Please type at least 4 letters or a more specific card name."
                        ),
                    )
                    .await;
                    return;
                }

                info!("Partial card search started for query='{}'", card_name);
                match db::search_cards_by_partial_name(&self.pool, card_name, MULTI_RESULTS_QUERY_LIMIT).await {
                    Ok(cards) if cards.is_empty() => {
                        info!("Partial search returned no matches for query='{}'", card_name);
                        send_status_embed(
                            &ctx,
                            &msg,
                            0xe63a24,
                            "Card Not Found",
                            format!("Card '{card_name}' not found in the database."),
                        )
                        .await;
                    }
                    Ok(cards) => {
                        info!("Partial search returned {} match(es) for query='{}'", cards.len(), card_name);
                        send_multi_result_embed(&ctx, &msg, card_name, cards).await;
                    }
                    Err(why) => {
                        error!(
                            "Partial search DB error for user={} channel={} query='{}': {why:?}",
                            msg.author.id,
                            msg.channel_id,
                            card_name
                        );
                        send_status_embed(
                            &ctx,
                            &msg,
                            0xe63a24,
                            "Database Error",
                            "Error reading from the database.",
                        )
                        .await;
                    }
                }
            }
            Err(why) => {
                error!(
                    "Exact search DB error for user={} channel={} query='{}': {why:?}",
                    msg.author.id,
                    msg.channel_id,
                    card_name
                );
                send_status_embed(
                    &ctx,
                    &msg,
                    0xe63a24,
                    "Database Error",
                    "Error reading from the database.",
                )
                .await;
            }
        }
    }

    async fn ready(&self, _: Context, ready: Ready) {
        info!("{} is connected!", ready.user.name);
    }
}

async fn describe_channel(ctx: &Context, channel_id: serenity::model::id::ChannelId) -> String {
    match channel_id.to_channel(&ctx.http).await {
        Ok(Channel::Guild(channel)) => format!("#{}", channel.name),
        Ok(Channel::Private(channel)) => channel.name(),
        Ok(channel) => channel.id().to_string(),
        Err(_) => channel_id.to_string(),
    }
}

async fn send_intro_message(ctx: &Context, msg: &Message, emoji_map: &HashMap<String, String>) {
    let ethan_emoji = emoji::emoji_tag("ethan_allfire", emoji_map);
    let embed = CreateEmbed::new()
        .colour(0xe5b61b)
        .description(format!("Hello, I am Ethan! {ethan_emoji}\nTry: ```!ethan <card name>```\nor: ```!ethan -img <card name>``` for card image"))
        .timestamp(Timestamp::now());
    let builder = CreateMessage::new().embed(embed);
    if let Err(why) = msg.channel_id.send_message(&ctx.http, builder).await {
        error!("Failed to send intro message to channel={}: {why:?}", msg.channel_id);
    }
}

async fn send_status_embed(
    ctx: &Context,
    msg: &Message,
    colour: u32,
    title: &str,
    description: impl Into<String>,
) {
    let embed = CreateEmbed::new()
        .colour(colour)
        .title(title)
        .description(description)
        .timestamp(Timestamp::now());
    let builder = CreateMessage::new().embed(embed);
    if let Err(why) = msg.channel_id.send_message(&ctx.http, builder).await {
        error!("Failed to send status embed to channel={}: {why:?}", msg.channel_id);
    }
}

async fn send_multi_result_embed(ctx: &Context, msg: &Message, card_name: &str, cards: Vec<db::StoredCard>) {
    let truncated = cards.len() > MULTI_RESULTS_LIMIT;
    let visible_count = cards.len().min(MULTI_RESULTS_LIMIT);
    let listed = cards.into_iter().take(MULTI_RESULTS_LIMIT);
    let has_single = visible_count == 1;

    let mut embed = CreateEmbed::new()
        .colour(0xe5b61b)
        .title(if has_single { "Possible Match" } else { "Multiple Results" })
        .description(format!("Found similar cards for `{card_name}`. Please type the full card name:"))
        .timestamp(Timestamp::now());

    for (index, card) in listed.enumerate() {
        embed = embed.field(format!("{}.", index + 1), card.name, false);
    }

    if truncated {
        embed = embed.field(
            "Tip",
            format!("Showing first {MULTI_RESULTS_LIMIT} results. Add more words to narrow it down."),
            false,
        );
    }

    let builder = CreateMessage::new().embed(embed);
    if let Err(why) = msg.channel_id.send_message(&ctx.http, builder).await {
        error!("Failed to send multi-result embed to channel={}: {why:?}", msg.channel_id);
    }
}

async fn send_card_embed(
    ctx: &Context,
    msg: &Message,
    pool: &SqlitePool,
    card: db::StoredCard,
    emoji_map: &HashMap<String, String>,
) {
    let embed = build_card_embed(&card, emoji_map);
    let builder = build_card_message_with_images(pool, &card, embed).await;

    if let Err(why) = msg.channel_id.send_message(&ctx.http, builder).await {
        error!("Failed to send card embed to channel={} card='{}': {why:?}", msg.channel_id, card.name);
    }
}

async fn send_card_image(
    ctx: &Context,
    msg: &Message,
    pool: &SqlitePool,
    card: db::StoredCard,
) {
    let builder = build_card_image_message(pool, &card).await;

    if let Err(why) = msg.channel_id.send_message(&ctx.http, builder).await {
        error!("Failed to send card image to channel={} card='{}': {why:?}", msg.channel_id, card.name);
    }
}

fn build_card_embed(card: &db::StoredCard, emoji_map: &HashMap<String, String>) -> CreateEmbed {
    let mut embed = CreateEmbed::new()
        .colour(0xe5b61b)
        .title(card.name.clone())
        .timestamp(Timestamp::now());

    if let Some(value) = card.cost {
        embed = embed.field("Cost", value.to_string(), true);
    }
    if let Some(value) = card.card_type.as_deref().filter(|v| !v.trim().is_empty()) {
        embed = embed.field("Card Type", value, true);
    }
    if let Some(value) = card.landscape.as_deref().filter(|v| !v.trim().is_empty()) {
        let value = if let Some(emoji_name) = emoji::landscape_emoji_name(value) {
            format!("{} {value}", emoji::emoji_tag(emoji_name, emoji_map))
        } else {
            value.to_string()
        };
        embed = embed.field("Landscape", value, true);
    }
    if let Some(value) = card.ability.as_deref().filter(|v| !v.trim().is_empty()) {
        let value = emoji::expand_custom_emojis(value, emoji_map);
        embed = embed.field("Ability", value, false);
    }
    if let Some(value) = card.card_set.as_deref().filter(|v| !v.trim().is_empty()) {
        embed = embed.field("Set", value, true);
    }
    if let Some(value) = card.attack {
        embed = embed.field("Attack", value.to_string(), true);
    }
    if let Some(value) = card.defense {
        embed = embed.field("Defense", value.to_string(), true);
    }

    let footer = CreateEmbedFooter::new(format!("Card ID: {}", card.id));
    embed.footer(footer)
}

async fn build_card_message_with_images(
    pool: &SqlitePool,
    card: &db::StoredCard,
    embed: CreateEmbed,
) -> CreateMessage {
    let mut builder = CreateMessage::new();
    let image_paths = collect_card_image_paths(pool, card).await;

    if image_paths.is_empty() {
        return builder.embed(embed);
    }

    let mut embed = embed;
    let mut sent_any_image = false;

    for (index, image_path) in image_paths.into_iter().enumerate() {
        let resolved_path = resolve_card_image_path(&image_path);
        let fallback_name = format!("card_{}_{}.jpg", card.id, index + 1);
        let file_name = Path::new(&resolved_path)
            .file_name()
            .and_then(|v| v.to_str())
            .unwrap_or(&fallback_name)
            .to_string();

        if let Ok(attachment) = CreateAttachment::path(&resolved_path).await {
            builder = builder.add_file(attachment);

            if !sent_any_image {
                embed = embed.image(format!("attachment://{file_name}"));
                builder = builder.embed(embed.clone());
                sent_any_image = true;
            } else {
                let mut extra_embed = CreateEmbed::new()
                    .colour(0xe5b61b)
                    .image(format!("attachment://{file_name}"));

                if index == 1 {
                    extra_embed = extra_embed.title("Alternative Card Art");
                }

                builder = builder.add_embed(extra_embed);
            }
        }
    }

    if !sent_any_image {
        builder = builder.embed(embed);
    }

    builder
}

async fn build_card_image_message(pool: &SqlitePool, card: &db::StoredCard) -> CreateMessage {
    let mut builder = CreateMessage::new();

    let image_paths = collect_card_image_paths(pool, card).await;

    if image_paths.is_empty() {
        return builder;
    }

    for image_path in image_paths {
        let resolved_path = resolve_card_image_path(&image_path);
        if let Ok(attachment) = CreateAttachment::path(&resolved_path).await {
            builder = builder.add_file(attachment);
        }
    }

    builder
}

async fn collect_card_image_paths(pool: &SqlitePool, card: &db::StoredCard) -> Vec<String> {
    let mut image_paths = match db::list_card_image_paths(pool, card.id).await {
        Ok(paths) => paths,
        Err(why) => {
            error!("DB image path read error for card='{}' (id={}): {why:?}", card.name, card.id);
            Vec::new()
        }
    };

    if image_paths.is_empty()
        && let Some(legacy_path) = card.image_path.as_deref().filter(|v| !v.trim().is_empty())
    {
        image_paths.push(legacy_path.to_string());
    }

    image_paths
}

fn parse_query(raw_query: &str) -> (bool, &str) {
    let trimmed = raw_query.trim_start();
    if let Some(rest) = trimmed.strip_prefix("-img") {
        return (true, rest.trim_start());
    }

    (false, trimmed)
}

fn resolve_card_image_path(image_path: &str) -> String {
    if Path::new(image_path).is_absolute() || image_path.contains('/') || image_path.contains('\\') {
        image_path.to_string()
    } else {
        project_root()
            .join("assets")
            .join("cards")
            .join(image_path)
            .to_string_lossy()
            .into_owned()
    }
}