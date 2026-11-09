use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use crate::{db, emoji, project_root};
use log::{error, info, warn};
use serenity::all::ReactionType;
use serenity::async_trait;
use serenity::builder::{CreateAttachment, CreateEmbed, CreateEmbedFooter, CreateMessage};
use serenity::model::Timestamp;
use serenity::model::channel::Message;
use serenity::model::gateway::Ready;
use serenity::model::id::UserId;
use serenity::prelude::*;
use sqlx::SqlitePool;

pub struct Handler {
    pub pool: SqlitePool,
    pub emoji_map: HashMap<String, String>,
    pub last_request: Mutex<HashMap<UserId, Instant>>,
}

const MULTI_RESULTS_LIMIT: usize = 10;
const MULTI_RESULTS_QUERY_LIMIT: i64 = 11;
const MIN_PARTIAL_QUERY_ALNUM_LEN: usize = 3;
const COMMAND_COOLDOWN: Duration = Duration::from_secs(2);
const GOLD: u32 = 0x00e5_b61b;
const RED: u32 = 0x00e6_3a24;

#[async_trait]
impl EventHandler for Handler {
    async fn message(&self, ctx: Context, msg: Message) {
        if msg.author.bot {
            return;
        }

        let is_faq = msg.content.trim().eq_ignore_ascii_case("!faq");
        let command = strip_word(&msg.content, "!ethan");
        if !is_faq && command.is_none() {
            return;
        }

        let on_cooldown = {
            let mut last_request = self
                .last_request
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            is_on_cooldown(&mut last_request, msg.author.id, Instant::now())
        };
        if on_cooldown {
            return;
        }

        info!(
            "Request: user={} ({}) channel={} guild={} text={:?}",
            msg.author.name,
            msg.author.id,
            msg.channel_id,
            msg.guild_id
                .map_or_else(|| "DM".to_string(), |guild_id| guild_id.to_string()),
            msg.content
        );

        let ethan_emoji = emoji::emoji_tag("ethan_allfire", &self.emoji_map);
        if let Ok(ethan_reaction) = ReactionType::try_from(ethan_emoji) {
            let _ = msg.react(&ctx.http, ethan_reaction).await;
        }

        let Some(raw_query) = command else {
            send_faq_message(&ctx, &msg, &self.emoji_map).await;
            return;
        };

        let (use_image, card_name) = parse_query(raw_query);
        if card_name.is_empty() {
            send_intro_message(&ctx, &msg, &self.emoji_map).await;
            return;
        }

        match db::find_card_by_name(&self.pool, card_name).await {
            Ok(Some(card)) => {
                send_card(&ctx, &msg, &self.pool, &card, use_image, &self.emoji_map).await;
            }
            Ok(None) => {
                self.send_partial_matches(&ctx, &msg, card_name, use_image)
                    .await;
            }
            Err(why) => report_db_error(&ctx, &msg, card_name, &why).await,
        }
    }

    async fn ready(&self, _: Context, ready: Ready) {
        info!("{} is connected!", ready.user.name);
    }
}

impl Handler {
    async fn send_partial_matches(
        &self,
        ctx: &Context,
        msg: &Message,
        card_name: &str,
        use_image: bool,
    ) {
        let query_alnum_len = card_name.chars().filter(|c| c.is_alphanumeric()).count();
        if query_alnum_len < MIN_PARTIAL_QUERY_ALNUM_LEN {
            send_status_embed(
                ctx,
                msg,
                RED,
                "Search Too Broad",
                format!(
                    "Search `{card_name}` is too broad. Please type at least {MIN_PARTIAL_QUERY_ALNUM_LEN} letters or a more specific card name."
                ),
            )
            .await;
            return;
        }

        match db::search_cards_by_partial_name(&self.pool, card_name, MULTI_RESULTS_QUERY_LIMIT)
            .await
        {
            Ok(cards) => match <[db::StoredCard; 1]>::try_from(cards) {
                Ok([card]) => {
                    send_card(ctx, msg, &self.pool, &card, use_image, &self.emoji_map).await;
                }
                Err(cards) if cards.is_empty() => {
                    // The search text is not repeated: Discord would render it as a clickable link.
                    send_status_embed(
                        ctx,
                        msg,
                        RED,
                        "Card Not Found",
                        "Card not found in the database.",
                    )
                    .await;
                }
                Err(cards) => send_multi_result_embed(ctx, msg, card_name, cards).await,
            },
            Err(why) => report_db_error(ctx, msg, card_name, &why).await,
        }
    }
}

async fn report_db_error(ctx: &Context, msg: &Message, card_name: &str, why: &sqlx::Error) {
    error!(
        "Card search DB error for user={} channel={} query={card_name:?}: {why:?}",
        msg.author.id, msg.channel_id
    );
    send_status_embed(
        ctx,
        msg,
        RED,
        "Database Error",
        "Error reading from the database.",
    )
    .await;
}

async fn send_intro_message(ctx: &Context, msg: &Message, emoji_map: &HashMap<String, String>) {
    let ethan_emoji = emoji::emoji_tag("ethan_allfire", emoji_map);
    let embed = CreateEmbed::new()
        .colour(GOLD)
        .description(format!("Hello, I am Ethan! {ethan_emoji}\nTry: ```!ethan <card name>```\nor: ```!ethan -img <card name>``` for card image"))
        .timestamp(Timestamp::now());
    let builder = CreateMessage::new().embed(embed);
    if let Err(why) = msg.channel_id.send_message(&ctx.http, builder).await {
        error!(
            "Failed to send intro message to channel={}: {why:?}",
            msg.channel_id
        );
    }
}

const KICKSTARTER_FAQ_URL: &str =
    "https://www.kickstarter.com/projects/cze/adventure-time-card-wars-2026/faqs";
const KICKSTARTER_URL: &str =
    "https://www.kickstarter.com/projects/cze/adventure-time-card-wars-2026/";

async fn send_faq_message(ctx: &Context, msg: &Message, emoji_map: &HashMap<String, String>) {
    let ethan_emoji = emoji::emoji_tag("ethan_allfire", emoji_map);
    let embed = CreateEmbed::new()
        .colour(GOLD)
        .title("Kickstarter 3 is LIVE!")
        .url(KICKSTARTER_URL)
        .description(format!(
            "Hey, Ethan is here! {ethan_emoji} The flames are HOT today, because **Adventure Time Card Wars Kickstarter 3** has officially launched!\n\n\
            Got questions about pledges, shipping, add-ons, VAT or the mystery \"?\" coin? \n\n\
            I already burned through them all, and every answer is waiting for you in the campaign FAQ:\n\n\
            > **[Read the Kickstarter FAQ]({KICKSTARTER_FAQ_URL})**\n\n\
            Now go back it, Floop The Pig and get ready for The Dark Pig!"
        ))
        .footer(CreateEmbedFooter::new("Card Wars 2026 • Kickstarter 3"))
        .timestamp(Timestamp::now());
    let mut builder = CreateMessage::new();
    let banner_path = project_root().join("assets").join("kickstarter_3.jpg");
    match CreateAttachment::path(&banner_path).await {
        Ok(attachment) => {
            builder = builder
                .add_file(attachment)
                .embed(embed.image("attachment://kickstarter_3.jpg"));
        }
        Err(why) => {
            warn!("FAQ banner missing at {}: {why:?}", banner_path.display());
            builder = builder.embed(embed);
        }
    }
    if let Err(why) = msg.channel_id.send_message(&ctx.http, builder).await {
        error!(
            "Failed to send FAQ message to channel={}: {why:?}",
            msg.channel_id
        );
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
        error!(
            "Failed to send status embed to channel={}: {why:?}",
            msg.channel_id
        );
    }
}

async fn send_multi_result_embed(
    ctx: &Context,
    msg: &Message,
    card_name: &str,
    cards: Vec<db::StoredCard>,
) {
    let truncated = cards.len() > MULTI_RESULTS_LIMIT;
    let listed = cards.into_iter().take(MULTI_RESULTS_LIMIT);
    let mut embed = CreateEmbed::new()
        .colour(GOLD)
        .title("Multiple Results")
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
                "Showing first {MULTI_RESULTS_LIMIT} results. Add more words to narrow it down."
            ),
            false,
        );
    }

    let builder = CreateMessage::new().embed(embed);
    if let Err(why) = msg.channel_id.send_message(&ctx.http, builder).await {
        error!(
            "Failed to send multi-result embed to channel={}: {why:?}",
            msg.channel_id
        );
    }
}

async fn send_card(
    ctx: &Context,
    msg: &Message,
    pool: &SqlitePool,
    card: &db::StoredCard,
    use_image: bool,
    emoji_map: &HashMap<String, String>,
) {
    let attachments = load_card_attachments(pool, card).await;
    let builder = if use_image {
        attachments
            .into_iter()
            .fold(CreateMessage::new(), CreateMessage::add_file)
    } else {
        build_card_message(card, attachments, emoji_map)
    };

    if let Err(why) = msg.channel_id.send_message(&ctx.http, builder).await {
        error!(
            "Failed to send card to channel={} card='{}': {why:?}",
            msg.channel_id, card.name
        );
    }
}

fn build_card_embed(card: &db::StoredCard, emoji_map: &HashMap<String, String>) -> CreateEmbed {
    let mut embed = CreateEmbed::new()
        .colour(GOLD)
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

fn build_card_message(
    card: &db::StoredCard,
    attachments: Vec<CreateAttachment>,
    emoji_map: &HashMap<String, String>,
) -> CreateMessage {
    let embed = build_card_embed(card, emoji_map);
    let builder = CreateMessage::new();
    let mut attachments = attachments.into_iter();

    let Some(first) = attachments.next() else {
        return builder.embed(embed);
    };

    let mut builder = builder
        .embed(embed.image(format!("attachment://{}", first.filename)))
        .add_file(first);

    for (index, attachment) in attachments.enumerate() {
        let mut extra_embed = CreateEmbed::new()
            .colour(GOLD)
            .image(format!("attachment://{}", attachment.filename));

        if index == 0 {
            extra_embed = extra_embed.title("Alternative Card Art");
        }

        builder = builder.add_embed(extra_embed).add_file(attachment);
    }

    builder
}

async fn load_card_attachments(pool: &SqlitePool, card: &db::StoredCard) -> Vec<CreateAttachment> {
    let image_paths = match db::list_card_image_paths(pool, card.id).await {
        Ok(paths) => paths,
        Err(why) => {
            error!(
                "DB image path read error for card='{}' (id={}): {why:?}",
                card.name, card.id
            );
            Vec::new()
        }
    };

    let mut attachments = Vec::with_capacity(image_paths.len());
    for image_path in image_paths {
        if let Ok(attachment) = CreateAttachment::path(resolve_card_image_path(&image_path)).await {
            attachments.push(attachment);
        }
    }

    attachments
}

fn strip_word<'a>(text: &'a str, word: &str) -> Option<&'a str> {
    let rest = text.strip_prefix(word)?;
    (rest.is_empty() || rest.starts_with(char::is_whitespace)).then_some(rest)
}

fn parse_query(raw_query: &str) -> (bool, &str) {
    let trimmed = raw_query.trim_start();
    strip_word(trimmed, "-img").map_or((false, trimmed), |rest| (true, rest.trim_start()))
}

fn is_on_cooldown(last_request: &mut HashMap<UserId, Instant>, user: UserId, now: Instant) -> bool {
    last_request.retain(|_, at| now.duration_since(*at) < COMMAND_COOLDOWN);
    if last_request.contains_key(&user) {
        return true;
    }
    last_request.insert(user, now);
    false
}

fn resolve_card_image_path(image_path: &str) -> PathBuf {
    let file_name = Path::new(image_path).file_name().unwrap_or_default();
    project_root().join("assets").join("cards").join(file_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_word_requires_a_word_boundary() {
        assert_eq!(strip_word("!ethan", "!ethan"), Some(""));
        assert_eq!(strip_word("!ethan cow", "!ethan"), Some(" cow"));
        assert_eq!(strip_word("!ethanol", "!ethan"), None);
        assert_eq!(strip_word("hello", "!ethan"), None);
    }

    #[test]
    fn parse_query_detects_image_mode() {
        for (input, expected) in [
            (" cow", (false, "cow")),
            (" -img cow", (true, "cow")),
            (" -img", (true, "")),
            (" -imgoblin", (false, "-imgoblin")),
            ("", (false, "")),
        ] {
            assert_eq!(parse_query(input), expected);
        }
    }

    #[test]
    fn image_paths_stay_inside_the_cards_folder() {
        let cards = project_root().join("assets").join("cards");
        for (input, file) in [
            ("cow.jpg", "cow.jpg"),
            ("../.env", ".env"),
            ("/etc/passwd", "passwd"),
            ("a/b/c.jpg", "c.jpg"),
        ] {
            assert_eq!(resolve_card_image_path(input), cards.join(file));
        }
        assert_eq!(resolve_card_image_path(".."), cards);
    }

    #[test]
    fn cooldown_blocks_repeats_per_user_and_expires() {
        let mut last_request = HashMap::new();
        let start = Instant::now();
        let (first, second) = (UserId::new(1), UserId::new(2));

        assert!(!is_on_cooldown(&mut last_request, first, start));
        assert!(is_on_cooldown(&mut last_request, first, start));
        assert!(!is_on_cooldown(&mut last_request, second, start));
        assert!(!is_on_cooldown(
            &mut last_request,
            first,
            start + COMMAND_COOLDOWN
        ));
        assert_eq!(last_request.len(), 1);
    }
}
