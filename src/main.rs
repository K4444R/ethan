mod db;

use std::env;
use std::path::Path;
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
}

#[async_trait]
impl EventHandler for Handler {
    async fn message(&self, ctx: Context, msg: Message) {
        let Some(raw_query) = msg.content.strip_prefix("!ethan") else {
            return;
        };

        let card_name = raw_query.trim();
        if card_name.is_empty() {
            let embed = CreateEmbed::new()
                .colour(0xe5b61b)
                .description("Try: !ethan <card name>")
                .timestamp(Timestamp::now());
            let builder = CreateMessage::new().embed(embed);
            let _ = msg.channel_id.send_message(&ctx.http, builder).await;
            return;
        }

        match db::find_card_by_name(&self.pool, card_name).await {
            Ok(Some(card)) => {
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
                    embed = embed.field("Landscape", value, true);
                }
                if let Some(value) = card.ability.filter(|v| !v.trim().is_empty()) {
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
                    let file_name = Path::new(&image_path)
                        .file_name()
                        .and_then(|v| v.to_str())
                        .unwrap_or("card_image.png");

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
            Ok(None) => {
                let embed = CreateEmbed::new()
                    .colour(0xe5b61b)
                    .title("Card Not Found")
                    .description(format!("Card '{card_name}' not found in the database."))
                    .timestamp(Timestamp::now());
                let builder = CreateMessage::new().embed(embed);
                let _ = msg.channel_id.send_message(&ctx.http, builder).await;
            }
            Err(why) => {
                println!("DB read error: {why:?}");
                let embed = CreateEmbed::new()
                    .colour(0xe5b61b)
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

#[tokio::main]
async fn main() {
    dotenv().ok();

    let pool = db::connect_and_init("ethan.db")
        .await
        .expect("Failed to initialize SQLite database");

    let cards_count = db::count_cards(&pool).await.unwrap_or(0);
    if cards_count <= 1 {
        match db::import_cards_from_csv(&pool, "cards_template.csv").await {
            Ok(inserted) => println!("Imported {inserted} cards from cards_template.csv"),
            Err(why) => println!("CSV import skipped: {why}"),
        }
    }

    // Configure the client with your Discord bot token in the environment.
    let token = env::var("DISCORD_TOKEN").expect("Expected a token in the environment");
    let intents = GatewayIntents::GUILD_MESSAGES
        | GatewayIntents::DIRECT_MESSAGES
        | GatewayIntents::MESSAGE_CONTENT;
    let handler = Handler { pool };
    let mut client =
        Client::builder(&token, intents).event_handler(handler).await.expect("Err creating client");

    if let Err(why) = client.start().await {
        println!("Client error: {why:?}");
    }
}