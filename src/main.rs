mod bot;
mod db;
mod emoji;

use std::env;
use std::path::PathBuf;

use bot::Handler;
use dotenvy::dotenv;

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

    let token = env::var("DISCORD_TOKEN").expect("Expected a token in the environment");
    let emoji_map = emoji::load_emoji_map();
    let intents = serenity::model::gateway::GatewayIntents::GUILD_MESSAGES
        | serenity::model::gateway::GatewayIntents::DIRECT_MESSAGES
        | serenity::model::gateway::GatewayIntents::MESSAGE_CONTENT;
    let handler = Handler { pool, emoji_map };
    let mut client = serenity::Client::builder(&token, intents)
        .event_handler(handler)
        .await
        .expect("Err creating client");

    if let Err(why) = client.start().await {
        println!("Client error: {why:?}");
    }
}

pub fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}
