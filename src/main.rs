mod bot;
mod db;
mod emoji;

use std::env;
use std::fs;
use std::path::PathBuf;

use bot::Handler;
use dotenvy::dotenv;
use log::{info, warn, error, LevelFilter};
use fern::Dispatch;

const MAX_LOG_LINES: usize = 500;


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let project_root = project_root();
    let log_path = project_root.join("logs").join("output.log");

    trim_log_file(&log_path, MAX_LOG_LINES)?;
    setup_logger(&log_path)?;

    dotenv().ok();
    info!("File .env was loaded successfully.");

    let db_path = project_root.join("ethan.db");
    let csv_path = project_root.join("cards_template.csv");

    let pool = db::connect_and_init(db_path.to_string_lossy().as_ref())
        .await
        .expect("Failed to initialize SQLite database");

    let cards_count = db::count_cards(&pool).await.unwrap_or(0);
    if cards_count <= 1 {
        match db::import_cards_from_csv(&pool, csv_path.to_string_lossy().as_ref()).await {
            Ok(inserted) => info!("Imported {inserted} cards from {}", csv_path.display()),
            Err(why) => warn!("CSV import skipped: {why}"),
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
        error!("Client error: {why:?}");
    }

    Ok(())
}

pub fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn setup_logger(log_path: &std::path::Path) -> Result<(), fern::InitError> {
    Dispatch::new()
        .filter(|metadata| !metadata.target().starts_with("tracing"))
        .format(|out, message, record| {
            out.finish(format_args!(
                "[{} {} {}] {}",
                chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
                record.level(),
                record.target(),
                message
            ))
        })
        .level(LevelFilter::Info)
        .level_for("ethan::bot", LevelFilter::Info)
        .level_for("ethan::db",  LevelFilter::Info)
        .level_for("serenity", LevelFilter::Warn)
        .level_for("sqlx", LevelFilter::Warn)
        .level_for("tracing", LevelFilter::Off)
        .chain(std::io::stdout())
        .chain(fern::log_file(log_path)?)
        .apply()?;
    Ok(())
}

fn trim_log_file(log_path: &std::path::Path, max_lines: usize) -> Result<(), std::io::Error> {
    if !log_path.exists() {
        return Ok(());
    }

    let content = fs::read_to_string(log_path)?;
    let total_lines = content.lines().count();
    if total_lines <= max_lines {
        return Ok(());
    }

    let retained = content
        .lines()
        .skip(total_lines - max_lines)
        .collect::<Vec<_>>()
        .join("\n");

    fs::write(log_path, format!("{retained}\n"))?;
    Ok(())
}
