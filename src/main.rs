mod bot;
mod db;
mod emoji;

use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::Mutex;

use bot::Handler;
use dotenvy::dotenv;
use fern::Dispatch;
use log::{LevelFilter, error, info, warn};

const MAX_LOG_LINES: usize = 500;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let project_root = project_root();
    let log_dir = project_root.join("logs");
    let log_path = log_dir.join("output.log");

    // logs/ is gitignored, so a fresh clone does not have it.
    fs::create_dir_all(&log_dir)?;
    trim_log_file(&log_path, MAX_LOG_LINES)?;
    setup_logger(&log_path)?;

    match dotenv() {
        Ok(path) => info!("Loaded environment from {}", path.display()),
        Err(why) => warn!("File .env was not loaded: {why}"),
    }

    let db_path = project_root.join("ethan.db");
    let csv_path = project_root.join("cards_template.csv");

    let pool = db::connect_and_init(db_path.to_string_lossy().as_ref()).await?;

    match db::import_cards_from_csv(&pool, csv_path.to_string_lossy().as_ref()).await {
        Ok(inserted) => info!("Imported {inserted} cards from {}", csv_path.display()),
        Err(why) => warn!("CSV import of {} skipped: {why}", csv_path.display()),
    }

    let token = env::var("DISCORD_TOKEN").map_err(|_| "DISCORD_TOKEN is not set")?;
    let emoji_map = emoji::load_emoji_map();
    let intents = serenity::model::gateway::GatewayIntents::GUILD_MESSAGES
        | serenity::model::gateway::GatewayIntents::DIRECT_MESSAGES
        | serenity::model::gateway::GatewayIntents::MESSAGE_CONTENT;
    let handler = Handler {
        pool,
        emoji_map,
        last_request: Mutex::default(),
    };
    let mut client = serenity::Client::builder(&token, intents)
        .event_handler(handler)
        .await?;

    if let Err(why) = client.start().await {
        error!("Client error: {why:?}");
        return Err(why.into());
    }

    Ok(())
}

// Relative to the working directory, so the binary still works after being moved.
pub(crate) fn project_root() -> PathBuf {
    PathBuf::from(".")
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
            ));
        })
        .level(LevelFilter::Info)
        .level_for("ethan::bot", LevelFilter::Info)
        .level_for("ethan::db", LevelFilter::Info)
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
