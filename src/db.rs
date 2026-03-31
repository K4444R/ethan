use std::collections::HashMap;

use csv::ReaderBuilder;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::Row;
use sqlx::SqlitePool;

pub struct Card<'a> {
    pub name: &'a str,
    pub card_type: Option<&'a str>,
    pub landscape: Option<&'a str>,
    pub ability: Option<&'a str>,
    pub card_set: Option<&'a str>,
    pub image_path: Option<&'a str>,
    pub cost: Option<i32>,
    pub attack: Option<i32>,
    pub defense: Option<i32>,
}

#[derive(sqlx::FromRow)]
pub struct StoredCard {
    pub id: i64,
    pub name: String,
    pub card_type: Option<String>,
    pub landscape: Option<String>,
    pub ability: Option<String>,
    pub card_set: Option<String>,
    pub image_path: Option<String>,
    pub cost: Option<i32>,
    pub attack: Option<i32>,
    pub defense: Option<i32>,
}

pub async fn connect_and_init(db_path: &str) -> Result<SqlitePool, sqlx::Error> {
    let options = SqliteConnectOptions::new().filename(db_path).create_if_missing(true);

    let pool = SqlitePoolOptions::new().max_connections(5).connect_with(options).await?;

    init_schema(&pool).await?;
    Ok(pool)
}

async fn init_schema(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    let table_exists: Option<(String,)> = sqlx::query_as(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'cards'",
    )
    .fetch_optional(pool)
    .await?;

    if table_exists.is_none() {
        create_cards_table(pool).await?;
        return Ok(());
    }

    let table_info = sqlx::query("PRAGMA table_info(cards)").fetch_all(pool).await?;

    let mut columns: HashMap<String, bool> = HashMap::new();
    for row in table_info {
        let name: String = row.try_get("name")?;
        let not_null: i64 = row.try_get("notnull")?;
        columns.insert(name, not_null == 1);
    }

    let required_columns = [
        "name",
        "card_type",
        "landscape",
        "ability",
        "card_set",
        "image_path",
        "cost",
        "attack",
        "defense",
    ];

    let missing_columns = required_columns.iter().any(|c| !columns.contains_key(*c));

    let optional_columns_should_be_nullable = [
        "card_type",
        "landscape",
        "ability",
        "card_set",
        "image_path",
        "cost",
        "attack",
        "defense",
    ];

    let has_wrong_not_null = optional_columns_should_be_nullable
        .iter()
        .any(|c| columns.get(*c).copied().unwrap_or(false));

    if missing_columns || has_wrong_not_null {
        migrate_cards_table(pool, columns.contains_key("image_path")).await?;
    }

    normalize_excel_escaped_prefixes(pool).await?;
    enforce_cards_name_uniqueness(pool).await?;

    Ok(())
}

async fn create_cards_table(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS cards (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            card_type TEXT,
            landscape TEXT,
            ability TEXT,
            card_set TEXT,
            image_path TEXT,
            cost INTEGER,
            attack INTEGER,
            defense INTEGER
        );
        "#,
    )
    .execute(pool)
    .await?;

    enforce_cards_name_uniqueness(pool).await?;

    Ok(())
}

async fn migrate_cards_table(pool: &SqlitePool, old_has_image_path: bool) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;

    sqlx::query("ALTER TABLE cards RENAME TO cards_old;").execute(&mut *tx).await?;

    sqlx::query(
        r#"
        CREATE TABLE cards (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            card_type TEXT,
            landscape TEXT,
            ability TEXT,
            card_set TEXT,
            image_path TEXT,
            cost INTEGER,
            attack INTEGER,
            defense INTEGER
        );
        "#,
    )
    .execute(&mut *tx)
    .await?;

    let migration_insert = if old_has_image_path {
        r#"
        INSERT INTO cards (name, card_type, landscape, ability, card_set, image_path, cost, attack, defense)
        SELECT
            name,
            NULLIF(card_type, ''),
            NULLIF(landscape, ''),
            NULLIF(ability, ''),
            NULLIF(card_set, ''),
            NULLIF(image_path, ''),
            CAST(NULLIF(cost, '') AS INTEGER),
            CAST(NULLIF(attack, '') AS INTEGER),
            CAST(NULLIF(defense, '') AS INTEGER)
        FROM cards_old;
        "#
    } else {
        r#"
        INSERT INTO cards (name, card_type, landscape, ability, card_set, image_path, cost, attack, defense)
        SELECT
            name,
            NULLIF(card_type, ''),
            NULLIF(landscape, ''),
            NULLIF(ability, ''),
            NULLIF(card_set, ''),
            NULL,
            CAST(NULLIF(cost, '') AS INTEGER),
            CAST(NULLIF(attack, '') AS INTEGER),
            CAST(NULLIF(defense, '') AS INTEGER)
        FROM cards_old;
        "#
    };

    sqlx::query(migration_insert).execute(&mut *tx).await?;
    sqlx::query("DROP TABLE cards_old;").execute(&mut *tx).await?;

    tx.commit().await?;

    enforce_cards_name_uniqueness(pool).await?;
    Ok(())
}

async fn enforce_cards_name_uniqueness(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    // Keep the oldest row per card name (case-insensitive), remove duplicates.
    sqlx::query(
        r#"
        DELETE FROM cards
        WHERE id NOT IN (
            SELECT MIN(id)
            FROM cards
            GROUP BY lower(trim(name))
        );
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_cards_name_unique_nocase ON cards(name COLLATE NOCASE);",
    )
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn insert_card(pool: &SqlitePool, card: &Card<'_>) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO cards (
            name,
            card_type,
            landscape,
            ability,
            card_set,
            image_path,
            cost,
            attack,
            defense
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(name) DO UPDATE SET
            card_type = excluded.card_type,
            landscape = excluded.landscape,
            ability = excluded.ability,
            card_set = excluded.card_set,
            image_path = excluded.image_path,
            cost = excluded.cost,
            attack = excluded.attack,
            defense = excluded.defense
        "#,
    )
    .bind(card.name)
    .bind(card.card_type)
    .bind(card.landscape)
    .bind(card.ability)
    .bind(card.card_set)
    .bind(card.image_path)
    .bind(card.cost)
    .bind(card.attack)
    .bind(card.defense)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn count_cards(pool: &SqlitePool) -> Result<i64, sqlx::Error> {
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM cards").fetch_one(pool).await?;
    Ok(count.0)
}

pub async fn import_cards_from_csv(pool: &SqlitePool, csv_path: &str) -> Result<usize, String> {
    let mut reader = ReaderBuilder::new()
        .delimiter(b';')
        .has_headers(true)
        .from_path(csv_path)
        .map_err(|e| format!("Failed to open CSV {csv_path}: {e}"))?;

    let mut inserted = 0usize;

    for row in reader.records() {
        let record = row.map_err(|e| format!("CSV read error: {e}"))?;

        let name = record.get(0).map(str::trim).unwrap_or("");
        if name.is_empty() {
            continue;
        }

        let card = Card {
            name,
            card_type: non_empty(record.get(1)).map(normalize_excel_text),
            landscape: non_empty(record.get(2)).map(normalize_excel_text),
            ability: non_empty(record.get(3)).map(normalize_excel_text),
            card_set: non_empty(record.get(4)).map(normalize_excel_text),
            image_path: non_empty(record.get(5)).map(normalize_excel_text),
            cost: parse_optional_i32(record.get(6)),
            attack: parse_optional_i32(record.get(7)),
            defense: parse_optional_i32(record.get(8)),
        };

        insert_card(pool, &card).await.map_err(|e| format!("DB insert error: {e}"))?;
        inserted += 1;
    }

    Ok(inserted)
}

pub async fn find_card_by_name(pool: &SqlitePool, name: &str) -> Result<Option<StoredCard>, sqlx::Error> {
    sqlx::query_as::<_, StoredCard>(
        r#"
        SELECT id, name, card_type, landscape, ability, card_set, image_path, cost, attack, defense
        FROM cards
        WHERE lower(name) = lower(?)
        ORDER BY id ASC
        LIMIT 1
        "#,
    )
    .bind(name)
    .fetch_optional(pool)
    .await
}

pub async fn search_cards_by_partial_name(
    pool: &SqlitePool,
    query: &str,
    limit: i64,
) -> Result<Vec<StoredCard>, sqlx::Error> {
    sqlx::query_as::<_, StoredCard>(
        r#"
        SELECT id, name, card_type, landscape, ability, card_set, image_path, cost, attack, defense
        FROM cards
        WHERE lower(name) LIKE '%' || lower(?) || '%'
        ORDER BY
            CASE WHEN lower(name) LIKE lower(?) || '%' THEN 0 ELSE 1 END,
            length(name) ASC,
            name COLLATE NOCASE ASC
        LIMIT ?
        "#,
    )
    .bind(query)
    .bind(query)
    .bind(limit)
    .fetch_all(pool)
    .await
}

fn non_empty(value: Option<&str>) -> Option<&str> {
    let trimmed = value?.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

fn parse_optional_i32(value: Option<&str>) -> Option<i32> {
    let trimmed = value?.trim();
    if trimmed.is_empty() {
        None
    } else {
        trimmed.parse::<i32>().ok()
    }
}

fn normalize_excel_text(value: &str) -> &str {
    if let Some(rest) = value.strip_prefix('\'') {
        return match rest.chars().next() {
            Some('+') | Some('-') | Some('=') | Some('@') => rest,
            _ => value,
        };
    }

    if let Some(rest) = value.strip_prefix('`') {
        return match rest.chars().next() {
            Some('+') | Some('-') | Some('=') | Some('@') => rest,
            _ => value,
        };
    }

    value
}

async fn normalize_excel_escaped_prefixes(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    let text_columns = ["card_type", "landscape", "ability", "card_set", "image_path"];

    for column in text_columns {
        let query = format!(
            "UPDATE cards SET {col} = substr({col}, 2) WHERE {col} LIKE '''+%' OR {col} LIKE '''-%' OR {col} LIKE '''=%' OR {col} LIKE '''@%' OR {col} LIKE '`+%' OR {col} LIKE '`-%' OR {col} LIKE '`=%' OR {col} LIKE '`@%'",
            col = column
        );
        sqlx::query(&query).execute(pool).await?;
    }

    Ok(())
}

