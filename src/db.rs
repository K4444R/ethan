use std::fmt;

use csv::ReaderBuilder;
use log::info;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{SqliteConnection, SqlitePool};

#[allow(clippy::struct_field_names)] 
pub struct Card<'a> {
    pub name: &'a str,
    pub card_type: Option<&'a str>,
    pub landscape: Option<&'a str>,
    pub ability: Option<&'a str>,
    pub card_set: Option<&'a str>,
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
    pub cost: Option<i32>,
    pub attack: Option<i32>,
    pub defense: Option<i32>,
}

#[derive(Debug)]
pub enum ImportError {
    Csv(csv::Error),
    Db(sqlx::Error),
}

impl fmt::Display for ImportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Csv(why) => write!(f, "CSV error: {why}"),
            Self::Db(why) => write!(f, "database error: {why}"),
        }
    }
}

impl std::error::Error for ImportError {}

impl From<csv::Error> for ImportError {
    fn from(why: csv::Error) -> Self {
        Self::Csv(why)
    }
}

impl From<sqlx::Error> for ImportError {
    fn from(why: sqlx::Error) -> Self {
        Self::Db(why)
    }
}

pub async fn connect_and_init(db_path: &str) -> Result<SqlitePool, sqlx::Error> {
    let options = SqliteConnectOptions::new()
        .filename(db_path)
        .create_if_missing(true);

    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await?;

    create_cards_table(&pool).await?;
    create_card_images_table(&pool).await?;
    info!("Database was initialized.");
    Ok(pool)
}

async fn create_cards_table(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    sqlx::query(
        r"
        CREATE TABLE IF NOT EXISTS cards (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            card_type TEXT,
            landscape TEXT,
            ability TEXT,
            card_set TEXT,
            cost INTEGER,
            attack INTEGER,
            defense INTEGER
        );
        ",
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

async fn create_card_images_table(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    sqlx::query(
        r"
        CREATE TABLE IF NOT EXISTS card_images (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            card_id INTEGER NOT NULL,
            image_path TEXT NOT NULL,
            sort_order INTEGER NOT NULL DEFAULT 1,
            FOREIGN KEY(card_id) REFERENCES cards(id) ON DELETE CASCADE
        );
        ",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_card_images_unique_path_per_card ON card_images(card_id, image_path COLLATE NOCASE);",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_card_images_card_sort ON card_images(card_id, sort_order, id);",
    )
    .execute(pool)
    .await?;

    Ok(())
}

async fn insert_card(conn: &mut SqliteConnection, card: &Card<'_>) -> Result<i64, sqlx::Error> {
    let (id,): (i64,) = sqlx::query_as(
        r"
        INSERT INTO cards (
            name,
            card_type,
            landscape,
            ability,
            card_set,
            cost,
            attack,
            defense
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(name) DO UPDATE SET
            card_type = excluded.card_type,
            landscape = excluded.landscape,
            ability = excluded.ability,
            card_set = excluded.card_set,
            cost = excluded.cost,
            attack = excluded.attack,
            defense = excluded.defense
        RETURNING id
        ",
    )
    .bind(card.name)
    .bind(card.card_type)
    .bind(card.landscape)
    .bind(card.ability)
    .bind(card.card_set)
    .bind(card.cost)
    .bind(card.attack)
    .bind(card.defense)
    .fetch_one(conn)
    .await?;

    Ok(id)
}

pub async fn import_cards_from_csv(
    pool: &SqlitePool,
    csv_path: &str,
) -> Result<usize, ImportError> {
    info!("CSV import started: path={csv_path}");
    let mut reader = ReaderBuilder::new()
        .delimiter(b';')
        .has_headers(true)
        .from_path(csv_path)?;

    let mut tx = pool.begin().await?;

    sqlx::query("DELETE FROM cards").execute(&mut *tx).await?;
    sqlx::query("DELETE FROM sqlite_sequence WHERE name = 'cards'")
        .execute(&mut *tx)
        .await?;

    let mut inserted = 0usize;

    for row in reader.records() {
        let record = row?;

        let name = record.get(0).map_or("", str::trim);
        if name.is_empty() {
            continue;
        }

        let card = Card {
            name,
            card_type: non_empty(record.get(1)).map(normalize_excel_text),
            landscape: non_empty(record.get(2)).map(normalize_excel_text),
            ability: non_empty(record.get(3)).map(normalize_excel_text),
            card_set: non_empty(record.get(4)).map(normalize_excel_text),
            cost: parse_optional_i32(record.get(6)),
            attack: parse_optional_i32(record.get(7)),
            defense: parse_optional_i32(record.get(8)),
        };
        let image_paths = split_image_paths(non_empty(record.get(5)).map(normalize_excel_text));

        let card_id = insert_card(&mut tx, &card).await?;
        replace_card_images(&mut tx, card_id, &image_paths).await?;
        inserted += 1;
    }

    tx.commit().await?;
    info!("CSV import finished: path={csv_path} inserted={inserted}");
    Ok(inserted)
}

pub async fn list_card_image_paths(
    pool: &SqlitePool,
    card_id: i64,
) -> Result<Vec<String>, sqlx::Error> {
    let rows: Vec<(String,)> = sqlx::query_as(
        r"
        SELECT image_path
        FROM card_images
        WHERE card_id = ?
        ORDER BY sort_order ASC, id ASC
        ",
    )
    .bind(card_id)
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(|(path,)| path).collect())
}

pub async fn find_card_by_name(
    pool: &SqlitePool,
    name: &str,
) -> Result<Option<StoredCard>, sqlx::Error> {
    sqlx::query_as::<_, StoredCard>(
        r"
        SELECT id, name, card_type, landscape, ability, card_set, cost, attack, defense
        FROM cards
        WHERE lower(name) = lower(?)
        ORDER BY id ASC
        LIMIT 1
        ",
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
        r"
        SELECT id, name, card_type, landscape, ability, card_set, cost, attack, defense
        FROM cards
        WHERE instr(lower(name), lower(?)) > 0
        ORDER BY
            CASE WHEN instr(lower(name), lower(?)) = 1 THEN 0 ELSE 1 END,
            length(name) ASC,
            name COLLATE NOCASE ASC
        LIMIT ?
        ",
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
            Some('+' | '-' | '=' | '@') => rest,
            _ => value,
        };
    }

    if let Some(rest) = value.strip_prefix('`') {
        return match rest.chars().next() {
            Some('+' | '-' | '=' | '@') => rest,
            _ => value,
        };
    }

    value
}

fn split_image_paths(raw: Option<&str>) -> Vec<String> {
    let Some(raw) = raw else {
        return Vec::new();
    };

    raw.split('|')
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

async fn replace_card_images(
    conn: &mut SqliteConnection,
    card_id: i64,
    image_paths: &[String],
) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM card_images WHERE card_id = ?")
        .bind(card_id)
        .execute(&mut *conn)
        .await?;

    for (sort_order, image_path) in (1_i64..).zip(image_paths) {
        sqlx::query("INSERT INTO card_images (card_id, image_path, sort_order) VALUES (?, ?, ?)")
            .bind(card_id)
            .bind(image_path)
            .bind(sort_order)
            .execute(&mut *conn)
            .await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEADER: &str = "Name;Card Type;Landscape;Ability;Set;Image Path;Cost;Attack;Defense\n";

    #[test]
    fn normalize_excel_text_removes_only_escape_marks() {
        for (input, expected) in [
            ("'+1 ATK", "+1 ATK"),
            ("`-1 DEF", "-1 DEF"),
            ("'hello", "'hello"),
            ("plain", "plain"),
            ("'", "'"),
        ] {
            assert_eq!(normalize_excel_text(input), expected);
        }
    }

    #[test]
    fn split_image_paths_ignores_blanks() {
        assert_eq!(
            split_image_paths(Some(" a.jpg | |b.jpg")),
            ["a.jpg", "b.jpg"]
        );
        assert!(split_image_paths(None).is_empty());
    }

    #[tokio::test]
    async fn import_replaces_cards_and_rolls_back_on_bad_row() {
        let dir = std::env::temp_dir().join(format!("ethan_test_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let csv = dir.join("cards.csv");
        let csv_path = csv.to_str().unwrap();
        let pool = connect_and_init(dir.join("test.db").to_str().unwrap())
            .await
            .unwrap();
        let write_csv = |rows: &str| std::fs::write(&csv, format!("{HEADER}{rows}")).unwrap();

        write_csv("Alpha;;;old;;a.jpg|a2.jpg;1;;\nBeta;;;;;b.jpg;;;\n");
        assert_eq!(import_cards_from_csv(&pool, csv_path).await.unwrap(), 2);
        let alpha = find_card_by_name(&pool, "ALPHA").await.unwrap().unwrap();
        assert_eq!(alpha.ability.as_deref(), Some("old"));
        assert_eq!(
            list_card_image_paths(&pool, alpha.id).await.unwrap(),
            ["a.jpg", "a2.jpg"]
        );

        write_csv("Alpha;;;new;;a.jpg;;;\nbad;row\n");
        assert!(import_cards_from_csv(&pool, csv_path).await.is_err());
        let alpha = find_card_by_name(&pool, "Alpha").await.unwrap().unwrap();
        assert_eq!(alpha.ability.as_deref(), Some("old"));
        assert!(find_card_by_name(&pool, "Beta").await.unwrap().is_some());

        write_csv("Alpha;;;new;;a.jpg;;;\n");
        assert_eq!(import_cards_from_csv(&pool, csv_path).await.unwrap(), 1);
        let alpha = find_card_by_name(&pool, "Alpha").await.unwrap().unwrap();
        assert_eq!(alpha.ability.as_deref(), Some("new"));
        assert_eq!(
            list_card_image_paths(&pool, alpha.id).await.unwrap(),
            ["a.jpg"]
        );
        assert!(find_card_by_name(&pool, "Beta").await.unwrap().is_none());

        let by_part = |query: &'static str| search_cards_by_partial_name(&pool, query, 10);
        assert_eq!(by_part("lph").await.unwrap().len(), 1);
        assert!(by_part("A%a").await.unwrap().is_empty());

        pool.close().await;
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
