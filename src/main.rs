use std::{collections::HashMap, env};
use axum::{Json, Router, extract::State, http::StatusCode, routing::{get, post}};
use serde::Deserialize;
use sqlx::{PgPool, postgres::PgPoolOptions, prelude::FromRow};

#[derive(Deserialize, Hash, Eq, PartialEq, Debug)]
#[serde(rename_all = "lowercase")]
enum Operation {
    Insert,
    Update,
    Delete
}

#[derive(Deserialize, Debug, FromRow)]
struct FileEntry {
    file_path: String,
    file_hash: Option<String>,
    file_size: i64,
    modified_time: i64
}

type FileSyncPayload = HashMap<Operation, Vec<FileEntry>>;

#[tokio::main]
async fn main() {
    let db_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let pool = PgPoolOptions::new().connect(&db_url).await.expect("Failed to connect to DB");
    sqlx::migrate!().run(&pool).await.expect("Migrations failed");

    let app = Router::new()
        .route("/", get(root))
        .route("/sync", post(handle_sync))
        .with_state(pool);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8000").await.unwrap();
    println!("Server running on port 8000");
    axum::serve(listener, app).await.unwrap();
}

async fn root() -> &'static str {
    "Pocket Drive is running!"
}

async fn handle_sync(
    State(pool) : State<PgPool>,
    Json(payload): Json<FileSyncPayload>
) ->Result<(StatusCode, Json<Vec<FileEntry>>), StatusCode> {
    // Placeholder for sync logic
    for (cmd, files) in payload {
        dbg!(&cmd);
        match cmd {
            Operation::Insert => {
                let res: Vec<(StatusCode, FileEntry)> = Vec::new();
                for file in files {
                    let response = sqlx::query_as::<_, FileEntry>("INSERT INTO filehash (file_path, file_hash, file_size, modified_time) VALUES ($1, $2, $3, $4) RETURNING *")
                        .bind(file.file_path)
                        .bind(file.file_hash)
                        .bind(file.file_size)
                        .bind(file.modified_time)
                        .fetch_one(&pool).await
                        .map(|u| (StatusCode::CREATED, Json(u)))
                        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR);
                };

                unimplemented!()
            }
            Operation::Update => {
                dbg!(files);
            }
            Operation::Delete => {
                dbg!(files);
            }
        }
    };
    "Done"
}
