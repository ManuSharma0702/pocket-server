use std::{collections::HashMap, env};

use axum::{
    extract::{ State , Multipart},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use sqlx::{postgres::PgPoolOptions, FromRow, PgPool};

#[derive(Deserialize, Serialize, Hash, Eq, PartialEq, Debug)]
#[serde(rename_all = "lowercase")]
enum Operation {
    Insert,
    Update,
    Delete,
}

#[derive(Deserialize, Serialize, Debug, FromRow)]
struct FileEntry {
    file_path: String,
    file_hash: Option<String>,
    file_size: i64,
    modified_time: i64,
}

type FileSyncPayload = HashMap<Operation, Vec<FileEntry>>;


#[derive(Serialize)]
struct FileFailure {
    file_path: String,
    error: String
}

#[derive(Serialize)]
struct OperationResult {
    success: Vec<FileEntry>,
    failure: Vec<FileFailure>
}

type SyncResponse = HashMap<Operation, OperationResult>;

#[derive(Serialize)]
struct GetAllResponse {
    data: Option<Vec<FileEntry>>,
    error: Option<String>,
}

#[tokio::main]
async fn main() {
    let db_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let pool = PgPoolOptions::new()
        .connect(&db_url)
        .await
        .expect("Failed to connect to DB");

    sqlx::migrate!().run(&pool).await.expect("Migrations failed");

    let app = Router::new()
        .route("/", get(root))
        .route("/sync", post(handle_sync))
        .route("/get", get(handle_get_all))
        .with_state(pool);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8000")
        .await
        .unwrap();

    println!("Server running on http://0.0.0.0:8000");

    axum::serve(listener, app).await.unwrap();
}

async fn root() -> &'static str {
     println!("ROOT HIT");
    "Pocket Drive is running!"
}

async fn handle_sync(
    State(pool): State<PgPool>,
    mut multipart: Multipart,
) -> impl IntoResponse {
    let mut payload: Option<FileSyncPayload> = None;

    while let Ok(Some(field)) = multipart.next_field().await {
        let name = field.name().unwrap_or("");

        if name == "payload" {
            let text = field.text().await.unwrap();
            payload = Some(serde_json::from_str(&text).unwrap());
        } 
        else if name == "files" {
            let filename = field
                .file_name()
                .map(|s| s.to_string())
                .unwrap_or_else(|| "unknown".to_string());

            let data = field.bytes().await.unwrap();

            println!("Received file: {} ({} bytes)", filename, data.len());

            // TODO: save file if needed
        }
    }

    let payload = match payload {
        Some(p) => p,
        None => return (StatusCode::BAD_REQUEST, "Missing payload").into_response(),
    };

    // ---- your existing logic continues here ----
    println!("SYNCING");

    let mut response: SyncResponse = HashMap::new();

    for (cmd, files) in payload {
        let mut success = Vec::new();
        let mut failure = Vec::new();
        match cmd {
            Operation::Insert => {
                for file in files {
                    let data = sqlx::query_as::<_, FileEntry>(
                        r#"
                        INSERT INTO filehash (file_path, file_hash, file_size, modified_time)
                        VALUES ($1, $2, $3, $4)
                        RETURNING file_path, file_hash, file_size, modified_time
                        "#,
                    )
                    .bind(file.file_path.clone())
                    .bind(file.file_hash)
                    .bind(file.file_size)
                    .bind(file.modified_time)
                    .fetch_one(&pool)
                    .await;

                    match data {
                        Ok(res) => success.push(res),
                        Err(err) => failure.push(
                            FileFailure{
                                file_path: file.file_path,
                                error: err.to_string()
                            }
                        ),
                    };
                };
            }

            Operation::Update => {
                for file in files {
                    let data = sqlx::query_as::<_, FileEntry>(
                        r#"
                        UPDATE filehash
                        SET file_hash = $1,
                            file_size = $2,
                            modified_time = $3
                        WHERE file_path = $4
                        RETURNING file_path, file_hash, file_size, modified_time
                        "#,
                    )
                    .bind(file.file_hash)
                    .bind(file.file_size)
                    .bind(file.modified_time)
                    .bind(file.file_path.clone())
                    .fetch_one(&pool)
                    .await;

                    match data {
                        Ok(row) => success.push(row),
                        Err(e) => failure.push(FileFailure {
                            file_path: file.file_path,
                            error: e.to_string(),
                        }),
                    }
                }
            }

            Operation::Delete => {
                for file in files {
                    let data = sqlx::query(
                        r#"
                        DELETE FROM filehash
                        WHERE file_path = $1
                        "#,
                    )
                    .bind(file.file_path.clone())
                    .execute(&pool)
                    .await;

                    match data {
                        Ok(_) => success.push(file),
                        Err(e) => failure.push(FileFailure {
                            file_path: file.file_path,
                            error: e.to_string(),
                        })
                    }
                }
            }
        }
        response.insert(cmd, OperationResult { success, failure });
    }

    println!("SYNCED");
    (StatusCode::ACCEPTED, Json(response)).into_response()
}

async fn handle_get_all(
    State(pool): State<PgPool>,
) -> impl IntoResponse {
    println!("FETCHING");
    let result = sqlx::query_as::<_, FileEntry>(
        "SELECT file_path, file_hash, file_size, modified_time FROM filehash"
    )
    .fetch_all(&pool)
    .await;

    println!("FETCHED");
    match result {
        Ok(rows) => (
            StatusCode::OK,
            Json(GetAllResponse {
                data: Some(rows),
                error: None,
            }),
        ),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(GetAllResponse {
                data: None,
                error: Some(err.to_string()),
            }),
        ),
    }
}

