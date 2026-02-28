use std::{collections::HashMap, env, fs::{self, File}, io::Write, time::Duration};
use aws_sdk_s3::{self as s3, presigning::PresigningConfig, primitives::ByteStream, Client};


use axum::{
    extract::{ Multipart, Query, State},
    http::{header, StatusCode},
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
    file_name: String,
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

#[derive(Clone)]
struct AppState{
    pool: PgPool,
    s3client: Client
}

#[tokio::main]
async fn main() {
    let db_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let config = aws_config::load_from_env().await;
    let client = s3::Client::new(&config);
    
    let list_buckets_output = client.list_buckets().send().await.unwrap();
    if let Some(buckets) = list_buckets_output.buckets {
        for bucket in buckets {
            println!("Bucket name: {:?}", bucket.name());
        }
    }

    fs::create_dir_all("/data").unwrap();

    let pool = PgPoolOptions::new()
        .connect(&db_url)
        .await
        .expect("Failed to connect to DB");

    sqlx::migrate!().run(&pool).await.expect("Migrations failed");

    let appstate = AppState { pool, s3client: client };

    let app = Router::new()
        .route("/", get(root))
        .route("/sync", post(handle_sync))
        .route("/get", get(handle_get_all))
        .route("/download", get(handle_file_download))
        .with_state(appstate);

    let port = std::env::var("PORT")
        .unwrap_or_else(|_| "8000".to_string());

    let addr = format!("0.0.0.0:{}", port);

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .unwrap();

    println!("Server running on {}", addr);

    axum::serve(listener, app).await.unwrap();
}

async fn root() -> &'static str {
     println!("ROOT HIT");
    "Pocket Drive is running!"
}

async fn handle_sync(
    State(state): State<AppState>,
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
            let key = generate_system_path(&filename);
            state.s3client
                .put_object()
                .bucket("pocket-directory")
                .key(&key)
                .body(ByteStream::from(data.to_vec()))
                .content_type("application/octet-stream")
                .send()
                .await
                .unwrap();

            //Instead of saving, save the file to s3
            println!("Uploaded to S3 with key: {}", key);
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
                    let filename = generate_system_path(&file.file_name);
                    let data = sqlx::query_as::<_, FileEntry>(
                        r#"
                        INSERT INTO filehash (file_path, file_hash, file_size, modified_time, system_path)
                        VALUES ($1, $2, $3, $4, $5)
                        RETURNING file_path, file_hash, file_size, modified_time, system_path AS file_name
                        "#,
                    )
                    .bind(file.file_path.clone())
                    .bind(file.file_hash)
                    .bind(file.file_size)
                    .bind(file.modified_time)
                    .bind(filename)
                    .fetch_one(&state.pool)
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
                        RETURNING file_path, file_hash, file_size, modified_time, system_path AS file_name
                        "#,
                    )
                    .bind(file.file_hash)
                    .bind(file.file_size)
                    .bind(file.modified_time)
                    .bind(file.file_path.clone())
                    .fetch_one(&state.pool)
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
                    let data = sqlx::query_scalar::<_, String>(
                        r#"
                        DELETE FROM filehash
                        WHERE file_path = $1
                        RETURNING system_path
                        "#
                    )
                    .bind(file.file_path.clone())
                    .fetch_optional(&state.pool)
                    .await;

                    match data {
                        Ok(Some(system_path)) => {
                            match state.s3client
                                .delete_object()
                                .bucket("pocket-directory")
                                .key(&system_path)
                                .send()
                                .await {
                                Ok(_) => success.push(file),
                                Err(e) => failure.push(FileFailure {
                                    file_path: file.file_path,
                                    error: format!("File delete failed: {}", e),
                                }),
                            }
                        },
                        Ok(None) => {
                            failure.push(FileFailure { file_path: file.file_path, error: "file not found in DB" .into() });
                        }
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
    State(state): State<AppState>,
) -> impl IntoResponse {
    println!("FETCHING");
    let result = sqlx::query_as::<_, FileEntry>(
        "SELECT file_path, file_hash, file_size, modified_time, system_path AS file_name FROM filehash"
    )
    .fetch_all(&state.pool)
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

async fn handle_file_download(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {

    let key = match params.get("path") {
        Some(k) => k,
        None => return (StatusCode::BAD_REQUEST, Json(serde_json::json!({
            "error": "Missing path"
        }))).into_response(),
    };

    let presigned_request = match state.s3client
        .get_object()
        .bucket("pocket-directory")
        .key(key)
        .presigned(
            PresigningConfig::expires_in(Duration::from_secs(300))
                .unwrap()
        )
        .await
    {
        Ok(req) => req,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": format!("Failed to generate URL: {}", e)
                }))
            ).into_response()
        }
    };

    let url = presigned_request.uri().to_string();

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "url": url,
            "expires_in_seconds": 300
        }))
    ).into_response()
}


fn generate_system_path(filename: &str) -> String {
    let mut s = String::from("data/");
    s.push_str(filename);
    s
}
