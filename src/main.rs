use std::env;
use axum::{Router, routing::get};
use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() {
    let db_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let pool = PgPoolOptions::new().connect(&db_url).await.expect("Failed to connect to DB");
    sqlx::migrate!().run(&pool).await.expect("Migrations failed");

    let app = Router::new()
        .route("/", get(root))
        .with_state(pool);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8000").await.unwrap();
    println!("Server running on port 8000");
    axum::serve(listener, app).await.unwrap();
}

async fn root() -> &'static str {
    "Pocket Drive is running!"
}
