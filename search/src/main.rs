use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use common::{LogEntry, SearchQuery};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tower_http::trace::TraceLayer;
use tracing::{error, info};

struct AppState {
    storage_url: String,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let state = Arc::new(AppState {
        storage_url: "http://localhost:8002".to_string(),
    });

    let app = Router::new()
        .route("/search", post(search_logs))
        .route("/search", get(search_logs_get))
        .route("/health", get(|| async { "OK" }))
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    info!("Search API service starting on :8004");
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8004")
        .await
        .unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn search_logs(
    State(state): State<Arc<AppState>>,
    Json(query): Json<SearchQuery>,
) -> impl IntoResponse {
    info!("Received search request: {:?}", query);

    let client = reqwest::Client::new();
    match client
        .post(&format!("{}/search", state.storage_url))
        .json(&query)
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() => {
            match resp.json::<Vec<LogEntry>>().await {
                Ok(logs) => {
                    info!("Found {} logs", logs.len());
                    (StatusCode::OK, Json(SearchResponse { logs })).into_response()
                }
                Err(e) => {
                    error!("Failed to parse response: {}", e);
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "Failed to parse results",
                    )
                        .into_response()
                }
            }
        }
        Ok(resp) => {
            error!("Storage returned {}", resp.status());
            (StatusCode::INTERNAL_SERVER_ERROR, "Storage error").into_response()
        }
        Err(e) => {
            error!("Failed to connect to storage: {}", e);
            (StatusCode::SERVICE_UNAVAILABLE, "Storage unavailable").into_response()
        }
    }
}

#[derive(Debug, Deserialize)]
struct SearchQueryParams {
    app_name: Option<String>,
    level: Option<String>,
    limit: Option<usize>,
}

async fn search_logs_get(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SearchQueryParams>,
) -> impl IntoResponse {
    info!("Received GET search request: {:?}", params);

    let level = params.level.and_then(|l| match l.as_str() {
        "Debug" => Some(common::LogLevel::Debug),
        "Info" => Some(common::LogLevel::Info),
        "Warn" => Some(common::LogLevel::Warn),
        "Error" => Some(common::LogLevel::Error),
        _ => None,
    });

    let query = SearchQuery {
        app_name: params.app_name,
        level,
        from: None,
        to: None,
        attributes: None,
        limit: params.limit,
    };

    let client = reqwest::Client::new();
    match client
        .post(&format!("{}/search", state.storage_url))
        .json(&query)
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() => {
            match resp.json::<Vec<LogEntry>>().await {
                Ok(logs) => {
                    info!("Found {} logs", logs.len());
                    (StatusCode::OK, Json(SearchResponse { logs })).into_response()
                }
                Err(e) => {
                    error!("Failed to parse response: {}", e);
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "Failed to parse results",
                    )
                        .into_response()
                }
            }
        }
        Ok(resp) => {
            error!("Storage returned {}", resp.status());
            (StatusCode::INTERNAL_SERVER_ERROR, "Storage error").into_response()
        }
        Err(e) => {
            error!("Failed to connect to storage: {}", e);
            (StatusCode::SERVICE_UNAVAILABLE, "Storage unavailable").into_response()
        }
    }
}

#[derive(Debug, Serialize)]
struct SearchResponse {
    logs: Vec<LogEntry>,
}
