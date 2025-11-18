use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::post, Json, Router};
use chrono::{Duration, Utc};
use common::{LogBatch, LogEntry, SearchQuery};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

/// Упрощенное хранилище (в реальности - Elasticsearch)
//TODO: Replace with actual storage
struct LogStorage {
    hot_storage: Arc<RwLock<Vec<LogEntry>>>, // 7 дней
    cold_storage: Arc<RwLock<Vec<LogEntry>>>, // 30 дней
    index: Arc<RwLock<HashMap<String, Vec<usize>>>>, // app_name -> индексы
}

impl LogStorage {
    fn new() -> Self {
        Self {
            hot_storage: Arc::new(RwLock::new(Vec::new())),
            cold_storage: Arc::new(RwLock::new(Vec::new())),
            index: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn store(&self, batch: LogBatch) {
        let mut hot = self.hot_storage.write().await;
        let mut index = self.index.write().await;

        for log in batch.logs {
            let idx = hot.len();
            
            index
                .entry(log.app_name.clone())
                .or_insert_with(Vec::new)
                .push(idx);

            hot.push(log);
        }

        info!("Stored batch, total logs: {}", hot.len());
    }

    async fn search(&self, query: SearchQuery) -> Vec<LogEntry> {
        let hot = self.hot_storage.read().await;
        let cold = self.cold_storage.read().await;

        let mut results = Vec::new();

        for log in hot.iter() {
            if self.matches(&log, &query) {
                results.push(log.clone());
            }
        }

        if results.len() < query.limit.unwrap_or(100) {
            for log in cold.iter() {
                if self.matches(&log, &query) {
                    results.push(log.clone());
                }
            }
        }

        results.truncate(query.limit.unwrap_or(100));
        results
    }

    fn matches(&self, log: &LogEntry, query: &SearchQuery) -> bool {
        if let Some(ref app) = query.app_name {
            if &log.app_name != app {
                return false;
            }
        }

        if let Some(ref level) = query.level {
            if &log.level != level {
                return false;
            }
        }

        if let Some(from) = query.from {
            if log.timestamp < from {
                return false;
            }
        }

        if let Some(to) = query.to {
            if log.timestamp > to {
                return false;
            }
        }

        if let Some(ref attrs) = query.attributes {
            for (key, value) in attrs {
                if log.attributes.get(key) != Some(value) {
                    return false;
                }
            }
        }

        true
    }

    async fn start_archiving(&self) {
        let hot = self.hot_storage.clone();
        let cold = self.cold_storage.clone();

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await; 

                let mut hot_store = hot.write().await;
                let mut cold_store = cold.write().await;

                let now = Utc::now();
                let seven_days_ago = now - Duration::days(7);

                let (old, new): (Vec<LogEntry>, Vec<LogEntry>) = hot_store
                    .drain(..)
                    .partition(|log| log.timestamp < seven_days_ago);

                cold_store.extend(old);
                *hot_store = new;

                info!("Archived {} logs to cold storage", cold_store.len());

                let thirty_days_ago = now - Duration::days(30);
                cold_store.retain(|log| log.timestamp > thirty_days_ago);
            }
        });
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let storage = Arc::new(LogStorage::new());
    storage.start_archiving().await;

    let app = Router::new()
        .route("/store", post(store_logs))
        .route("/search", post(search_logs))
        .with_state(storage);

    info!("Storage service starting on :8002");
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8002").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn store_logs(
    State(storage): State<Arc<LogStorage>>,
    Json(batch): Json<LogBatch>,
) -> impl IntoResponse {
    storage.store(batch).await;
    StatusCode::OK
}

async fn search_logs(
    State(storage): State<Arc<LogStorage>>,
    Json(query): Json<SearchQuery>,
) -> impl IntoResponse {
    let results = storage.search(query).await;
    (StatusCode::OK, Json(results))
}