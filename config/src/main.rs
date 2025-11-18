use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::{get, post}, Json, Router};
use common::QuotaConfig;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

struct ConfigStore {
    quotas: Arc<RwLock<HashMap<String, QuotaConfig>>>,
}

impl ConfigStore {
    fn new() -> Self {
        let mut quotas = HashMap::new();
        
        quotas.insert(
            "user-service".to_string(),
            QuotaConfig {
                app_name: "user-service".to_string(),
                logs_per_second: 1000,
            },
        );
        quotas.insert(
            "payment-service".to_string(),
            QuotaConfig {
                app_name: "payment-service".to_string(),
                logs_per_second: 5000,
            },
        );

        Self {
            quotas: Arc::new(RwLock::new(quotas)),
        }
    }

    async fn get_quotas(&self) -> Vec<QuotaConfig> {
        self.quotas.read().await.values().cloned().collect()
    }

    async fn update_quota(&self, config: QuotaConfig) {
        let mut quotas = self.quotas.write().await;
        info!("Updating quota for {}: {} logs/sec", config.app_name, config.logs_per_second);
        quotas.insert(config.app_name.clone(), config);
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let store = Arc::new(ConfigStore::new());

    let app = Router::new()
        .route("/quotas", get(get_quotas))
        .route("/quotas", post(update_quota))
        .with_state(store);

    info!("Config service starting on :8003");
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8003").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn get_quotas(State(store): State<Arc<ConfigStore>>) -> impl IntoResponse {
    let quotas = store.get_quotas().await;
    (StatusCode::OK, Json(quotas))
}

async fn update_quota(
    State(store): State<Arc<ConfigStore>>,
    Json(config): Json<QuotaConfig>,
) -> impl IntoResponse {
    store.update_quota(config).await;
    StatusCode::OK
}