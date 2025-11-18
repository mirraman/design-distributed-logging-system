use axum::{
	extract::State,
	http::StatusCode,
	response::IntoResponse,
	routing::post,
	Json, Router,
};
use common::{LogBatch, LogSystemError, QuotaConfig};
use flate2::read::GzDecoder;
use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;
use tokio::sync::RwLock;
use tower_http::trace::TraceLayer;
use tracing::{error, info};

struct RateLimiter {
	quotas: Arc<RwLock<HashMap<String, QuotaConfig>>>,
	tokens: Arc<RwLock<HashMap<String, (u64, std::time::Instant)>>>,
}

impl RateLimiter {
	fn new() -> Self {
			Self {
					quotas: Arc::new(RwLock::new(HashMap::new())),
					tokens: Arc::new(RwLock::new(HashMap::new())),
			}
	}

	async fn check_rate(&self, app_name: &str, count: u64) -> Result<(), LogSystemError> {
			let quotas = self.quotas.read().await;
			let limit = quotas
					.get(app_name)
					.map(|q| q.logs_per_second)
					.unwrap_or(1000); 

			let mut tokens = self.tokens.write().await;
			let now = std::time::Instant::now();

			let (available, last_update) = tokens
					.get(app_name)
					.copied()
					.unwrap_or((limit, now));

			let elapsed = now.duration_since(last_update).as_secs_f64();
			let new_tokens = (available as f64 + elapsed * limit as f64).min(limit as f64) as u64;

			if new_tokens >= count {
					tokens.insert(app_name.to_string(), (new_tokens - count, now));
					Ok(())
			} else {
					Err(LogSystemError::RateLimitExceeded(app_name.to_string()))
			}
	}

	async fn update_quota(&self, config: QuotaConfig) {
			let mut quotas = self.quotas.write().await;
			quotas.insert(config.app_name.clone(), config);
			info!("Updated quota for {}", quotas.len());
	}

	async fn load_quotas_from_config(&self, config_url: &str) {
			let limiter = self.clone();
			let url = config_url.to_string();

			tokio::spawn(async move {
					loop {
							tokio::time::sleep(std::time::Duration::from_secs(10)).await;

					match reqwest::get(&format!("{}/quotas", url)).await {
							Ok(resp) => {
									if let Ok(configs) = resp.json::<Vec<QuotaConfig>>().await {
													for config in configs {
															limiter.update_quota(config).await;
													}
											}
									}
									Err(e) => error!("Failed to fetch quotas: {}", e),
							}
					}
			});
	}
}

impl Clone for RateLimiter {
	fn clone(&self) -> Self {
			Self {
					quotas: self.quotas.clone(),
					tokens: self.tokens.clone(),
			}
	}
}

struct AppState {
	rate_limiter: RateLimiter,
	storage_url: String,
}

#[tokio::main]
async fn main() {
	tracing_subscriber::fmt::init();

	let rate_limiter = RateLimiter::new();
	rate_limiter.load_quotas_from_config("http://localhost:8003").await;

	let state = Arc::new(AppState {
			rate_limiter,
			storage_url: "http://localhost:8002".to_string(),
	});

	let app = Router::new()
			.route("/ingest", post(ingest_logs))
			.route("/health", axum::routing::get(|| async { "OK" }))
			.layer(TraceLayer::new_for_http())
			.with_state(state);

	info!("Ingestion service starting on :8001");
	let listener = tokio::net::TcpListener::bind("0.0.0.0:8001").await.unwrap();
	axum::serve(listener, app).await.unwrap();
}

async fn ingest_logs(
	State(state): State<Arc<AppState>>,
	body: axum::body::Bytes,
) -> impl IntoResponse {
	// Распаковка gzip
	let mut decoder = GzDecoder::new(&body[..]);
	let mut decompressed = Vec::new();
	if let Err(e) = decoder.read_to_end(&mut decompressed) {
			error!("Decompression error: {}", e);
			return (StatusCode::BAD_REQUEST, "Invalid gzip").into_response();
	}

	let mut batch: LogBatch = match serde_json::from_slice(&decompressed) {
			Ok(b) => b,
			Err(e) => {
					error!("JSON parse error: {}", e);
					return (StatusCode::BAD_REQUEST, "Invalid JSON").into_response();
			}
	};

	// Проверка квоты
	if !batch.logs.is_empty() {
			let app_name = &batch.logs[0].app_name;
			let count = batch.logs.len() as u64;

			if let Err(e) = state.rate_limiter.check_rate(app_name, count).await {
					error!("Rate limit exceeded: {}", e);
					return (StatusCode::TOO_MANY_REQUESTS, format!("{}", e)).into_response();
			}
	}

	for log in &mut batch.logs {
			log.mask_secrets();
	}

	let client = reqwest::Client::new();
	match client
			.post(&format!("{}/store", state.storage_url))
			.json(&batch)
			.send()
			.await
	{
			Ok(resp) if resp.status().is_success() => {
					info!("Stored batch {} with {} logs", batch.batch_id, batch.logs.len());
					(StatusCode::OK, Json(serde_json::json!({"status": "ok"}))).into_response()
			}
			Ok(resp) => {
					error!("Storage returned {}", resp.status());
					(StatusCode::INTERNAL_SERVER_ERROR, "Storage error").into_response()
			}
			Err(e) => {
					error!("Failed to send to storage: {}", e);
					(StatusCode::INTERNAL_SERVER_ERROR, "Storage unavailable").into_response()
			}
	}
}