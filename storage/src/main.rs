use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::post, Json, Router};
use chrono::{DateTime, Duration, Utc};
use common::{LogBatch, LogEntry, LogLevel, SearchQuery};
use elasticsearch::{
    http::transport::{SingleNodeConnectionPool, TransportBuilder},
    Elasticsearch, SearchParts, DeleteByQueryParts, BulkOperation,
};
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::{error, info, warn};
use url::Url;

const HOT_INDEX: &str = "logs-hot";
const COLD_INDEX: &str = "logs-cold";

struct LogStorage {
    client: Elasticsearch,
}

impl LogStorage {
    async fn new(elasticsearch_url: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let url = Url::parse(elasticsearch_url)?;
        
        let conn_pool = SingleNodeConnectionPool::new(url);
        let transport = TransportBuilder::new(conn_pool).disable_proxy().build()?;
        let client = Elasticsearch::new(transport);

        match client.ping().send().await {
            Ok(_) => info!("Connected to Elasticsearch at {}", elasticsearch_url),
            Err(e) => {
                error!("Failed to connect to Elasticsearch: {}", e);
                return Err(Box::new(e));
            }
        }

        let storage = Self { client };
        
        storage.init_indices().await?;
        
        Ok(storage)
    }

    async fn init_indices(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.create_index_if_not_exists(HOT_INDEX).await?;
        
        self.create_index_if_not_exists(COLD_INDEX).await?;
        
        Ok(())
    }

    async fn create_index_if_not_exists(&self, index_name: &str) -> Result<(), Box<dyn std::error::Error>> {
        let exists = self
            .client
            .indices()
            .exists(elasticsearch::indices::IndicesExistsParts::Index(&[index_name]))
            .send()
            .await?;

        if exists.status_code().is_success() {
            info!("Index '{}' already exists", index_name);
            return Ok(());
        }

        let response = self
            .client
            .indices()
            .create(elasticsearch::indices::IndicesCreateParts::Index(index_name))
            .body(json!({
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0,
                    "refresh_interval": "5s"
                },
                "mappings": {
                    "properties": {
                        "id": { "type": "keyword" },
                        "app_name": { "type": "keyword" },
                        "level": { "type": "keyword" },
                        "timestamp": { "type": "date" },
                        "message": { 
                            "type": "text",
                            "fields": {
                                "keyword": { "type": "keyword", "ignore_above": 256 }
                            }
                        },
                        "attributes": { "type": "object" }
                    }
                }
            }))
            .send()
            .await?;

        if response.status_code().is_success() {
            info!("Created index '{}'", index_name);
        } else {
            warn!("Failed to create index '{}': {:?}", index_name, response.status_code());
        }

        Ok(())
    }

    async fn store(&self, batch: LogBatch) {
        let mut operations: Vec<BulkOperation<_>> = Vec::new();

        for log in &batch.logs {
            let doc = json!({
                "id": log.id,
                "app_name": log.app_name,
                "level": format!("{:?}", log.level),
                "timestamp": log.timestamp.to_rfc3339(),
                "message": log.message,
                "attributes": log.attributes
            });
            
            operations.push(BulkOperation::index(doc).id(&log.id).into());
        }

        let response = self
            .client
            .bulk(elasticsearch::BulkParts::Index(HOT_INDEX))
            .body(operations)
            .send()
            .await;

        match response {
            Ok(resp) => {
                if resp.status_code().is_success() {
                    info!("Stored batch {} with {} logs to Elasticsearch", batch.batch_id, batch.logs.len());
                } else {
                    error!("Failed to store batch: {:?}", resp.status_code());
                }
            }
            Err(e) => error!("Elasticsearch error: {}", e),
        }
    }

    async fn search(&self, query: SearchQuery) -> Vec<LogEntry> {
        let mut must_clauses: Vec<Value> = Vec::new();

        if let Some(app_name) = &query.app_name {
            must_clauses.push(json!({ "term": { "app_name": app_name } }));
        }

        if let Some(level) = &query.level {
            must_clauses.push(json!({ "term": { "level": format!("{:?}", level) } }));
        }

        if query.from.is_some() || query.to.is_some() {
            let mut range = json!({});
            if let Some(from) = query.from {
                range["gte"] = json!(from.to_rfc3339());
            }
            if let Some(to) = query.to {
                range["lte"] = json!(to.to_rfc3339());
            }
            must_clauses.push(json!({ "range": { "timestamp": range } }));
        }

        if let Some(attributes) = &query.attributes {
            for (key, value) in attributes {
                must_clauses.push(json!({
                    "term": { format!("attributes.{}", key): value }
                }));
            }
        }

        let search_body = json!({
            "query": {
                "bool": {
                    "must": if must_clauses.is_empty() { 
                        vec![json!({ "match_all": {} })] 
                    } else { 
                        must_clauses 
                    }
                }
            },
            "size": query.limit.unwrap_or(100),
            "sort": [{ "timestamp": { "order": "desc" } }]
        });

        let response = self
            .client
            .search(SearchParts::Index(&[HOT_INDEX, COLD_INDEX]))
            .body(search_body)
            .send()
            .await;

        match response {
            Ok(resp) => {
                if let Ok(body) = resp.json::<Value>().await {
                    let hits = body["hits"]["hits"].as_array();
                    
                    if let Some(hits) = hits {
                        let logs: Vec<LogEntry> = hits
                            .iter()
                            .filter_map(|hit| {
                                let source = &hit["_source"];
                                self.parse_log_entry(source)
                            })
                            .collect();

                        info!("Found {} logs matching query", logs.len());
                        return logs;
                    }
                }
                error!("Failed to parse search response");
                Vec::new()
            }
            Err(e) => {
                error!("Search error: {}", e);
                Vec::new()
            }
        }
    }

    fn parse_log_entry(&self, source: &Value) -> Option<LogEntry> {
        let level_str = source["level"].as_str()?;
        let level = match level_str {
            "Debug" => LogLevel::Debug,
            "Info" => LogLevel::Info,
            "Warn" => LogLevel::Warn,
            "Error" => LogLevel::Error,
            _ => return None,
        };

        let timestamp_str = source["timestamp"].as_str()?;
        let timestamp = DateTime::parse_from_rfc3339(timestamp_str)
            .ok()?
            .with_timezone(&Utc);

        let attributes = source["attributes"]
            .as_object()
            .map(|obj| {
                obj.iter()
                    .filter_map(|(k, v)| {
                        v.as_str().map(|s| (k.clone(), s.to_string()))
                    })
                    .collect()
            })
            .unwrap_or_default();

        Some(LogEntry {
            id: source["id"].as_str()?.to_string(),
            app_name: source["app_name"].as_str()?.to_string(),
            level,
            timestamp,
            message: source["message"].as_str()?.to_string(),
            attributes,
        })
    }

    async fn start_archiving(&self) {
        let client = self.client.clone();

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(3600)).await;

                info!("Starting archiving process...");

                let now = Utc::now();
                let seven_days_ago = now - Duration::days(7);

                let response = client
                    .reindex()
                    .body(json!({
                        "source": {
                            "index": HOT_INDEX,
                            "query": {
                                "range": {
                                    "timestamp": {
                                        "lt": seven_days_ago.to_rfc3339()
                                    }
                                }
                            }
                        },
                        "dest": {
                            "index": COLD_INDEX
                        }
                    }))
                    .send()
                    .await;

                match response {
                    Ok(resp) => {
                        if resp.status_code().is_success() {
                            info!("Moved old logs to cold storage");

                            let delete_response = client
                                .delete_by_query(DeleteByQueryParts::Index(&[HOT_INDEX]))
                                .body(json!({
                                    "query": {
                                        "range": {
                                            "timestamp": {
                                                "lt": seven_days_ago.to_rfc3339()
                                            }
                                        }
                                    }
                                }))
                                .send()
                                .await;

                            if let Ok(del_resp) = delete_response {
                                if del_resp.status_code().is_success() {
                                    info!("Cleaned up hot storage");
                                }
                            }
                        }
                    }
                    Err(e) => error!("Archiving error: {}", e),
                }

                let thirty_days_ago = now - Duration::days(30);
                let cleanup_response = client
                    .delete_by_query(DeleteByQueryParts::Index(&[COLD_INDEX]))
                    .body(json!({
                        "query": {
                            "range": {
                                "timestamp": {
                                    "lt": thirty_days_ago.to_rfc3339()
                                }
                            }
                        }
                    }))
                    .send()
                    .await;

                match cleanup_response {
                    Ok(resp) => {
                        if resp.status_code().is_success() {
                            info!("Cleaned up cold storage (>30 days)");
                        }
                    }
                    Err(e) => error!("Cleanup error: {}", e),
                }

                info!("Archiving process completed");
            }
        });
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let elasticsearch_url = std::env::var("ELASTICSEARCH_URL")
        .unwrap_or_else(|_| "http://localhost:9200".to_string());

    info!("Starting Storage service...");
    info!("Elasticsearch URL: {}", elasticsearch_url);

    let storage = match LogStorage::new(&elasticsearch_url).await {
        Ok(s) => Arc::new(s),
        Err(e) => {
            error!("Failed to initialize Elasticsearch storage: {}", e);
            error!("Make sure Elasticsearch is running at {}", elasticsearch_url);
            std::process::exit(1);
        }
    };

    storage.start_archiving().await;

    let app = Router::new()
        .route("/store", post(store_logs))
        .route("/search", post(search_logs))
        .with_state(storage);

    info!("Storage service ready on :8002");
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
