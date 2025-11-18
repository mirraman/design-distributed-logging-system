use agent::LogAgent;
use common::{LogEntry, LogLevel, SearchQuery};
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
struct SearchResponse {
    logs: Vec<LogEntry>,
}

/// Integration test - requires all services running:
/// 1. cargo run -p config
/// 2. cargo run -p storage  
/// 3. cargo run -p ingestion
/// 4. cargo run -p search
/// 
/// Then run: cargo test --test integration_test -- --ignored
#[tokio::test]
#[ignore]
async fn test_full_flow() {
    let agent = LogAgent::new("http://localhost:8001".to_string(), 10);
    agent.start_flush_loop().await;

    for i in 0..50 {
        let mut attrs = HashMap::new();
        attrs.insert("test_id".to_string(), format!("test-{}", i));
        attrs.insert("batch".to_string(), "integration-test".to_string());

        let log = LogEntry::new(
            "test-app".to_string(),
            LogLevel::Info,
            format!("Test log #{}", i),
            attrs,
        );
        agent.log(log).await;
    }

    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    let client = reqwest::Client::new();
    let response = client
        .get("http://localhost:8004/search?app_name=test-app&limit=100")
        .send()
        .await
        .expect("Failed to connect to search API");

    assert!(
        response.status().is_success(),
        "Search API returned error: {}",
        response.status()
    );

    let search_result: SearchResponse = response
        .json()
        .await
        .expect("Failed to parse search response");

    assert!(
        search_result.logs.len() >= 50,
        "Expected at least 50 logs, got {}",
        search_result.logs.len()
    );

    println!(" Found {} logs via GET search", search_result.logs.len());
}

#[tokio::test]
#[ignore]
async fn test_search_by_level() {
    let agent = LogAgent::new("http://localhost:8001".to_string(), 5);
    agent.start_flush_loop().await;

    for i in 0..20 {
        let level = match i % 4 {
            0 => LogLevel::Debug,
            1 => LogLevel::Info,
            2 => LogLevel::Warn,
            _ => LogLevel::Error,
        };

        let log = LogEntry::new(
            "level-test-app".to_string(),
            level,
            format!("Level test log #{}", i),
            HashMap::new(),
        );
        agent.log(log).await;
    }

    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    let client = reqwest::Client::new();
    let response = client
        .get("http://localhost:8004/search?app_name=level-test-app&level=Error&limit=100")
        .send()
        .await
        .expect("Failed to connect to search API");

    assert!(response.status().is_success());

    let search_result: SearchResponse = response
        .json()
        .await
        .expect("Failed to parse search response");

    assert!(
        search_result.logs.len() >= 5,
        "Expected at least 5 Error logs, got {}",
        search_result.logs.len()
    );

    for log in &search_result.logs {
        if log.app_name == "level-test-app" {
            assert_eq!(
                log.level,
                LogLevel::Error,
                "Found non-Error log in Error-filtered results"
            );
        }
    }

    println!(
        "Found {} Error level logs",
        search_result.logs.len()
    );
}

#[tokio::test]
#[ignore]
async fn test_search_with_post() {
    let agent = LogAgent::new("http://localhost:8001".to_string(), 5);
    agent.start_flush_loop().await;

    for i in 0..10 {
        let log = LogEntry::new(
            "post-test-app".to_string(),
            LogLevel::Info,
            format!("POST test log #{}", i),
            HashMap::new(),
        );
        agent.log(log).await;
    }

    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    let query = SearchQuery {
        app_name: Some("post-test-app".to_string()),
        level: Some(LogLevel::Info),
        from: None,
        to: None,
        attributes: None,
        limit: Some(20),
    };

    let client = reqwest::Client::new();
    let response = client
        .post("http://localhost:8004/search")
        .json(&query)
        .send()
        .await
        .expect("Failed to connect to search API");

    assert!(response.status().is_success());

    let search_result: SearchResponse = response
        .json()
        .await
        .expect("Failed to parse search response");

    assert!(
        search_result.logs.len() >= 10,
        "Expected at least 10 logs, got {}",
        search_result.logs.len()
    );

    println!(" Found {} logs via POST search", search_result.logs.len());
}

#[tokio::test]
#[ignore]
async fn test_health_endpoints() {
    let client = reqwest::Client::new();

    // Test Ingestion health
    let response = client
        .get("http://localhost:8001/health")
        .send()
        .await
        .expect("Ingestion service not available");
    assert!(response.status().is_success());
    println!(" Ingestion service healthy");

    // Test Search health
    let response = client
        .get("http://localhost:8004/health")
        .send()
        .await
        .expect("Search service not available");
    assert!(response.status().is_success());
    println!(" Search service healthy");
}

#[tokio::test]
#[ignore]
async fn test_rate_limiting() {
    let agent = LogAgent::new("http://localhost:8001".to_string(), 100);
    agent.start_flush_loop().await;


    for i in 0..2000 {
        let log = LogEntry::new(
            "rate-limit-test".to_string(),
            LogLevel::Info,
            format!("Rate limit test #{}", i),
            HashMap::new(),
        );
        agent.log(log).await;
    }

    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    println!(" Rate limiting test completed (check ingestion logs for rate limit messages)");
}

