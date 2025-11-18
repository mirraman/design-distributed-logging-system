use agent::LogAgent;
use common::{LogEntry, LogLevel};
use std::collections::HashMap;
use tracing::info;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let agent = LogAgent::new("http://localhost:8001".to_string(), 300);
    
    agent.start_flush_loop().await;

    info!("Example app started, generating logs...");

    for i in 0..1000 {
        let mut attrs = HashMap::new();
        attrs.insert("request_id".to_string(), format!("req-{}", i));
        attrs.insert("user_id".to_string(), format!("user-{}", i % 100));

        let level = match i % 4 {
            0 => LogLevel::Debug,
            1 => LogLevel::Info,
            2 => LogLevel::Warn,
            _ => LogLevel::Error,
        };

        let log = LogEntry::new(
            "user-service".to_string(),
            level,
            format!("Processing request #{}", i),
            attrs,
        );

        agent.log(log).await;

        if i % 100 == 0 {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            info!("Generated {} logs", i);
        }
    }

    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    info!("All logs sent!");
}

