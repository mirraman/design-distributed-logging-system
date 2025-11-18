use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum LogLevel {
	Debug,
	Info,
	Warn,
	Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
	pub id: String,
	pub app_name: String,
	pub level: LogLevel,
	pub timestamp: DateTime<Utc>,
	pub message: String,
	pub attributes: HashMap<String, String>,
}

impl LogEntry {
	pub fn new(
		app_name: String,
		level: LogLevel,
		message: String,
		attributes: HashMap<String, String>,
	) -> Self {
			Self{
				id: Uuid::new_v4().to_string(),
				app_name,
				level,
				timestamp: Utc::now(),
				message,
				attributes,
			}
	}

	pub fn mask_secrets(&mut self) {
		use regex::Regex;

		let patterns = vec![
			(Regex::new(r"\b\d{16}\b").unwrap(), "****-****-****-****"), 
			(Regex::new(r"password[=:]\s*\S+").unwrap(), "password=***"),
			(Regex::new(r"token[=:]\s*\S+").unwrap(), "token=***"),      
			(Regex::new(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b").unwrap(), "***@***.com"), 
	];

		for (pattern, replacement) in &patterns {
			self.message = pattern.replace_all(&self.message, *replacement).to_string();
	}	
		for(key, value) in self.attributes.iter_mut() {
			if key.to_lowercase().contains("password") 
			|| key.to_lowercase().contains("token")
			|| key.to_lowercase().contains("secret") {
				*value = "***".to_string();
			}
		}
	}	
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogBatch {
	pub logs: Vec<LogEntry>,
	pub batch_id: String,
}

impl LogBatch {
	pub fn new(logs: Vec<LogEntry>) -> Self {
		Self {
			logs,
			batch_id: Uuid::new_v4().to_string(),
		}
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchQuery {
	pub app_name: Option<String>,
	pub level: Option<LogLevel>,
	pub from: Option<DateTime<Utc>>,
	pub to: Option<DateTime<Utc>>,
	pub attributes: Option<HashMap<String, String>>,
	pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaConfig {
	pub app_name: String, 
	pub logs_per_second: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum LogSystemError {
	#[error("Rate limit exceeded: {0}")]
	RateLimitExceeded(String),
	#[error("Storage error: {0}")]
	StorageError(String),
	#[error("Network error: {0}")]
	NetworkError(String),
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_log_entry_creation() {
		let mut attrs = HashMap::new();
		attrs.insert("user_id".to_string(), "123".to_string());

		let log = LogEntry::new(
			"test-app".to_string(),
			LogLevel::Info,
			"Test message".to_string(),
			attrs.clone(),
		);

		assert_eq!(log.app_name, "test-app");
		assert_eq!(log.level, LogLevel::Info);
		assert_eq!(log.message, "Test message");
		assert_eq!(log.attributes.get("user_id"), Some(&"123".to_string()));
		assert!(!log.id.is_empty());
	}

	#[test]
	fn test_mask_credit_card() {
		let mut log = LogEntry::new(
			"payment-app".to_string(),
			LogLevel::Info,
			"Payment with card 1234567812345678 processed".to_string(),
			HashMap::new(),
		);

		log.mask_secrets();

		assert!(log.message.contains("****-****-****-****"));
		assert!(!log.message.contains("1234567812345678"));
	}

	#[test]
	fn test_mask_password() {
		let mut log = LogEntry::new(
			"auth-app".to_string(),
			LogLevel::Debug,
			"User login with password=secret123".to_string(),
			HashMap::new(),
		);

		log.mask_secrets();

		assert!(log.message.contains("password=***"));
		assert!(!log.message.contains("secret123"));
	}

	#[test]
	fn test_mask_token() {
		let mut log = LogEntry::new(
			"api-app".to_string(),
			LogLevel::Info,
			"API request with token:Bearer_abc123xyz".to_string(),
			HashMap::new(),
		);

		log.mask_secrets();

		assert!(log.message.contains("token=***"));
		assert!(!log.message.contains("Bearer_abc123xyz"));
	}

	#[test]
	fn test_mask_email() {
		let mut log = LogEntry::new(
			"user-app".to_string(),
			LogLevel::Info,
			"User registered: test@example.com".to_string(),
			HashMap::new(),
		);

		log.mask_secrets();

		assert!(log.message.contains("***@***.com"));
		assert!(!log.message.contains("test@example.com"));
	}

	#[test]
	fn test_mask_attributes() {
		let mut attrs = HashMap::new();
		attrs.insert("user_password".to_string(), "secret".to_string());
		attrs.insert("api_token".to_string(), "abc123".to_string());
		attrs.insert("user_secret".to_string(), "hidden".to_string());
		attrs.insert("user_name".to_string(), "John".to_string());

		let mut log = LogEntry::new(
			"test-app".to_string(),
			LogLevel::Info,
			"Test".to_string(),
			attrs,
		);

		log.mask_secrets();

		assert_eq!(log.attributes.get("user_password"), Some(&"***".to_string()));
		assert_eq!(log.attributes.get("api_token"), Some(&"***".to_string()));
		assert_eq!(log.attributes.get("user_secret"), Some(&"***".to_string()));
		assert_eq!(log.attributes.get("user_name"), Some(&"John".to_string()));
	}

	#[test]
	fn test_log_batch_creation() {
		let logs = vec![
			LogEntry::new(
				"app1".to_string(),
				LogLevel::Info,
				"Log 1".to_string(),
				HashMap::new(),
			),
			LogEntry::new(
				"app2".to_string(),
				LogLevel::Error,
				"Log 2".to_string(),
				HashMap::new(),
			),
		];

		let batch = LogBatch::new(logs.clone());

		assert_eq!(batch.logs.len(), 2);
		assert!(!batch.batch_id.is_empty());
	}

	#[test]
	fn test_log_level_serialization() {
		let info = LogLevel::Info;
		let json = serde_json::to_string(&info).unwrap();
		assert_eq!(json, "\"Info\"");

		let deserialized: LogLevel = serde_json::from_str(&json).unwrap();
		assert_eq!(deserialized, LogLevel::Info);
	}

	#[test]
	fn test_search_query_creation() {
		let query = SearchQuery {
			app_name: Some("test-app".to_string()),
			level: Some(LogLevel::Error),
			from: None,
			to: None,
			attributes: None,
			limit: Some(100),
		};

		assert_eq!(query.app_name, Some("test-app".to_string()));
		assert_eq!(query.level, Some(LogLevel::Error));
		assert_eq!(query.limit, Some(100));
	}
}
