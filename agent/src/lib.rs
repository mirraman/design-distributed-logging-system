use common::{LogBatch, LogEntry};
use flate2::write::GzEncoder;
use flate2::Compression;
use std::collections::VecDeque;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{error, info};

pub struct LogAgent {
	buffer: Arc<Mutex<VecDeque<LogEntry>>>,
	batch_size: usize,
	ingestion_url: String,
	client: reqwest::Client,
}

impl LogAgent {
	pub fn new(ingestion_url: String, batch_size: usize) -> Self {
		Self {
			buffer: Arc::new(Mutex::new(VecDeque::new())),
			batch_size,
			ingestion_url,
			client: reqwest::Client::new(),
		}
}

pub async fn log(&self, entry: LogEntry) {
	let mut buffer = self.buffer.lock().await;
	buffer.push_back(entry);

	if buffer.len() >= self.batch_size {
		let logs: Vec<LogEntry> = buffer.drain(..).collect();
		drop(buffer);

		let agent = self.clone();
		tokio::spawn(async move {
			agent.send_batch(logs).await;
		});
	}
}

pub async fn start_flush_loop(&self) {
	let buffer = self.buffer.clone();
	let agent = self.clone();

	tokio::spawn(async move {
		loop {
			sleep(Duration::from_secs(1)).await;

			let mut buf = buffer.lock().await;
			if !buf.is_empty() {
				let logs: Vec<LogEntry> = buf.drain(..).collect();
				drop(buf);

				agent.send_batch(logs).await;
			}
		}
	});
}

async fn send_batch(&self, logs: Vec<LogEntry>) {
	if logs.is_empty() {
		return;
	}

	let batch = LogBatch::new(logs);
	let compressed = Self::compress_batch(&batch);

	for attempt in 1..=3 {
		match self.send_with_compression(&compressed).await {
			Ok(_) => {
				info!("Sent batch {} with {} logs", batch.batch_id, batch.logs.len());
				return;
			}
			Err(e) => {
				error!("Attempt {}/3 failed: {}", attempt, e);
				if attempt < 3 {
					sleep(Duration::from_secs(2u64.pow(attempt))).await;
				} else {
					error!("Failed to send batch after 3 attempts, would save to disk");
                    self.save_to_disk(&batch).await.ok();
				}
			}
		}
	}
}


fn compress_batch(batch: &LogBatch) -> Vec<u8> {
	let json = serde_json::to_vec(batch).unwrap();
	let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
	encoder.write_all(&json).unwrap();
	encoder.finish().unwrap()
}

async fn send_with_compression(&self, data: &[u8]) -> Result<(), anyhow::Error> {
	let response = self
	.client
	.post(&format!("{}/ingest", self.ingestion_url))
	.header("Content-Encoding", "gzip")
	.body(data.to_vec())
	.send()
	.await?;

	if response.status().is_success() {
		Ok(())
	} else {
		Err(anyhow::anyhow!("HTTP {}", response.status()))
	}
}

async fn save_to_disk(&self, batch: &LogBatch) -> Result<(), anyhow::Error> {
	let filename = format!("failed_batch_{}.json", batch.batch_id);
	let json = serde_json::to_string_pretty(batch)?;
	tokio::fs::write(&filename, json).await?;
	info!("Saved batch to {}", filename);
	Ok(())
}
}

impl Clone for LogAgent {
	fn clone(&self) -> Self {
		Self {
			buffer: self.buffer.clone(),
			batch_size: self.batch_size,
			ingestion_url: self.ingestion_url.clone(),
			client: self.client.clone(),
		}
	}
}