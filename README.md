# Distributed Log System

## Architecture

```
[Apps] → [Agent] → [Ingestion:8001] → [Storage:8002] → [Search API:8004]
                          ↓
                   [Config:8003]
```

## Quick Start

### 1. Start All Services

```bash
# Terminal 1: Config Service (Port 8003)
cargo run -p config
# Terminal 2: Storage Service (Port 8002)
cargo run -p storage
# Terminal 3: Ingestion Service (Port 8001)
cargo run -p ingestion
# Terminal 4: Search API (Port 8004)
cargo run -p search
```
