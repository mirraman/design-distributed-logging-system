# Integration Tests

This directory contains integration tests for the distributed logging system.

## Prerequisites

Before running integration tests, you need to start all services:

### Terminal 1: Config Service

```bash
cargo run -p config
```

### Terminal 2: Storage Service

```bash
cargo run -p storage
```

### Terminal 3: Ingestion Service

```bash
cargo run -p ingestion
```

### Terminal 4: Search API Service

```bash
cargo run -p search
```

## Running Tests

Once all services are running:

```bash
# Run all integration tests
cargo test --package integration-tests -- --ignored

# Run specific test
cargo test --package integration-tests test_full_flow -- --ignored

# Run with output
cargo test --package integration-tests -- --ignored --nocapture
```

## Available Tests

- **test_full_flow**: Tests the complete flow from agent → ingestion → storage → search
- **test_search_by_level**: Tests filtering logs by level (Debug, Info, Warn, Error)
- **test_search_with_post**: Tests POST search endpoint with JSON body
- **test_health_endpoints**: Verifies all service health endpoints
- **test_rate_limiting**: Tests rate limiting behavior (may not trigger with default limits)

## Expected Results

All tests should pass if services are running correctly. You should see output like:

```
running 5 tests
test test_health_endpoints ... ok
test test_full_flow ... ok
test test_search_by_level ... ok
test test_search_with_post ... ok
test test_rate_limiting ... ok

test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured
```

## Troubleshooting

If tests fail:

1. **Connection refused**: Make sure all 4 services are running on their correct ports:

   - Config: 8003
   - Storage: 8002
   - Ingestion: 8001
   - Search: 8004

2. **No logs found**: Wait a few seconds after starting services for them to initialize

3. **Rate limit errors**: This is expected behavior for the rate limiting test
