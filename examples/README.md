# Examples

Complete, runnable examples demonstrating each concurrency pattern.

## Available Examples

### 1. Process Sharding Example
**Path**: `ProcessShardingExample/`  
**Pattern**: Multi-process work distribution via Redis PID registry  
**Scenario**: 3 workers coordinate to process 10,000 orders  

**How to run**:
```bash
cd ProcessShardingExample

# Terminal 1: Start worker 1
dotnet run

# Terminal 2: Start worker 2
dotnet run

# Terminal 3: Start worker 3
dotnet run

# Watch logs: each worker processes exactly 1/3 of orders
# Kill one worker → others pick up its share within 60 seconds
```

**Expected output**:
```
Worker 1 (PID 12345): Processing 3334 orders
Worker 2 (PID 12346): Processing 3333 orders
Worker 3 (PID 12347): Processing 3333 orders
```

---

### 2. Async Pipeline Example
**Path**: `AsyncPipelineExample/`  
**Pattern**: SemaphoreSlim-based concurrent processing  
**Scenario**: Process 1000 balance updates with max 50 concurrent tasks  

**How to run**:
```bash
cd AsyncPipelineExample
dotnet run

# Processes 1000 balance updates in ~10 seconds
# Watch logs: max 50 concurrent tasks, constant DB connection pool
```

**Expected output**:
```
[00:00] Starting pipeline with 1000 items, max concurrency: 50
[00:02] Progress: 200/1000 (20%)
[00:05] Progress: 500/1000 (50%)
[00:08] Progress: 800/1000 (80%)
[00:10] Complete: 1000/1000 succeeded, 0 failed
Total time: 9.8 seconds
Throughput: 102 items/second
```

---

### 3. TraceID Correlation Example
**Path**: `TraceIDCorrelationExample/`  
**Pattern**: AsyncLocal-based request tracking  
**Scenario**: Trace a request through multiple async tasks  

**How to run**:
```bash
cd TraceIDCorrelationExample
dotnet run

# Logs show TraceID flowing through all async tasks
```

**Expected output**:
```
[TraceID: abc-123-def] [Task: 0001] Order fetched: ORD001
[TraceID: abc-123-def] [Task: 0001] Balance checked
[TraceID: abc-123-def] [Task: 0002] Balance updated
[TraceID: abc-123-def] [Task: 0003] Webhook sent
[TraceID: abc-123-def] [Task: 0001] Order completed

# Grep by TraceID to see entire request chain:
$ grep "abc-123-def" logs/*.log
```

---

### 4. Post-Commit Tasks Example
**Path**: `PostCommitTasksExample/`  
**Pattern**: Transaction-aware callback queue  
**Scenario**: Update balance in DB, then send webhook (only if commit succeeds)  

**How to run**:
```bash
cd PostCommitTasksExample
dotnet run

# Demonstrates transactional safety: side effects only run on commit
```

**Expected output**:
```
[00:00] Starting transaction
[00:00] Balance updated: $1000
[00:00] Registering post-commit task: SendWebhook
[00:00] Registering post-commit task: InvalidateCache
[00:00] Transaction committed ✓
[00:01] Executing post-commit tasks...
[00:01] SendWebhook completed
[00:01] InvalidateCache completed
[00:01] All 2 post-commit tasks succeeded
```

---

### 5. Long-Running Job Example
**Path**: `LongRunningJobExample/`  
**Pattern**: BackgroundService with checkpointing and health checks  
**Scenario**: 24/7 daemon processing data every hour  

**How to run**:
```bash
cd LongRunningJobExample
dotnet run

# Runs continuously, processing batches every hour
# Press Ctrl+C to test graceful shutdown
```

**Expected output**:
```
[00:00] Starting daily stats daemon
[00:00] Loading checkpoint: last processed at 2024-01-15 10:00:00
[00:00] Processing batch 1/10...
[00:05] Processing batch 2/10...
[00:30] Batch complete, saving checkpoint
[00:30] Sleeping for 1 hour...
^C
[00:31] Shutdown requested, finishing current iteration...
[00:32] Daemon stopped gracefully
```

---

## Prerequisites

All examples require:

```bash
# .NET 8.0 SDK
dotnet --version  # Should be 8.0 or higher

# Redis (for coordination and checkpointing)
docker run -d -p 6379:6379 redis:7-alpine

# Install packages (in each example folder)
dotnet restore
```

---

## Project Structure

Each example follows this structure:

```
ExampleName/
├── ExampleName.csproj           # Project file
├── Program.cs                   # Entry point
├── appsettings.json             # Configuration (Redis connection, etc.)
├── README.md                    # Example-specific instructions
└── Models/                      # Domain models (Order, Balance, etc.)
```

---

## Common Configuration

All examples use `appsettings.json` for configuration:

```json
{
  "Redis": {
    "ConnectionString": "localhost:6379"
  },
  "Logging": {
    "LogLevel": {
      "Default": "Information",
      "Microsoft": "Warning"
    }
  }
}
```

---

## Tips for Learning

1. **Start with Process Sharding**: Easiest to visualize (multiple terminals)
2. **Then Async Pipeline**: See concurrency control in action
3. **Add TraceID Correlation**: Understand request tracking
4. **Combine patterns**: Use Process Sharding + Async Pipeline + TraceID together

---

## Running All Examples

```bash
# Run all examples in parallel (different terminals)
./run-all-examples.sh

# Or run individually
cd ProcessShardingExample && dotnet run &
cd AsyncPipelineExample && dotnet run &
cd TraceIDCorrelationExample && dotnet run &
```

---

## Troubleshooting

**Redis connection error**:
```bash
# Check Redis is running
docker ps | grep redis

# Start Redis if not running
docker run -d -p 6379:6379 redis:7-alpine
```

**Port already in use**:
```bash
# Change port in appsettings.json
"Urls": "http://localhost:5001"  # Use different port
```

**Performance issues**:
```bash
# Reduce concurrency in AsyncPipelineExample
var pipeline = new AsyncPipeline<T>(maxConcurrency: 10);  # Lower value
```
