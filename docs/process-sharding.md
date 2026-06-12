# Process Sharding Pattern

## Real Scenario (Production Python Code)

The order timeout job needs to process **90,000 pending orders per hour**. A single process can't handle this load due to CPU and database connection bottlenecks.

### The Original Python Implementation

```python
# From: time_out_v2.py (Production Code)

def get_active_processes_count(self):
    """
    Register current process in Redis and get active process count
    
    Returns:
        Tuple of (total_processes, current_index)
    """
    try:
        # Register current process with 180-second TTL
        process_key_prefix = "active_processes_timeout"
        current_pid = os.getpid()
        current_process_key = f"{process_key_prefix}:{current_pid}"
        self.redis.setex(current_process_key, 180, int(time.time()))
        
        # Query all active process keys
        active_process_keys = self.redis.keys(f"{process_key_prefix}:*")
        
        # Extract PIDs and sort in ascending order
        pids = []
        for key in active_process_keys:
            key_str = key.decode('utf-8') if isinstance(key, bytes) else key
            pid = int(key_str.split(':')[-1])
            pids.append(pid)
        
        pids.sort()  # Stable ordering across all processes
        
        total_processes = len(pids)
        current_index = pids.index(current_pid)
        
        logger.info(
            f"[PROCESS_SHARD] PID={current_pid}, Total={total_processes}, "
            f"Index={current_index}, Active PIDs={pids}"
        )
        
        return total_processes, current_index
        
    except Exception as e:
        logger.error(f"[PROCESS_SHARD] Failed: {e}")
        return 1, 0  # Fallback to single process mode


def get_process_allocated_orders(self, orders, total_processes, current_index):
    """
    Round-robin allocation: order[i] goes to worker (i % total_processes)
    """
    if not orders or total_processes <= 1:
        return orders
    
    # Sort orders by code for stable ordering
    sorted_orders = sorted(orders, key=lambda x: x['code'])
    
    # Allocate using modulo
    allocated_orders = []
    for i, order in enumerate(sorted_orders):
        if i % total_processes == current_index:
            allocated_orders.append(order)
    
    logger.info(
        f"[PROCESS_SHARD] PID={os.getpid()}, "
        f"Allocated={len(allocated_orders)}/{len(sorted_orders)}"
    )
    
    return allocated_orders
```

### Production Metrics

- **3 workers** running simultaneously
- **90,000 orders/hour** total (30,000 per worker)
- **<2ms** registration latency (Redis PID registry)
- **<60 seconds** rebalancing time when worker crashes
- **<2% variance** in order count across workers (fair distribution)

---

## The Problem

**Naive approach:**
- Run 3 instances, each queries ALL 90K orders from DB
- Each process tries to process the same orders → race conditions, duplicate work
- Lock-based coordination → deadlocks, 20-second waits

**Production requirements:**
- 3 workers must evenly split the workload (30K orders each)
- If worker #2 crashes, workers #1 and #3 pick up its share automatically
- No central coordinator (single point of failure)
- Sub-second coordination overhead

---

## The Pattern

**Distributed work partitioning using Redis PID registry + deterministic hashing.**

### How It Works

```
Step 1: Each worker registers its PID in Redis with 180s TTL
        Redis Keys: 
        - active_processes_timeout:1001 (expires in 180s)
        - active_processes_timeout:1003 (expires in 180s)
        - active_processes_timeout:1005 (expires in 180s)

Step 2: Each worker queries Redis to get all active PIDs
        Result: [1001, 1003, 1005] (sorted for stable ordering)

Step 3: Each worker calculates its position in the sorted list
        Worker 1001 → index 0 (1st of 3)
        Worker 1003 → index 1 (2nd of 3)
        Worker 1005 → index 2 (3rd of 3)

Step 4: Each worker partitions work using modulo
        Orders: [A, B, C, D, E, F, G, H, I]
        
        Worker 1001 (index 0): i % 3 == 0 → A, D, G (3 orders)
        Worker 1003 (index 1): i % 3 == 1 → B, E, H (3 orders)
        Worker 1005 (index 2): i % 3 == 2 → C, F, I (3 orders)

Step 5: If worker 1003 crashes, next iteration has only 2 PIDs:
        Active PIDs: [1001, 1005]
        
        Worker 1001 (index 0): i % 2 == 0 → A, C, E, G, I (5 orders)
        Worker 1005 (index 1): i % 2 == 1 → B, D, F, H    (4 orders)
        
        Automatic rebalancing — no manual intervention needed!
```

---

## C# Implementation

### 1. Interface Definition

```csharp
public interface IProcessCoordinator
{
    /// <summary>
    /// Register current process and get its position in the worker pool
    /// </summary>
    Task<(int TotalProcesses, int MyIndex)> RegisterAndGetPositionAsync();
    
    /// <summary>
    /// Partition work items using round-robin allocation
    /// </summary>
    IEnumerable<T> PartitionWork<T>(
        IEnumerable<T> items, 
        int totalProcesses, 
        int myIndex);
}
```

### 2. Redis Implementation

```csharp
using StackExchange.Redis;
using Microsoft.Extensions.Logging;

namespace PaymentPlatform.Concurrency;

public class RedisProcessCoordinator : IProcessCoordinator
{
    private readonly IDatabase _redis;
    private readonly string _jobName;
    private readonly int _registrationTtlSeconds;
    private readonly ILogger<RedisProcessCoordinator> _logger;

    public RedisProcessCoordinator(
        IConnectionMultiplexer redis,
        string jobName,
        int registrationTtlSeconds = 180,
        ILogger<RedisProcessCoordinator>? logger = null)
    {
        _redis = redis.GetDatabase();
        _jobName = jobName;
        _registrationTtlSeconds = registrationTtlSeconds;
        _logger = logger ?? NullLogger<RedisProcessCoordinator>.Instance;
    }

    public async Task<(int TotalProcesses, int MyIndex)> RegisterAndGetPositionAsync()
    {
        var pid = Environment.ProcessId;
        var keyPrefix = $"active_processes:{_jobName}";
        var myKey = $"{keyPrefix}:{pid}";

        try
        {
            // 1. Register current process with TTL
            var timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            await _redis.StringSetAsync(
                myKey, 
                timestamp, 
                TimeSpan.FromSeconds(_registrationTtlSeconds));

            // 2. Query all active process keys
            var server = _redis.Multiplexer
                .GetServer(_redis.Multiplexer.GetEndPoints().First());
            var pattern = $"{keyPrefix}:*";
            var keys = server.Keys(pattern: pattern).ToArray();

            // 3. Extract PIDs and sort for stable ordering
            var pids = keys
                .Select(k => k.ToString().Split(':').Last())
                .Where(s => int.TryParse(s, out _))
                .Select(int.Parse)
                .OrderBy(p => p)
                .ToList();

            var totalProcesses = pids.Count;
            var myIndex = pids.IndexOf(pid);

            if (myIndex == -1)
            {
                _logger.LogWarning(
                    "[SHARD] PID {Pid} not found in registry {Pids}, defaulting to index 0",
                    pid, pids);
                myIndex = 0;
            }

            _logger.LogInformation(
                "[SHARD] PID={Pid}, Total={Total}, Index={Index}, All PIDs={Pids}",
                pid, totalProcesses, myIndex, string.Join(", ", pids));

            return (totalProcesses, myIndex);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[SHARD] Registration failed, falling back to single process");
            return (1, 0);
        }
    }

    public IEnumerable<T> PartitionWork<T>(
        IEnumerable<T> items, 
        int totalProcesses, 
        int myIndex)
    {
        if (totalProcesses <= 0)
            throw new ArgumentException("Total processes must be > 0", nameof(totalProcesses));

        if (myIndex < 0 || myIndex >= totalProcesses)
            throw new ArgumentException(
                $"Index {myIndex} out of range [0, {totalProcesses})", 
                nameof(myIndex));

        if (totalProcesses == 1)
            return items; // Fast path: no sharding needed

        // Round-robin partitioning: item[i] belongs to worker (i % totalProcesses)
        var allocated = items
            .Select((item, i) => new { Item = item, Index = i })
            .Where(x => x.Index % totalProcesses == myIndex)
            .Select(x => x.Item)
            .ToList();

        _logger.LogInformation(
            "[SHARD] PID={Pid} allocated {Count} items",
            Environment.ProcessId, allocated.Count);

        return allocated;
    }
}
```

### 3. Background Job Integration

```csharp
public class OrderTimeoutJob : BackgroundService
{
    private readonly IProcessCoordinator _coordinator;
    private readonly IOrderRepository _orders;
    private readonly ILogger<OrderTimeoutJob> _logger;

    public OrderTimeoutJob(
        IProcessCoordinator coordinator,
        IOrderRepository orders,
        ILogger<OrderTimeoutJob> logger)
    {
        _coordinator = coordinator;
        _orders = orders;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        _logger.LogInformation("[JOB] Starting order timeout daemon, PID={Pid}", 
            Environment.ProcessId);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                // 1. Register and get shard position
                var (totalProcs, myIndex) = await _coordinator
                    .RegisterAndGetPositionAsync();

                // 2. Query ALL pending orders (all workers see the same dataset)
                var allOrders = await _orders.GetPendingTimeoutsAsync(ct);

                // 3. Partition work using deterministic round-robin
                var myOrders = _coordinator
                    .PartitionWork(allOrders, totalProcs, myIndex);

                _logger.LogInformation(
                    "[JOB] Processing {Count}/{Total} orders",
                    myOrders.Count(), allOrders.Count);

                // 4. Process assigned orders
                foreach (var order in myOrders)
                {
                    await ProcessTimeoutAsync(order, ct);
                }

                await Task.Delay(TimeSpan.FromSeconds(60), ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                _logger.LogInformation("[JOB] Shutdown requested");
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[JOB] Iteration failed, retrying in 5s");
                await Task.Delay(TimeSpan.FromSeconds(5), ct);
            }
        }

        _logger.LogInformation("[JOB] Daemon stopped");
    }

    private async Task ProcessTimeoutAsync(Order order, CancellationToken ct)
    {
        // Timeout logic: unfreeze balance, mark order as cancelled, etc.
        _logger.LogInformation("[PROCESS] Order {Code} timeout", order.Code);
        await Task.CompletedTask;
    }
}
```

### 4. Dependency Injection Setup

```csharp
// Program.cs
var builder = WebApplication.CreateBuilder(args);

// Register Redis
builder.Services.AddSingleton<IConnectionMultiplexer>(sp =>
    ConnectionMultiplexer.Connect("localhost:6379"));

// Register Process Coordinator
builder.Services.AddSingleton<IProcessCoordinator>(sp =>
    new RedisProcessCoordinator(
        sp.GetRequiredService<IConnectionMultiplexer>(),
        jobName: "order_timeout",
        registrationTtlSeconds: 180,
        sp.GetRequiredService<ILogger<RedisProcessCoordinator>>()));

// Register Background Job
builder.Services.AddHostedService<OrderTimeoutJob>();

var app = builder.Build();
app.Run();
```

---

## Key Design Decisions

### 1. Why TTL-based registration?

**Problem**: If a worker crashes, it never unregisters from Redis.

**Solution**: Each PID key expires after 180s. Workers re-register every iteration (e.g., every 60s). Crashed workers' keys expire → automatic removal from registry.

### 2. Why sort PIDs?

**Problem**: Redis `KEYS` returns unordered results. Different workers might see different orderings → same item assigned to multiple workers.

**Solution**: Sort PIDs deterministically (`OrderBy`). All workers see `[1001, 1003, 1005]` in the same order → same item assigned to same worker.

### 3. Why modulo partitioning?

**Alternative**: Hash-based partitioning (consistent hashing).

**Trade-off**:
- Modulo is simple, zero state, deterministic
- Consistent hashing minimizes data movement when workers scale up/down

**Decision**: Modulo is sufficient for batch jobs where iteration cost is cheap. Consistent hashing is better for stateful systems (cache sharding).

---

## Testing Strategy

### Unit Test: Deterministic Partitioning

```csharp
[Fact]
public void PartitionWork_RoundRobin_DistributesEvenly()
{
    var coordinator = new RedisProcessCoordinator(...);
    var items = Enumerable.Range(1, 9).ToList(); // [1,2,3,4,5,6,7,8,9]

    var worker0 = coordinator.PartitionWork(items, totalProcesses: 3, myIndex: 0);
    var worker1 = coordinator.PartitionWork(items, totalProcesses: 3, myIndex: 1);
    var worker2 = coordinator.PartitionWork(items, totalProcesses: 3, myIndex: 2);

    Assert.Equal(new[] { 1, 4, 7 }, worker0); // index % 3 == 0
    Assert.Equal(new[] { 2, 5, 8 }, worker1); // index % 3 == 1
    Assert.Equal(new[] { 3, 6, 9 }, worker2); // index % 3 == 2
}
```

### Integration Test: Worker Crash Recovery

```csharp
[Fact]
public async Task RegisterAndGetPosition_WorkerCrashes_AutomaticRebalancing()
{
    var redis = await CreateRedisAsync();
    var coordinator1 = new RedisProcessCoordinator(redis, "test_job");
    var coordinator2 = new RedisProcessCoordinator(redis, "test_job");

    // Initial: 2 workers
    var (total1, idx1) = await coordinator1.RegisterAndGetPositionAsync();
    var (total2, idx2) = await coordinator2.RegisterAndGetPositionAsync();

    Assert.Equal(2, total1);
    Assert.Equal(2, total2);
    Assert.NotEqual(idx1, idx2);

    // Simulate worker2 crash (don't re-register, let TTL expire)
    await Task.Delay(TimeSpan.FromSeconds(181)); // > TTL

    // Worker1 re-registers, now sees only itself
    var (totalAfter, idxAfter) = await coordinator1.RegisterAndGetPositionAsync();

    Assert.Equal(1, totalAfter);
    Assert.Equal(0, idxAfter);
}
```

---

## Production Trade-offs

| Gain | Cost |
|------|------|
| Horizontal scalability (linear throughput with worker count) | Registration latency (1-5ms per iteration) |
| Automatic crash recovery (TTL-based expiration) | No graceful shutdown signal (worker just stops re-registering) |
| Zero single point of failure (Redis as coordination layer) | Requires Redis uptime (but failures are transient — next iteration recovers) |
| Deterministic work assignment (same input → same output) | Uneven distribution if workload is non-uniform (1 order takes 10s, others take 1s) |

---

## Real Production Behavior

In the live system, **3 workers process 90K orders/hour** with:
- **<2% variance** in order count across workers
- **<60 seconds** rebalancing when worker crashes
- **Zero code changes** to scale from 1 to 5 workers

**The secret**: Decentralized coordination (Redis PID registry) + deterministic partitioning (modulo-based round-robin) + TTL-based liveness (crashed workers auto-expire).

---

## When to Use This Pattern

✅ **Use when**:
- Multiple instances of same batch job need to coordinate
- Horizontal scalability required (add workers → increase throughput)
- Automatic crash recovery needed (no manual intervention)
- Work items are independent (no inter-dependencies)

❌ **Don't use when**:
- Single instance sufficient (adds unnecessary complexity)
- Work items have dependencies (need DAG-based scheduling)
- Sub-millisecond latency required (Redis adds 1-5ms overhead)
- Stateful processing (need consistent hashing instead of modulo)
