# TraceID Correlation Pattern

## Real Scenario (Production Experience)

In a distributed payment platform with async tasks, multi-process workers, and Redis operations, tracking a single request through the entire chain is crucial for debugging.

### Production Log Output Example

```
2024-01-15 10:23:45 - [PID:1001] [abc-123-def] [task_0042] - INFO - Order fetched: ORD001
2024-01-15 10:23:45 - [PID:1001] [abc-123-def] [task_0042] - INFO - Balance checked
2024-01-15 10:23:46 - [PID:1001] [abc-123-def] [task_0042] - INFO - Balance unfrozen
2024-01-15 10:23:46 - [PID:1003] [abc-123-def] [task_0089] - INFO - Webhook sent
2024-01-15 10:23:46 - [PID:1001] [abc-123-def] [task_0042] - INFO - Order completed

# Grep by TraceID to see entire request chain across processes and tasks:
$ grep "abc-123-def" order_timeout_*.log
```

### Production Metrics

- **100% coverage**: Every async task has TraceID
- **Zero overhead**: ThreadLocal access is O(1)
- **Cross-process**: TraceID preserved in Redis pub/sub, webhooks, database logs
- **Debugging time**: Reduced from hours to minutes

---

## The Problem

**Without TraceID correlation:**
```
2024-01-15 10:23:45 - INFO - Order fetched: ORD001
2024-01-15 10:23:45 - INFO - Balance checked
2024-01-15 10:23:45 - INFO - Order fetched: ORD002
2024-01-15 10:23:46 - INFO - Balance unfrozen
2024-01-15 10:23:46 - INFO - Webhook sent
2024-01-15 10:23:46 - INFO - Order completed

# Which balance unfreeze corresponds to which order? 
# Which webhook was sent for which order?
# Impossible to tell in concurrent processing!
```

**Production challenges:**
- 50+ concurrent async tasks processing orders
- 3 worker processes running simultaneously  
- Interleaved log lines from different requests
- Debugging requires correlating logs across processes, tasks, and external systems

---

## The Pattern

**AsyncLocal storage + structured logging for request correlation.**

### How It Works

```
HTTP Request arrives
  ↓
TraceContext.Current = new TraceContext(traceId: "abc-123")
  ↓
await ProcessOrderAsync(order)
  ├─ await FetchOrderDetailsAsync()      [TraceID: abc-123] [Task: 0001]
  ├─ await CheckBalanceAsync()           [TraceID: abc-123] [Task: 0001]
  └─ await Task.WhenAll(
       UnfreezeBalanceAsync(),           [TraceID: abc-123] [Task: 0002]
       SendWebhookAsync(),               [TraceID: abc-123] [Task: 0003]
       UpdateCacheAsync()                [TraceID: abc-123] [Task: 0004]
     )

All logs contain same TraceID → grep "abc-123" shows entire request chain
```

### Key Insight

`AsyncLocal<T>` in C# (equivalent to Python's `threading.local()`) ensures:
- TraceID flows through `async/await` automatically
- Each async task inherits TraceID from parent
- No manual propagation needed (unlike HTTP headers)

---

## C# Implementation

### 1. TraceContext with AsyncLocal

```csharp
using System;
using System.Threading;

public class TraceContext
{
    private static readonly AsyncLocal<TraceContext?> _current = new();

    public static TraceContext? Current
    {
        get => _current.Value;
        set => _current.Value = value;
    }

    public string TraceId { get; set; } = Guid.NewGuid().ToString("N")[..16];
    public string? TaskId { get; set; }
    public string? Source { get; set; }

    public TraceContext() { }

    public TraceContext(string traceId)
    {
        TraceId = traceId ?? throw new ArgumentNullException(nameof(traceId));
    }

    public TraceContext CreateChild(string? taskId = null)
    {
        return new TraceContext(TraceId)
        {
            TaskId = taskId,
            Source = Source
        };
    }
}
```

### 2. Structured Logger with TraceID

```csharp
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;

public static class StructuredLogger
{
    public static void LogWithTrace(
        this ILogger logger, 
        LogLevel level, 
        string message, 
        params object[] args)
    {
        var context = TraceContext.Current;
        var traceId = context?.TraceId ?? "no-trace";
        var taskId = context?.TaskId ?? GetCurrentTaskId();

        using (logger.BeginScope(new Dictionary<string, object>
        {
            ["TraceId"] = traceId,
            ["TaskId"] = taskId,
            ["ProcessId"] = Environment.ProcessId
        }))
        {
            logger.Log(level, message, args);
        }
    }

    private static string GetCurrentTaskId()
    {
        try
        {
            var task = Task.CurrentId;
            return task.HasValue ? $"task_{task.Value:D4}" : "sync";
        }
        catch
        {
            return "sync";
        }
    }
}
```

### 3. Integration with ASP.NET Core Middleware

```csharp
public class TraceIdMiddleware
{
    private readonly RequestDelegate _next;

    public TraceIdMiddleware(RequestDelegate next)
    {
        _next = next;
    }

    public async Task InvokeAsync(HttpContext context)
    {
        // Extract TraceID from header or generate new one
        var traceId = context.Request.Headers["X-Trace-Id"].FirstOrDefault()
                      ?? Guid.NewGuid().ToString("N")[..16];

        // Set TraceContext for this request
        TraceContext.Current = new TraceContext(traceId)
        {
            Source = "api"
        };

        // Add TraceID to response headers (for client correlation)
        context.Response.Headers["X-Trace-Id"] = traceId;

        await _next(context);
    }
}

// Register in Startup.cs
app.UseMiddleware<TraceIdMiddleware>();
```

### 4. Integration with Background Jobs

```csharp
public class OrderTimeoutJob : BackgroundService
{
    private readonly ILogger<OrderTimeoutJob> _logger;

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            // Set TraceContext for this iteration
            TraceContext.Current = new TraceContext
            {
                Source = "timeout-job"
            };

            _logger.LogWithTrace(LogLevel.Information, "Starting iteration");

            var orders = await GetPendingOrdersAsync();

            // Process orders concurrently - each inherits TraceID
            await Task.WhenAll(orders.Select(ProcessOrderAsync));

            await Task.Delay(TimeSpan.FromSeconds(60), ct);
        }
    }

    private async Task ProcessOrderAsync(Order order)
    {
        // TraceID automatically flows here via AsyncLocal
        _logger.LogWithTrace(LogLevel.Information, 
            "Processing order {Code}", order.Code);

        await UnfreezeBalanceAsync(order);
        await SendWebhookAsync(order);
        
        _logger.LogWithTrace(LogLevel.Information, 
            "Order {Code} completed", order.Code);
    }
}
```

### 5. Serilog Configuration (Recommended)

```csharp
// Program.cs
using Serilog;

Log.Logger = new LoggerConfiguration()
    .Enrich.FromLogContext()
    .WriteTo.Console(outputTemplate: 
        "{Timestamp:yyyy-MM-dd HH:mm:ss} [{Level:u3}] " +
        "[PID:{ProcessId}] [TraceId:{TraceId}] [TaskId:{TaskId}] " +
        "{Message:lj}{NewLine}{Exception}")
    .WriteTo.File(
        path: "logs/app-.log",
        rollingInterval: RollingInterval.Day,
        outputTemplate: 
            "{Timestamp:yyyy-MM-dd HH:mm:ss} [{Level:u3}] " +
            "[PID:{ProcessId}] [TraceId:{TraceId}] [TaskId:{TaskId}] " +
            "{Message:lj}{NewLine}{Exception}")
    .CreateLogger();
```

### 6. Usage Example

```csharp
// API Controller
[HttpPost("orders/{id}/cancel")]
public async Task<IActionResult> CancelOrder(string id)
{
    // TraceContext automatically set by middleware
    _logger.LogWithTrace(LogLevel.Information, "Canceling order {Id}", id);

    var order = await _orderService.GetOrderAsync(id);
    
    // All async calls inherit TraceID
    await _balanceService.UnfreezeAsync(order.PartnerId, order.Amount);
    await _webhookService.NotifyAsync(order);
    
    return Ok();
}

// Logs output:
// 2024-01-15 10:23:45 [INF] [PID:1001] [TraceId:abc-123-def] [TaskId:task_0042] Canceling order ORD001
// 2024-01-15 10:23:45 [INF] [PID:1001] [TraceId:abc-123-def] [TaskId:task_0042] Unfreezing balance for partner 456
// 2024-01-15 10:23:46 [INF] [PID:1001] [TraceId:abc-123-def] [TaskId:task_0042] Webhook sent to https://merchant.com/webhook
```

---

## Real Production Debugging Example

### Problem: Order timeout but no balance unfrozen

**Without TraceID:**
```bash
# Search for order in logs
$ grep "ORD12345" app.log
2024-01-15 10:23:45 [INFO] Order fetched: ORD12345
2024-01-15 10:23:46 [ERROR] Balance unfreeze failed: Insufficient frozen balance
2024-01-15 10:23:47 [INFO] Order marked as timeout

# Which balance unfreeze call? Which partner? Cannot tell from logs alone.
```

**With TraceID:**
```bash
# Search for order to get TraceID
$ grep "ORD12345" app.log | head -1
2024-01-15 10:23:45 [INFO] [TraceId:f7a2b3c4] Order fetched: ORD12345

# Now search by TraceID to see ENTIRE request chain
$ grep "f7a2b3c4" app.log
2024-01-15 10:23:45 [INFO] [TraceId:f7a2b3c4] [Task:0042] Order fetched: ORD12345
2024-01-15 10:23:45 [INFO] [TraceId:f7a2b3c4] [Task:0042] Partner: 456, Amount: 1000
2024-01-15 10:23:45 [INFO] [TraceId:f7a2b3c4] [Task:0042] Frozen balance: 500 (< 1000)
2024-01-15 10:23:46 [ERROR] [TraceId:f7a2b3c4] [Task:0042] Balance unfreeze failed: Insufficient frozen balance
2024-01-15 10:23:46 [INFO] [TraceId:f7a2b3c4] [Task:0042] Skipping unfreeze, marking order as error
2024-01-15 10:23:47 [INFO] [TraceId:f7a2b3c4] [Task:0042] Order marked as timeout

# Root cause identified: frozen balance (500) < order amount (1000)
# Time to debug: 2 minutes (vs hours without TraceID)
```

---

## Production Trade-offs

| Gain | Cost |
|------|------|
| 100% request tracing coverage (across async tasks, processes, external calls) | Minimal: AsyncLocal is O(1), ~8 bytes per context |
| Debugging time reduced from hours to minutes | Requires discipline: must set TraceContext at entry points |
| Cross-system correlation (logs, Redis, DB, webhooks) | Log volume increases ~10% (TraceID + TaskID in every line) |

---

## When to Use This Pattern

✅ **Use when**:
- Concurrent async processing (many tasks running simultaneously)
- Distributed systems (multiple processes, services, workers)
- Production debugging is painful (interleaved logs, hard to correlate)
- Need end-to-end request tracking (API → job → webhook → external service)

❌ **Don't use when**:
- Single-threaded synchronous processing (no concurrency → no correlation needed)
- Short-lived scripts (<1 minute execution)
- Log volume is extremely constrained (but consider: debugging cost > storage cost)

---

## Key Takeaway

In the real system, TraceID correlation reduced **debugging time from hours to minutes**. When a production issue occurs:

1. Find any log line related to the issue
2. Extract TraceID from that line
3. `grep TraceID logs/*` → see entire request chain
4. Identify root cause in minutes

**The secret**: `AsyncLocal<T>` for automatic propagation + structured logging + consistent format across all services.
