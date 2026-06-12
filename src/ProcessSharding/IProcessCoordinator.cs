using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace PaymentPlatform.Concurrency.ProcessSharding;

/// <summary>
/// Coordinates work distribution across multiple processes using Redis-based PID registry.
/// 
/// Pattern: Distributed work partitioning without central coordinator.
/// Use case: Multiple instances of the same batch job need to split work evenly.
/// </summary>
public interface IProcessCoordinator
{
    /// <summary>
    /// Register current process in Redis and get its position in the worker pool.
    /// </summary>
    /// <returns>
    /// Tuple of (TotalProcesses, MyIndex) where:
    /// - TotalProcesses: Number of currently active processes
    /// - MyIndex: Current process index in sorted PID list (0-based)
    /// </returns>
    /// <remarks>
    /// This method should be called at the beginning of each job iteration.
    /// Registration has a TTL (typically 180s), so crashed workers auto-expire.
    /// </remarks>
    Task<(int TotalProcesses, int MyIndex)> RegisterAndGetPositionAsync();
    
    /// <summary>
    /// Partition work items using round-robin allocation based on process index.
    /// </summary>
    /// <typeparam name="T">Type of work item</typeparam>
    /// <param name="items">All work items to partition</param>
    /// <param name="totalProcesses">Total number of active processes</param>
    /// <param name="myIndex">Current process index (0-based)</param>
    /// <returns>Work items assigned to current process</returns>
    /// <remarks>
    /// Algorithm: item[i] is assigned to worker (i % totalProcesses).
    /// This ensures even distribution and deterministic assignment.
    /// </remarks>
    IEnumerable<T> PartitionWork<T>(
        IEnumerable<T> items, 
        int totalProcesses, 
        int myIndex);
}
