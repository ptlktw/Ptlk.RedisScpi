using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Ptlk.RedisScpi.Configuration;
using Ptlk.RedisScpi.Data;
using Ptlk.RedisScpi.Models;
using Ptlk.RedisScpi.Services.Redis;
using Ptlk.SCADA.Interop.Redis;
using Ptlk.SCADA.Interop.Runtime;
using StackExchange.Redis;
using ProducerRedisConnectionFactory = Ptlk.RedisScpi.Services.Redis.RedisConnectionFactory;

namespace Ptlk.RedisScpi.Services.Ownership;

public sealed class PointOwnershipReleaseCoordinator(
    IDbContextFactory<AppDbContext> dbFactory,
    ProducerRedisConnectionFactory redis,
    PointOwnershipReleaseExecutor executor,
    PointRuntimeLifecycleGate gate,
    PointOwnershipReleaseSignal signal,
    PointOwnershipReleaseRuntimeState releaseState,
    IOptions<RedisScpiOptions> options,
    ILogger<PointOwnershipReleaseCoordinator> logger) : BackgroundService
{
    private readonly TaskCompletionSource initialPassCompleted = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public Task InitialPassCompleted => initialPassCompleted.Task;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await using (var db = await dbFactory.CreateDbContextAsync(stoppingToken))
        {
            gate.RestoreReleasing(await db.PointOwnershipReleaseIntents.AsNoTracking()
                .Where(item => item.Status != PointOwnershipReleaseStatuses.Applied)
                .Select(item => item.RedisKey).ToListAsync(stoppingToken));
        }
        await ProcessDueAsync(stoppingToken);
        initialPassCompleted.TrySetResult();
        while (!stoppingToken.IsCancellationRequested)
        {
            await signal.WaitAsync(TimeSpan.FromMilliseconds(250), stoppingToken);
            await ProcessDueAsync(stoppingToken);
        }
    }

    private async Task ProcessDueAsync(CancellationToken cancellationToken)
    {
        await using var db = await dbFactory.CreateDbContextAsync(cancellationToken);
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var ids = await db.PointOwnershipReleaseIntents.AsNoTracking()
            .Where(item => item.Status == PointOwnershipReleaseStatuses.RedisReleased
                || (item.Status == PointOwnershipReleaseStatuses.PendingRelease && item.NextAttemptAt <= now))
            .OrderBy(item => item.RequestedAt).Select(item => item.Id).Take(100).ToListAsync(cancellationToken);
        foreach (var id in ids)
            await ProcessOneAsync(id, cancellationToken);
        await RefreshRuntimeStateAsync(db, cancellationToken);
        if (ids.Count == 100)
            signal.Pulse();
    }

    private async Task RefreshRuntimeStateAsync(AppDbContext db, CancellationToken cancellationToken)
    {
        var summary = await db.PointOwnershipReleaseIntents.AsNoTracking()
            .GroupBy(_ => 1)
            .Select(group => new
            {
                Pending = group.Count(item => item.Status == PointOwnershipReleaseStatuses.PendingRelease
                    || item.Status == PointOwnershipReleaseStatuses.RedisReleased),
                NeedsAttention = group.Count(item => item.Status == PointOwnershipReleaseStatuses.NeedsAttention),
                Applied = group.Count(item => item.Status == PointOwnershipReleaseStatuses.Applied),
                OldestPendingAt = group
                    .Where(item => item.Status == PointOwnershipReleaseStatuses.PendingRelease
                        || item.Status == PointOwnershipReleaseStatuses.RedisReleased)
                    .Select(item => (long?)item.RequestedAt)
                    .Min()
            })
            .SingleOrDefaultAsync(cancellationToken);
        var lastResult = await db.PointOwnershipReleaseIntents.AsNoTracking()
            .OrderByDescending(item => item.UpdatedAt)
            .Select(item => item.LastResultCode)
            .FirstOrDefaultAsync(cancellationToken);
        releaseState.Update(new PointOwnershipReleaseRuntimeSnapshot(
            summary?.Pending ?? 0,
            summary?.NeedsAttention ?? 0,
            summary?.Applied ?? 0,
            lastResult,
            summary?.OldestPendingAt));
    }

    private async Task ProcessOneAsync(int id, CancellationToken cancellationToken)
    {
        await using var db = await dbFactory.CreateDbContextAsync(cancellationToken);
        var intent = await db.PointOwnershipReleaseIntents.SingleOrDefaultAsync(item => item.Id == id, cancellationToken);
        if (intent is null || intent.Status is PointOwnershipReleaseStatuses.Applied or PointOwnershipReleaseStatuses.NeedsAttention)
            return;
        await using var pointLease = await gate.AcquireReleaseAttemptAsync(intent.RedisKey, cancellationToken);
        if (intent.Status == PointOwnershipReleaseStatuses.RedisReleased)
        {
            await FinalizeAsync(db, intent, cancellationToken);
            return;
        }
        try
        {
            var result = await executor.ExecuteAsync(
                await redis.GetDatabaseAsync(cancellationToken),
                intent.RedisKey,
                intent.ConverterId,
                intent.OperationId,
                options.Value.SourceName,
                cancellationToken);
            intent.AttemptCount++;
            intent.LastResultCode = result.Code;
            intent.LastErrorMessage = result.Detail;
            intent.UpdatedAt = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            if (result.Applied)
            {
                intent.Status = PointOwnershipReleaseStatuses.RedisReleased;
                intent.RedisReleasedAt = intent.UpdatedAt;
                await db.SaveChangesAsync(cancellationToken);
                await FinalizeAsync(db, intent, cancellationToken);
            }
            else if (result.Retryable)
            {
                ScheduleRetry(intent);
                await db.SaveChangesAsync(cancellationToken);
            }
            else
            {
                intent.Status = PointOwnershipReleaseStatuses.NeedsAttention;
                await db.SaveChangesAsync(cancellationToken);
            }
        }
        catch (Exception exception) when (exception is RedisException or TimeoutException)
        {
            intent.AttemptCount++;
            intent.LastResultCode = "redis_unavailable";
            intent.LastErrorMessage = Bounded(exception.Message);
            intent.UpdatedAt = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            ScheduleRetry(intent);
            await db.SaveChangesAsync(cancellationToken);
        }
        catch (Exception exception) when (!cancellationToken.IsCancellationRequested)
        {
            logger.LogError(exception, "Ownership release processing failed for {RedisKey}", intent.RedisKey);
            intent.AttemptCount++;
            intent.LastResultCode = "coordinator_error";
            intent.LastErrorMessage = Bounded(exception.Message);
            intent.Status = PointOwnershipReleaseStatuses.NeedsAttention;
            intent.UpdatedAt = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            await db.SaveChangesAsync(cancellationToken);
        }
    }

    private async Task FinalizeAsync(AppDbContext db, PointOwnershipReleaseIntent intent, CancellationToken cancellationToken)
    {
        if (intent.CompletionAction == PointOwnershipReleaseCompletionActions.ActivateReplacement)
        {
            if (string.IsNullOrWhiteSpace(intent.ReplacementSourcePath)
                || string.IsNullOrWhiteSpace(intent.ReplacementRedisKey)
                || !await db.ScpiPointConfigs.AnyAsync(point => point.SourcePath == intent.ReplacementSourcePath, cancellationToken)
                || await db.RedisMappings.AnyAsync(
                    mapping => mapping.SourcePath == intent.ReplacementSourcePath || mapping.RedisKey == intent.ReplacementRedisKey,
                    cancellationToken))
            {
                intent.Status = PointOwnershipReleaseStatuses.NeedsAttention;
                intent.LastResultCode = "replacement_collision";
                intent.LastErrorMessage = "Replacement source or Redis key is unavailable.";
                intent.UpdatedAt = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                await db.SaveChangesAsync(cancellationToken);
                return;
            }
            db.RedisMappings.Add(new RedisMapping
            {
                SourcePath = intent.ReplacementSourcePath,
                RedisKey = intent.ReplacementRedisKey
            });
        }
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        intent.Status = PointOwnershipReleaseStatuses.Applied;
        intent.AppliedAt = now;
        intent.UpdatedAt = now;
        await db.SaveChangesAsync(cancellationToken);
        gate.CompleteRelease(intent.RedisKey);
    }

    private static void ScheduleRetry(PointOwnershipReleaseIntent intent)
    {
        intent.Status = PointOwnershipReleaseStatuses.PendingRelease;
        var exponent = Math.Min(Math.Max(intent.AttemptCount - 1, 0), 5);
        intent.NextAttemptAt = checked(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + Math.Min(5_000, 250 * (1 << exponent)));
    }

    private static string Bounded(string value) => value.Length <= 1000 ? value : value[..1000];
}
