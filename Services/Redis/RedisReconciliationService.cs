using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Ptlk.RedisScpi.Configuration;
using Ptlk.RedisScpi.Data;
using Ptlk.RedisScpi.Services.Scpi;
using Ptlk.RedisScpi.Services.Startup;

namespace Ptlk.RedisScpi.Services.Redis;

public sealed class RedisReconciliationService(
    IDbContextFactory<AppDbContext> dbFactory,
    ScpiValueCache cache,
    RedisPointOwnershipService ownership,
    RedisPointStateService pointState,
    PointUpdateIdentity identity,
    RuntimeModeService runtime,
    IOptions<RedisScpiOptions> options,
    ILogger<RedisReconciliationService> logger) : BackgroundService
{
    private int ready;
    private string cycleId = Guid.NewGuid().ToString("N");

    public bool IsReady => Volatile.Read(ref ready) == 1;
    public string Status => IsReady ? "ready" : "pending";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                if (!runtime.Current.RedisConnected || !runtime.Current.AssetInitialized)
                {
                    if (cache.BeginOutage()) cycleId = Guid.NewGuid().ToString("N");
                    Volatile.Write(ref ready, 0);
                }
                else if (!IsReady)
                {
                    await ReconcileAsync(stoppingToken);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                Volatile.Write(ref ready, 0);
                logger.LogWarning(ex, "RedisScpi reconciliation failed and will be retried.");
            }

            await Task.Delay(250, stoppingToken);
        }
    }

    private async Task ReconcileAsync(CancellationToken cancellationToken)
    {
        var snapshot = cache.SnapshotForReconciliation();
        await using var db = await dbFactory.CreateDbContextAsync(cancellationToken);
        var mappings = await db.RedisMappings.AsNoTracking()
            .ToDictionaryAsync(mapping => mapping.SourcePath, StringComparer.OrdinalIgnoreCase, cancellationToken);
        var failures = 0;

        foreach (var item in snapshot.Values)
        {
            if (!mappings.TryGetValue(item.Key, out var mapping)) continue;
            if (!await ownership.EnsureOwnedAsync(item.Key, mapping.RedisKey, cancellationToken))
            {
                failures++;
                continue;
            }

            try
            {
                await pointState.UpdateDynamicFieldsAsync(
                    mapping,
                    item.Value.Value,
                    item.Value.Quality,
                    options.Value.SourceName,
                    cancellationToken,
                    PointUpdateReasons.ReconnectSync,
                    timestamp: item.Value.UpdatedAt.ToUnixTimeMilliseconds(),
                    operationIdOverride: PointOperationId.ReconnectSync(
                        identity.InstanceId,
                        cycleId,
                        item.Value.Sequence,
                        mapping.RedisKey));
            }
            catch (Exception ex)
            {
                failures++;
                logger.LogDebug(ex, "Reconnect sync failed for {SourcePath}.", item.Key);
            }
        }

        if (failures == 0 && cache.TryCompleteReconciliation(snapshot.ObservedSequence))
            Volatile.Write(ref ready, 1);
    }
}
