using Microsoft.Extensions.Options;
using Ptlk.RedisScpi.Configuration;
using Ptlk.RedisScpi.Contracts.Redis;
using Ptlk.RedisScpi.Services.Redis;
using Ptlk.SCADA.Interop.Runtime;

namespace Ptlk.RedisScpi.Services.Startup;

public sealed class RedisScpiStatusService(
    RuntimeModeService runtime,
    EdgeStatePublisher edgeState,
    EdgeRuntimeIdentity runtimeIdentity,
    PointOwnershipReleaseRuntimeState releaseState,
    RedisPointOwnershipService ownership,
    RedisReconciliationService reconciliation,
    IOptions<RedisScpiOptions> redisScpiOptions,
    IOptions<RedisScpiRuntimeOptions> runtimeOptions,
    ILogger<RedisScpiStatusService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await PublishAsync("RedisProtocolConverter.online", "online", stoppingToken);
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(runtimeOptions.Value.HeartbeatIntervalMs, stoppingToken);
                await PublishAsync("RedisProtocolConverter.heartbeat", "heartbeat", stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogDebug(ex, "Failed to publish RedisScpi heartbeat.");
            }
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await PublishAsync("RedisProtocolConverter.offline", "offline", cancellationToken);
        await base.StopAsync(cancellationToken);
    }

    private async Task PublishAsync(string type, string status, CancellationToken cancellationToken)
    {
        try
        {
            var options = redisScpiOptions.Value;
            var current = runtime.Current;
            var claims = ownership.Snapshot();
            var runtimeDiagnostics = current.RuntimeDiagnostics;
            var metadata = new Dictionary<string, string?>
            {
                ["instanceId"] = runtimeIdentity.InstanceId,
                ["mode"] = current.Mode.ToString(),
                ["message"] = current.Message,
                ["redisConnected"] = BoolText(current.RedisConnected),
                ["assetInitialized"] = BoolText(current.AssetInitialized),
                ["reconciliation"] = reconciliation.Status,
                ["redisOutputStatus"] = current.RedisOutputStatus.ToString(),
                ["redisOutputMessage"] = current.RedisOutputMessage,
                ["redisOutputDiagnosticsCount"] = current.RedisOutputDiagnostics.Count.ToString(),
                ["endpointStatus"] = current.EndpointStatus.ToString(),
                ["endpointMessage"] = current.EndpointMessage,
                ["transportStatus"] = current.TransportStatus.ToString(),
                ["transportMessage"] = current.TransportMessage,
                ["pollingStatus"] = current.PollingStatus.ToString(),
                ["pollingMessage"] = current.PollingMessage,
                ["commandStatus"] = current.CommandStatus.ToString(),
                ["commandMessage"] = current.CommandMessage,
                ["scpiStatus"] = current.ScpiStatus.ToString(),
                ["scpiMessage"] = current.ScpiMessage,
                ["runtimeDiagnosticsCount"] = runtimeDiagnostics.Count.ToString(),
                ["endpointDiagnosticsCount"] = CountDiagnostics(runtimeDiagnostics, "endpoint"),
                ["transportDiagnosticsCount"] = CountDiagnostics(runtimeDiagnostics, "transport"),
                ["pollingDiagnosticsCount"] = CountDiagnostics(runtimeDiagnostics, "polling"),
                ["commandDiagnosticsCount"] = CountDiagnostics(runtimeDiagnostics, "command"),
                ["scpiDiagnosticsCount"] = CountDiagnostics(runtimeDiagnostics, "scpi"),
                ["ownershipClaims"] = claims.Count.ToString(),
                ["ownershipAcquired"] = claims.Count(claim => claim.Acquired).ToString(),
                ["ownershipNotAcquired"] = claims.Count(claim => !claim.Acquired).ToString()
            };
            PointOwnershipReleaseRuntimeState.AddMetadata(metadata, releaseState.Current);
            var effectiveStatus = status != "offline" && releaseState.Current.NeedsAttention > 0
                ? "error"
                : status;
            var evt = new RedisProtocolConverterStatusEventContract(
                Schema: 1,
                Type: type,
                MessageId: Guid.NewGuid().ToString("N"),
                ConverterId: options.ConverterId,
                Status: effectiveStatus,
                Timestamp: DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                Source: options.SourceName,
                Metadata: metadata);

            await edgeState.PublishAsync(evt, runtimeOptions.Value.HeartbeatIntervalMs, cancellationToken);
            if (status != "offline")
                runtimeIdentity.MarkOnlinePublished();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "Failed to publish RedisScpi status {Status}.", status);
        }
    }

    private static string BoolText(bool value) => value ? "true" : "false";

    private static string CountDiagnostics(
        IReadOnlyList<RuntimeDiagnostic> diagnostics,
        string subsystem) =>
        diagnostics.Count(diagnostic => diagnostic.Subsystem.Equals(subsystem, StringComparison.OrdinalIgnoreCase))
            .ToString();
}
