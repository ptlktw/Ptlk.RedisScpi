using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Ptlk.RedisScpi.Configuration;
using Ptlk.RedisScpi.Contracts.Scpi;
using Ptlk.RedisScpi.Data;
using Ptlk.RedisScpi.Services.Logs;
using Ptlk.RedisScpi.Services.Redis;
using Ptlk.RedisScpi.Services.Startup;

namespace Ptlk.RedisScpi.Services.Scpi;

public sealed record ScpiDirectWriteOutcome(
    bool Success,
    string Message,
    string? ErrorCode,
    ScpiDirectWriteResult? Result);

public sealed class ScpiDirectWriteService(
    IDbContextFactory<AppDbContext> dbFactory,
    RedisPointOwnershipService ownership,
    IScpiClientService client,
    EndpointOperationScheduler scheduler,
    LogService log,
    RuntimeModeService runtime,
    IOptions<RedisScpiOptions> options,
    ILogger<ScpiDirectWriteService> logger)
{
    public async Task<ScpiDirectWriteOutcome> WriteAsync(
        string sourcePath,
        JsonElement value,
        string requestedBy,
        CancellationToken cancellationToken = default)
    {
        await using var db = await dbFactory.CreateDbContextAsync(cancellationToken);
        var mapping = await db.RedisMappings.AsNoTracking()
            .FirstOrDefaultAsync(item => item.SourcePath == sourcePath, cancellationToken);

        var point = await db.ScpiPointConfigs.AsNoTracking()
            .Include(item => item.EnumOptions)
            .Include(item => item.EndpointConfig)
            .FirstOrDefaultAsync(item => item.SourcePath == sourcePath, cancellationToken);
        if (point?.EndpointConfig is null)
        {
            return Failure(ScpiErrorCodes.PointNotFound, $"SCPI point '{sourcePath}' does not exist.");
        }

        if (!point.EndpointConfig.ConverterId.Equals(options.Value.ConverterId, StringComparison.Ordinal))
        {
            return Failure(
                ScpiErrorCodes.EndpointNotFound,
                $"Endpoint '{point.EndpointConfig.EndpointId}' belongs to another RedisScpi converter.");
        }

        if (!point.EndpointConfig.Enabled)
        {
            return Failure(ScpiErrorCodes.EndpointDisabled, $"Endpoint '{point.EndpointConfig.EndpointId}' is disabled.");
        }
        if (!point.Enabled)
        {
            return Failure(ScpiErrorCodes.PointDisabled, $"Point '{sourcePath}' is disabled.");
        }
        if (!ScpiAccessModes.Readwrite.Equals(point.Access, StringComparison.OrdinalIgnoreCase))
        {
            return Failure(ScpiErrorCodes.PointReadonly, $"Point '{sourcePath}' is readonly.");
        }

        try
        {
            var result = await scheduler.RunAsync(
                point.EndpointConfig.EndpointId,
                async token =>
                {
                    if (runtime.IsRedisOutputReady)
                    {
                        if (mapping is null)
                        {
                            throw new ScpiValidationException(
                                ScpiErrorCodes.PointNotFound,
                                $"No RedisMapping exists for '{sourcePath}', so ownership cannot be verified.");
                        }

                        var claim = await ownership.ClaimAsync(mapping.SourcePath, mapping.RedisKey, token);
                        if (!claim.Acquired)
                        {
                            throw new ScpiValidationException(
                                ScpiErrorCodes.OwnershipNotAcquired,
                                $"This RedisScpi instance does not own '{sourcePath}'.");
                        }
                    }

                    return await client.DirectWriteWithinEndpointLockAsync(
                        point.EndpointConfig,
                        point,
                        value,
                        token);
                },
                cancellationToken);
            await SafeLogAsync(
                "Info",
                $"Direct SCPI write completed for '{sourcePath}' by '{requestedBy}'. Runtime value awaits normal polling.",
                cancellationToken);
            return new ScpiDirectWriteOutcome(true, "Direct SCPI write completed; normal polling will refresh the actual value.", null, result);
        }
        catch (ScpiException ex)
        {
            if (ex.ErrorCode == ScpiErrorCodes.OwnershipNotAcquired)
            {
                await SafeLogAsync(
                    "Warning",
                    $"Direct SCPI write ignored because this converter does not own '{sourcePath}'.",
                    cancellationToken);
            }
            return Failure(ex.ErrorCode, ex.Message);
        }
        catch (Exception ex)
        {
            return Failure(ScpiErrorCodes.TransportError, ex.Message);
        }
    }

    private static ScpiDirectWriteOutcome Failure(string code, string message) =>
        new(false, message, code, null);

    private async Task SafeLogAsync(string level, string message, CancellationToken cancellationToken)
    {
        try
        {
            await log.AddSystemAsync("DirectWrite", level, message, null, cancellationToken);
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "Failed to persist direct SCPI write diagnostic.");
        }
    }
}
