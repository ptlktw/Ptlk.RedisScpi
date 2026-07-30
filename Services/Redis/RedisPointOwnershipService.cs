using System.Collections.Concurrent;
using System.Globalization;
using Microsoft.Extensions.Options;
using Ptlk.RedisScpi.Configuration;
using Ptlk.RedisScpi.Services.Startup;
using Ptlk.SCADA.Interop.PointValues;
using Ptlk.SCADA.Interop.Runtime;
using StackExchange.Redis;

namespace Ptlk.RedisScpi.Services.Redis;

public sealed record PointOwnershipClaimResult(
    string SourcePath,
    string RedisKey,
    bool Acquired,
    string Status,
    string? Owner,
    DateTimeOffset CheckedAt);

public sealed class RedisPointOwnershipService(
    RedisConnectionFactory redis,
    IOptions<RedisScpiOptions> options,
    RuntimeModeService runtime,
    ILogger<RedisPointOwnershipService> logger,
    RedisMappingActivationService? activation = null,
    PointRuntimeLifecycleGate? lifecycleGate = null)
{
    private static readonly TimeSpan ClaimRefreshInterval = TimeSpan.FromSeconds(5);

    private const string ClaimScript = """
        if redis.call('EXISTS', KEYS[2]) == 1
            and redis.call('HEXISTS', KEYS[2], 'retirement_token') == 1 then
            return {'edge_retiring'}
        end
        if redis.call('EXISTS', KEYS[1]) == 0 then
            return {'missing'}
        end

        local keyType = redis.call('TYPE', KEYS[1])
        if type(keyType) == 'table' then
            keyType = keyType.ok
        end
        if keyType ~= 'hash' then
            return {'invalid_key_type'}
        end

        local currentOwner = redis.call('HGET', KEYS[1], 'owner')
        if not currentOwner or currentOwner == '' or currentOwner == ARGV[1] then
            redis.call('HSET', KEYS[1],
                'owner', ARGV[1],
                'owner_source', ARGV[2],
                'owner_acquired_at', ARGV[3])
            return {'ok', ARGV[1]}
        end

        return {'owned_by_other', currentOwner}
        """;

    private readonly ConcurrentDictionary<string, PointOwnershipClaimResult> _claims = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, DateTimeOffset> _lastClaimAttempts = new(StringComparer.OrdinalIgnoreCase);

    public event Action? Changed;

    public bool IsOwned(string sourcePath)
    {
        if (!RequiresOwnership(sourcePath))
        {
            return false;
        }
        return _claims.TryGetValue(sourcePath, out var claim)
            && claim.Acquired
            && lifecycleGate?.IsReleasing(claim.RedisKey) != true;
    }

    public async Task<bool> EnsureOwnedAsync(
        string sourcePath,
        string redisKey,
        CancellationToken cancellationToken = default)
    {
        if (lifecycleGate?.IsReleasing(redisKey) == true)
            return false;

        if (!RequiresOwnership(sourcePath))
        {
            return false;
        }
        if (activation is not null)
        {
            var activationResult = await activation.EvaluateAsync(sourcePath, redisKey, cancellationToken);
            if (!activationResult.CanActivate)
            {
                StoreInactive(sourcePath, redisKey, activationResult);
                return false;
            }
        }

        var now = DateTimeOffset.UtcNow;
        if (_claims.TryGetValue(sourcePath, out var claim)
            && claim.Acquired
            && now - claim.CheckedAt < ClaimRefreshInterval)
        {
            return true;
        }

        if (_lastClaimAttempts.TryGetValue(sourcePath, out var lastAttempt)
            && now - lastAttempt < ClaimRefreshInterval)
        {
            return false;
        }

        _lastClaimAttempts[sourcePath] = now;
        claim = await ClaimAsync(sourcePath, redisKey, cancellationToken);
        return claim.Acquired;
    }

    public IReadOnlyCollection<PointOwnershipClaimResult> Snapshot() =>
        _claims.Values
            .OrderBy(claim => claim.SourcePath, StringComparer.OrdinalIgnoreCase)
            .ToList();

    public async Task<string?> ReadOwnerAsync(string redisKey, CancellationToken cancellationToken = default)
    {
        var database = await redis.GetDatabaseAsync(cancellationToken);
        var owner = await database.HashGetAsync(redisKey, "owner");
        return owner.HasValue ? owner.ToString() : null;
    }

    public async Task<PointOwnershipClaimResult> ClaimAsync(
        string sourcePath,
        string redisKey,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sourcePath);
        ArgumentException.ThrowIfNullOrWhiteSpace(redisKey);
        var pointLease = lifecycleGate is null
            ? null
            : await lifecycleGate.TryAcquireRuntimeAsync(redisKey, cancellationToken);
        await using var pointLeaseCleanup = pointLease;
        if (lifecycleGate is not null && pointLease is null)
        {
            return Store(new PointOwnershipClaimResult(
                sourcePath,
                redisKey,
                false,
                "ownership_releasing",
                options.Value.ConverterId,
                DateTimeOffset.UtcNow));
        }
        if (!RequiresOwnership(sourcePath))
        {
            return Store(new PointOwnershipClaimResult(
                sourcePath,
                redisKey,
                false,
                "unsupported_source",
                null,
                DateTimeOffset.UtcNow));
        }
        if (activation is not null)
        {
            var activationResult = await activation.EvaluateAsync(sourcePath, redisKey, cancellationToken);
            if (!activationResult.CanActivate)
            {
                return StoreInactive(sourcePath, redisKey, activationResult);
            }
        }

        try
        {
            var database = await redis.GetDatabaseAsync(cancellationToken);
            var checkedAt = DateTimeOffset.UtcNow;
            var timestamp = checkedAt.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture);
            var result = (RedisResult[]?)await database.ScriptEvaluateAsync(
                ClaimScript,
                [redisKey, $"edge:{options.Value.ConverterId}"],
                [options.Value.ConverterId, options.Value.SourceName, timestamp]);
            var status = result is { Length: > 0 }
                ? result[0].ToString() ?? ""
                : "";
            var owner = result is { Length: > 1 }
                ? result[1].ToString()
                : null;
            var claim = Store(new PointOwnershipClaimResult(
                sourcePath,
                redisKey,
                status == "ok",
                string.IsNullOrWhiteSpace(status) ? "unexpected_result" : status,
                owner,
                checkedAt));

            if (claim.Acquired)
            {
                runtime.ClearRedisOutputDiagnostic("ownership", sourcePath, redisKey);
            }
            else
            {
                ReportClaimDiagnostic(claim);
                logger.LogWarning(
                    "RedisScpi ownership was not acquired for {SourcePath} -> {RedisKey}; status={Status}; owner={Owner}.",
                    sourcePath,
                    redisKey,
                    claim.Status,
                    claim.Owner);
            }

            return claim;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            var claim = Store(new PointOwnershipClaimResult(
                sourcePath,
                redisKey,
                false,
                "ownership_check_failed",
                null,
                DateTimeOffset.UtcNow));
            runtime.ReportRedisOutputDiagnostic(
                "ownership",
                sourcePath,
                redisKey,
                claim.Status,
                $"Ownership check failed for {sourcePath} -> {redisKey}: {ex.Message}");
            logger.LogWarning(ex, "RedisScpi ownership check failed for {SourcePath} -> {RedisKey}.", sourcePath, redisKey);
            return claim;
        }
    }

    public void RetainClaims(IEnumerable<string> sourcePaths)
    {
        var retained = sourcePaths.ToHashSet(StringComparer.OrdinalIgnoreCase);
        var changed = false;
        foreach (var sourcePath in _claims.Keys)
        {
            if (!retained.Contains(sourcePath))
            {
                if (_claims.TryRemove(sourcePath, out var removed))
                {
                    runtime.ClearRedisOutputDiagnostic("ownership", removed.SourcePath, removed.RedisKey);
                    changed = true;
                }
                _lastClaimAttempts.TryRemove(sourcePath, out _);
            }
        }

        if (changed)
        {
            NotifyChanged();
        }
    }

    public static bool RequiresOwnership(string sourcePath) =>
        !string.IsNullOrWhiteSpace(sourcePath)
        && sourcePath.StartsWith("scpi:", StringComparison.OrdinalIgnoreCase);

    private PointOwnershipClaimResult Store(PointOwnershipClaimResult claim)
    {
        var changed = !_claims.TryGetValue(claim.SourcePath, out var previous)
                      || previous.Acquired != claim.Acquired
                      || !previous.Status.Equals(claim.Status, StringComparison.Ordinal)
                      || !previous.RedisKey.Equals(claim.RedisKey, StringComparison.Ordinal)
                      || !string.Equals(previous.Owner, claim.Owner, StringComparison.Ordinal);
        _claims[claim.SourcePath] = claim;
        if (changed)
        {
            NotifyChanged();
        }
        return claim;
    }

    private PointOwnershipClaimResult StoreInactive(
        string sourcePath,
        string redisKey,
        PointMappingActivationResult activationResult)
    {
        var claim = Store(new PointOwnershipClaimResult(
            sourcePath,
            redisKey,
            false,
            activationResult.DiagnosticCode ?? PointValueDiagnosticCodes.MappingTypeIncompatible,
            null,
            DateTimeOffset.UtcNow));
        runtime.ReportRedisOutputDiagnostic(
            "mapping_activation",
            sourcePath,
            redisKey,
            claim.Status,
            activationResult.DiagnosticMessage ?? "Mapping is inactive.");
        logger.LogWarning(
            "RedisScpi mapping is inactive for {SourcePath} -> {RedisKey}; code={Code}; sourceType={SourceType}; pointType={PointType}; reason={Reason}.",
            sourcePath,
            redisKey,
            claim.Status,
            activationResult.SourcePointType,
            activationResult.TargetPointType,
            activationResult.DiagnosticMessage);
        return claim;
    }

    private void NotifyChanged()
    {
        var handlers = Changed;
        if (handlers is null)
        {
            return;
        }

        foreach (Action handler in handlers.GetInvocationList())
        {
            try
            {
                handler();
            }
            catch (Exception ex)
            {
                logger.LogDebug(ex, "An ownership change subscriber failed.");
            }
        }
    }

    private void ReportClaimDiagnostic(PointOwnershipClaimResult claim)
    {
        var message = claim.Status switch
        {
            "missing" => $"Redis point key '{claim.RedisKey}' does not exist.",
            "invalid_key_type" => $"Redis key '{claim.RedisKey}' is not a Hash.",
            "owned_by_other" => $"Redis point key '{claim.RedisKey}' is owned by {claim.Owner ?? "another converter"}.",
            "unsupported_source" => $"SourcePath '{claim.SourcePath}' is not a SCPI source.",
            _ => $"Ownership was not acquired for {claim.SourcePath} -> {claim.RedisKey}; status={claim.Status}."
        };
        runtime.ReportRedisOutputDiagnostic(
            "ownership",
            claim.SourcePath,
            claim.RedisKey,
            claim.Status == "missing" ? "missing_key" : claim.Status,
            message);
    }
}
