using System.Globalization;
using Microsoft.EntityFrameworkCore;
using Ptlk.RedisScpi.Contracts.Redis;
using Ptlk.RedisScpi.Data;
using Ptlk.RedisScpi.Models;
using Ptlk.RedisScpi.Services.Paths;
using Ptlk.RedisScpi.Services.Ownership;
using Ptlk.RedisScpi.Services.Scpi;
using Ptlk.SCADA.Interop.PointValues;
using StackExchange.Redis;

namespace Ptlk.RedisScpi.Services.Redis;

public sealed record MappingValidationResult(
    bool Success,
    string? Error,
    PointMappingActivationResult? Activation = null);

public sealed record RedisMappingKeyCheckResult(string SourcePath, string RedisKey);

public sealed record RedisMappingRuntimeIssue(
    string SourcePath,
    string RedisKey,
    string Status,
    string Message);

public sealed class RedisMappingValidationService(
    IDbContextFactory<AppDbContext> dbFactory,
    RedisConnectionFactory redis,
    RedisMappingActivationService? activation = null,
    PointOwnershipReleaseIntentService? releases = null)
{
    private static readonly RedisValue[] RequiredPointFields =
    [
        "quality",
        "type",
        "timestamp",
        "version",
        "source",
        "access",
        "unit"
    ];

    public async Task<List<RedisMapping>> ListAsync(CancellationToken cancellationToken = default)
    {
        await using var db = await dbFactory.CreateDbContextAsync(cancellationToken);
        return await db.RedisMappings
            .AsNoTracking()
            .OrderBy(mapping => mapping.SourcePath)
            .ToListAsync(cancellationToken);
    }

    public MappingValidationResult Validate(string sourcePath, string redisKey)
    {
        var normalizedSourcePath = sourcePath.Trim();
        var normalizedRedisKey = redisKey.Trim();
        if (!ScpiSourcePathRules.TryParsePointSourcePath(normalizedSourcePath, out _, out _))
        {
            return new MappingValidationResult(
                false,
                "SourcePath must use scpi:{endpointId}/{pointId} with non-empty ASCII identifiers.");
        }

        if (string.IsNullOrWhiteSpace(normalizedRedisKey))
        {
            return new MappingValidationResult(false, "RedisKey is required.");
        }

        if (!normalizedRedisKey.StartsWith(RedisContractNames.PointPrefix, StringComparison.Ordinal)
            || normalizedRedisKey.Length == RedisContractNames.PointPrefix.Length)
        {
            return new MappingValidationResult(false, "RedisKey must be a complete point:{path} key.");
        }

        return new MappingValidationResult(true, null);
    }

    public async Task<MappingValidationResult> ValidateAsync(
        string sourcePath,
        string redisKey,
        int? editId = null,
        CancellationToken cancellationToken = default)
    {
        await using var db = await dbFactory.CreateDbContextAsync(cancellationToken);
        return await ValidateAsync(db, sourcePath, redisKey, editId, cancellationToken);
    }

    private async Task<MappingValidationResult> ValidateAsync(
        AppDbContext db,
        string sourcePath,
        string redisKey,
        int? editId,
        CancellationToken cancellationToken)
    {
        var result = Validate(sourcePath, redisKey);
        if (!result.Success)
        {
            return result;
        }

        var normalizedSourcePath = sourcePath.Trim();
        var normalizedRedisKey = redisKey.Trim();
        var effectiveEditId = editId ?? 0;
        if (await db.RedisMappings.AnyAsync(
                mapping => mapping.SourcePath == normalizedSourcePath && mapping.Id != effectiveEditId,
                cancellationToken))
        {
            return new MappingValidationResult(
                false,
                $"SourcePath '{normalizedSourcePath}' is already used by another mapping.");
        }

        if (await db.RedisMappings.AnyAsync(
                mapping => mapping.RedisKey == normalizedRedisKey && mapping.Id != effectiveEditId,
                cancellationToken))
        {
            return new MappingValidationResult(
                false,
                $"RedisKey '{normalizedRedisKey}' is already used by another mapping.");
        }

        if (!await db.ScpiPointConfigs.AnyAsync(
                point => point.SourcePath == normalizedSourcePath,
                cancellationToken))
        {
            return new MappingValidationResult(
                false,
                $"SourcePath '{normalizedSourcePath}' does not match any SCPI point.");
        }

        if (activation is null)
        {
            return new MappingValidationResult(true, null);
        }

        var activationResult = await activation.EvaluateAsync(
            normalizedSourcePath,
            normalizedRedisKey,
            cancellationToken);
        return new MappingValidationResult(
            true,
            activationResult.CanActivate
                ? null
                : $"{activationResult.DiagnosticCode}: {activationResult.DiagnosticMessage}",
            activationResult);
    }

    public async Task<RedisMapping> CreateOrUpdateAsync(
        int? id,
        string sourcePath,
        string redisKey,
        string? expectedConcurrencyStamp = null,
        CancellationToken cancellationToken = default)
    {
        await using var db = await dbFactory.CreateDbContextAsync(cancellationToken);
        await using var transaction = await db.Database.BeginTransactionAsync(cancellationToken);
        var validation = await ValidateAsync(db, sourcePath, redisKey, id, cancellationToken);
        if (!validation.Success)
        {
            throw new InvalidOperationException(validation.Error);
        }

        var mapping = id is > 0
            ? await db.RedisMappings.FirstAsync(item => item.Id == id.Value, cancellationToken)
            : new RedisMapping();
        if (id is > 0)
        {
            if (string.IsNullOrWhiteSpace(expectedConcurrencyStamp)
                || !mapping.ConcurrencyStamp.Equals(expectedConcurrencyStamp, StringComparison.Ordinal))
            {
                throw new ScpiConfigurationConcurrencyException(
                    "The RedisMapping changed after it was opened. Reload and apply the edit again.");
            }
            db.Entry(mapping).Property(item => item.ConcurrencyStamp).OriginalValue = expectedConcurrencyStamp;

            var normalizedSourcePath = sourcePath.Trim();
            if (!mapping.SourcePath.Equals(normalizedSourcePath, StringComparison.Ordinal))
            {
                await EnsurePointCanLoseMappingAsync(db, mapping.SourcePath, cancellationToken);
            }

            var normalizedRedisKey = redisKey.Trim();
            if (releases is not null && !mapping.RedisKey.Equals(normalizedRedisKey, StringComparison.Ordinal))
            {
                await EnsurePointCanLoseMappingAsync(db, mapping.SourcePath, cancellationToken);
                await transaction.RollbackAsync(cancellationToken);
                await releases.RequestRemapAsync(
                    mapping.Id,
                    normalizedSourcePath,
                    normalizedRedisKey,
                    cancellationToken);
                return new RedisMapping
                {
                    SourcePath = normalizedSourcePath,
                    RedisKey = normalizedRedisKey
                };
            }
        }
        mapping.SourcePath = sourcePath.Trim();
        mapping.RedisKey = redisKey.Trim();
        mapping.ConcurrencyStamp = Guid.NewGuid().ToString("N");
        mapping.UpdatedAt = DateTimeOffset.UtcNow;
        if (id is null or <= 0)
        {
            db.RedisMappings.Add(mapping);
        }

        try
        {
            await db.SaveChangesAsync(cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            return mapping;
        }
        catch (DbUpdateConcurrencyException)
        {
            throw new ScpiConfigurationConcurrencyException(
                "The RedisMapping changed after it was opened. Reload and apply the edit again.");
        }
        catch (DbUpdateException ex)
        {
            throw new InvalidOperationException(
                "The mapping could not be saved because its SourcePath or RedisKey is already in use.",
                ex);
        }
    }

    public async Task DeleteAsync(
        int id,
        string? expectedConcurrencyStamp = null,
        CancellationToken cancellationToken = default)
    {
        await using var db = await dbFactory.CreateDbContextAsync(cancellationToken);
        await using var transaction = await db.Database.BeginTransactionAsync(cancellationToken);
        var mapping = await db.RedisMappings.FindAsync([id], cancellationToken);
        if (mapping is null)
        {
            return;
        }

        if (!string.IsNullOrWhiteSpace(expectedConcurrencyStamp))
        {
            if (!mapping.ConcurrencyStamp.Equals(expectedConcurrencyStamp, StringComparison.Ordinal))
            {
                throw new ScpiConfigurationConcurrencyException(
                    "The RedisMapping changed after it was opened. Reload before deleting it.");
            }
            db.Entry(mapping).Property(item => item.ConcurrencyStamp).OriginalValue = expectedConcurrencyStamp;
        }

        await EnsurePointCanLoseMappingAsync(db, mapping.SourcePath, cancellationToken);
        if (releases is not null)
        {
            await transaction.RollbackAsync(cancellationToken);
            _ = await releases.RequestDeleteAsync(id, cancellationToken);
            return;
        }

        db.RedisMappings.Remove(mapping);
        try
        {
            await db.SaveChangesAsync(cancellationToken);
            await transaction.CommitAsync(cancellationToken);
        }
        catch (DbUpdateConcurrencyException)
        {
            throw new ScpiConfigurationConcurrencyException(
                "The RedisMapping changed after it was opened. Reload before deleting it.");
        }
    }

    private static async Task EnsurePointCanLoseMappingAsync(
        AppDbContext db,
        string sourcePath,
        CancellationToken cancellationToken)
    {
        var point = await db.ScpiPointConfigs.AsNoTracking()
            .Where(item => item.SourcePath == sourcePath)
            .Select(item => new { item.Enabled, item.PollingEnabled })
            .FirstOrDefaultAsync(cancellationToken);
        if (point is { Enabled: true } || point is { PollingEnabled: true })
        {
            throw new InvalidOperationException(
                $"Disable point '{sourcePath}' and its polling before removing or moving its RedisMapping.");
        }
    }

    public async Task<IReadOnlyList<RedisMappingKeyCheckResult>> VerifyExistingRedisKeysAsync(
        CancellationToken cancellationToken = default)
    {
        await using var db = await dbFactory.CreateDbContextAsync(cancellationToken);
        var mappings = await db.RedisMappings.AsNoTracking().ToListAsync(cancellationToken);
        if (mappings.Count == 0)
        {
            return [];
        }

        var database = await redis.GetDatabaseAsync(cancellationToken);
        var missing = new List<RedisMappingKeyCheckResult>();
        foreach (var mapping in mappings)
        {
            if (!await database.KeyExistsAsync(mapping.RedisKey))
            {
                missing.Add(new RedisMappingKeyCheckResult(mapping.SourcePath, mapping.RedisKey));
            }
        }

        return missing;
    }

    public async Task<IReadOnlyList<RedisMappingRuntimeIssue>> VerifyRuntimeMappingsAsync(
        CancellationToken cancellationToken = default)
    {
        await using var db = await dbFactory.CreateDbContextAsync(cancellationToken);
        var points = await db.ScpiPointConfigs
            .AsNoTracking()
            .Where(point => point.Enabled
                            && point.EndpointConfig != null
                            && point.EndpointConfig.Enabled)
            .OrderBy(point => point.SourcePath)
            .Select(point => new { point.SourcePath })
            .ToListAsync(cancellationToken);
        if (points.Count == 0)
        {
            return [];
        }

        var mappings = await db.RedisMappings
            .AsNoTracking()
            .ToDictionaryAsync(
                mapping => mapping.SourcePath,
                StringComparer.OrdinalIgnoreCase,
                cancellationToken);
        var database = await redis.GetDatabaseAsync(cancellationToken);
        var issues = new List<RedisMappingRuntimeIssue>();
        foreach (var point in points)
        {
            if (!mappings.TryGetValue(point.SourcePath, out var mapping))
            {
                issues.Add(new RedisMappingRuntimeIssue(
                    point.SourcePath,
                    "",
                    "missing_mapping",
                    $"Enabled SCPI point '{point.SourcePath}' has no Redis mapping."));
                continue;
            }

            if (activation is not null)
            {
                var activationResult = await activation.EvaluateAsync(
                    mapping.SourcePath,
                    mapping.RedisKey,
                    cancellationToken);
                if (!activationResult.CanActivate)
                {
                    issues.Add(new RedisMappingRuntimeIssue(
                        mapping.SourcePath,
                        mapping.RedisKey,
                        activationResult.DiagnosticCode ?? PointValueDiagnosticCodes.MappingTypeIncompatible,
                        activationResult.DiagnosticMessage ?? "Mapping is inactive."));
                    continue;
                }
            }

            var issue = await ValidateRedisPointAsync(database, mapping, cancellationToken);
            if (issue is not null)
            {
                issues.Add(issue);
            }
        }

        return issues;
    }

    private static async Task<RedisMappingRuntimeIssue?> ValidateRedisPointAsync(
        IDatabase database,
        RedisMapping mapping,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var type = await database.KeyTypeAsync(mapping.RedisKey);
        if (type == RedisType.None)
        {
            return new RedisMappingRuntimeIssue(
                mapping.SourcePath,
                mapping.RedisKey,
                "missing_key",
                $"Redis point key '{mapping.RedisKey}' does not exist.");
        }

        if (type != RedisType.Hash)
        {
            return new RedisMappingRuntimeIssue(
                mapping.SourcePath,
                mapping.RedisKey,
                "invalid_key_type",
                $"Redis key '{mapping.RedisKey}' is not a Hash.");
        }

        var values = await database.HashGetAsync(mapping.RedisKey, RequiredPointFields);
        var diagnostics = new List<string>();
        for (var index = 0; index < RequiredPointFields.Length; index++)
        {
            if (values[index].IsNull)
            {
                diagnostics.Add($"required_field_missing:{RequiredPointFields[index]}");
            }
        }

        if (!values[0].IsNull && values[0].ToString() is not ("unset" or "good" or "uncertain" or "bad"))
        {
            diagnostics.Add("required_field_invalid:quality");
        }

        if (!values[1].IsNull && values[1].ToString() is not ("int" or "double" or "bool" or "string"))
        {
            diagnostics.Add("required_field_invalid:type");
        }

        if (!values[2].IsNull && !TryParseNonNegativeLong(values[2], out _))
        {
            diagnostics.Add("required_field_invalid:timestamp");
        }

        if (!values[3].IsNull && !TryParseNonNegativeLong(values[3], out _))
        {
            diagnostics.Add("required_field_invalid:version");
        }

        if (!values[4].IsNull && string.IsNullOrWhiteSpace(values[4].ToString()))
        {
            diagnostics.Add("required_field_invalid:source");
        }

        if (!values[5].IsNull && values[5].ToString() is not ("readonly" or "readwrite"))
        {
            diagnostics.Add("required_field_invalid:access");
        }

        if (diagnostics.Count == 0)
        {
            return null;
        }

        return new RedisMappingRuntimeIssue(
            mapping.SourcePath,
            mapping.RedisKey,
            diagnostics[0],
            $"Redis point key '{mapping.RedisKey}' is not canonical: {string.Join(", ", diagnostics)}.");
    }

    private static bool TryParseNonNegativeLong(RedisValue value, out long parsed) =>
        long.TryParse(
            value.ToString(),
            NumberStyles.Integer,
            CultureInfo.InvariantCulture,
            out parsed)
        && parsed >= 0;

}
