using Microsoft.EntityFrameworkCore;
using Ptlk.RedisScpi.Data;
using Ptlk.SCADA.Interop.PointValues;

namespace Ptlk.RedisScpi.Services.Redis;

public sealed class RedisMappingActivationService(
    IDbContextFactory<AppDbContext> dbFactory,
    RedisConnectionFactory redis)
{
    public async Task<PointMappingActivationResult> EvaluateAsync(
        string sourcePath,
        string redisKey,
        CancellationToken cancellationToken = default)
    {
        var targetType = await ReadTargetTypeAsync(redisKey, cancellationToken);
        if (targetType is null)
        {
            return PointMappingActivationResult.Inactive(
                "<unknown>",
                "<unknown>",
                $"Redis metadata for '{redisKey}' is unavailable or has no canonical type.",
                PointValueDiagnosticCodes.MappingMetadataUnavailable);
        }

        await using var db = await dbFactory.CreateDbContextAsync(cancellationToken);
        var point = await db.ScpiPointConfigs
            .AsNoTracking()
            .Where(item => item.SourcePath == sourcePath)
            .Select(item => new
            {
                item.DataType,
                item.NumberType,
                item.EnumFormat
            })
            .FirstOrDefaultAsync(cancellationToken);
        if (point is null)
        {
            return SourceUnavailable(sourcePath, targetType);
        }

        return EvaluateScpiType(
                point.DataType,
                point.NumberType,
                point.EnumFormat,
                targetType)
            ?? SourceUnavailable(sourcePath, targetType);
    }

    public static PointMappingActivationResult? EvaluateScpiType(
        string dataType,
        string? numberType,
        string? enumFormat,
        string targetType)
    {
        (string PointType, bool SafeForDouble)? source = dataType.ToLowerInvariant() switch
        {
            "number" when string.Equals(numberType, "int", StringComparison.OrdinalIgnoreCase) =>
                (PointType: "int", SafeForDouble: false),
            "number" when string.Equals(numberType, "double", StringComparison.OrdinalIgnoreCase) =>
                (PointType: "double", SafeForDouble: false),
            "string" =>
                (PointType: "string", SafeForDouble: false),
            "enum" when string.Equals(enumFormat, "code", StringComparison.OrdinalIgnoreCase) =>
                (PointType: "int", SafeForDouble: true),
            "enum" when string.Equals(enumFormat, "value", StringComparison.OrdinalIgnoreCase) =>
                (PointType: "string", SafeForDouble: false),
            _ => null
        };
        return source is null
            ? null
            : PointMappingCompatibility.Evaluate(
                source.Value.PointType,
                targetType,
                sourceIntegerRangeIsSafeForDouble: source.Value.SafeForDouble);
    }

    private async Task<string?> ReadTargetTypeAsync(
        string redisKey,
        CancellationToken cancellationToken)
    {
        try
        {
            var database = await redis.GetDatabaseAsync(cancellationToken);
            var value = await database.HashGetAsync(redisKey, "type");
            return value.IsNull ? null : PointValueCanonicalizer.NormalizePointType(value.ToString());
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch
        {
            return null;
        }
    }

    private static PointMappingActivationResult SourceUnavailable(string sourcePath, string targetType) =>
        PointMappingActivationResult.Inactive(
            "<unknown>",
            targetType,
            $"Source type for '{sourcePath}' is unavailable.",
            PointValueDiagnosticCodes.MappingSourceTypeUnavailable);
}
