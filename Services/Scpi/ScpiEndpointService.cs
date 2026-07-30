using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Ptlk.RedisScpi.Configuration;
using Ptlk.RedisScpi.Contracts.Scpi;
using Ptlk.RedisScpi.Data;
using Ptlk.RedisScpi.Models;
using Ptlk.RedisScpi.Services.Ownership;
using Ptlk.RedisScpi.Services.Paths;

namespace Ptlk.RedisScpi.Services.Scpi;

public sealed class ScpiConfigurationConcurrencyException(string message) : InvalidOperationException(message);

public sealed class ScpiEndpointService(
    IDbContextFactory<AppDbContext> dbFactory,
    ScpiSourcePathService paths,
    IOptions<RedisScpiOptions> redisScpiOptions,
    PointOwnershipReleaseIntentService? releases = null)
{
    public const string TcpTransport = "tcp";

    public async Task<List<ScpiEndpointConfig>> ListAsync(CancellationToken cancellationToken = default)
    {
        await using var db = await dbFactory.CreateDbContextAsync(cancellationToken);
        return await db.ScpiEndpointConfigs
            .AsNoTracking()
            .AsSplitQuery()
            .Include(endpoint => endpoint.Points)
                .ThenInclude(point => point.EnumOptions)
            .OrderBy(endpoint => endpoint.EndpointId)
            .ToListAsync(cancellationToken);
    }

    public async Task<ScpiEndpointConfig?> GetAsync(
        int id,
        CancellationToken cancellationToken = default)
    {
        await using var db = await dbFactory.CreateDbContextAsync(cancellationToken);
        return await db.ScpiEndpointConfigs
            .AsNoTracking()
            .AsSplitQuery()
            .Include(endpoint => endpoint.Points)
                .ThenInclude(point => point.EnumOptions)
            .FirstOrDefaultAsync(endpoint => endpoint.Id == id, cancellationToken);
    }

    public async Task<ScpiEndpointConfig> CreateOrUpdateAsync(
        ScpiEndpointConfig input,
        CancellationToken cancellationToken = default)
    {
        var normalized = NormalizeAndValidate(input);
        await using var db = await dbFactory.CreateDbContextAsync(cancellationToken);

        if (await db.ScpiEndpointConfigs.AnyAsync(
                endpoint => endpoint.EndpointId == normalized.EndpointId && endpoint.Id != input.Id,
                cancellationToken))
        {
            throw new InvalidOperationException($"EndpointId '{normalized.EndpointId}' is already in use.");
        }

        var entity = input.Id > 0
            ? await db.ScpiEndpointConfigs
                .Include(endpoint => endpoint.Points)
                .FirstOrDefaultAsync(endpoint => endpoint.Id == input.Id, cancellationToken)
            : null;

        if (input.Id > 0 && entity is null)
        {
            throw new InvalidOperationException($"SCPI endpoint {input.Id} was not found.");
        }

        if (entity is null)
        {
            entity = new ScpiEndpointConfig();
            Apply(entity, normalized);
            db.ScpiEndpointConfigs.Add(entity);
        }
        else
        {
            EnsureCurrent(input.ConcurrencyStamp, entity.ConcurrencyStamp, $"endpoint '{entity.EndpointId}'");
            db.Entry(entity).Property(endpoint => endpoint.ConcurrencyStamp).OriginalValue = input.ConcurrencyStamp;

            var oldEndpointId = entity.EndpointId;
            if (!oldEndpointId.Equals(normalized.EndpointId, StringComparison.Ordinal))
            {
                await RenamePointSourcesAsync(db, entity, normalized.EndpointId, cancellationToken);
            }

            Apply(entity, normalized);
        }

        try
        {
            await db.SaveChangesAsync(cancellationToken);
            return entity;
        }
        catch (DbUpdateConcurrencyException)
        {
            throw new ScpiConfigurationConcurrencyException(
                "The endpoint or one of its mappings changed after it was opened. Reload and apply the edit again.");
        }
        catch (DbUpdateException ex)
        {
            throw new InvalidOperationException("The endpoint could not be saved because a unique setting is already in use.", ex);
        }
    }

    public async Task DeleteAsync(
        int id,
        string expectedConcurrencyStamp,
        CancellationToken cancellationToken = default)
    {
        await using var db = await dbFactory.CreateDbContextAsync(cancellationToken);
        var endpoint = await db.ScpiEndpointConfigs
            .Include(item => item.Points)
            .FirstOrDefaultAsync(item => item.Id == id, cancellationToken);
        if (endpoint is null)
        {
            return;
        }

        EnsureCurrent(expectedConcurrencyStamp, endpoint.ConcurrencyStamp, $"endpoint '{endpoint.EndpointId}'");
        db.Entry(endpoint).Property(item => item.ConcurrencyStamp).OriginalValue = expectedConcurrencyStamp;

        var sourcePaths = endpoint.Points.Select(point => point.SourcePath).ToList();
        if (sourcePaths.Count > 0)
        {
            var mappings = await db.RedisMappings
                .Where(mapping => sourcePaths.Contains(mapping.SourcePath))
                .ToListAsync(cancellationToken);
            if (releases is not null)
            {
                try
                {
                    await releases.RequestSourceDeleteAsync(
                        mappings.Select(mapping => mapping.Id).ToArray(),
                        async (sourceDb, token) =>
                        {
                            var current = await sourceDb.ScpiEndpointConfigs
                                .Include(item => item.Points)
                                .FirstOrDefaultAsync(item => item.Id == id, token)
                                ?? throw new ScpiConfigurationConcurrencyException(
                                    "The endpoint changed after it was opened. Reload before deleting it.");
                            EnsureCurrent(expectedConcurrencyStamp, current.ConcurrencyStamp, $"endpoint '{current.EndpointId}'");
                            sourceDb.Entry(current).Property(item => item.ConcurrencyStamp).OriginalValue = expectedConcurrencyStamp;
                            sourceDb.ScpiEndpointConfigs.Remove(current);
                        },
                        cancellationToken);
                    return;
                }
                catch (DbUpdateConcurrencyException)
                {
                    throw new ScpiConfigurationConcurrencyException(
                        "The endpoint or one of its mappings changed after it was opened. Reload before deleting it.");
                }
            }
            db.RedisMappings.RemoveRange(mappings);
        }
        else if (releases is not null)
        {
            await releases.RequestSourceDeleteAsync(
                [],
                async (sourceDb, token) =>
                {
                    var current = await sourceDb.ScpiEndpointConfigs
                        .FirstOrDefaultAsync(item => item.Id == id, token)
                        ?? throw new ScpiConfigurationConcurrencyException(
                            "The endpoint changed after it was opened. Reload before deleting it.");
                    EnsureCurrent(expectedConcurrencyStamp, current.ConcurrencyStamp, $"endpoint '{current.EndpointId}'");
                    sourceDb.Entry(current).Property(item => item.ConcurrencyStamp).OriginalValue = expectedConcurrencyStamp;
                    sourceDb.ScpiEndpointConfigs.Remove(current);
                },
                cancellationToken);
            return;
        }

        db.ScpiEndpointConfigs.Remove(endpoint);
        try
        {
            await db.SaveChangesAsync(cancellationToken);
        }
        catch (DbUpdateConcurrencyException)
        {
            throw new ScpiConfigurationConcurrencyException(
                "The endpoint or one of its mappings changed after it was opened. Reload before deleting it.");
        }
    }

    public ScpiEndpointConfig NormalizeAndValidate(ScpiEndpointConfig input)
    {
        var endpointId = paths.NormalizeToken(input.EndpointId, nameof(input.EndpointId));
        RequireText(input.DisplayName, nameof(input.DisplayName), 160);

        var transport = input.Transport?.Trim().ToLowerInvariant() ?? "";
        if (transport != TcpTransport)
        {
            throw new InvalidOperationException("Transport must be tcp in the first SCPI implementation.");
        }

        RequireText(input.TcpHost, nameof(input.TcpHost), 255);
        if (input.TcpPort is not (> 0 and <= 65535))
        {
            throw new InvalidOperationException("TCP port must be between 1 and 65535.");
        }
        if (input.TimeoutMs <= 0)
        {
            throw new InvalidOperationException("TimeoutMs must be greater than zero.");
        }
        if (input.PollingIntervalMs < 100)
        {
            throw new InvalidOperationException("PollingIntervalMs must be at least 100 ms.");
        }

        var converterId = string.IsNullOrWhiteSpace(input.ConverterId)
            ? redisScpiOptions.Value.ConverterId
            : input.ConverterId.Trim();
        _ = paths.NormalizeToken(converterId, nameof(input.ConverterId));

        var errorCheckMode = input.ErrorCheckMode?.Trim().ToLowerInvariant() ?? "";
        if (!ScpiErrorCheckModes.IsValid(errorCheckMode))
        {
            throw new InvalidOperationException("ErrorCheckMode must be none, after-write, or after-command.");
        }

        RequireText(input.ErrorQueueQuery, nameof(input.ErrorQueueQuery), 1000);
        ValidateTerminator(input.CommandTerminator, nameof(input.CommandTerminator));
        ValidateTerminator(input.ResponseTerminator, nameof(input.ResponseTerminator));

        return new ScpiEndpointConfig
        {
            Id = input.Id,
            EndpointId = endpointId,
            DisplayName = input.DisplayName.Trim(),
            Enabled = input.Enabled,
            Transport = transport,
            TcpHost = input.TcpHost!.Trim(),
            TcpPort = input.TcpPort,
            TimeoutMs = input.TimeoutMs,
            PollingIntervalMs = input.PollingIntervalMs,
            ConverterId = converterId,
            ErrorCheckMode = errorCheckMode,
            ErrorQueueQuery = input.ErrorQueueQuery.Trim(),
            CommandTerminator = input.CommandTerminator,
            ResponseTerminator = input.ResponseTerminator,
            ConcurrencyStamp = input.ConcurrencyStamp
        };
    }

    private async Task RenamePointSourcesAsync(
        AppDbContext db,
        ScpiEndpointConfig endpoint,
        string newEndpointId,
        CancellationToken cancellationToken)
    {
        var oldPaths = endpoint.Points.Select(point => point.SourcePath).ToList();
        var mappings = oldPaths.Count == 0
            ? []
            : await db.RedisMappings
                .Where(mapping => oldPaths.Contains(mapping.SourcePath))
                .ToListAsync(cancellationToken);
        var mappingsBySource = mappings.ToDictionary(
            mapping => mapping.SourcePath,
            StringComparer.OrdinalIgnoreCase);

        foreach (var point in endpoint.Points)
        {
            var oldPath = point.SourcePath;
            var newPath = paths.BuildPointSourcePath(newEndpointId, point.PointId);
            point.SourcePath = newPath;
            if (mappingsBySource.TryGetValue(oldPath, out var mapping))
            {
                mapping.SourcePath = newPath;
            }
        }
    }

    private static void Apply(ScpiEndpointConfig entity, ScpiEndpointConfig normalized)
    {
        entity.EndpointId = normalized.EndpointId;
        entity.DisplayName = normalized.DisplayName;
        entity.Enabled = normalized.Enabled;
        entity.Transport = normalized.Transport;
        entity.TcpHost = normalized.TcpHost;
        entity.TcpPort = normalized.TcpPort;
        entity.TimeoutMs = normalized.TimeoutMs;
        entity.PollingIntervalMs = normalized.PollingIntervalMs;
        entity.ConverterId = normalized.ConverterId;
        entity.ErrorCheckMode = normalized.ErrorCheckMode;
        entity.ErrorQueueQuery = normalized.ErrorQueueQuery;
        entity.CommandTerminator = normalized.CommandTerminator;
        entity.ResponseTerminator = normalized.ResponseTerminator;
    }

    private static void EnsureCurrent(string expected, string actual, string label)
    {
        if (string.IsNullOrWhiteSpace(expected)
            || !expected.Equals(actual, StringComparison.Ordinal))
        {
            throw new ScpiConfigurationConcurrencyException(
                $"The {label} changed after it was opened. Reload and apply the edit again.");
        }
    }

    private static void RequireText(string? value, string fieldName, int maxLength)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new InvalidOperationException($"{fieldName} is required.");
        }
        if (value.Trim().Length > maxLength)
        {
            throw new InvalidOperationException($"{fieldName} must be {maxLength} characters or fewer.");
        }
    }

    private static void ValidateTerminator(string? value, string fieldName)
    {
        if (string.IsNullOrEmpty(value))
        {
            throw new InvalidOperationException($"{fieldName} is required.");
        }
        if (value.Length > 16)
        {
            throw new InvalidOperationException($"{fieldName} must be 16 characters or fewer.");
        }
        if (value.Contains('\0'))
        {
            throw new InvalidOperationException($"{fieldName} must not contain a null character.");
        }
    }
}
