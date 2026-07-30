using Microsoft.EntityFrameworkCore;
using Ptlk.RedisScpi.Contracts.Scpi;
using Ptlk.RedisScpi.Data;
using Ptlk.RedisScpi.Models;
using Ptlk.RedisScpi.Services.Ownership;
using Ptlk.RedisScpi.Services.Paths;

namespace Ptlk.RedisScpi.Services.Scpi;

public sealed class ScpiPointService(
    IDbContextFactory<AppDbContext> dbFactory,
    ScpiSourcePathService paths,
    ScpiTemplateRenderer templates,
    PointOwnershipReleaseIntentService? releases = null)
{
    public async Task<List<ScpiPointConfig>> ListAsync(CancellationToken cancellationToken = default)
    {
        await using var db = await dbFactory.CreateDbContextAsync(cancellationToken);
        return await PointQuery(db)
            .OrderBy(point => point.EndpointConfig!.EndpointId)
            .ThenBy(point => point.PointId)
            .ToListAsync(cancellationToken);
    }

    public async Task<List<ScpiPointConfig>> ListAsync(
        int endpointConfigId,
        CancellationToken cancellationToken = default)
    {
        await using var db = await dbFactory.CreateDbContextAsync(cancellationToken);
        return await PointQuery(db)
            .Where(point => point.EndpointConfigId == endpointConfigId)
            .OrderBy(point => point.PointId)
            .ToListAsync(cancellationToken);
    }

    public async Task<ScpiPointConfig?> GetAsync(
        int id,
        CancellationToken cancellationToken = default)
    {
        await using var db = await dbFactory.CreateDbContextAsync(cancellationToken);
        return await PointQuery(db).FirstOrDefaultAsync(point => point.Id == id, cancellationToken);
    }

    public async Task<IReadOnlyDictionary<string, string>> ListMappingKeysAsync(
        CancellationToken cancellationToken = default)
    {
        await using var db = await dbFactory.CreateDbContextAsync(cancellationToken);
        return await db.RedisMappings
            .AsNoTracking()
            .OrderBy(mapping => mapping.SourcePath)
            .ToDictionaryAsync(
                mapping => mapping.SourcePath,
                mapping => mapping.RedisKey,
                StringComparer.OrdinalIgnoreCase,
                cancellationToken);
    }

    public async Task<ScpiPointConfig> CreateOrUpdateAsync(
        ScpiPointConfig input,
        CancellationToken cancellationToken = default)
    {
        input = SnapshotPoint(input);
        ValidatePoint(input);
        await using var db = await dbFactory.CreateDbContextAsync(cancellationToken);
        await using var transaction = await db.Database.BeginTransactionAsync(cancellationToken);
        var endpoint = await db.ScpiEndpointConfigs
            .FirstOrDefaultAsync(item => item.Id == input.EndpointConfigId, cancellationToken)
            ?? throw new InvalidOperationException("The selected SCPI endpoint does not exist.");
        var normalizedPointId = paths.NormalizeToken(input.PointId, nameof(input.PointId));
        var sourcePath = paths.BuildPointSourcePath(endpoint.EndpointId, normalizedPointId);

        if (await db.ScpiPointConfigs.AnyAsync(
                point => point.EndpointConfigId == endpoint.Id
                         && point.PointId == normalizedPointId
                         && point.Id != input.Id,
                cancellationToken))
        {
            throw new InvalidOperationException(
                $"PointId '{normalizedPointId}' is already in use by endpoint '{endpoint.EndpointId}'.");
        }

        if (await db.ScpiPointConfigs.AnyAsync(
                point => point.SourcePath == sourcePath && point.Id != input.Id,
                cancellationToken))
        {
            throw new InvalidOperationException($"SourcePath '{sourcePath}' is already in use.");
        }

        var entity = input.Id > 0
            ? await db.ScpiPointConfigs
                .Include(point => point.EndpointConfig)
                .Include(point => point.EnumOptions)
                .FirstOrDefaultAsync(point => point.Id == input.Id, cancellationToken)
            : null;

        if (input.Id > 0 && entity is null)
        {
            throw new InvalidOperationException($"SCPI point {input.Id} was not found.");
        }

        RedisMapping? mapping = null;
        if (entity is not null)
        {
            EnsureCurrent(input.ConcurrencyStamp, entity.ConcurrencyStamp, $"point '{entity.SourcePath}'");
            db.Entry(entity).Property(point => point.ConcurrencyStamp).OriginalValue = input.ConcurrencyStamp;
            if (entity.EndpointConfigId != input.EndpointConfigId)
            {
                throw new InvalidOperationException("Moving an existing SCPI point to another endpoint is not supported.");
            }

            mapping = await db.RedisMappings
                .FirstOrDefaultAsync(item => item.SourcePath == entity.SourcePath, cancellationToken);
        }

        if (input.Enabled || input.PollingEnabled)
        {
            mapping ??= await db.RedisMappings
                .FirstOrDefaultAsync(item => item.SourcePath == sourcePath, cancellationToken);
            ValidateRequiredMapping(sourcePath, mapping);
        }

        if (entity is null)
        {
            entity = new ScpiPointConfig
            {
                EndpointConfigId = endpoint.Id,
                SourcePath = sourcePath
            };
            ApplyPoint(entity, input, normalizedPointId, sourcePath);
            SyncEnumOptions(db, entity, input.EnumOptions);
            db.ScpiPointConfigs.Add(entity);
        }
        else
        {
            var oldSourcePath = entity.SourcePath;
            ApplyPoint(entity, input, normalizedPointId, sourcePath);
            SyncEnumOptions(db, entity, input.EnumOptions);
            if (mapping is not null && !oldSourcePath.Equals(sourcePath, StringComparison.Ordinal))
            {
                mapping.SourcePath = sourcePath;
            }
        }

        TouchEndpoint(endpoint);

        try
        {
            await db.SaveChangesAsync(cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            return entity;
        }
        catch (DbUpdateConcurrencyException)
        {
            throw new ScpiConfigurationConcurrencyException(
                "The point, endpoint, or mapping changed after it was opened. Reload and apply the edit again.");
        }
        catch (DbUpdateException ex)
        {
            throw new InvalidOperationException(
                "The point could not be saved because a point identity, enum value, or enum code is already in use.",
                ex);
        }
    }

    public async Task DeleteAsync(
        int id,
        string expectedConcurrencyStamp,
        CancellationToken cancellationToken = default)
    {
        await using var db = await dbFactory.CreateDbContextAsync(cancellationToken);
        var point = await db.ScpiPointConfigs
            .Include(item => item.EndpointConfig)
            .FirstOrDefaultAsync(item => item.Id == id, cancellationToken);
        if (point is null)
        {
            return;
        }

        EnsureCurrent(expectedConcurrencyStamp, point.ConcurrencyStamp, $"point '{point.SourcePath}'");
        db.Entry(point).Property(item => item.ConcurrencyStamp).OriginalValue = expectedConcurrencyStamp;
        var mapping = await db.RedisMappings
            .FirstOrDefaultAsync(item => item.SourcePath == point.SourcePath, cancellationToken);
        if (releases is not null)
        {
            try
            {
                await releases.RequestSourceDeleteAsync(
                    mapping is null ? [] : [mapping.Id],
                    async (sourceDb, token) =>
                    {
                        var current = await sourceDb.ScpiPointConfigs
                            .Include(item => item.EndpointConfig)
                            .FirstOrDefaultAsync(item => item.Id == id, token)
                            ?? throw new ScpiConfigurationConcurrencyException(
                                "The point changed after it was opened. Reload before deleting it.");
                        EnsureCurrent(expectedConcurrencyStamp, current.ConcurrencyStamp, $"point '{current.SourcePath}'");
                        sourceDb.Entry(current).Property(item => item.ConcurrencyStamp).OriginalValue = expectedConcurrencyStamp;
                        if (current.EndpointConfig is not null)
                            TouchEndpoint(current.EndpointConfig);
                        sourceDb.ScpiPointConfigs.Remove(current);
                    },
                    cancellationToken);
                return;
            }
            catch (DbUpdateConcurrencyException)
            {
                throw new ScpiConfigurationConcurrencyException(
                    "The point or its mapping changed after it was opened. Reload before deleting it.");
            }
        }

        if (mapping is not null)
        {
            db.RedisMappings.Remove(mapping);
        }

        if (point.EndpointConfig is not null)
        {
            TouchEndpoint(point.EndpointConfig);
        }
        db.ScpiPointConfigs.Remove(point);

        try
        {
            await db.SaveChangesAsync(cancellationToken);
        }
        catch (DbUpdateConcurrencyException)
        {
            throw new ScpiConfigurationConcurrencyException(
                "The point or its mapping changed after it was opened. Reload before deleting it.");
        }
    }

    public async Task<ScpiPointConfig> CreateOrUpdateEnumOptionAsync(
        int pointConfigId,
        ScpiEnumOption input,
        string expectedPointConcurrencyStamp,
        CancellationToken cancellationToken = default)
    {
        var point = await GetAsync(pointConfigId, cancellationToken)
            ?? throw new InvalidOperationException($"SCPI point {pointConfigId} was not found.");
        EnsureCurrent(expectedPointConcurrencyStamp, point.ConcurrencyStamp, $"point '{point.SourcePath}'");
        if (!point.DataType.Equals(ScpiDataTypes.Enum, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException("Enum options can only be edited for an enum point.");
        }

        var options = point.EnumOptions.Select(CloneOption).ToList();
        if (input.Id > 0)
        {
            var index = options.FindIndex(option => option.Id == input.Id);
            if (index < 0)
            {
                throw new InvalidOperationException($"Enum option {input.Id} does not belong to this point.");
            }
            options[index] = CloneOption(input);
        }
        else
        {
            options.Add(CloneOption(input));
        }

        point.EnumOptions = options;
        point.ConcurrencyStamp = expectedPointConcurrencyStamp;
        return await CreateOrUpdateAsync(point, cancellationToken);
    }

    public async Task<ScpiPointConfig> DeleteEnumOptionAsync(
        int pointConfigId,
        int enumOptionId,
        string expectedPointConcurrencyStamp,
        CancellationToken cancellationToken = default)
    {
        var point = await GetAsync(pointConfigId, cancellationToken)
            ?? throw new InvalidOperationException($"SCPI point {pointConfigId} was not found.");
        EnsureCurrent(expectedPointConcurrencyStamp, point.ConcurrencyStamp, $"point '{point.SourcePath}'");
        if (!point.EnumOptions.Any(option => option.Id == enumOptionId))
        {
            return point;
        }

        point.EnumOptions = point.EnumOptions
            .Where(option => option.Id != enumOptionId)
            .Select(CloneOption)
            .ToList();
        point.ConcurrencyStamp = expectedPointConcurrencyStamp;
        return await CreateOrUpdateAsync(point, cancellationToken);
    }

    public void ValidatePoint(ScpiPointConfig point)
    {
        _ = paths.NormalizeToken(point.PointId, nameof(point.PointId));
        RequireText(point.Name, nameof(point.Name), 160);
        OptionalText(point.DisplayName, nameof(point.DisplayName), 160);
        OptionalText(point.Unit, nameof(point.Unit), 80);

        if (!ScpiAccessModes.IsValid(point.Access))
        {
            throw new InvalidOperationException("Access must be readonly or readwrite.");
        }
        if (!ScpiDataTypes.IsValid(point.DataType))
        {
            throw new InvalidOperationException("DataType must be number, string, or enum.");
        }
        if (point.PollingEnabled && !point.Enabled)
        {
            throw new InvalidOperationException("PollingEnabled requires the point to be enabled.");
        }
        if (point.PollingIntervalMs is < 100)
        {
            throw new InvalidOperationException("Point PollingIntervalMs must be at least 100 ms when set.");
        }

        RequireText(point.ReadTemplate, nameof(point.ReadTemplate), 1000);
        EnsureTemplateBraces(point.ReadTemplate);
        templates.ValidateReadTemplate(point.ReadTemplate);

        if (point.Access.Equals(ScpiAccessModes.Readwrite, StringComparison.OrdinalIgnoreCase))
        {
            RequireText(point.WriteTemplate, nameof(point.WriteTemplate), 1000);
            EnsureTemplateBraces(point.WriteTemplate!);
            templates.ValidateWriteTemplate(point.WriteTemplate);
        }
        else if (!string.IsNullOrWhiteSpace(point.WriteTemplate))
        {
            throw new InvalidOperationException("WriteTemplate must be empty for a readonly point.");
        }

        if (point.DataType.Equals(ScpiDataTypes.Number, StringComparison.OrdinalIgnoreCase)
            && !ScpiNumberTypes.IsValid(point.NumberType))
        {
            throw new InvalidOperationException("NumberType must be int or double for a number point.");
        }
        if (point.DataType.Equals(ScpiDataTypes.String, StringComparison.OrdinalIgnoreCase)
            && !ScpiStringFormats.IsValid(point.StringFormat))
        {
            throw new InvalidOperationException("StringFormat must be raw or quoted for a string point.");
        }
        if (point.DataType.Equals(ScpiDataTypes.Enum, StringComparison.OrdinalIgnoreCase))
        {
            if (!ScpiEnumFormats.IsValid(point.EnumFormat))
            {
                throw new InvalidOperationException("EnumFormat must be value or code for an enum point.");
            }
            if (point.EnumOptions.Count == 0)
            {
                throw new InvalidOperationException("An enum point must contain at least one enum option.");
            }
        }

        ValidateEnumOptions(point.EnumOptions);
    }

    private static IQueryable<ScpiPointConfig> PointQuery(AppDbContext db) =>
        db.ScpiPointConfigs
            .AsNoTracking()
            .Include(point => point.EndpointConfig)
            .Include(point => point.EnumOptions.OrderBy(option => option.SortOrder).ThenBy(option => option.Id));

    private static void ApplyPoint(
        ScpiPointConfig entity,
        ScpiPointConfig input,
        string normalizedPointId,
        string sourcePath)
    {
        entity.PointId = normalizedPointId;
        entity.SourcePath = sourcePath;
        entity.Name = input.Name.Trim();
        entity.DisplayName = NullIfWhiteSpace(input.DisplayName);
        entity.Enabled = input.Enabled;
        entity.Access = input.Access.Trim().ToLowerInvariant();
        entity.DataType = input.DataType.Trim().ToLowerInvariant();
        entity.NumberType = NullIfWhiteSpace(input.NumberType)?.ToLowerInvariant();
        entity.StringFormat = NullIfWhiteSpace(input.StringFormat)?.ToLowerInvariant();
        entity.EnumFormat = NullIfWhiteSpace(input.EnumFormat)?.ToLowerInvariant();
        entity.ReadTemplate = input.ReadTemplate.Trim();
        entity.WriteTemplate = NullIfWhiteSpace(input.WriteTemplate);
        entity.Unit = NullIfWhiteSpace(input.Unit);
        entity.PollingEnabled = input.PollingEnabled;
        entity.PollingIntervalMs = input.PollingIntervalMs;
        entity.InitialRead = input.InitialRead;
    }

    private static void SyncEnumOptions(
        AppDbContext db,
        ScpiPointConfig entity,
        IReadOnlyCollection<ScpiEnumOption> incoming)
    {
        var remainingIds = incoming.Where(option => option.Id > 0).Select(option => option.Id).ToHashSet();
        var removed = entity.EnumOptions.Where(option => option.Id > 0 && !remainingIds.Contains(option.Id)).ToList();
        if (removed.Count > 0)
        {
            db.ScpiEnumOptions.RemoveRange(removed);
            foreach (var option in removed)
            {
                entity.EnumOptions.Remove(option);
            }
        }

        foreach (var option in incoming)
        {
            var target = option.Id > 0
                ? entity.EnumOptions.FirstOrDefault(existing => existing.Id == option.Id)
                : null;
            if (option.Id > 0 && target is null)
            {
                throw new InvalidOperationException($"Enum option {option.Id} does not belong to this point.");
            }

            if (target is null)
            {
                target = new ScpiEnumOption();
                entity.EnumOptions.Add(target);
            }

            target.DisplayName = option.DisplayName.Trim();
            target.Value = option.Value.Trim();
            target.Code = option.Code;
            target.SortOrder = option.SortOrder;
        }
    }

    private static void ValidateEnumOptions(IReadOnlyCollection<ScpiEnumOption> options)
    {
        foreach (var option in options)
        {
            RequireText(option.DisplayName, "Enum option DisplayName", 160);
            RequireText(option.Value, "Enum option Value", 320);
        }

        var duplicateValue = options
            .GroupBy(option => option.Value.Trim(), StringComparer.OrdinalIgnoreCase)
            .FirstOrDefault(group => group.Count() > 1);
        if (duplicateValue is not null)
        {
            throw new InvalidOperationException($"Enum value '{duplicateValue.Key}' is duplicated.");
        }

        var duplicateCode = options.GroupBy(option => option.Code).FirstOrDefault(group => group.Count() > 1);
        if (duplicateCode is not null)
        {
            throw new InvalidOperationException($"Enum code '{duplicateCode.Key}' is duplicated.");
        }
    }

    private static void ValidateRequiredMapping(string sourcePath, RedisMapping? mapping)
    {
        if (mapping is null)
        {
            throw new InvalidOperationException(
                $"Point '{sourcePath}' must have a RedisMapping before it can be enabled or polled.");
        }
        if (!mapping.RedisKey.StartsWith("point:", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                $"RedisMapping for '{sourcePath}' must target a point: key.");
        }
    }

    private static void EnsureTemplateBraces(string template)
    {
        var withoutKnownVariables = template
            .Replace("{name}", "", StringComparison.Ordinal)
            .Replace("{value}", "", StringComparison.Ordinal);
        if (withoutKnownVariables.Contains('{') || withoutKnownVariables.Contains('}'))
        {
            throw new ScpiTemplateException("SCPI template contains an unsupported or malformed variable.");
        }
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

    private static void TouchEndpoint(ScpiEndpointConfig endpoint) =>
        endpoint.ConcurrencyStamp = Guid.NewGuid().ToString("N");

    private static ScpiEnumOption CloneOption(ScpiEnumOption option) =>
        new()
        {
            Id = option.Id,
            ScpiPointConfigId = option.ScpiPointConfigId,
            DisplayName = option.DisplayName,
            Value = option.Value,
            Code = option.Code,
            SortOrder = option.SortOrder,
            CreatedAt = option.CreatedAt,
            UpdatedAt = option.UpdatedAt
        };

    private static ScpiPointConfig SnapshotPoint(ScpiPointConfig point) =>
        new()
        {
            Id = point.Id,
            EndpointConfigId = point.EndpointConfigId,
            PointId = point.PointId,
            SourcePath = point.SourcePath,
            Name = point.Name,
            DisplayName = point.DisplayName,
            Enabled = point.Enabled,
            Access = point.Access,
            DataType = point.DataType,
            NumberType = point.NumberType,
            StringFormat = point.StringFormat,
            EnumFormat = point.EnumFormat,
            ReadTemplate = point.ReadTemplate,
            WriteTemplate = point.WriteTemplate,
            Unit = point.Unit,
            PollingEnabled = point.PollingEnabled,
            PollingIntervalMs = point.PollingIntervalMs,
            InitialRead = point.InitialRead,
            CreatedAt = point.CreatedAt,
            UpdatedAt = point.UpdatedAt,
            ConcurrencyStamp = point.ConcurrencyStamp,
            EnumOptions = point.EnumOptions.Select(CloneOption).ToList()
        };

    private static string? NullIfWhiteSpace(string? value) =>
        string.IsNullOrWhiteSpace(value) ? null : value.Trim();

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

    private static void OptionalText(string? value, string fieldName, int maxLength)
    {
        if (value is not null && value.Trim().Length > maxLength)
        {
            throw new InvalidOperationException($"{fieldName} must be {maxLength} characters or fewer.");
        }
    }
}
