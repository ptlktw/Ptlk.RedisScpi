using System.Globalization;
using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Options;
using Ptlk.RedisScpi.Configuration;
using Ptlk.RedisScpi.Contracts.Scpi;
using Ptlk.RedisScpi.Data;
using Ptlk.RedisScpi.Models;
using Ptlk.RedisScpi.Services.Ownership;
using Ptlk.SCADA.Interop.Runtime;

namespace Ptlk.RedisScpi.Services.ImportExport;

public sealed record CsvImportResult(int ImportedRows, IReadOnlyList<string> Errors)
{
    public bool Success => Errors.Count == 0;
}

public sealed class CsvConfigService(
    IDbContextFactory<AppDbContext> dbFactory,
    IOptions<ImportExportOptions> options,
    PointOwnershipReleaseIntentService? releases = null)
{
    public async Task<Stream> ExportAsync(CancellationToken cancellationToken = default)
    {
        await using var db = await dbFactory.CreateDbContextAsync(cancellationToken);
        await using var transaction = await db.Database.BeginTransactionAsync(cancellationToken);
        var builder = new BoundedCsvBuilder(options.Value.SingleCsvLimitBytes);
        builder.AppendRecord(ScpiConfigCsvSchema.Headers);

        var endpoints = db.ScpiEndpointConfigs
            .AsNoTracking()
            .OrderBy(endpoint => endpoint.EndpointId)
            .AsAsyncEnumerable();
        await foreach (var endpoint in endpoints.WithCancellation(cancellationToken))
        {
            AppendRow(builder, ScpiConfigCsvSchema.EndpointKind, new Dictionary<string, string?>
            {
                ["endpoint_id"] = endpoint.EndpointId,
                ["endpoint_display_name"] = endpoint.DisplayName,
                ["endpoint_enabled"] = Bool(endpoint.Enabled),
                ["transport"] = endpoint.Transport,
                ["tcp_host"] = endpoint.TcpHost,
                ["tcp_port"] = Int(endpoint.TcpPort),
                ["timeout_ms"] = Int(endpoint.TimeoutMs),
                ["endpoint_polling_interval_ms"] = Int(endpoint.PollingIntervalMs),
                ["converter_id"] = endpoint.ConverterId,
                ["error_check_mode"] = endpoint.ErrorCheckMode,
                ["error_queue_query"] = endpoint.ErrorQueueQuery,
                ["command_terminator"] = endpoint.CommandTerminator,
                ["response_terminator"] = endpoint.ResponseTerminator
            });
        }

        var points = db.ScpiPointConfigs
            .AsNoTracking()
            .Include(point => point.EndpointConfig)
            .OrderBy(point => point.SourcePath)
            .AsAsyncEnumerable();
        await foreach (var point in points.WithCancellation(cancellationToken))
        {
            AppendRow(builder, ScpiConfigCsvSchema.PointKind, new Dictionary<string, string?>
            {
                ["endpoint_id"] = point.EndpointConfig?.EndpointId,
                ["point_id"] = point.PointId,
                ["point_name"] = point.Name,
                ["point_display_name"] = point.DisplayName,
                ["point_enabled"] = Bool(point.Enabled),
                ["access"] = point.Access,
                ["data_type"] = point.DataType,
                ["number_type"] = point.DataType == ScpiDataTypes.Number ? point.NumberType : null,
                ["string_format"] = point.DataType == ScpiDataTypes.String ? point.StringFormat : null,
                ["enum_format"] = point.DataType == ScpiDataTypes.Enum ? point.EnumFormat : null,
                ["read_template"] = point.ReadTemplate,
                ["write_template"] = point.WriteTemplate,
                ["unit"] = point.Unit,
                ["point_polling_enabled"] = Bool(point.PollingEnabled),
                ["point_polling_interval_ms"] = Int(point.PollingIntervalMs),
                ["initial_read"] = Bool(point.InitialRead)
            });
        }

        var enumOptions = db.ScpiEnumOptions
            .AsNoTracking()
            .Include(option => option.ScpiPointConfig)
            .ThenInclude(point => point!.EndpointConfig)
            .OrderBy(option => option.ScpiPointConfig!.SourcePath)
            .ThenBy(option => option.SortOrder)
            .ThenBy(option => option.Value)
            .AsAsyncEnumerable();
        await foreach (var enumOption in enumOptions.WithCancellation(cancellationToken))
        {
            AppendRow(builder, ScpiConfigCsvSchema.EnumOptionKind, new Dictionary<string, string?>
            {
                ["endpoint_id"] = enumOption.ScpiPointConfig?.EndpointConfig?.EndpointId,
                ["point_id"] = enumOption.ScpiPointConfig?.PointId,
                ["enum_display_name"] = enumOption.DisplayName,
                ["enum_value"] = enumOption.Value,
                ["enum_code"] = Int(enumOption.Code),
                ["enum_sort_order"] = Int(enumOption.SortOrder)
            });
        }

        var mappings = db.RedisMappings
            .AsNoTracking()
            .OrderBy(mapping => mapping.SourcePath)
            .AsAsyncEnumerable();
        await foreach (var mapping in mappings.WithCancellation(cancellationToken))
        {
            AppendRow(builder, ScpiConfigCsvSchema.MappingKind, new Dictionary<string, string?>
            {
                ["mapping_source_path"] = mapping.SourcePath,
                ["mapping_redis_key"] = mapping.RedisKey
            });
        }

        await transaction.CommitAsync(cancellationToken);
        var bytes = builder.ToUtf8Bytes();

        return new MemoryStream(bytes, writable: false);
    }

    public async Task<CsvImportResult> ImportAsync(
        Stream stream,
        CancellationToken cancellationToken = default)
    {
        ParsedScpiConfigImport parsed;
        try
        {
            var content = await ConfigStreamLimits.ReadUtf8Async(
                stream,
                options.Value.SingleCsvLimitBytes,
                "CSV",
                cancellationToken);
            var document = CanonicalCsv.Parse(content);
            parsed = ScpiConfigCsvRowParser.Parse(document);
        }
        catch (CanonicalCsvException ex)
        {
            return new CsvImportResult(0, [$"Line {ex.Line}: {ex.Message}"]);
        }
        catch (InvalidDataException ex)
        {
            return new CsvImportResult(0, [ex.Message]);
        }

        if (parsed.Errors.Count > 0)
        {
            return new CsvImportResult(0, parsed.Errors);
        }

        await using var db = await dbFactory.CreateDbContextAsync(cancellationToken);
        await using var transaction = await db.Database.BeginTransactionAsync(cancellationToken);
        var errors = new List<string>();
        PointOwnershipReleasePreparation? remapPreparation = null;
        try
        {
            await ApplyEndpointsAsync(db, parsed.Endpoints, errors, cancellationToken);
            if (errors.Count > 0)
            {
                return await RollbackAsync(db, transaction, errors, cancellationToken);
            }

            await ApplyPointsAsync(db, parsed.Points, errors, cancellationToken);
            if (errors.Count > 0)
            {
                return await RollbackAsync(db, transaction, errors, cancellationToken);
            }

            await ApplyEnumOptionsAsync(db, parsed.Points, parsed.EnumOptions, errors, cancellationToken);
            if (errors.Count > 0)
            {
                return await RollbackAsync(db, transaction, errors, cancellationToken);
            }

            remapPreparation = await ApplyMappingsAsync(db, parsed.Points, parsed.Mappings, errors, cancellationToken);
            if (errors.Count > 0)
            {
                return await RollbackAsync(db, transaction, errors, cancellationToken);
            }

            await transaction.CommitAsync(cancellationToken);
            remapPreparation?.MarkCommitted();
            return new CsvImportResult(parsed.RowCount, []);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            await RollbackAndClearAsync(db, transaction, CancellationToken.None);
            throw;
        }
        catch (Exception ex)
        {
            errors.Add($"Import failed: {InnermostMessage(ex)}");
            return await RollbackAsync(db, transaction, errors, cancellationToken);
        }
        finally
        {
            if (remapPreparation is not null)
                await remapPreparation.DisposeAsync();
        }
    }

    private async Task ApplyEndpointsAsync(
        AppDbContext db,
        IReadOnlyList<EndpointImportRow> rows,
        List<string> errors,
        CancellationToken cancellationToken)
    {
        if (rows.Count == 0)
        {
            return;
        }

        var endpoints = await db.ScpiEndpointConfigs.ToListAsync(cancellationToken);
        var endpointsById = endpoints.ToDictionary(endpoint => endpoint.EndpointId, StringComparer.OrdinalIgnoreCase);

        foreach (var row in rows)
        {
            if (!endpointsById.TryGetValue(row.EndpointId, out var endpoint))
            {
                endpoint = new ScpiEndpointConfig { EndpointId = row.EndpointId };
                endpointsById.Add(row.EndpointId, endpoint);
                db.ScpiEndpointConfigs.Add(endpoint);
            }

            endpoint.DisplayName = row.DisplayName;
            endpoint.Enabled = row.Enabled;
            endpoint.Transport = row.Transport;
            endpoint.TcpHost = row.TcpHost;
            endpoint.TcpPort = row.TcpPort;
            endpoint.TimeoutMs = row.TimeoutMs;
            endpoint.PollingIntervalMs = row.PollingIntervalMs;
            endpoint.ConverterId = row.ConverterId;
            endpoint.ErrorCheckMode = row.ErrorCheckMode;
            endpoint.ErrorQueueQuery = row.ErrorQueueQuery;
            endpoint.CommandTerminator = row.CommandTerminator;
            endpoint.ResponseTerminator = row.ResponseTerminator;
        }

        await db.SaveChangesAsync(cancellationToken);
    }

    private async Task ApplyPointsAsync(
        AppDbContext db,
        IReadOnlyList<PointImportRow> rows,
        List<string> errors,
        CancellationToken cancellationToken)
    {
        if (rows.Count == 0)
        {
            return;
        }

        var endpoints = await db.ScpiEndpointConfigs.ToListAsync(cancellationToken);
        var endpointsById = endpoints.ToDictionary(endpoint => endpoint.EndpointId, StringComparer.OrdinalIgnoreCase);
        var points = await db.ScpiPointConfigs.ToListAsync(cancellationToken);
        var pointsByKey = points.ToDictionary(
            point => PointKey(point.EndpointConfigId, point.PointId),
            StringComparer.OrdinalIgnoreCase);
        var pointsBySourcePath = points.ToDictionary(point => point.SourcePath, StringComparer.OrdinalIgnoreCase);

        foreach (var row in rows)
        {
            if (!endpointsById.TryGetValue(row.EndpointId, out var endpoint))
            {
                errors.Add($"Line {row.Line}: endpoint '{row.EndpointId}' does not exist.");
                continue;
            }

            var key = PointKey(endpoint.Id, row.PointId);
            pointsByKey.TryGetValue(key, out var point);
            var canonicalPointId = point?.PointId ?? row.PointId;
            var sourcePath = BuildSourcePath(endpoint.EndpointId, canonicalPointId);
            if (pointsBySourcePath.TryGetValue(sourcePath, out var conflictingPoint)
                && conflictingPoint.Id != point?.Id)
            {
                errors.Add($"Line {row.Line}: source path '{sourcePath}' is already used by another point.");
                continue;
            }

            if (point is null)
            {
                point = new ScpiPointConfig
                {
                    EndpointConfigId = endpoint.Id,
                    EndpointConfig = endpoint,
                    PointId = row.PointId,
                    SourcePath = sourcePath
                };
                pointsByKey.Add(key, point);
                pointsBySourcePath.Add(sourcePath, point);
                db.ScpiPointConfigs.Add(point);
            }

            point.SourcePath = sourcePath;
            point.Name = row.Name;
            point.DisplayName = row.DisplayName;
            point.Enabled = row.Enabled;
            point.Access = row.Access;
            point.DataType = row.DataType;
            point.NumberType = row.DataType == ScpiDataTypes.Number ? row.NumberType : null;
            point.StringFormat = row.DataType == ScpiDataTypes.String ? row.StringFormat : null;
            point.EnumFormat = row.DataType == ScpiDataTypes.Enum ? row.EnumFormat : null;
            point.ReadTemplate = row.ReadTemplate;
            point.WriteTemplate = row.Access == ScpiAccessModes.Readwrite ? row.WriteTemplate : null;
            point.Unit = row.Unit;
            point.PollingEnabled = row.PollingEnabled;
            point.PollingIntervalMs = row.PollingIntervalMs;
            point.InitialRead = row.InitialRead;
            endpoint.ConcurrencyStamp = Guid.NewGuid().ToString("N");
        }

        if (errors.Count == 0)
        {
            await db.SaveChangesAsync(cancellationToken);
        }
    }

    private async Task ApplyEnumOptionsAsync(
        AppDbContext db,
        IReadOnlyList<PointImportRow> pointRows,
        IReadOnlyList<EnumOptionImportRow> optionRows,
        List<string> errors,
        CancellationToken cancellationToken)
    {
        if (pointRows.Count == 0 && optionRows.Count == 0)
        {
            return;
        }

        var endpoints = await db.ScpiEndpointConfigs.ToListAsync(cancellationToken);
        var endpointIds = endpoints.ToDictionary(endpoint => endpoint.Id, endpoint => endpoint.EndpointId);
        var endpointsByDatabaseId = endpoints.ToDictionary(endpoint => endpoint.Id);
        var points = await db.ScpiPointConfigs.ToListAsync(cancellationToken);
        var pointsByExternalKey = points.ToDictionary(
            point => ExternalPointKey(endpointIds[point.EndpointConfigId], point.PointId),
            StringComparer.OrdinalIgnoreCase);
        var existingOptions = await db.ScpiEnumOptions.ToListAsync(cancellationToken);
        var optionsByPointId = existingOptions
            .GroupBy(option => option.ScpiPointConfigId)
            .ToDictionary(group => group.Key, group => group.ToList());
        var optionGroups = optionRows
            .GroupBy(row => ExternalPointKey(row.EndpointId, row.PointId), StringComparer.OrdinalIgnoreCase)
            .ToDictionary(group => group.Key, group => group.ToList(), StringComparer.OrdinalIgnoreCase);
        var touchedPointIds = new HashSet<int>();
        var replacementOptions = new List<ScpiEnumOption>();

        foreach (var pointRow in pointRows.Where(row => row.DataType != ScpiDataTypes.Enum))
        {
            if (!pointsByExternalKey.TryGetValue(ExternalPointKey(pointRow.EndpointId, pointRow.PointId), out var point))
            {
                continue;
            }

            if (optionsByPointId.TryGetValue(point.Id, out var obsoleteOptions))
            {
                db.ScpiEnumOptions.RemoveRange(obsoleteOptions);
                optionsByPointId.Remove(point.Id);
                touchedPointIds.Add(point.Id);
            }
        }

        foreach (var (key, group) in optionGroups)
        {
            if (!pointsByExternalKey.TryGetValue(key, out var point))
            {
                errors.Add($"Line {group[0].Line}: enum option references a point that does not exist.");
                continue;
            }

            if (!point.DataType.Equals(ScpiDataTypes.Enum, StringComparison.OrdinalIgnoreCase))
            {
                errors.Add($"Line {group[0].Line}: enum options can only be applied to an enum point.");
                continue;
            }

            if (optionsByPointId.TryGetValue(point.Id, out var previousOptions))
            {
                db.ScpiEnumOptions.RemoveRange(previousOptions);
            }

            var replacements = group.Select(row => new ScpiEnumOption
            {
                ScpiPointConfigId = point.Id,
                ScpiPointConfig = point,
                DisplayName = row.DisplayName,
                Value = row.Value,
                Code = row.Code,
                SortOrder = row.SortOrder
            }).ToList();
            replacementOptions.AddRange(replacements);
            optionsByPointId[point.Id] = replacements;
            touchedPointIds.Add(point.Id);
        }

        foreach (var pointRow in pointRows.Where(row => row.DataType == ScpiDataTypes.Enum))
        {
            if (!pointsByExternalKey.TryGetValue(ExternalPointKey(pointRow.EndpointId, pointRow.PointId), out var point))
            {
                continue;
            }

            if (!optionsByPointId.TryGetValue(point.Id, out var resultingOptions) || resultingOptions.Count == 0)
            {
                errors.Add($"Line {pointRow.Line}: enum point '{point.SourcePath}' must have at least one enum option.");
            }
        }

        if (errors.Count == 0)
        {
            foreach (var point in points.Where(point => touchedPointIds.Contains(point.Id)))
            {
                point.ConcurrencyStamp = Guid.NewGuid().ToString("N");
                endpointsByDatabaseId[point.EndpointConfigId].ConcurrencyStamp = Guid.NewGuid().ToString("N");
            }

            // Delete the previous collection first so replacing an option with the
            // same unique value/code cannot collide inside SQLite.
            await db.SaveChangesAsync(cancellationToken);
            db.ScpiEnumOptions.AddRange(replacementOptions);
            await db.SaveChangesAsync(cancellationToken);
        }
    }

    private async Task<PointOwnershipReleasePreparation?> ApplyMappingsAsync(
        AppDbContext db,
        IReadOnlyList<PointImportRow> pointRows,
        IReadOnlyList<MappingImportRow> mappingRows,
        List<string> errors,
        CancellationToken cancellationToken)
    {
        var points = await db.ScpiPointConfigs.AsNoTracking().ToListAsync(cancellationToken);
        var canonicalPointSources = points.ToDictionary(
            point => point.SourcePath,
            point => point.SourcePath,
            StringComparer.OrdinalIgnoreCase);
        var mappings = await db.RedisMappings.ToListAsync(cancellationToken);
        var mappingsBySource = mappings.ToDictionary(mapping => mapping.SourcePath, StringComparer.OrdinalIgnoreCase);
        var desiredKeys = mappings.ToDictionary(
            mapping => mapping.SourcePath,
            mapping => mapping.RedisKey,
            StringComparer.OrdinalIgnoreCase);
        var canonicalRows = new List<(MappingImportRow Row, string SourcePath)>();

        foreach (var row in mappingRows)
        {
            if (!canonicalPointSources.TryGetValue(row.SourcePath, out var canonicalSourcePath))
            {
                errors.Add($"Line {row.Line}: mapping source '{row.SourcePath}' does not match an existing SCPI point.");
                continue;
            }

            canonicalRows.Add((row, canonicalSourcePath));
            desiredKeys[canonicalSourcePath] = row.RedisKey;
        }

        foreach (var duplicate in desiredKeys.GroupBy(pair => pair.Value, StringComparer.Ordinal).Where(group => group.Count() > 1))
        {
            var importedConflicts = mappingRows.Where(row => row.RedisKey == duplicate.Key).ToList();
            if (importedConflicts.Count == 0)
            {
                errors.Add($"Redis key '{duplicate.Key}' would be used by more than one mapping.");
            }
            else
            {
                foreach (var row in importedConflicts)
                {
                    errors.Add($"Line {row.Line}: Redis key '{duplicate.Key}' would be used by more than one mapping.");
                }
            }
        }

        foreach (var pointRow in pointRows.Where(row => row.Enabled || row.PollingEnabled))
        {
            var sourcePath = BuildSourcePath(pointRow.EndpointId, pointRow.PointId);
            if (!canonicalPointSources.TryGetValue(sourcePath, out var canonicalSourcePath)
                || !desiredKeys.ContainsKey(canonicalSourcePath))
            {
                errors.Add($"Line {pointRow.Line}: enabled or polling-enabled point '{sourcePath}' requires a Redis mapping.");
            }
        }

        if (errors.Count > 0)
        {
            return null;
        }

        var changedMappings = new List<(RedisMapping Mapping, string RedisKey)>();
        var newMappings = new List<RedisMapping>();
        foreach (var (row, sourcePath) in canonicalRows)
        {
            if (mappingsBySource.TryGetValue(sourcePath, out var existing))
            {
                if (!existing.RedisKey.Equals(row.RedisKey, StringComparison.Ordinal))
                {
                    changedMappings.Add((existing, row.RedisKey));
                }
                continue;
            }

            var newMapping = new RedisMapping
            {
                SourcePath = sourcePath,
                RedisKey = row.RedisKey
            };
            newMappings.Add(newMapping);
            mappingsBySource.Add(sourcePath, newMapping);
        }

        PointOwnershipReleasePreparation? preparation = null;
        if (changedMappings.Count > 0 && releases is not null)
        {
            var requests = changedMappings.Select(item => new PointOwnershipRemapPreparationRequest(
                item.Mapping.Id,
                item.Mapping.SourcePath,
                item.Mapping.RedisKey,
                item.Mapping.SourcePath,
                item.RedisKey)).ToArray();
            preparation = await releases.PrepareRemapsAsync(db, requests, cancellationToken);
        }
        else if (changedMappings.Count > 0)
        {
            var occupiedKeys = mappings.Select(mapping => mapping.RedisKey)
                .Concat(desiredKeys.Values)
                .ToHashSet(StringComparer.Ordinal);
            foreach (var (mapping, _) in changedMappings)
            {
                string temporaryKey;
                do
                {
                    temporaryKey = $"point:__redis_scpi_import__:{Guid.NewGuid():N}";
                }
                while (!occupiedKeys.Add(temporaryKey));

                mapping.RedisKey = temporaryKey;
            }

            // Free every previous key before assigning final values. This permits
            // key swaps while retaining each mapping's identity and CreatedAt.
            await db.SaveChangesAsync(cancellationToken);
            foreach (var (mapping, redisKey) in changedMappings)
            {
                mapping.RedisKey = redisKey;
            }
        }

        try
        {
            db.RedisMappings.AddRange(newMappings);
            await db.SaveChangesAsync(cancellationToken);
            return preparation;
        }
        catch
        {
            if (preparation is not null)
                await preparation.DisposeAsync();
            throw;
        }
    }

    private async Task<CsvImportResult> RollbackAsync(
        AppDbContext db,
        IDbContextTransaction transaction,
        IReadOnlyList<string> errors,
        CancellationToken cancellationToken)
    {
        await RollbackAndClearAsync(db, transaction, cancellationToken);
        return new CsvImportResult(0, errors.ToArray());
    }

    private async Task RollbackAndClearAsync(
        AppDbContext db,
        IDbContextTransaction transaction,
        CancellationToken cancellationToken)
    {
        try
        {
            await transaction.RollbackAsync(cancellationToken);
        }
        finally
        {
            db.ChangeTracker.Clear();
        }
    }

    private static void AppendRow(
        BoundedCsvBuilder builder,
        string kind,
        IReadOnlyDictionary<string, string?> values)
    {
        builder.AppendRecord(
            ScpiConfigCsvSchema.Headers.Select(header =>
                header == "kind"
                    ? kind
                    : values.GetValueOrDefault(header)));
    }

    private static string BuildSourcePath(string endpointId, string pointId) =>
        $"scpi:{endpointId}/{pointId}";

    private static string PointKey(int endpointConfigId, string pointId) =>
        $"{endpointConfigId.ToString(CultureInfo.InvariantCulture)}\u001F{pointId}";

    private static string ExternalPointKey(string endpointId, string pointId) =>
        $"{endpointId}\u001F{pointId}";

    private static string Bool(bool value) => value ? "true" : "false";

    private static string Int(int value) => value.ToString(CultureInfo.InvariantCulture);

    private static string Int(int? value) =>
        value?.ToString(CultureInfo.InvariantCulture) ?? string.Empty;

    private static string InnermostMessage(Exception exception)
    {
        while (exception.InnerException is not null)
        {
            exception = exception.InnerException;
        }

        return exception.Message;
    }

    private sealed class BoundedCsvBuilder(long limitBytes)
    {
        private static readonly UTF8Encoding Utf8 = new(encoderShouldEmitUTF8Identifier: false);
        private readonly StringBuilder _builder = new();
        private long _byteCount;

        public void AppendRecord(IEnumerable<string?> values)
        {
            var record = new StringBuilder();
            CanonicalCsv.AppendRecord(record, values);
            var text = record.ToString();
            var recordBytes = Utf8.GetByteCount(text);
            if (recordBytes > limitBytes - _byteCount)
            {
                throw new InvalidOperationException($"Exported CSV size exceeds {limitBytes} bytes.");
            }

            _builder.Append(text);
            _byteCount += recordBytes;
        }

        public byte[] ToUtf8Bytes() => Utf8.GetBytes(_builder.ToString());
    }
}
