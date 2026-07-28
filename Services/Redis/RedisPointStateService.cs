using System.Globalization;
using System.Text.Json;
using Microsoft.Extensions.Options;
using Ptlk.RedisScpi.Configuration;
using Ptlk.RedisScpi.Contracts.Redis;
using Ptlk.RedisScpi.Contracts.Scpi;
using Ptlk.RedisScpi.Models;
using Ptlk.RedisScpi.Services.Startup;
using StackExchange.Redis;

namespace Ptlk.RedisScpi.Services.Redis;

public class RedisPointStateException(
    string status,
    string reason,
    string redisKey,
    string message,
    Exception? innerException = null) : InvalidOperationException(message, innerException)
{
    public string Status { get; } = status;
    public string Reason { get; } = reason;
    public string RedisKey { get; } = redisKey;
}

public sealed class RedisPointUpdateException(
    string status,
    string reason,
    string redisKey,
    string message,
    Exception? innerException = null) : RedisPointStateException(status, reason, redisKey, message, innerException);

public sealed class RedisPointStateService(
    RedisConnectionFactory redis,
    AtomicPointUpdateService atomicPointUpdate,
    PointUpdateIdentity operationIdentity,
    IOptions<RedisScpiOptions> redisScpiOptions,
    RuntimeModeService runtime)
{
    public RedisPointStateService(
        RedisConnectionFactory redis,
        IRedisPubSubService pubSub,
        IOptions<RedisScpiOptions> redisScpiOptions,
        RuntimeModeService runtime)
        : this(redis, new AtomicPointUpdateService(redis), new PointUpdateIdentity(redisScpiOptions), redisScpiOptions, runtime)
    {
    }
    private static readonly string[] RequiredFields =
    [
        "quality",
        "type",
        "timestamp",
        "version",
        "source",
        "access",
        "unit"
    ];

    private const string UpdateDynamicFieldsScript = """
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
        if not currentOwner or currentOwner == '' then
            return {'ownership_missing'}
        end
        if currentOwner ~= ARGV[7] then
            return {'owned_by_other', currentOwner}
        end

        local pointType = redis.call('HGET', KEYS[1], 'type')
        if not pointType then
            return {'type_missing'}
        end
        if pointType == '' then
            return {'type_invalid'}
        end
        if pointType ~= 'int' and pointType ~= 'double' and pointType ~= 'bool' and pointType ~= 'string' then
            return {'type_invalid'}
        end

        local access = redis.call('HGET', KEYS[1], 'access')
        if not access then
            return {'access_missing'}
        end
        if access ~= 'readonly' and access ~= 'readwrite' then
            return {'access_invalid'}
        end

        if redis.call('HEXISTS', KEYS[1], 'unit') == 0 then
            return {'unit_missing'}
        end

        local currentQuality = redis.call('HGET', KEYS[1], 'quality')
        if not currentQuality then
            return {'quality_missing'}
        end
        if currentQuality ~= 'unset' and currentQuality ~= 'good'
            and currentQuality ~= 'uncertain' and currentQuality ~= 'bad' then
            return {'quality_invalid'}
        end

        local currentTimestampValue = redis.call('HGET', KEYS[1], 'timestamp')
        if not currentTimestampValue or currentTimestampValue == '' then
            return {'timestamp_missing'}
        end
        local currentTimestamp = tonumber(currentTimestampValue)
        if not currentTimestamp or currentTimestamp < 0 or currentTimestamp % 1 ~= 0 then
            return {'timestamp_invalid'}
        end

        local currentSource = redis.call('HGET', KEYS[1], 'source')
        if not currentSource then
            return {'source_missing'}
        end
        if currentSource == '' or string.match(currentSource, '^%s*$') then
            return {'source_invalid'}
        end

        local currentVersionValue = redis.call('HGET', KEYS[1], 'version')
        if not currentVersionValue or currentVersionValue == '' then
            return {'version_missing'}
        end
        local currentVersion = tonumber(currentVersionValue)
        if not currentVersion or currentVersion < 0 or currentVersion % 1 ~= 0 then
            return {'version_invalid'}
        end

        if ARGV[1] == '1' then
            local valueKind = ARGV[3]
            if pointType == 'string' and valueKind ~= 'string' then
                return {'value_type_mismatch'}
            end
            if (pointType == 'int' or pointType == 'double') and valueKind ~= 'number' then
                return {'value_type_mismatch'}
            end
            if pointType == 'bool' and valueKind ~= 'boolean' then
                return {'value_type_mismatch'}
            end
            if pointType == 'int' then
                local numericValue = tonumber(ARGV[2])
                if not numericValue or numericValue % 1 ~= 0 then
                    return {'value_type_mismatch'}
                end
            end
            if pointType == 'double' and not tonumber(ARGV[2]) then
                return {'value_type_mismatch'}
            end
        end

        local nextVersion = currentVersion + 1
        if ARGV[1] == '1' then
            redis.call('HSET', KEYS[1], 'value', ARGV[2])
        else
            redis.call('HDEL', KEYS[1], 'value')
        end

        redis.call('HSET', KEYS[1],
            'quality', ARGV[4],
            'timestamp', ARGV[5],
            'version', tostring(nextVersion),
            'source', ARGV[6])

        return {
            'ok',
            tostring(nextVersion),
            pointType,
            access,
            redis.call('HGET', KEYS[1], 'unit'),
            currentOwner,
            redis.call('HGET', KEYS[1], 'owner_source') or '',
            redis.call('HGET', KEYS[1], 'owner_acquired_at') or ''
        }
        """;

    public async Task<PointStateContract?> ReadAsync(
        string key,
        CancellationToken cancellationToken = default)
    {
        var inspection = await InspectAsync(key, cancellationToken);
        if (inspection.Status == RedisPointInspectionStatus.Missing)
        {
            return null;
        }

        if (!inspection.IsComplete)
        {
            throw new RedisPointStateException(
                ScpiErrorCodes.PointStateInvalid,
                inspection.Diagnostics.FirstOrDefault() ?? "point_state_invalid",
                key,
                $"Redis point state '{key}' is incomplete or invalid: {string.Join(", ", inspection.Diagnostics)}");
        }

        return inspection.State;
    }

    public async Task<RedisPointInspection> InspectAsync(
        string key,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        var database = await redis.GetDatabaseAsync(cancellationToken);
        var keyType = await database.KeyTypeAsync(key);
        if (keyType == RedisType.None)
        {
            return new RedisPointInspection(key, RedisPointInspectionStatus.Missing, null, ["missing_key"]);
        }

        if (keyType != RedisType.Hash)
        {
            return new RedisPointInspection(key, RedisPointInspectionStatus.Invalid, null, ["invalid_key_type"]);
        }

        var entries = await database.HashGetAllAsync(key);
        var fields = entries.ToDictionary(
            entry => entry.Name.ToString(),
            entry => entry.Value.ToString(),
            StringComparer.Ordinal);
        var diagnostics = new List<string>();
        foreach (var requiredField in RequiredFields)
        {
            if (!fields.ContainsKey(requiredField))
            {
                diagnostics.Add($"required_field_missing:{requiredField}");
            }
        }

        if (diagnostics.Count > 0)
        {
            return new RedisPointInspection(key, RedisPointInspectionStatus.Incomplete, null, diagnostics);
        }

        var quality = fields["quality"];
        var type = fields["type"];
        var source = fields["source"];
        var access = fields["access"];
        var unit = fields["unit"];
        if (!IsCanonicalQuality(quality))
        {
            diagnostics.Add("required_field_invalid:quality");
        }

        if (!IsCanonicalType(type))
        {
            diagnostics.Add("required_field_invalid:type");
        }

        if (string.IsNullOrWhiteSpace(source))
        {
            diagnostics.Add("required_field_invalid:source");
        }

        if (!IsCanonicalAccess(access))
        {
            diagnostics.Add("required_field_invalid:access");
        }

        if (!TryParseNonNegativeLong(fields["timestamp"], out var timestamp))
        {
            diagnostics.Add("required_field_invalid:timestamp");
        }

        if (!TryParseNonNegativeLong(fields["version"], out var version))
        {
            diagnostics.Add("required_field_invalid:version");
        }

        JsonElement? value = null;
        var hasValue = fields.TryGetValue("value", out var valueText);
        if (hasValue && IsCanonicalType(type) && !TryParseValue(type, valueText!, out value))
        {
            diagnostics.Add("value_invalid");
        }

        var owner = fields.GetValueOrDefault("owner");
        var ownerSource = fields.GetValueOrDefault("owner_source");
        long? ownerAcquiredAt = null;
        if (fields.TryGetValue("owner_acquired_at", out var ownerAcquiredAtText))
        {
            if (TryParseNonNegativeLong(ownerAcquiredAtText, out var parsedOwnerAcquiredAt))
            {
                ownerAcquiredAt = parsedOwnerAcquiredAt;
            }
            else
            {
                diagnostics.Add("ownership_field_invalid:owner_acquired_at");
            }
        }

        if (diagnostics.Count > 0)
        {
            return new RedisPointInspection(key, RedisPointInspectionStatus.Invalid, null, diagnostics);
        }

        return new RedisPointInspection(
            key,
            RedisPointInspectionStatus.Complete,
            new PointStateContract(
                key,
                value,
                hasValue ? valueText : null,
                hasValue,
                quality,
                type,
                timestamp,
                version,
                source,
                access,
                unit,
                string.IsNullOrEmpty(owner) ? null : owner,
                string.IsNullOrEmpty(ownerSource) ? null : ownerSource,
                ownerAcquiredAt),
            []);
    }

    public async Task<PointStateContract> UpdateDynamicFieldsAsync(
        RedisMapping mapping,
        JsonElement? value,
        string quality,
        string source,
        CancellationToken cancellationToken = default,
        string updateReason = PointUpdateReasons.Acquisition,
        string? commandId = null,
        long? timestamp = null,
        string? operationIdOverride = null)
    {
        ArgumentNullException.ThrowIfNull(mapping);
        ValidateRuntimeArguments(mapping, quality, source);
        var normalizedValue = NormalizeValue(value);
        var now = timestamp ?? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var effectiveUpdateReason = updateReason == PointUpdateReasons.Acquisition && quality == ScpiQuality.Bad
            ? PointUpdateReasons.AcquisitionFailure
            : updateReason;
        try
        {
            var inspection = await InspectAsync(mapping.RedisKey, cancellationToken);
            if (!inspection.IsComplete || inspection.State is null)
                throw CreateUpdateException(mapping, "redis_output_failed", "point_state_invalid", $"Redis point key '{mapping.RedisKey}' is incomplete.");
            var current = inspection.State;
            var operationId = operationIdOverride
                ?? (effectiveUpdateReason == PointUpdateReasons.CommandWrite
                    ? PointOperationId.CommandWrite(
                        redisScpiOptions.Value.ConverterId,
                        commandId ?? throw new ArgumentException("commandId is required for command_write.", nameof(commandId)),
                        mapping.RedisKey)
                    : operationIdentity.Create(effectiveUpdateReason, mapping.RedisKey, commandId));
            var request = new AtomicPointUpdateRequest(
                    mapping.RedisKey,
                    redisScpiOptions.Value.ConverterId,
                    current.Version,
                    operationId,
                    normalizedValue.HasValue ? normalizedValue.HashValue : null,
                    normalizedValue.JsonValue,
                    quality,
                    now,
                    source,
                    effectiveUpdateReason);
            AtomicPointUpdateResult result;
            try
            {
                result = await atomicPointUpdate.ApplyAsync(request, cancellationToken);
            }
            catch (RedisTimeoutException) when (!cancellationToken.IsCancellationRequested)
            {
                result = await atomicPointUpdate.ApplyAsync(request, cancellationToken);
            }
            catch (RedisConnectionException) when (!cancellationToken.IsCancellationRequested)
            {
                result = await atomicPointUpdate.ApplyAsync(request, cancellationToken);
            }
            if (result.Status is not ("applied" or "already_applied") || result.Version is null)
                throw CreateUpdateException(mapping, "redis_output_failed", result.Status, $"Redis point key '{mapping.RedisKey}' atomic update returned {result.Status}.");

            var updated = current with
            {
                Value = normalizedValue.JsonValue,
                ValueText = normalizedValue.HasValue ? normalizedValue.HashValue : null,
                HasValueField = normalizedValue.HasValue,
                Quality = quality,
                Timestamp = now,
                Version = result.Version.Value,
                Source = source
            };
            runtime.ClearRedisOutputDiagnostic("redis_writer", mapping.SourcePath, mapping.RedisKey);
            return updated;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw CreateUpdateException(
                mapping,
                "redis_output_failed",
                "redis_output_failed",
                $"Redis point key '{mapping.RedisKey}' dynamic update failed: {ex.Message}",
                ex);
        }

    }

    private static (bool HasValue, string HashValue, string Kind, JsonElement? JsonValue) NormalizeValue(JsonElement? value)
    {
        if (value is null || value.Value.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
        {
            return (false, "", "null", null);
        }

        var element = value.Value;
        return element.ValueKind switch
        {
            JsonValueKind.String => (true, element.GetString() ?? "", "string", element.Clone()),
            JsonValueKind.Number => (true, element.GetRawText(), "number", element.Clone()),
            JsonValueKind.True => (true, "true", "boolean", element.Clone()),
            JsonValueKind.False => (true, "false", "boolean", element.Clone()),
            _ => throw new RedisPointStateException(
                ScpiErrorCodes.InvalidValueType,
                "value_not_scalar",
                "",
                "Redis point values must be a JSON scalar or null.")
        };
    }

    private static bool TryParseValue(string type, string valueText, out JsonElement? value)
    {
        value = null;
        switch (type)
        {
            case "string":
                value = JsonSerializer.SerializeToElement(valueText, RedisContractJson.WebOptions);
                return true;
            case "int":
                if (long.TryParse(valueText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var integer))
                {
                    value = JsonSerializer.SerializeToElement(integer, RedisContractJson.WebOptions);
                    return true;
                }

                return false;
            case "double":
                if (double.TryParse(valueText, NumberStyles.Float, CultureInfo.InvariantCulture, out var number)
                    && double.IsFinite(number))
                {
                    value = JsonSerializer.SerializeToElement(number, RedisContractJson.WebOptions);
                    return true;
                }

                return false;
            case "bool":
                if (bool.TryParse(valueText, out var boolean))
                {
                    value = JsonSerializer.SerializeToElement(boolean, RedisContractJson.WebOptions);
                    return true;
                }

                return false;
            default:
                return false;
        }
    }

    private void ValidateRuntimeArguments(RedisMapping mapping, string quality, string source)
    {
        if (string.IsNullOrWhiteSpace(mapping.SourcePath)
            || !mapping.SourcePath.StartsWith("scpi:", StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException("Redis mapping SourcePath must be a SCPI source.", nameof(mapping));
        }

        if (string.IsNullOrWhiteSpace(mapping.RedisKey)
            || !mapping.RedisKey.StartsWith(RedisContractNames.PointPrefix, StringComparison.Ordinal))
        {
            throw new ArgumentException("Redis mapping key must start with point:.", nameof(mapping));
        }

        if (!IsCanonicalQuality(quality))
        {
            throw new ArgumentException($"Unsupported Redis point quality '{quality}'.", nameof(quality));
        }

        if (!source.Equals(redisScpiOptions.Value.SourceName, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                $"Redis point source must be configured SourceName '{redisScpiOptions.Value.SourceName}'.",
                nameof(source));
        }
    }

    private RedisPointUpdateException CreateScriptResultException(
        RedisMapping mapping,
        string status,
        IReadOnlyList<RedisResult> result)
    {
        var owner = result.Count > 1 ? result[1].ToString() : null;
        return status switch
        {
            "missing" => CreateUpdateException(
                mapping,
                "missing_key",
                "missing_key",
                $"Redis point key '{mapping.RedisKey}' does not exist."),
            "invalid_key_type" => CreateUpdateException(
                mapping,
                ScpiErrorCodes.PointStateInvalid,
                "invalid_key_type",
                $"Redis key '{mapping.RedisKey}' is not a Hash."),
            "ownership_missing" => CreateUpdateException(
                mapping,
                ScpiErrorCodes.OwnershipNotAcquired,
                "ownership_missing",
                $"Redis point key '{mapping.RedisKey}' has no owner."),
            "owned_by_other" => CreateUpdateException(
                mapping,
                ScpiErrorCodes.OwnershipNotAcquired,
                "owned_by_other",
                $"Redis point key '{mapping.RedisKey}' is owned by {owner ?? "another converter"}."),
            "value_type_mismatch" => CreateUpdateException(
                mapping,
                ScpiErrorCodes.InvalidValueType,
                "value_type_mismatch",
                $"Value type does not match Redis point '{mapping.RedisKey}' metadata."),
            "quality_missing" or "type_missing" or "timestamp_missing" or "source_missing"
                or "access_missing" or "unit_missing" or "version_missing" => CreateUpdateException(
                mapping,
                ScpiErrorCodes.PointStateInvalid,
                $"required_field_missing:{FieldFromStatus(status, "_missing")}",
                $"Redis point key '{mapping.RedisKey}' is missing required field '{FieldFromStatus(status, "_missing")}'."),
            "quality_invalid" or "type_invalid" or "timestamp_invalid" or "source_invalid"
                or "access_invalid" or "version_invalid" => CreateUpdateException(
                mapping,
                ScpiErrorCodes.PointStateInvalid,
                $"required_field_invalid:{FieldFromStatus(status, "_invalid")}",
                $"Redis point key '{mapping.RedisKey}' has invalid required field '{FieldFromStatus(status, "_invalid")}'."),
            _ => CreateUpdateException(
                mapping,
                "redis_output_failed",
                string.IsNullOrWhiteSpace(status) ? "unexpected_result" : status,
                $"Redis point key '{mapping.RedisKey}' dynamic update returned status '{status}'.")
        };
    }

    private RedisPointUpdateException CreateUpdateException(
        RedisMapping mapping,
        string status,
        string reason,
        string message,
        Exception? innerException = null)
    {
        runtime.ReportRedisOutputDiagnostic(
            "redis_writer",
            mapping.SourcePath,
            mapping.RedisKey,
            reason,
            message);
        return new RedisPointUpdateException(status, reason, mapping.RedisKey, message, innerException);
    }

    private static bool TryParseNonNegativeLong(string text, out long value) =>
        long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out value) && value >= 0;

    private static bool IsCanonicalQuality(string value) =>
        value is "unset" or "good" or "uncertain" or "bad";

    private static bool IsCanonicalType(string value) =>
        value is "int" or "double" or "bool" or "string";

    private static bool IsCanonicalAccess(string value) =>
        value is "readonly" or "readwrite";

    private static string? NullIfEmpty(string? value) =>
        string.IsNullOrEmpty(value) ? null : value;

    private static string FieldFromStatus(string status, string suffix) =>
        status.EndsWith(suffix, StringComparison.Ordinal)
            ? status[..^suffix.Length]
            : status;
}
