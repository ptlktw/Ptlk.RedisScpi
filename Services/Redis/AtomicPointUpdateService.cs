using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using StackExchange.Redis;

namespace Ptlk.RedisScpi.Services.Redis;

public static class PointUpdateReasons
{
    public const string Acquisition = "acquisition";
    public const string AcquisitionFailure = "acquisition_failure";
    public const string CommandWrite = "command_write";
    public const string ReconnectSync = "reconnect_sync";
    public const string SupervisorQuality = "supervisor_quality";
    public static bool IsKnown(string value) => value is Acquisition or AcquisitionFailure or CommandWrite or ReconnectSync or SupervisorQuality;
}

public static class PointOperationId
{
    private const string Prefix = "ptlk-point-operation-v1";
    private const int MaxElements = 16;
    private const int MaxElementLength = 4096;
    private static readonly byte[] NamespaceBytes = Convert.FromHexString("c8f0c703dd25496b99c815e0ed579f97");
    private static readonly JsonSerializerOptions CanonicalOptions = new() { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping };
    public static string Acquisition(string instanceId, long sequence, string key) => Create(Prefix, PointUpdateReasons.Acquisition, instanceId, Nonnegative(sequence), key);
    public static string AcquisitionFailure(string instanceId, long sequence, string key) => Create(Prefix, PointUpdateReasons.AcquisitionFailure, instanceId, Nonnegative(sequence), key);
    public static string CommandWrite(string converterId, string commandId, string key) => Create(Prefix, PointUpdateReasons.CommandWrite, converterId, commandId, key);
    public static string ReconnectSync(string instanceId, string cycleId, long sequence, string key) => Create(Prefix, PointUpdateReasons.ReconnectSync, instanceId, cycleId, Nonnegative(sequence), key);
    public static string SupervisorQuality(string instanceId, long transitionAt, string key, long version) => Create(Prefix, PointUpdateReasons.SupervisorQuality, instanceId, Nonnegative(transitionAt), key, Nonnegative(version));
    public static string Number(long value) => Nonnegative(value);

    public static string Create(params string[] identity)
    {
        ArgumentNullException.ThrowIfNull(identity);
        if (identity.Length is 0 or > MaxElements) throw new ArgumentException($"Identity must contain 1 to {MaxElements} elements.", nameof(identity));
        foreach (var element in identity)
            if (string.IsNullOrWhiteSpace(element) || element.Length > MaxElementLength || HasUnpairedSurrogate(element))
                throw new ArgumentException($"Identity elements must be valid Unicode strings of 1 to {MaxElementLength} characters.", nameof(identity));
        var canonicalName = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(identity, CanonicalOptions));
        var hashInput = new byte[NamespaceBytes.Length + canonicalName.Length];
        NamespaceBytes.CopyTo(hashInput, 0); canonicalName.CopyTo(hashInput, NamespaceBytes.Length);
        var hash = SHA1.HashData(hashInput); hash[6] = (byte)((hash[6] & 0x0f) | 0x50); hash[8] = (byte)((hash[8] & 0x3f) | 0x80);
        return Convert.ToHexString(hash.AsSpan(0, 16)).ToLowerInvariant();
    }

    private static string Nonnegative(long value) => value >= 0 ? value.ToString(CultureInfo.InvariantCulture) : throw new ArgumentOutOfRangeException(nameof(value));
    private static bool HasUnpairedSurrogate(string value)
    {
        for (var index = 0; index < value.Length; index++)
        {
            if (char.IsHighSurrogate(value[index]))
            {
                if (++index >= value.Length || !char.IsLowSurrogate(value[index])) return true;
            }
            else if (char.IsLowSurrogate(value[index])) return true;
        }
        return false;
    }
}

public sealed record AtomicPointUpdateRequest(string Key, string ExpectedOwner, long ExpectedVersion, string OperationId, string? Value, JsonElement? EventValue, string Quality, long Timestamp, string Source, string UpdateReason);
public sealed record AtomicPointUpdateResult(string Status, long? Version = null, string? Detail = null)
{
    public bool Succeeded => Status is "applied" or "already_applied";
    public bool AlreadyApplied => Status == "already_applied";
}

public static class AtomicPointUpdateLua
{
    public const string Script = """
        if redis.call('EXISTS', KEYS[1]) == 0 then return {'point_missing'} end
        local keyType = redis.call('TYPE', KEYS[1]); if type(keyType) == 'table' then keyType = keyType.ok end
        if keyType ~= 'hash' then return {'invalid_key_type'} end
        local required = {'quality','type','timestamp','version','source','access','unit'}
        for _, field in ipairs(required) do if redis.call('HEXISTS', KEYS[1], field) == 0 then return {'required_field_missing', field} end end
        local owner = redis.call('HGET', KEYS[1], 'owner'); if not owner or owner == '' then return {'ownership_missing'} end
        if owner ~= ARGV[1] then return {'owned_by_other', owner} end
        local pointType = redis.call('HGET', KEYS[1], 'type'); if pointType ~= 'int' and pointType ~= 'double' and pointType ~= 'bool' and pointType ~= 'string' then return {'required_field_invalid','type'} end
        local access = redis.call('HGET', KEYS[1], 'access'); if access ~= 'readonly' and access ~= 'readwrite' then return {'required_field_invalid','access'} end
        local quality = redis.call('HGET', KEYS[1], 'quality'); if quality ~= 'unset' and quality ~= 'good' and quality ~= 'uncertain' and quality ~= 'bad' then return {'required_field_invalid','quality'} end
        local timestamp = redis.call('HGET', KEYS[1], 'timestamp'); if not timestamp or (not string.match(timestamp, '^0$') and not string.match(timestamp, '^[1-9][0-9]*$')) then return {'required_field_invalid','timestamp'} end
        local source = redis.call('HGET', KEYS[1], 'source'); if not source or source == '' or string.match(source, '^%s*$') then return {'required_field_invalid','source'} end
        local currentVersion = redis.call('HGET', KEYS[1], 'version'); if not currentVersion or (not string.match(currentVersion, '^0$') and not string.match(currentVersion, '^[1-9][0-9]*$')) then return {'required_field_invalid','version'} end
        local lastOperation = redis.call('HGET', KEYS[1], 'last_update_operation_id')
        if lastOperation == ARGV[3] then if currentVersion == ARGV[10] then return {'already_applied', currentVersion} end return {'stale_conflict', currentVersion} end
        if currentVersion ~= ARGV[2] then return {'stale_conflict', currentVersion} end
        if ARGV[4] == '1' then redis.call('HSET', KEYS[1], 'value', ARGV[5]) else redis.call('HDEL', KEYS[1], 'value') end
        redis.call('HSET', KEYS[1], 'quality', ARGV[6], 'timestamp', ARGV[7], 'source', ARGV[8], 'version', ARGV[10], 'last_update_operation_id', ARGV[3])
        redis.call('PUBLISH', KEYS[2], ARGV[9]); return {'applied', ARGV[10]}
        """;
}

public sealed class AtomicPointUpdateService(RedisConnectionFactory redis)
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    public async Task<AtomicPointUpdateResult> ApplyAsync(AtomicPointUpdateRequest request, CancellationToken cancellationToken = default)
    {
        Validate(request);
        var nextVersion = checked(request.ExpectedVersion + 1);
        var payload = JsonSerializer.Serialize(new { schema = 1, type = "value.updated", messageId = request.OperationId, key = request.Key, value = request.EventValue, quality = request.Quality, version = nextVersion, timestamp = request.Timestamp, source = request.Source, updateReason = request.UpdateReason }, JsonOptions);
        var result = (RedisResult[]?)await (await redis.GetDatabaseAsync(cancellationToken)).ScriptEvaluateAsync(
            AtomicPointUpdateLua.Script,
            [request.Key, "evt:value-updated"],
            [request.ExpectedOwner, request.ExpectedVersion.ToString(CultureInfo.InvariantCulture), request.OperationId, request.Value is null ? "0" : "1", request.Value ?? "", request.Quality, request.Timestamp.ToString(CultureInfo.InvariantCulture), request.Source, payload, nextVersion.ToString(CultureInfo.InvariantCulture)]);
        var status = result is { Length: > 0 } ? result[0].ToString() ?? "unexpected_result" : "unexpected_result";
        var version = result is { Length: > 1 } && long.TryParse(result[1].ToString(), NumberStyles.None, CultureInfo.InvariantCulture, out var parsed) ? parsed : (long?)null;
        return new(status, version, status is "applied" or "already_applied" ? null : result is { Length: > 1 } ? result[1].ToString() : null);
    }

    private static void Validate(AtomicPointUpdateRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (string.IsNullOrWhiteSpace(request.Key) || string.IsNullOrWhiteSpace(request.ExpectedOwner) || request.ExpectedVersion < 0 || request.Timestamp < 0 || string.IsNullOrWhiteSpace(request.Source) || !PointUpdateReasons.IsKnown(request.UpdateReason))
            throw new ArgumentException("Atomic point update request is invalid.", nameof(request));
        if (request.OperationId.Length != 32 || request.OperationId.Any(c => !char.IsAsciiHexDigit(c) || char.IsUpper(c)))
            throw new ArgumentException("OperationId must be 32 lowercase hexadecimal characters.", nameof(request));
        if (request.Quality is not ("unset" or "good" or "uncertain" or "bad"))
            throw new ArgumentException("Quality is not canonical.", nameof(request));
        ValidateValuePair(request.Value, request.EventValue);
    }

    private static void ValidateValuePair(string? redisValue, JsonElement? eventValue)
    {
        if (redisValue is null)
        {
            if (eventValue is not null) throw new ArgumentException("A missing Redis value requires a null event value.");
            return;
        }
        if (eventValue is null) throw new ArgumentException("A Redis value requires an event value.");
        var canonicalEventValue = eventValue.Value.ValueKind switch
        {
            JsonValueKind.String => eventValue.Value.GetString(),
            JsonValueKind.Number => eventValue.Value.GetRawText(),
            JsonValueKind.True => "true",
            JsonValueKind.False => "false",
            _ => throw new ArgumentException("Event value must be a JSON string, number, or boolean.")
        };
        if (!string.Equals(redisValue, canonicalEventValue, StringComparison.Ordinal))
            throw new ArgumentException("Redis value and event value must represent the same canonical value.");
    }
}
