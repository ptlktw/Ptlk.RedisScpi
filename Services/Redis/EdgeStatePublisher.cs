using System.Globalization;
using System.Text.Json;
using Ptlk.RedisScpi.Contracts.Redis;
using StackExchange.Redis;

namespace Ptlk.RedisScpi.Services.Redis;

public sealed class EdgeStatePublisher(RedisConnectionFactory redis)
{
    private const string Script = """
        local previousInstance = redis.call('HGET', KEYS[1], 'instance_id')
        local previousStatus = redis.call('HGET', KEYS[1], 'status')
        local lastSeen = ARGV[6]
        if ARGV[4] == 'offline' and previousInstance == ARGV[2] then
          local existingLastSeen = redis.call('HGET', KEYS[1], 'last_seen_at')
          if existingLastSeen and existingLastSeen ~= '' then lastSeen = existingLastSeen end
        end
        local transitionAt = ''
        local edgeStatus = ARGV[4]
        if edgeStatus == 'heartbeat' then edgeStatus = 'online' end
        if edgeStatus == 'error' then edgeStatus = 'degraded' end
        if edgeStatus == 'offline' then
          if previousInstance == ARGV[2] and previousStatus == 'offline' then
            transitionAt = redis.call('HGET', KEYS[1], 'stale_transition_at') or ''
          end
          if transitionAt == '' then transitionAt = ARGV[6] end
        end
        local connectedAt = redis.call('HGET', KEYS[1], 'connected_at') or '0'
        local disconnectedAt = redis.call('HGET', KEYS[1], 'disconnected_at') or '0'
        if edgeStatus == 'offline' then disconnectedAt = ARGV[6]
        elseif connectedAt == '0' or previousStatus == 'offline' or previousStatus == 'stale' then connectedAt = ARGV[6] end
        redis.call('HSET', KEYS[1],
          'schema', '1',
          'converter_id', ARGV[1],
          'instance_id', ARGV[2],
          'source', ARGV[3],
          'status', edgeStatus,
          'runtime_mode', ARGV[12],
          'redis_connectivity', ARGV[13],
          'redis_output', ARGV[14],
          'reconciliation', ARGV[15],
          'timestamp', ARGV[6],
          'last_seen_at', lastSeen,
          'heartbeat_interval_ms', ARGV[7],
          'expires_at', ARGV[8],
          'connected_at', connectedAt,
          'disconnected_at', disconnectedAt,
          'reconnect_count', ARGV[16],
          'message', ARGV[9],
          'metadata_json', ARGV[10])
        if transitionAt ~= '' then
          redis.call('HSET', KEYS[1], 'stale_transition_at', transitionAt)
        else
          redis.call('HDEL', KEYS[1], 'stale_transition_at')
        end
        redis.call('SADD', KEYS[2], ARGV[1])
        redis.call('PUBLISH', KEYS[3], ARGV[11])
        return transitionAt
        """;

    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    public async Task PublishAsync(
        RedisProtocolConverterStatusEventContract statusEvent,
        int heartbeatIntervalMs,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(statusEvent);
        if (string.IsNullOrWhiteSpace(statusEvent.ConverterId) || heartbeatIntervalMs <= 0)
            throw new ArgumentException("Edge identity and heartbeat interval are required.");

        var instanceId = statusEvent.Metadata?.GetValueOrDefault("instanceId");
        if (string.IsNullOrWhiteSpace(instanceId))
            instanceId = statusEvent.ConverterId;
        var expiryWindow = Math.Max(checked((long)heartbeatIntervalMs * 3), 30_000L);
        var expiresAt = checked(statusEvent.Timestamp + expiryWindow);
        var metadataJson = JsonSerializer.Serialize(statusEvent.Metadata, JsonOptions);
        var payload = JsonSerializer.Serialize(statusEvent, JsonOptions);
        var runtimeMode = Metadata(statusEvent, "runtimeMode", "mode") ?? "unknown";
        var redisConnectivity = Metadata(statusEvent, "redisConnectivity")
            ?? (Metadata(statusEvent, "redisConnected") == "true" ? "connected" : "disconnected");
        var redisOutput = Metadata(statusEvent, "redisOutput", "redisOutputStatus") ?? "unknown";
        var reconciliation = Metadata(statusEvent, "reconciliation")
            ?? (Metadata(statusEvent, "assetInitialized") == "true" ? "ready" : "pending");
        var reconnectCount = long.TryParse(Metadata(statusEvent, "reconnectCount"), NumberStyles.None, CultureInfo.InvariantCulture, out var parsedReconnect)
            && parsedReconnect >= 0 ? parsedReconnect : 0;
        var db = await redis.GetDatabaseAsync(cancellationToken);
        await db.ScriptEvaluateAsync(
            Script,
            [$"edge:{statusEvent.ConverterId}", "edge:index", "evt:edge-status"],
            [
                statusEvent.ConverterId,
                instanceId,
                statusEvent.Source,
                statusEvent.Status,
                statusEvent.Type,
                statusEvent.Timestamp.ToString(CultureInfo.InvariantCulture),
                heartbeatIntervalMs.ToString(CultureInfo.InvariantCulture),
                expiresAt.ToString(CultureInfo.InvariantCulture),
                Bounded(Metadata(statusEvent, "message"), 2_048),
                metadataJson,
                payload,
                runtimeMode,
                redisConnectivity,
                redisOutput,
                reconciliation,
                reconnectCount.ToString(CultureInfo.InvariantCulture)
            ]);
    }

    private static string? Metadata(RedisProtocolConverterStatusEventContract statusEvent, params string[] names)
    {
        foreach (var name in names)
            if (statusEvent.Metadata?.TryGetValue(name, out var value) == true && !string.IsNullOrWhiteSpace(value))
                return value;
        return null;
    }

    private static string Bounded(string? value, int limit) =>
        string.IsNullOrEmpty(value) ? "" : value.Length <= limit ? value : value[..limit];
}

