using System.Text.Json;

namespace Ptlk.RedisScpi.Contracts.Redis;

public sealed record ValueUpdatedEventContract(
    int Schema,
    string Type,
    string MessageId,
    string Key,
    JsonElement? Value,
    string Quality,
    long Version,
    long Timestamp,
    string Source,
    string? UpdateReason = null);
