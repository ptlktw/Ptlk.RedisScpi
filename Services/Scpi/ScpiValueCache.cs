using System.Collections.Concurrent;
using System.Text.Json;

namespace Ptlk.RedisScpi.Services.Scpi;

public sealed record ScpiCachedValue(
    string SourcePath,
    string EndpointId,
    string PointId,
    JsonElement? Value,
    string? RedisValue,
    string Quality,
    DateTimeOffset UpdatedAt,
    string Operation,
    string? RawResponse,
    string? ErrorCode,
    string? ErrorMessage,
    bool Stale,
    string? StaleReason,
    long Sequence = 0);

public sealed class ScpiValueCache
{
    private readonly ConcurrentDictionary<string, ScpiCachedValue> _values =
        new(StringComparer.OrdinalIgnoreCase);
    private readonly object _sync = new();
    private long _sequence;
    private long _outageBoundary;
    private bool _outageActive = true;

    public event Action? Changed;

    public ScpiCachedValue Set(ScpiCachedValue value)
    {
        ScpiCachedValue updated;
        lock (_sync)
        {
            if (value.Quality == Contracts.Scpi.ScpiQuality.Bad
                && _values.TryGetValue(value.SourcePath, out var current)
                && string.Equals(current.RedisValue, value.RedisValue, StringComparison.Ordinal)
                && current.Quality == value.Quality
                && string.Equals(current.ErrorCode, value.ErrorCode, StringComparison.Ordinal)
                && string.Equals(current.ErrorMessage, value.ErrorMessage, StringComparison.Ordinal))
            {
                return current;
            }

            updated = value with { Sequence = ++_sequence };
            _values[value.SourcePath] = updated;
        }
        Changed?.Invoke();
        return updated;
    }

    public ScpiCachedValue SetGood(
        string sourcePath,
        string endpointId,
        string pointId,
        JsonElement value,
        string redisValue,
        string operation,
        string? rawResponse)
    {
        return Set(new ScpiCachedValue(
            sourcePath,
            endpointId,
            pointId,
            value.Clone(),
            redisValue,
            Contracts.Scpi.ScpiQuality.Good,
            DateTimeOffset.UtcNow,
            operation,
            rawResponse,
            null,
            null,
            false,
            null));
    }

    public ScpiCachedValue SetBad(
        string sourcePath,
        string endpointId,
        string pointId,
        string operation,
        string? rawResponse,
        string errorCode,
        string errorMessage)
    {
        return Set(new ScpiCachedValue(
            sourcePath,
            endpointId,
            pointId,
            null,
            null,
            Contracts.Scpi.ScpiQuality.Bad,
            DateTimeOffset.UtcNow,
            operation,
            rawResponse,
            errorCode,
            errorMessage,
            false,
            null));
    }

    public void MarkStale(string sourcePath, string reason)
    {
        if (!_values.TryGetValue(sourcePath, out var existing))
        {
            return;
        }

        _values[sourcePath] = existing with
        {
            Stale = true,
            StaleReason = reason
        };
        Changed?.Invoke();
    }

    public ScpiCachedValue? Get(string sourcePath) =>
        _values.TryGetValue(sourcePath, out var value) ? value : null;

    public IReadOnlyList<ScpiCachedValue> Snapshot() =>
        _values.Values.OrderBy(value => value.SourcePath, StringComparer.OrdinalIgnoreCase).ToList();

    public bool BeginOutage()
    {
        lock (_sync)
        {
            if (_outageActive) return false;
            _outageBoundary = _sequence;
            _outageActive = true;
            return true;
        }
    }

    public (long ObservedSequence, IReadOnlyDictionary<string, ScpiCachedValue> Values) SnapshotForReconciliation()
    {
        lock (_sync)
        {
            return (
                _sequence,
                _values
                    .Where(item => item.Value.Sequence > _outageBoundary)
                    .ToDictionary(item => item.Key, item => item.Value, StringComparer.OrdinalIgnoreCase));
        }
    }

    public bool TryCompleteReconciliation(long observedSequence)
    {
        lock (_sync)
        {
            if (_sequence != observedSequence) return false;
            _outageBoundary = _sequence;
            _outageActive = false;
            return true;
        }
    }
}
