using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Ptlk.RedisScpi.Configuration;
using Ptlk.RedisScpi.Data;
using Ptlk.RedisScpi.Models;
using Ptlk.SCADA.Interop.Runtime;

namespace Ptlk.RedisScpi.Services.Ownership;

public sealed class PointOwnershipReleaseIntentService(
    IDbContextFactory<AppDbContext> dbFactory,
    PointRuntimeLifecycleGate gate,
    PointOwnershipReleaseSignal signal,
    IOptions<RedisScpiOptions> options)
{
    public async Task<List<PointOwnershipReleaseIntent>> ListAsync(CancellationToken cancellationToken = default)
    {
        await using var db = await dbFactory.CreateDbContextAsync(cancellationToken);
        return await db.PointOwnershipReleaseIntents.AsNoTracking()
            .OrderByDescending(item => item.RequestedAt)
            .ToListAsync(cancellationToken);
    }

    public async Task<PointOwnershipReleaseIntent?> FindNonterminalAsync(string redisKey, CancellationToken cancellationToken = default)
    {
        await using var db = await dbFactory.CreateDbContextAsync(cancellationToken);
        return await db.PointOwnershipReleaseIntents.AsNoTracking()
            .Where(item => item.RedisKey == redisKey && item.Status != PointOwnershipReleaseStatuses.Applied)
            .OrderByDescending(item => item.RequestedAt)
            .FirstOrDefaultAsync(cancellationToken);
    }

    public Task<PointOwnershipReleaseIntent?> RequestDeleteAsync(int mappingId, CancellationToken cancellationToken = default) =>
        RequestAsync(mappingId, PointOwnershipReleaseReasons.MappingDeleted, null, null, cancellationToken);

    public Task<PointOwnershipReleaseIntent?> RequestRemapAsync(
        int mappingId,
        string replacementSourcePath,
        string replacementRedisKey,
        CancellationToken cancellationToken = default) =>
        RequestAsync(mappingId, PointOwnershipReleaseReasons.RedisKeyRemapped, replacementSourcePath, replacementRedisKey, cancellationToken);

    public async Task RetryAsync(int intentId, CancellationToken cancellationToken = default)
    {
        await using var db = await dbFactory.CreateDbContextAsync(cancellationToken);
        var intent = await db.PointOwnershipReleaseIntents.FindAsync([intentId], cancellationToken)
            ?? throw new InvalidOperationException("Ownership release intent was not found.");
        if (intent.Status != PointOwnershipReleaseStatuses.NeedsAttention)
            return;
        intent.Status = intent.RedisReleasedAt.HasValue ? PointOwnershipReleaseStatuses.RedisReleased : PointOwnershipReleaseStatuses.PendingRelease;
        intent.NextAttemptAt = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        intent.UpdatedAt = intent.NextAttemptAt;
        intent.LastErrorMessage = null;
        await db.SaveChangesAsync(cancellationToken);
        gate.RestoreReleasing([intent.RedisKey]);
        signal.Pulse();
    }

    public async Task<IReadOnlyList<PointOwnershipReleaseIntent>> RequestSourceDeleteAsync(
        IReadOnlyCollection<int> mappingIds,
        Func<AppDbContext, CancellationToken, Task> deleteSource,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(mappingIds);
        ArgumentNullException.ThrowIfNull(deleteSource);
        var distinctIds = mappingIds.Distinct().ToArray();
        await using var initial = await dbFactory.CreateDbContextAsync(cancellationToken);
        var snapshots = await initial.RedisMappings.AsNoTracking()
            .Where(item => distinctIds.Contains(item.Id))
            .ToListAsync(cancellationToken);
        if (snapshots.Count != distinctIds.Length)
            throw new InvalidOperationException("One or more mappings changed before source deletion.");

        var redisKeys = snapshots.Select(item => item.RedisKey)
            .Distinct(StringComparer.Ordinal)
            .Order(StringComparer.Ordinal)
            .ToArray();
        var leases = await gate.BeginReleaseManyAsync(redisKeys, cancellationToken);
        var committed = false;
        try
        {
            await using var db = await dbFactory.CreateDbContextAsync(cancellationToken);
            await using var transaction = await db.Database.BeginTransactionAsync(cancellationToken);
            var mappings = await db.RedisMappings
                .Where(item => distinctIds.Contains(item.Id))
                .ToListAsync(cancellationToken);
            if (mappings.Count != distinctIds.Length)
                throw new InvalidOperationException("One or more mappings changed during source deletion.");
            if (await db.CommandExecutions.AnyAsync(
                    item => redisKeys.Contains(item.RedisKey)
                        && item.Status != "completed"
                        && item.Status != "failed"
                        && item.Status != "ignored",
                    cancellationToken))
            {
                throw new InvalidOperationException("A source mapping has a nonterminal command and cannot be released.");
            }

            var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var intents = mappings.Select(mapping => CreateIntent(
                mapping,
                PointOwnershipReleaseReasons.SourceDeleted,
                replacementSourcePath: null,
                replacementRedisKey: null,
                now)).ToList();
            db.PointOwnershipReleaseIntents.AddRange(intents);
            db.RedisMappings.RemoveRange(mappings);
            await deleteSource(db, cancellationToken);
            await db.SaveChangesAsync(cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            committed = true;
            signal.Pulse();
            return intents;
        }
        finally
        {
            if (!committed)
            {
                foreach (var redisKey in redisKeys)
                    gate.CompleteRelease(redisKey);
            }
            for (var index = leases.Count - 1; index >= 0; index--)
                await leases[index].DisposeAsync();
        }
    }

    public async Task<PointOwnershipReleasePreparation> PrepareRemapsAsync(
        AppDbContext db,
        IReadOnlyCollection<PointOwnershipRemapPreparationRequest> requests,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(db);
        ArgumentNullException.ThrowIfNull(requests);
        var ordered = requests.OrderBy(item => item.ExpectedRedisKey, StringComparer.Ordinal).ToArray();
        if (ordered.Select(item => item.MappingId).Distinct().Count() != ordered.Length)
            throw new InvalidOperationException("A mapping can only be remapped once per configuration transaction.");
        if (ordered.Select(item => item.ReplacementRedisKey).Distinct(StringComparer.Ordinal).Count() != ordered.Length)
            throw new InvalidOperationException("Replacement Redis keys must be unique.");

        var redisKeys = ordered.Select(item => item.ExpectedRedisKey).Distinct(StringComparer.Ordinal).ToArray();
        var leases = await gate.BeginReleaseManyAsync(redisKeys, cancellationToken);
        var preparation = new PointOwnershipReleasePreparation(gate, redisKeys, leases, signal.Pulse);
        try
        {
            var mappingIds = ordered.Select(item => item.MappingId).ToArray();
            var mappings = await db.RedisMappings
                .Where(item => mappingIds.Contains(item.Id))
                .ToDictionaryAsync(item => item.Id, cancellationToken);
            if (mappings.Count != ordered.Length)
                throw new InvalidOperationException("One or more mappings changed before remap commit.");
            if (await db.CommandExecutions.AnyAsync(
                    item => redisKeys.Contains(item.RedisKey)
                        && item.Status != "completed"
                        && item.Status != "failed"
                        && item.Status != "ignored",
                    cancellationToken))
            {
                throw new InvalidOperationException("A mapping has a nonterminal command and cannot be remapped.");
            }

            var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            foreach (var request in ordered)
            {
                var mapping = mappings[request.MappingId];
                if (!mapping.SourcePath.Equals(request.ExpectedSourcePath, StringComparison.Ordinal)
                    || !mapping.RedisKey.Equals(request.ExpectedRedisKey, StringComparison.Ordinal))
                {
                    throw new InvalidOperationException($"Mapping '{request.ExpectedSourcePath}' changed before remap commit.");
                }

                db.PointOwnershipReleaseIntents.Add(CreateIntent(
                    mapping,
                    PointOwnershipReleaseReasons.RedisKeyRemapped,
                    request.ReplacementSourcePath,
                    request.ReplacementRedisKey,
                    now));
                db.RedisMappings.Remove(mapping);
            }
            return preparation;
        }
        catch
        {
            await preparation.DisposeAsync();
            throw;
        }
    }

    private async Task<PointOwnershipReleaseIntent?> RequestAsync(
        int mappingId,
        string reason,
        string? replacementSourcePath,
        string? replacementRedisKey,
        CancellationToken cancellationToken)
    {
        await using var initial = await dbFactory.CreateDbContextAsync(cancellationToken);
        var snapshot = await initial.RedisMappings.AsNoTracking().SingleOrDefaultAsync(item => item.Id == mappingId, cancellationToken);
        if (snapshot is null)
            return null;

        await using var pointLease = await gate.BeginReleaseAsync(snapshot.RedisKey, cancellationToken);
        var committed = false;
        try
        {
            await using var db = await dbFactory.CreateDbContextAsync(cancellationToken);
            await using var transaction = await db.Database.BeginTransactionAsync(cancellationToken);
            var mapping = await db.RedisMappings.SingleAsync(item => item.Id == mappingId, cancellationToken);
            if (await db.CommandExecutions.AnyAsync(
                    item => item.RedisKey == mapping.RedisKey
                        && item.Status != "completed"
                        && item.Status != "failed"
                        && item.Status != "ignored",
                    cancellationToken))
            {
                throw new InvalidOperationException("Mapping has a nonterminal command and cannot be released.");
            }

            var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var intent = CreateIntent(mapping, reason, replacementSourcePath, replacementRedisKey, now);
            db.PointOwnershipReleaseIntents.Add(intent);
            db.RedisMappings.Remove(mapping);
            await db.SaveChangesAsync(cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            committed = true;
            signal.Pulse();
            return intent;
        }
        finally
        {
            if (!committed)
                gate.CompleteRelease(snapshot.RedisKey);
        }
    }

    private PointOwnershipReleaseIntent CreateIntent(
        RedisMapping mapping,
        string reason,
        string? replacementSourcePath,
        string? replacementRedisKey,
        long now) =>
        new()
        {
            OperationId = Guid.NewGuid().ToString("N"),
            ConverterId = options.Value.ConverterId,
            SourcePath = mapping.SourcePath,
            RedisKey = mapping.RedisKey,
            Reason = reason,
            CompletionAction = replacementRedisKey is null
                ? PointOwnershipReleaseCompletionActions.CompleteOnly
                : PointOwnershipReleaseCompletionActions.ActivateReplacement,
            ReplacementSourcePath = replacementSourcePath,
            ReplacementRedisKey = replacementRedisKey,
            Status = PointOwnershipReleaseStatuses.PendingRelease,
            NextAttemptAt = now,
            RequestedAt = now,
            UpdatedAt = now
        };
}
