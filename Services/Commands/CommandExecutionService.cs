using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Ptlk.RedisScpi.Configuration;
using Ptlk.RedisScpi.Contracts.Redis;
using Ptlk.RedisScpi.Contracts.Scpi;
using Ptlk.RedisScpi.Data;
using Ptlk.RedisScpi.Models;
using Ptlk.RedisScpi.Services.Logs;
using Ptlk.RedisScpi.Services.Redis;
using Ptlk.RedisScpi.Services.Scpi;
using Ptlk.RedisScpi.Services.Startup;
using Ptlk.SCADA.Interop.PointValues;
using CommandResultEventContract = Ptlk.SCADA.Interop.Contracts.Redis.CommandResultEventContract;

namespace Ptlk.RedisScpi.Services.Commands;

public sealed class CommandExecutionService(
    IDbContextFactory<AppDbContext> dbFactory,
    IRedisPubSubService pubSub,
    RedisPointOwnershipService ownership,
    RedisReconciliationService reconciliation,
    RedisPointStateService pointState,
    IScpiClientService scpi,
    EndpointOperationScheduler scheduler,
    ScpiValueCache cache,
    LogService log,
    RuntimeModeService runtime,
    IOptions<RedisScpiOptions> redisScpiOptions,
    IOptions<RedisScpiRuntimeOptions> runtimeOptions,
    ILogger<CommandExecutionService> logger)
{
    public async Task<CommandDispatchResult> AcceptAsync(
        DeviceWriteCommandContract command,
        string canonicalPayload,
        string? payloadValidationError = null,
        CancellationToken cancellationToken = default)
    {
        var existing = await FindExistingAsync(command.CommandId, cancellationToken);
        if (existing is not null)
        {
            return await HandleDuplicateAsync(command, canonicalPayload, existing, cancellationToken);
        }

        var target = await ResolveTargetAsync(command.Key, cancellationToken);
        if (target?.Mapping is null)
        {
            runtime.ReportRuntimeDiagnostic(
                "command_route",
                command.CommandId,
                "mapping_not_found",
                $"No local RedisMapping owns '{command.Key}'.");
            return new CommandDispatchResult("ignored", "No local RedisMapping owns this key.", command.CommandId);
        }
        if (!target.Mapping.SourcePath.StartsWith("scpi:", StringComparison.OrdinalIgnoreCase))
        {
            runtime.ReportRuntimeDiagnostic(
                "command_route",
                command.CommandId,
                "unsupported_source",
                $"Mapping '{target.Mapping.SourcePath}' is not a SCPI source.");
            return new CommandDispatchResult("ignored", "The local mapping is not a SCPI source.", command.CommandId);
        }

        var endpoint = target.Point?.EndpointConfig;
        if (endpoint is not null
            && !endpoint.ConverterId.Equals(redisScpiOptions.Value.ConverterId, StringComparison.Ordinal))
        {
            runtime.ReportRuntimeDiagnostic(
                "command_route",
                command.CommandId,
                "converter_not_responsible",
                $"Endpoint '{endpoint.EndpointId}' belongs to converter '{endpoint.ConverterId}'.");
            return new CommandDispatchResult("ignored", "The endpoint belongs to another RedisScpi converter.", command.CommandId);
        }

        if (!reconciliation.IsReady)
        {
            var pendingClaim = await ClaimAsync(command, canonicalPayload, cancellationToken);
            if (!pendingClaim.Winner)
            {
                return await HandleDuplicateAsync(command, canonicalPayload, pendingClaim.Execution, cancellationToken);
            }

            return await FinishFailedAsync(
                command,
                "redis_output_not_ready",
                "Redis reconciliation is pending.",
                cancellationToken);
        }

        if (!runtime.IsRedisOutputReady
            || !(await ownership.ClaimAsync(
                target.Mapping.SourcePath,
                target.Mapping.RedisKey,
                cancellationToken)).Acquired)
        {
            await SafeLogAsync(
                "Info",
                $"Ignored command '{command.CommandId}' because this converter does not currently own '{target.Mapping.SourcePath}'.",
                command.CommandId,
                cancellationToken);
            return new CommandDispatchResult("ignored", "Not owned by this RedisScpi instance.", command.CommandId);
        }

        var claim = await ClaimAsync(command, canonicalPayload, cancellationToken);
        if (!claim.Winner)
        {
            return await HandleDuplicateAsync(command, canonicalPayload, claim.Execution, cancellationToken);
        }

        try
        {
            if (payloadValidationError is not null)
            {
                return await FinishFailedAsync(
                    command,
                    ScpiErrorCodes.InvalidPayload,
                    payloadValidationError,
                    cancellationToken);
            }
            if (target.Point?.EndpointConfig is null)
            {
                return await FinishFailedAsync(
                    command,
                    ScpiErrorCodes.PointNotFound,
                    "The mapped SCPI point does not exist.",
                    cancellationToken);
            }

            var point = target.Point;
            endpoint = point.EndpointConfig;
            if (!endpoint.Enabled)
            {
                return await FinishFailedAsync(command, ScpiErrorCodes.EndpointDisabled, $"Endpoint '{endpoint.EndpointId}' is disabled.", cancellationToken);
            }
            if (!point.Enabled)
            {
                return await FinishFailedAsync(command, ScpiErrorCodes.PointDisabled, $"Point '{point.SourcePath}' is disabled.", cancellationToken);
            }
            if (!ScpiAccessModes.Readwrite.Equals(point.Access, StringComparison.OrdinalIgnoreCase))
            {
                return await FinishFailedAsync(command, ScpiErrorCodes.PointReadonly, $"Point '{point.SourcePath}' is readonly.", cancellationToken);
            }

            var effectiveTimeout = command.TimeoutMs is > 0
                ? Math.Min(endpoint.TimeoutMs, command.TimeoutMs.Value)
                : Math.Min(endpoint.TimeoutMs, runtimeOptions.Value.CommandDefaultTimeoutMs);
            endpoint.TimeoutMs = Math.Max(1, effectiveTimeout);

            var outcome = await scheduler.RunAsync(
                endpoint.EndpointId,
                token => ExecuteWithinEndpointLockAsync(command, target, endpoint, point, token),
                cancellationToken);

            if (outcome.Success)
            {
                return await FinishCompletedAsync(
                    command,
                    outcome.ActualValue!.Value,
                    outcome.Version!.Value,
                    cancellationToken);
            }

            return await FinishFailedAsync(
                command,
                outcome.ErrorCode!,
                outcome.ErrorMessage!,
                cancellationToken,
                outcome.ActualValue,
                outcome.Version);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Command {CommandId} failed unexpectedly.", command.CommandId);
            return await FinishFailedAsync(command, ScpiErrorCodes.TransportError, ex.Message, cancellationToken);
        }
    }

    private async Task<CommandOperationOutcome> ExecuteWithinEndpointLockAsync(
        DeviceWriteCommandContract command,
        ResolvedCommandTarget target,
        ScpiEndpointConfig endpoint,
        ScpiPointConfig point,
        CancellationToken cancellationToken)
    {
        var currentClaim = await ownership.ClaimAsync(
            target.Mapping.SourcePath,
            target.Mapping.RedisKey,
            cancellationToken);
        if (!currentClaim.Acquired)
        {
            cache.MarkStale(point.SourcePath, "Ownership was lost before command execution.");
            return CommandOperationOutcome.Failed(
                ScpiErrorCodes.OwnershipNotAcquired,
                "Point ownership is not held by this RedisScpi instance.");
        }

        PointStateContract state;
        try
        {
            state = await pointState.ReadAsync(target.Mapping.RedisKey, cancellationToken)
                ?? throw new RedisPointStateException(
                    ScpiErrorCodes.PointStateInvalid,
                    "missing_key",
                    target.Mapping.RedisKey,
                    $"Redis point '{target.Mapping.RedisKey}' does not exist.");
        }
        catch (RedisPointStateException ex)
        {
            return CommandOperationOutcome.Failed(
                IsOwnershipFailure(ex) ? ScpiErrorCodes.OwnershipNotAcquired : ScpiErrorCodes.PointStateInvalid,
                ex.Message);
        }

        var stateValidation = ValidatePointState(point, state);
        if (stateValidation is not null)
        {
            return CommandOperationOutcome.Failed(ScpiErrorCodes.PointStateInvalid, stateValidation);
        }
        if (command.ExpectedVersion is not null && state.Version != command.ExpectedVersion.Value)
        {
            return CommandOperationOutcome.Failed(
                ScpiErrorCodes.ExpectedVersionMismatch,
                $"Expected version {command.ExpectedVersion.Value}, actual version {state.Version}.");
        }

        ScpiWriteReadbackResult writeResult;
        try
        {
            writeResult = await scpi.WriteAndReadbackWithinEndpointLockAsync(
                endpoint,
                point,
                command.Value,
                command.CommandId,
                cancellationToken);
        }
        catch (ScpiWriteOperationException ex)
        {
            if (ex.CommandWasSent)
            {
                await UpdateBadAfterVerificationFailureAsync(target, point, command.CommandId, ex, cancellationToken);
            }
            return CommandOperationOutcome.Failed(ex.ErrorCode, ex.Message);
        }
        catch (ScpiException ex)
        {
            return CommandOperationOutcome.Failed(ex.ErrorCode, ex.Message);
        }

        PointStateContract updated;
        try
        {
            updated = await pointState.UpdateDynamicFieldsAsync(
                target.Mapping,
                writeResult.ActualValue.JsonValue,
                ScpiQuality.Good,
                redisScpiOptions.Value.SourceName,
                cancellationToken,
                PointUpdateReasons.CommandWrite,
                command.CommandId);
            cache.SetGood(
                point.SourcePath,
                endpoint.EndpointId,
                point.PointId,
                writeResult.ActualValue.JsonValue,
                writeResult.ActualValue.RedisValue,
                "command_readback",
                FormatResponses(writeResult.RawResponse, writeResult.ErrorQueueResponse));
            runtime.ClearRedisOutputDiagnostic("command", target.Mapping.SourcePath, target.Mapping.RedisKey);
        }
        catch (RedisPointStateException ex)
        {
            if (IsOwnershipFailure(ex))
            {
                cache.MarkStale(point.SourcePath, "Ownership was lost before the command readback state update.");
            }
            return CommandOperationOutcome.Failed(
                IsOwnershipFailure(ex) ? ScpiErrorCodes.OwnershipNotAcquired : ScpiErrorCodes.PointStateInvalid,
                ex.Message);
        }

        if (!writeResult.Matches)
        {
            runtime.ReportRuntimeDiagnostic(
                "command",
                command.CommandId,
                ScpiErrorCodes.WriteVerificationFailed,
                $"Expected {writeResult.ExpectedValue.RedisValue}; actual {writeResult.ActualValue.RedisValue}.");
            return CommandOperationOutcome.Failed(
                ScpiErrorCodes.WriteVerificationFailed,
                "SCPI write readback did not match the requested value.",
                writeResult.ActualValue.JsonValue,
                updated.Version);
        }

        return CommandOperationOutcome.Completed(writeResult.ActualValue.JsonValue, updated.Version);
    }

    private async Task<CommandExecution?> FindExistingAsync(
        string commandId,
        CancellationToken cancellationToken)
    {
        await using var db = await dbFactory.CreateDbContextAsync(cancellationToken);
        return await db.CommandExecutions.AsNoTracking()
            .FirstOrDefaultAsync(item => item.CommandId == commandId, cancellationToken);
    }

    private async Task<ResolvedCommandTarget?> ResolveTargetAsync(string redisKey, CancellationToken cancellationToken)
    {
        await using var db = await dbFactory.CreateDbContextAsync(cancellationToken);
        var mapping = await db.RedisMappings.AsNoTracking()
            .FirstOrDefaultAsync(item => item.RedisKey == redisKey, cancellationToken);
        if (mapping is null) return null;
        var point = await db.ScpiPointConfigs.AsNoTracking()
            .Include(item => item.EnumOptions)
            .Include(item => item.EndpointConfig)
            .FirstOrDefaultAsync(item => item.SourcePath == mapping.SourcePath, cancellationToken);
        return new ResolvedCommandTarget(mapping, point);
    }

    private async Task<ClaimOutcome> ClaimAsync(
        DeviceWriteCommandContract command,
        string canonicalPayload,
        CancellationToken cancellationToken)
    {
        await using var db = await dbFactory.CreateDbContextAsync(cancellationToken);
        var execution = new CommandExecution
        {
            CommandId = command.CommandId,
            RedisKey = command.Key,
            Status = "accepted",
            RequestedPayloadJson = canonicalPayload
        };
        db.CommandExecutions.Add(execution);
        try
        {
            await db.SaveChangesAsync(cancellationToken);
            return new ClaimOutcome(true, execution);
        }
        catch (DbUpdateException)
        {
            await using var lookup = await dbFactory.CreateDbContextAsync(cancellationToken);
            var existing = await lookup.CommandExecutions.AsNoTracking()
                .FirstOrDefaultAsync(item => item.CommandId == command.CommandId, cancellationToken);
            if (existing is null) throw;
            return new ClaimOutcome(false, existing);
        }
    }

    private async Task<CommandDispatchResult> HandleDuplicateAsync(
        DeviceWriteCommandContract command,
        string canonicalPayload,
        CommandExecution existing,
        CancellationToken cancellationToken)
    {
        if (!string.Equals(existing.RequestedPayloadJson, canonicalPayload, StringComparison.Ordinal))
        {
            runtime.ReportRuntimeDiagnostic(
                "command",
                command.CommandId,
                ScpiErrorCodes.CommandIdPayloadMismatch,
                "A duplicate commandId arrived with a different canonical payload; the first payload remains authoritative.");
            await SafeLogAsync(
                "Warning",
                $"Duplicate command '{command.CommandId}' payload mismatch; preserving the first result.",
                command.CommandId,
                cancellationToken);
        }

        if (existing.Status.Equals("accepted", StringComparison.OrdinalIgnoreCase))
        {
            await SafeLogAsync("Info", $"Duplicate command '{command.CommandId}' is still in flight.", command.CommandId, cancellationToken);
            return new CommandDispatchResult("accepted", "Duplicate command is still in flight.", command.CommandId);
        }

        if (!string.IsNullOrWhiteSpace(existing.ResultPayloadJson))
        {
            try
            {
                await pubSub.PublishRawAsync(RedisContractNames.CommandResultChannel, existing.ResultPayloadJson, cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                runtime.ReportRuntimeDiagnostic(
                    "command_result",
                    command.CommandId,
                    "replay_publish_failed",
                    $"The stored terminal result remains available, but replay publish failed: {ex.Message}");
                logger.LogWarning(ex, "Stored terminal result replay failed for {CommandId}.", command.CommandId);
            }
            return new CommandDispatchResult(existing.Status, "Stored terminal result replayed.", command.CommandId);
        }

        return new CommandDispatchResult(existing.Status, "Duplicate terminal command has no stored result.", command.CommandId);
    }

    private async Task<CommandDispatchResult> FinishCompletedAsync(
        DeviceWriteCommandContract command,
        JsonElement actualValue,
        long version,
        CancellationToken cancellationToken)
    {
        var state = await pointState.ReadAsync(command.Key, cancellationToken)
            ?? throw new InvalidOperationException($"Redis point '{command.Key}' is unavailable after command completion.");
        var pointType = PointValueCanonicalizer.NormalizePointType(state.Type)
            ?? throw new InvalidOperationException($"Redis point '{command.Key}' has no canonical point type.");
        var canonical = PointValueCanonicalizer.ValidateJson(pointType, actualValue);
        if (!canonical.Success)
        {
            throw new InvalidOperationException(
                $"{canonical.DiagnosticCode}: {canonical.DiagnosticMessage}");
        }
        var result = new CommandResultEventContract(
            1,
            "command.completed",
            Guid.NewGuid().ToString("N"),
            command.CommandId,
            command.Key,
            true,
            pointType,
            canonical.CanonicalValue!.JsonValue,
            version,
            null,
            null,
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            redisScpiOptions.Value.SourceName);
        await SaveTerminalAsync(command.CommandId, "completed", result, actualValue, version, null, null, cancellationToken);
        await PublishTerminalAsync(result, command.CommandId, cancellationToken);
        runtime.ClearRuntimeDiagnostic("command", command.CommandId);
        runtime.ClearRuntimeDiagnostic("command_accepted", command.CommandId);
        if (!runtime.Current.RuntimeDiagnostics.Any(item =>
                item.Subsystem.Equals("command", StringComparison.OrdinalIgnoreCase)
                || item.Subsystem.Equals("command_subscription", StringComparison.OrdinalIgnoreCase)))
        {
            runtime.SetCommand(RuntimeSubsystemStatus.Normal, "SCPI command processing is operating normally.");
        }
        await SafeLogAsync("Info", $"Completed command '{command.CommandId}'.", command.CommandId, cancellationToken);
        return new CommandDispatchResult("completed", "Command completed.", command.CommandId);
    }

    private async Task<CommandDispatchResult> FinishFailedAsync(
        DeviceWriteCommandContract command,
        string errorCode,
        string errorMessage,
        CancellationToken cancellationToken,
        JsonElement? diagnosticActualValue = null,
        long? diagnosticVersion = null)
    {
        var pointType = await ReadPointTypeOrNullAsync(command.Key, cancellationToken);
        var result = new CommandResultEventContract(
            1,
            "command.failed",
            Guid.NewGuid().ToString("N"),
            command.CommandId,
            command.Key,
            false,
            pointType,
            null,
            null,
            errorCode,
            errorMessage,
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            redisScpiOptions.Value.SourceName);
        await SaveTerminalAsync(
            command.CommandId,
            "failed",
            result,
            diagnosticActualValue,
            diagnosticVersion,
            errorCode,
            errorMessage,
            cancellationToken);
        await PublishTerminalAsync(result, command.CommandId, cancellationToken);
        runtime.ClearRuntimeDiagnostic("command_accepted", command.CommandId);
        runtime.SetCommand(RuntimeSubsystemStatus.Degraded, $"Command '{command.CommandId}' failed: {errorMessage}");
        runtime.ReportRuntimeDiagnostic("command", command.CommandId, errorCode, errorMessage);
        await SafeLogAsync("Warning", $"Command '{command.CommandId}' failed: {errorMessage}", command.CommandId, cancellationToken);
        return new CommandDispatchResult("failed", errorMessage, command.CommandId);
    }

    private async Task<string?> ReadPointTypeOrNullAsync(
        string key,
        CancellationToken cancellationToken)
    {
        try
        {
            return PointValueCanonicalizer.NormalizePointType(
                (await pointState.ReadAsync(key, cancellationToken))?.Type);
        }
        catch
        {
            return null;
        }
    }

    private async Task SaveTerminalAsync(
        string commandId,
        string status,
        CommandResultEventContract result,
        JsonElement? actualValue,
        long? version,
        string? errorCode,
        string? errorMessage,
        CancellationToken cancellationToken)
    {
        await using var db = await dbFactory.CreateDbContextAsync(cancellationToken);
        var execution = await db.CommandExecutions.FirstAsync(item => item.CommandId == commandId, cancellationToken);
        execution.Status = status;
        execution.ResultPayloadJson = JsonSerializer.Serialize(result, RedisContractJson.WebOptions);
        execution.ActualValueJson = actualValue is null ? null : actualValue.Value.GetRawText();
        execution.Version = version;
        execution.ErrorCode = errorCode;
        execution.ErrorMessage = errorMessage;
        execution.CompletedAt = DateTimeOffset.UtcNow;
        await db.SaveChangesAsync(cancellationToken);
    }

    private async Task PublishTerminalAsync(
        CommandResultEventContract result,
        string commandId,
        CancellationToken cancellationToken)
    {
        try
        {
            await pubSub.PublishAsync(RedisContractNames.CommandResultChannel, result, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            runtime.ReportRuntimeDiagnostic(
                "command_result",
                commandId,
                "publish_failed",
                $"The terminal result is persisted for replay, but publish failed: {ex.Message}");
            logger.LogWarning(ex, "Terminal command result {CommandId} was saved but could not be published.", commandId);
        }
    }

    private async Task UpdateBadAfterVerificationFailureAsync(
        ResolvedCommandTarget target,
        ScpiPointConfig point,
        string commandId,
        ScpiWriteOperationException exception,
        CancellationToken cancellationToken)
    {
        var rawContext = FormatResponses(exception.PointResponse, exception.ErrorQueueResponse);
        try
        {
            await pointState.UpdateDynamicFieldsAsync(
                target.Mapping,
                null,
                ScpiQuality.Bad,
                redisScpiOptions.Value.SourceName,
                cancellationToken,
                PointUpdateReasons.CommandWrite,
                commandId);
            cache.SetBad(
                point.SourcePath,
                point.EndpointConfig!.EndpointId,
                point.PointId,
                "command_readback",
                rawContext,
                exception.ErrorCode,
                exception.Message);
        }
        catch (RedisPointStateException redisException)
        {
            if (IsOwnershipFailure(redisException))
            {
                cache.MarkStale(point.SourcePath, "Ownership was lost before the command failure state update.");
            }
            runtime.ReportRedisOutputDiagnostic(
                "command_verification",
                target.Mapping.SourcePath,
                target.Mapping.RedisKey,
                redisException.Status,
                redisException.Message);
        }
    }

    private static bool IsOwnershipFailure(RedisPointStateException exception) =>
        exception.Reason.Equals("ownership_missing", StringComparison.OrdinalIgnoreCase)
        || exception.Reason.Equals("owned_by_other", StringComparison.OrdinalIgnoreCase)
        || exception.Reason.Equals(ScpiErrorCodes.OwnershipNotAcquired, StringComparison.OrdinalIgnoreCase);

    private static string? ValidatePointState(ScpiPointConfig point, PointStateContract state)
    {
        var expectedType = point.DataType switch
        {
            ScpiDataTypes.Number when point.NumberType == ScpiNumberTypes.Int => "int",
            ScpiDataTypes.Number when point.NumberType == ScpiNumberTypes.Double => "double",
            ScpiDataTypes.Enum when point.EnumFormat == ScpiEnumFormats.Code => "int",
            ScpiDataTypes.String or ScpiDataTypes.Enum => "string",
            _ => ""
        };
        if (!state.Type.Equals(expectedType, StringComparison.OrdinalIgnoreCase))
        {
            return $"Redis point type '{state.Type}' does not match configured SCPI type '{expectedType}'.";
        }
        if (!state.Access.Equals("readwrite", StringComparison.OrdinalIgnoreCase))
        {
            return $"Redis point access '{state.Access}' does not allow writes.";
        }
        return null;
    }

    private static string? FormatResponses(string? pointResponse, string? errorQueueResponse)
    {
        if (pointResponse is null) return errorQueueResponse is null ? null : $"error_queue: {errorQueueResponse}";
        if (errorQueueResponse is null) return $"point: {pointResponse}";
        return $"point: {pointResponse}\nerror_queue: {errorQueueResponse}";
    }

    private async Task SafeLogAsync(string level, string message, string? commandId, CancellationToken cancellationToken)
    {
        try
        {
            await log.AddSystemAsync("Command", level, message, commandId, cancellationToken);
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "Failed to persist command diagnostic.");
        }
    }

    private sealed record ResolvedCommandTarget(RedisMapping Mapping, ScpiPointConfig? Point);
    private sealed record ClaimOutcome(bool Winner, CommandExecution Execution);
    private sealed record CommandOperationOutcome(
        bool Success,
        string? ErrorCode,
        string? ErrorMessage,
        JsonElement? ActualValue,
        long? Version)
    {
        public static CommandOperationOutcome Completed(JsonElement value, long version) =>
            new(true, null, null, value.Clone(), version);

        public static CommandOperationOutcome Failed(
            string errorCode,
            string errorMessage,
            JsonElement? actualValue = null,
            long? version = null) =>
            new(false, errorCode, errorMessage, actualValue?.Clone(), version);
    }
}
