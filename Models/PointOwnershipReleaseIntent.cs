namespace Ptlk.RedisScpi.Models;

public sealed class PointOwnershipReleaseIntent
{
    public int Id { get; set; }
    public string OperationId { get; set; } = "";
    public string ConverterId { get; set; } = "";
    public string? SourcePath { get; set; }
    public string RedisKey { get; set; } = "";
    public string Reason { get; set; } = PointOwnershipReleaseReasons.MappingDeleted;
    public string CompletionAction { get; set; } = PointOwnershipReleaseCompletionActions.CompleteOnly;
    public string? ReplacementSourcePath { get; set; }
    public string? ReplacementRedisKey { get; set; }
    public string Status { get; set; } = PointOwnershipReleaseStatuses.PendingRelease;
    public int AttemptCount { get; set; }
    public long NextAttemptAt { get; set; }
    public string? LastResultCode { get; set; }
    public string? LastErrorMessage { get; set; }
    public long RequestedAt { get; set; }
    public long UpdatedAt { get; set; }
    public long? RedisReleasedAt { get; set; }
    public long? AppliedAt { get; set; }
}

public static class PointOwnershipReleaseStatuses
{
    public const string PendingRelease = "pending_release";
    public const string RedisReleased = "redis_released";
    public const string Applied = "applied";
    public const string NeedsAttention = "needs_attention";
    public static bool IsNonterminal(string status) => status is PendingRelease or RedisReleased or NeedsAttention;
}

public static class PointOwnershipReleaseReasons
{
    public const string MappingDeleted = "mapping_deleted";
    public const string SourceDeleted = "source_deleted";
    public const string RedisKeyRemapped = "redis_key_remapped";
    public const string ImportRemoved = "import_removed";
    public const string ExplicitRetirement = "explicit_retirement";
}

public static class PointOwnershipReleaseCompletionActions
{
    public const string CompleteOnly = "complete_only";
    public const string ActivateReplacement = "activate_replacement";
}
