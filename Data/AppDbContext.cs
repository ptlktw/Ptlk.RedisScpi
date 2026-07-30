using Microsoft.EntityFrameworkCore;
using Ptlk.RedisScpi.Models;

namespace Ptlk.RedisScpi.Data;

public sealed class AppDbContext(DbContextOptions<AppDbContext> options) : DbContext(options)
{
    public DbSet<ScpiEndpointConfig> ScpiEndpointConfigs => Set<ScpiEndpointConfig>();
    public DbSet<ScpiPointConfig> ScpiPointConfigs => Set<ScpiPointConfig>();
    public DbSet<ScpiEnumOption> ScpiEnumOptions => Set<ScpiEnumOption>();
    public DbSet<RedisMapping> RedisMappings => Set<RedisMapping>();
    public DbSet<CommandExecution> CommandExecutions => Set<CommandExecution>();
    public DbSet<ScpiLogEntry> ScpiLogEntries => Set<ScpiLogEntry>();
    public DbSet<SystemLogEntry> SystemLogEntries => Set<SystemLogEntry>();
    public DbSet<PointOwnershipReleaseIntent> PointOwnershipReleaseIntents => Set<PointOwnershipReleaseIntent>();

    public override int SaveChanges()
    {
        TouchAuditFields();
        return base.SaveChanges();
    }

    public override Task<int> SaveChangesAsync(CancellationToken cancellationToken = default)
    {
        TouchAuditFields();
        return base.SaveChangesAsync(cancellationToken);
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        modelBuilder.Entity<ScpiEndpointConfig>(entity =>
        {
            entity.HasIndex(endpoint => endpoint.EndpointId).IsUnique();
            entity.Property(endpoint => endpoint.EndpointId).HasMaxLength(160).UseCollation("NOCASE").IsRequired();
            entity.Property(endpoint => endpoint.DisplayName).HasMaxLength(160).IsRequired();
            entity.Property(endpoint => endpoint.Transport).HasMaxLength(32).IsRequired();
            entity.Property(endpoint => endpoint.TcpHost).HasMaxLength(255);
            entity.Property(endpoint => endpoint.ConverterId).HasMaxLength(160).IsRequired();
            entity.Property(endpoint => endpoint.ErrorCheckMode).HasMaxLength(32).IsRequired();
            entity.Property(endpoint => endpoint.ErrorQueueQuery).HasMaxLength(1000).IsRequired();
            entity.Property(endpoint => endpoint.CommandTerminator).HasMaxLength(16).IsRequired();
            entity.Property(endpoint => endpoint.ResponseTerminator).HasMaxLength(16).IsRequired();
            entity.Property(endpoint => endpoint.ConcurrencyStamp)
                .HasMaxLength(32)
                .IsRequired()
                .IsConcurrencyToken();
            entity.HasMany(endpoint => endpoint.Points)
                .WithOne(point => point.EndpointConfig)
                .HasForeignKey(point => point.EndpointConfigId)
                .OnDelete(DeleteBehavior.Cascade);
        });

        modelBuilder.Entity<ScpiPointConfig>(entity =>
        {
            entity.HasIndex(point => point.SourcePath).IsUnique();
            entity.HasIndex(point => new { point.EndpointConfigId, point.PointId }).IsUnique();
            entity.Property(point => point.PointId).HasMaxLength(160).UseCollation("NOCASE").IsRequired();
            entity.Property(point => point.SourcePath).HasMaxLength(320).UseCollation("NOCASE").IsRequired();
            entity.Property(point => point.Name).HasMaxLength(160).IsRequired();
            entity.Property(point => point.DisplayName).HasMaxLength(160);
            entity.Property(point => point.Access).HasMaxLength(16).IsRequired();
            entity.Property(point => point.DataType).HasMaxLength(16).IsRequired();
            entity.Property(point => point.NumberType).HasMaxLength(16);
            entity.Property(point => point.StringFormat).HasMaxLength(16);
            entity.Property(point => point.EnumFormat).HasMaxLength(16);
            entity.Property(point => point.ReadTemplate).HasMaxLength(1000).IsRequired();
            entity.Property(point => point.WriteTemplate).HasMaxLength(1000);
            entity.Property(point => point.Unit).HasMaxLength(80);
            entity.Property(point => point.ConcurrencyStamp)
                .HasMaxLength(32)
                .IsRequired()
                .IsConcurrencyToken();
            entity.HasMany(point => point.EnumOptions)
                .WithOne(option => option.ScpiPointConfig)
                .HasForeignKey(option => option.ScpiPointConfigId)
                .OnDelete(DeleteBehavior.Cascade);
        });

        modelBuilder.Entity<ScpiEnumOption>(entity =>
        {
            entity.HasIndex(option => new { option.ScpiPointConfigId, option.Value }).IsUnique();
            entity.HasIndex(option => new { option.ScpiPointConfigId, option.Code }).IsUnique();
            entity.Property(option => option.DisplayName).HasMaxLength(160).IsRequired();
            entity.Property(option => option.Value).HasMaxLength(320).UseCollation("NOCASE").IsRequired();
        });

        modelBuilder.Entity<RedisMapping>(entity =>
        {
            entity.HasIndex(mapping => mapping.SourcePath).IsUnique();
            entity.HasIndex(mapping => mapping.RedisKey).IsUnique();
            entity.Property(mapping => mapping.SourcePath).HasMaxLength(320).UseCollation("NOCASE").IsRequired();
            entity.Property(mapping => mapping.RedisKey).HasMaxLength(320).IsRequired();
            entity.Property(mapping => mapping.ConcurrencyStamp)
                .HasMaxLength(32)
                .IsRequired()
                .IsConcurrencyToken();
        });

        modelBuilder.Entity<CommandExecution>(entity =>
        {
            entity.HasIndex(command => command.CommandId).IsUnique();
            entity.HasIndex(command => command.RedisKey);
            entity.HasIndex(command => new { command.Status, command.CompletedAt });
            entity.Property(command => command.CommandId).HasMaxLength(160).IsRequired();
            entity.Property(command => command.RedisKey).HasMaxLength(320).IsRequired();
            entity.Property(command => command.Status).HasMaxLength(32).IsRequired();
            entity.Property(command => command.RequestedPayloadJson).IsRequired();
            entity.Property(command => command.ErrorCode).HasMaxLength(80);
            entity.Property(command => command.ErrorMessage).HasMaxLength(2000);
        });

        modelBuilder.Entity<ScpiLogEntry>(entity =>
        {
            entity.HasIndex(log => log.CreatedAt);
            entity.HasIndex(log => new { log.EndpointId, log.PointId });
            entity.HasIndex(log => log.CommandId);
            entity.Property(log => log.EndpointId).HasMaxLength(160);
            entity.Property(log => log.PointId).HasMaxLength(160);
            entity.Property(log => log.Operation).HasMaxLength(80).IsRequired();
            entity.Property(log => log.Level).HasMaxLength(32).IsRequired();
            entity.Property(log => log.Message).HasMaxLength(2000).IsRequired();
            entity.Property(log => log.CommandText).HasMaxLength(4000);
            entity.Property(log => log.ResponseText).HasMaxLength(8000);
            entity.Property(log => log.Quality).HasMaxLength(32);
            entity.Property(log => log.CommandId).HasMaxLength(160);
            entity.Property(log => log.ErrorCode).HasMaxLength(80);
        });

        modelBuilder.Entity<SystemLogEntry>(entity =>
        {
            entity.HasIndex(log => log.CreatedAt);
            entity.HasIndex(log => new { log.Category, log.Level });
            entity.HasIndex(log => log.CommandId);
            entity.Property(log => log.Category).HasMaxLength(80).IsRequired();
            entity.Property(log => log.Level).HasMaxLength(32).IsRequired();
            entity.Property(log => log.Message).HasMaxLength(2000).IsRequired();
            entity.Property(log => log.CommandId).HasMaxLength(160);
        });

        ConfigurePointOwnershipReleaseIntent(modelBuilder.Entity<PointOwnershipReleaseIntent>());
    }

    private static void ConfigurePointOwnershipReleaseIntent(Microsoft.EntityFrameworkCore.Metadata.Builders.EntityTypeBuilder<PointOwnershipReleaseIntent> entity)
    {
        entity.HasIndex(x => x.OperationId).IsUnique();
        entity.HasIndex(x => new { x.Status, x.NextAttemptAt, x.RequestedAt });
        entity.HasIndex(x => x.RedisKey).IsUnique().HasFilter("\"status\" <> 'applied'");
        entity.HasIndex(x => x.ReplacementRedisKey).IsUnique().HasFilter("\"replacement_redis_key\" IS NOT NULL AND \"status\" <> 'applied'");
        entity.HasIndex(x => new { x.SourcePath, x.Status });
        entity.Property(x => x.OperationId).HasMaxLength(32).IsRequired();
        entity.Property(x => x.ConverterId).HasMaxLength(160).IsRequired();
        entity.Property(x => x.SourcePath).HasMaxLength(320);
        entity.Property(x => x.RedisKey).HasMaxLength(320).IsRequired();
        entity.Property(x => x.Reason).HasMaxLength(40).IsRequired();
        entity.Property(x => x.CompletionAction).HasMaxLength(32).IsRequired();
        entity.Property(x => x.ReplacementSourcePath).HasMaxLength(320);
        entity.Property(x => x.ReplacementRedisKey).HasMaxLength(320);
        entity.Property(x => x.Status).HasMaxLength(32).IsRequired();
        entity.Property(x => x.LastResultCode).HasMaxLength(80);
        entity.Property(x => x.LastErrorMessage).HasMaxLength(1000);
    }

    private void TouchAuditFields()
    {
        var now = DateTimeOffset.UtcNow;
        foreach (var entry in ChangeTracker.Entries())
        {
            if (entry.State == EntityState.Added)
            {
                SetIfPresent(entry.Entity, nameof(ScpiEndpointConfig.CreatedAt), now);
                SetIfPresent(entry.Entity, nameof(ScpiEndpointConfig.UpdatedAt), now);
                SetConcurrencyStampIfPresent(entry.Entity);
            }
            else if (entry.State == EntityState.Modified)
            {
                SetIfPresent(entry.Entity, nameof(ScpiEndpointConfig.UpdatedAt), now);
                SetConcurrencyStampIfPresent(entry.Entity);
            }
        }
    }

    private static void SetIfPresent(object entity, string propertyName, DateTimeOffset value)
    {
        var property = entity.GetType().GetProperty(propertyName);
        if (property?.CanWrite == true && property.PropertyType == typeof(DateTimeOffset))
        {
            property.SetValue(entity, value);
        }
    }

    private static void SetConcurrencyStampIfPresent(object entity)
    {
        var property = entity.GetType().GetProperty(nameof(ScpiEndpointConfig.ConcurrencyStamp));
        if (property?.CanWrite == true && property.PropertyType == typeof(string))
        {
            property.SetValue(entity, Guid.NewGuid().ToString("N"));
        }
    }
}
