using Microsoft.AspNetCore.DataProtection;
using Microsoft.EntityFrameworkCore;
using Ptlk.RedisScpi.Components;
using Ptlk.RedisScpi.Configuration;
using Ptlk.RedisScpi.Data;
using Ptlk.RedisScpi.Services.Browser;
using Ptlk.RedisScpi.Services.Commands;
using Ptlk.RedisScpi.Services.ImportExport;
using Ptlk.RedisScpi.Services.Logs;
using Ptlk.RedisScpi.Services.Ownership;
using Ptlk.RedisScpi.Services.Paths;
using Ptlk.RedisScpi.Services.Redis;
using Ptlk.RedisScpi.Services.Scpi;
using Ptlk.RedisScpi.Services.Startup;
using Ptlk.RedisScpi.Services.Ui;
using Ptlk.SCADA.Interop.Runtime;
using Ptlk.Web.Hosting;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddPtlkPublicWebHosting(builder.Configuration, builder.Environment, "redis-scpi");

builder.Logging.ClearProviders();
builder.Logging.AddConsole();
builder.Logging.AddDebug();

builder.Services.AddRedisScpiOptions(builder.Configuration);

var connectionString = builder.Configuration.GetConnectionString("DefaultConnection")
    ?? "Data Source=/data/redis-scpi.db";
var dataProtectionKeysPath = builder.Configuration["DataProtection:KeysPath"]
    ?? (builder.Environment.IsDevelopment() ? "data-protection-keys" : "/data/data-protection-keys");
Directory.CreateDirectory(dataProtectionKeysPath);

builder.Services.AddDataProtection()
    .PersistKeysToFileSystem(new DirectoryInfo(dataProtectionKeysPath));

builder.Services.AddDbContextFactory<AppDbContext>(options =>
    options.UseSqlite(connectionString)
        .UseSnakeCaseNamingConvention());

builder.Services.AddSingleton<RuntimeModeService>();
builder.Services.AddSingleton<EdgeRuntimeIdentity>();
builder.Services.AddSingleton<PointRuntimeLifecycleGate>();
builder.Services.AddSingleton<PointOwnershipReleaseSignal>();
builder.Services.AddSingleton<PointOwnershipReleaseRuntimeState>();
builder.Services.AddSingleton<Ptlk.SCADA.Interop.Redis.PointOwnershipReleaseScript>();
builder.Services.AddSingleton<Ptlk.SCADA.Interop.Redis.PointOwnershipReleaseExecutor>();
builder.Services.AddSingleton<RedisConnectionFactory>();
builder.Services.AddSingleton<RedisPubSubService>();
builder.Services.AddSingleton<IRedisPubSubService>(provider => provider.GetRequiredService<RedisPubSubService>());
builder.Services.AddSingleton<EdgeStatePublisher>();
builder.Services.AddSingleton<RedisMappingActivationService>();
builder.Services.AddSingleton<RedisPointOwnershipService>();
builder.Services.AddSingleton<AtomicPointUpdateService>();
builder.Services.AddSingleton<PointUpdateIdentity>();
builder.Services.AddSingleton<RedisPointStateService>(provider => new RedisPointStateService(
    provider.GetRequiredService<RedisConnectionFactory>(),
    provider.GetRequiredService<AtomicPointUpdateService>(),
    provider.GetRequiredService<PointUpdateIdentity>(),
    provider.GetRequiredService<Microsoft.Extensions.Options.IOptions<RedisScpiOptions>>(),
    provider.GetRequiredService<RuntimeModeService>()));
builder.Services.AddSingleton<RedisReconciliationService>();
builder.Services.AddSingleton<RedisKeySuggestionService>();
builder.Services.AddSingleton<PointOwnershipReleaseCoordinator>();

builder.Services.AddSingleton<ScpiSourcePathService>();
builder.Services.AddSingleton<ScpiTemplateRenderer>();
builder.Services.AddSingleton<ScpiValueConverter>();
builder.Services.AddSingleton<ScpiQualityPolicy>();
builder.Services.AddSingleton<ScpiValueCache>();
builder.Services.AddSingleton<EndpointOperationScheduler>();
builder.Services.AddSingleton<ScpiTransportFactory>();
builder.Services.AddSingleton<ScpiErrorQueueService>();
builder.Services.AddSingleton<LogService>();
builder.Services.AddSingleton<ScpiClientService>();
builder.Services.AddSingleton<IScpiClientService>(provider => provider.GetRequiredService<ScpiClientService>());
builder.Services.AddSingleton<ScpiDirectWriteService>();
builder.Services.AddSingleton<BrowserSnapshotService>();
builder.Services.AddSingleton<CommandExecutionService>();

builder.Services.AddScoped<ScpiEndpointService>();
builder.Services.AddScoped<ScpiPointService>();
builder.Services.AddScoped<PathSuggestionService>();
builder.Services.AddScoped<RedisMappingValidationService>();
builder.Services.AddScoped<CommandDispatcherService>();
builder.Services.AddScoped<CsvConfigService>();
builder.Services.AddScoped<ZipConfigService>();
builder.Services.AddScoped<ScreenAlertService>();
builder.Services.AddScoped<PointOwnershipReleaseIntentService>();

builder.Services.AddHostedService<StartupGateService>();
builder.Services.AddHostedService<RedisScpiStatusService>();
builder.Services.AddHostedService(provider => provider.GetRequiredService<PointOwnershipReleaseCoordinator>());
builder.Services.AddHostedService<RedisPointOwnershipHostedService>();
builder.Services.AddHostedService(provider => provider.GetRequiredService<RedisReconciliationService>());
builder.Services.AddHostedService<ScpiPollingHostedService>();
builder.Services.AddHostedService<DeviceCommandSubscriptionService>();
builder.Services.AddHostedService<CommandExecutionCleanupHostedService>();

builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();

var app = builder.Build();
app.UsePtlkPublicPathBase();

await using (var scope = app.Services.CreateAsyncScope())
{
    var factory = scope.ServiceProvider.GetRequiredService<IDbContextFactory<AppDbContext>>();
    await using var db = await factory.CreateDbContextAsync();
    await db.Database.MigrateAsync();
}

if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error", createScopeForErrors: true);
    app.UseHsts();
}

app.UseStatusCodePagesWithReExecute("/not-found", createScopeForStatusCodePages: true);
app.UseAntiforgery();
app.MapStaticAssets();
app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode();

app.MapGet("/healthz", (RuntimeModeService runtime) => Results.Ok(runtime.Current));

app.Run();
