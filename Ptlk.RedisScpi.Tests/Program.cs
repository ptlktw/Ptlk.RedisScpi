using System.Collections.Concurrent;
using System.IO.Compression;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Ptlk.RedisScpi.Configuration;
using Ptlk.RedisScpi.Contracts.Redis;
using Ptlk.RedisScpi.Contracts.Scpi;
using Ptlk.RedisScpi.Data;
using Ptlk.RedisScpi.Models;
using Ptlk.RedisScpi.Services.Browser;
using Ptlk.RedisScpi.Services.Commands;
using Ptlk.RedisScpi.Services.ImportExport;
using Ptlk.RedisScpi.Services.Logs;
using Ptlk.RedisScpi.Services.Paths;
using Ptlk.RedisScpi.Services.Redis;
using Ptlk.RedisScpi.Services.Scpi;
using Ptlk.RedisScpi.Services.Startup;
using Ptlk.SCADA.Interop.PointValues;
using StackExchange.Redis;
using CommandResultEventContract = Ptlk.SCADA.Interop.Contracts.Redis.CommandResultEventContract;

var tests = new List<(string Name, Func<Task> Run)>
{
    ("Point operation UUID v5 golden vectors", RunSync(PointOperationGoldenVectors)),
    ("SCPI cache outage boundary and endpoint failure scope", RunSync(ScpiCacheOutageBoundaryAndFailureScope)),
    ("SourcePath normalization", RunSync(SourcePathNormalization)),
    ("Template renderer", RunSync(TemplateRenderer)),
    ("Number converter", RunSync(NumberConverter)),
    ("String converter", RunSync(StringConverter)),
    ("Enum converter", RunSync(EnumConverter)),
    ("Error Queue parser", RunSync(ErrorQueueParser)),
    ("Endpoint scheduler serializes one endpoint", EndpointSchedulerSerializesAsync),
    ("TCP transport terminator timeout and reconnect", TcpTransportAsync),
    ("Endpoint point CRUD and optimistic concurrency", ConfigurationCrudAsync),
    ("CSV round-trip phase order and rollback", CsvRoundTripAndRollbackAsync),
    ("ZIP manifest and non-seekable round-trip", ZipRoundTripAsync),
    ("CSV export/import and ZIP import size limits", ImportSizeLimitsAsync),
    ("Browser snapshot separates local and Redis state", BrowserSnapshotSeparationAsync),
    ("Runtime state exposes subsystem diagnostics", RunSync(RuntimeStateDiagnostics)),
    ("Command retention preserves accepted rows", CommandRetentionAsync),
    ("Canonical JSON ignores object property order", RunSync(CanonicalJsonNormalization)),
    ("Mapping activation distinguishes number and enum ranges", RunSync(MappingActivationDistinguishesNumberAndEnumRanges))
};

if (string.Equals(Environment.GetEnvironmentVariable("REDIS_SCPI_INTEGRATION"), "1", StringComparison.Ordinal))
{
    tests.Add(("Redis ownership writer and real PubSub integration", RedisOwnershipWriterIntegrationAsync));
    tests.Add(("Polling commits under endpoint lock and filters ConverterId", PollingIntegrationAsync));
    tests.Add(("Command idempotency readback mismatch and direct-write integration", CommandFlowIntegrationAsync));
}
if (Environment.GetEnvironmentVariable("PTLK_POINT_REDIS_INTEGRATION") == "1")
    tests.Add(("Atomic point retry stale conflict and large version", AtomicPointUpdateIntegrationAsync));

var passed = 0;
foreach (var test in tests)
{
    try
    {
        await test.Run();
        passed++;
        Console.WriteLine($"PASS {test.Name}");
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"FAIL {test.Name}: {ex}");
        Environment.ExitCode = 1;
        return;
    }
}

Console.WriteLine($"PASS {passed}/{tests.Count} RedisScpi checks.");

static Func<Task> RunSync(Action action) => () =>
{
    action();
    return Task.CompletedTask;
};

static void MappingActivationDistinguishesNumberAndEnumRanges()
{
    AssertEqual(
        PointMappingActivationStatus.RuntimeGuard,
        RedisMappingActivationService.EvaluateScpiType(
            ScpiDataTypes.Number,
            ScpiNumberTypes.Int,
            null,
            "double")!.Status);
    AssertEqual(
        PointMappingActivationStatus.Active,
        RedisMappingActivationService.EvaluateScpiType(
            ScpiDataTypes.Enum,
            null,
            ScpiEnumFormats.Code,
            "double")!.Status);
    var incompatible = RedisMappingActivationService.EvaluateScpiType(
        ScpiDataTypes.String,
        null,
        null,
        "int")!;
    AssertEqual(PointMappingActivationStatus.Inactive, incompatible.Status);
    AssertEqual(PointValueDiagnosticCodes.MappingTypeIncompatible, incompatible.DiagnosticCode);
}

static void PointOperationGoldenVectors()
{
    AssertEqual("609b97d27e2751289fb73b8faa381621", PointOperationId.Create("ptlk-point-operation-v1", "acquisition", "instance-01", "42", "point:site/device/temp"));
    AssertEqual("1905f762767351f699b94fb54f3eaef9", PointOperationId.Create("ptlk-point-operation-v1", "acquisition_failure", "instance-01", "43", "point:site/device/temp"));
    AssertEqual("b0659d8e251b5245a85a1bbbb63bc8a4", PointOperationId.Create("ptlk-point-operation-v1", "command_write", "redis-data-store", "cmd-0001", "point:site/device/temp"));
    AssertEqual("37789a49436555588359f9e5f27e3c04", PointOperationId.Create("ptlk-point-operation-v1", "reconnect_sync", "instance-01", "cycle-0001", "44", "point:site/device/temp"));
    AssertEqual("193cedcc55c351b5a7a5e560ef1e0f9b", PointOperationId.Create("ptlk-point-operation-v1", "supervisor_quality", "edge-instance-01", "1793059200000", "point:site/device/temp", "7"));
    AssertEqual("fab212a249225c83a79998e715dcafa7", PointOperationId.Create("ptlk-point-operation-v1", "acquisition", "instance-01", "9007199254740993", "point:site/device/temp"));
}

static async Task AtomicPointUpdateIntegrationAsync()
{
    await using var redis = new RedisConnectionFactory(
        Options.Create(new RedisOptions()),
        Microsoft.Extensions.Logging.Abstractions.NullLogger<RedisConnectionFactory>.Instance);
    var connection = await redis.GetConnectionAsync();
    var db = await redis.GetDatabaseAsync();
    var key = $"point:codex/phase3/scpi-{Guid.NewGuid():N}";
    var queue = await connection.GetSubscriber().SubscribeAsync(RedisChannel.Literal("evt:value-updated"));
    var eventCount = 0;
    var eventPayloads = new ConcurrentQueue<string>();
    queue.OnMessage(message =>
    {
        try
        {
            var payload = message.Message.ToString();
            using var document = JsonDocument.Parse(payload);
            if (document.RootElement.TryGetProperty("key", out var eventKey)
                && string.Equals(eventKey.GetString(), key, StringComparison.Ordinal))
            {
                eventPayloads.Enqueue(payload);
                Interlocked.Increment(ref eventCount);
            }
        }
        catch (JsonException)
        {
        }
    });
    try
    {
        const long initialVersion = 9007199254740993;
        await db.HashSetAsync(key,
        [
            new("quality", "unset"), new("type", "double"), new("timestamp", "0"),
            new("version", PointOperationId.Number(initialVersion)), new("source", "asset"),
            new("access", "readwrite"), new("unit", ""), new("owner", "phase3-scpi")
        ]);
        var service = new AtomicPointUpdateService(redis);
        var operation = PointOperationId.Create("ptlk-point-operation-v1", "acquisition", "instance-review", "1", key);
        var request = new AtomicPointUpdateRequest(
            key, "phase3-scpi", initialVersion, operation, "double", "12.5",
            "good", 1793059200000, "redis-scpi", "acquisition");
        AssertEqual("applied", (await service.ApplyAsync(request)).Status);
        AssertEqual("already_applied", (await service.ApplyAsync(request)).Status);
        var nextOperation = PointOperationId.Create("ptlk-point-operation-v1", "acquisition", "instance-review", "2", key);
        AssertEqual("applied", (await service.ApplyAsync(request with
        {
            ExpectedVersion = initialVersion + 1,
            OperationId = nextOperation,
            Value = "13.5",
            Timestamp = 1793059200001
        })).Status);
        AssertEqual("stale_conflict", (await service.ApplyAsync(request)).Status);
        await Task.Delay(100);
        AssertEqual(2, eventCount);
        AssertEqual(PointOperationId.Number(initialVersion + 2), (await db.HashGetAsync(key, "version")).ToString());
        AssertEqual(nextOperation, (await db.HashGetAsync(key, "last_update_operation_id")).ToString());

        const string configuredConverterId = "scpi-contract-review";
        await db.HashSetAsync(key, "owner", configuredConverterId);
        var configuredOptions = Options.Create(new RedisScpiOptions
        {
            ConverterId = configuredConverterId,
            SourceName = "redis-scpi-review"
        });
        var writer = new RedisPointStateService(
            redis,
            service,
            new PointUpdateIdentity(configuredOptions),
            configuredOptions,
            new RuntimeModeService());
        var command = await writer.UpdateDynamicFieldsAsync(
            new RedisMapping { SourcePath = "scpi:review/value", RedisKey = key },
            JsonSerializer.SerializeToElement(14.5),
            ScpiQuality.Good,
            "redis-scpi-review",
            updateReason: PointUpdateReasons.CommandWrite,
            commandId: "scpi-command-review");
        await Task.Delay(100);
        AssertEqual(initialVersion + 3, command.Version);
        AssertEqual(3, eventCount);
        AssertEqual(
            PointOperationId.CommandWrite(configuredConverterId, "scpi-command-review", key),
            (await db.HashGetAsync(key, "last_update_operation_id")).ToString());

        var beforeInvalidVersion = (await db.HashGetAsync(key, "version")).ToString();
        var rejected = false;
        try
        {
            await service.ApplyAsync(request with
            {
                ExpectedOwner = configuredConverterId,
                ExpectedVersion = initialVersion + 3,
                OperationId = Guid.NewGuid().ToString("N"),
                Value = "14.50",
                Timestamp = 1793059200002
            });
        }
        catch (ArgumentException)
        {
            rejected = true;
        }
        AssertTrue(rejected, "Non-canonical producer value must be rejected before Redis mutation.");
        await Task.Delay(50);
        AssertEqual(beforeInvalidVersion, (await db.HashGetAsync(key, "version")).ToString());
        AssertEqual(3, eventCount);

        var unavailable = await service.ApplyAsync(request with
        {
            ExpectedOwner = configuredConverterId,
            ExpectedVersion = initialVersion + 3,
            OperationId = PointOperationId.Create("ptlk-point-operation-v1", "acquisition_failure", "instance-review", "3", key),
            Value = null,
            Quality = ScpiQuality.Bad,
            Timestamp = 1793059200003,
            UpdateReason = PointUpdateReasons.AcquisitionFailure
        });
        AssertEqual("applied", unavailable.Status);
        await Task.Delay(100);
        AssertTrue((await db.HashGetAsync(key, "value")).IsNull, "Source-unavailable update must remove Hash value.");
        AssertEqual(4, eventCount);
        AssertTrue(
            eventPayloads.All(payload => Ptlk.SCADA.Interop.Redis.RedisContractValidator.ValidateValueUpdated(payload).IsValid),
            "Every SCPI value event must satisfy the shared Redis contract.");
        using var unavailableEvent = JsonDocument.Parse(eventPayloads.Last());
        AssertEqual(JsonValueKind.Null, unavailableEvent.RootElement.GetProperty("value").ValueKind);
    }
    finally
    {
        await queue.UnsubscribeAsync();
        await db.KeyDeleteAsync(key);
    }
}

static void ScpiCacheOutageBoundaryAndFailureScope()
{
    var cache = new ScpiValueCache();
    var value = JsonSerializer.SerializeToElement(9.5);
    var initial = cache.SetGood("scpi:e1/p1", "e1", "p1", value, "9.5", "poll", "9.5");
    var startup = cache.SnapshotForReconciliation();
    AssertEqual(1, startup.Values.Count);
    AssertTrue(cache.TryCompleteReconciliation(startup.ObservedSequence), "Startup reconciliation should complete.");
    AssertTrue(cache.BeginOutage(), "First outage observation should establish a boundary.");
    AssertFalse(cache.BeginOutage(), "Repeated outage observations must not move the boundary.");

    var failed = cache.SetBad(
        "scpi:e1/p1",
        "e1",
        "p1",
        "poll",
        null,
        ScpiErrorCodes.TransportError,
        "connection lost");
    AssertEqual<string?>(null, failed.RedisValue);
    AssertEqual(ScpiQuality.Bad, failed.Quality);
    AssertTrue(failed.Sequence > initial.Sequence, "Endpoint failure transition must advance the sequence.");
    var repeated = cache.SetBad(
        "scpi:e1/p1",
        "e1",
        "p1",
        "poll",
        null,
        ScpiErrorCodes.TransportError,
        "connection lost");
    AssertEqual(failed.Sequence, repeated.Sequence);

    var snapshot = cache.SnapshotForReconciliation();
    cache.SetBad("scpi:e1/p2", "e1", "p2", "poll", null, ScpiErrorCodes.ResponseInvalid, "invalid number");
    AssertFalse(cache.TryCompleteReconciliation(snapshot.ObservedSequence),
        "A racing acquisition result must keep reconciliation pending.");
    var retry = cache.SnapshotForReconciliation();
    AssertEqual(2, retry.Values.Count);
    AssertTrue(cache.TryCompleteReconciliation(retry.ObservedSequence), "Stable retry should complete.");
}

static void SourcePathNormalization()
{
    var paths = new ScpiSourcePathService();
    AssertEqual("scpi:power-01/output-voltage", paths.BuildPointSourcePath("power-01", "output-voltage"));
    AssertTrue(paths.TryParsePointSourcePath("scpi:power-01/output-voltage", out var endpoint, out var point), "Expected valid SourcePath.");
    AssertEqual("power-01", endpoint);
    AssertEqual("output-voltage", point);
    AssertFalse(paths.TryParsePointSourcePath("scpi:power-01/output/voltage", out _, out _), "Multiple separators must fail.");
    AssertFalse(paths.TryParsePointSourcePath("SCPI:power-01/output-voltage", out _, out _), "Canonical prefix casing must be enforced.");
    AssertFalse(paths.TryParsePointSourcePath("scpi:\u00C4/output-voltage", out _, out _), "SourcePath identity tokens must be ASCII.");
    AssertThrows<InvalidOperationException>(() => paths.BuildPointSourcePath("bad endpoint", "point"));
    AssertThrows<InvalidOperationException>(() => paths.BuildPointSourcePath("bad#endpoint", "point"));
    AssertThrows<InvalidOperationException>(() => paths.BuildPointSourcePath("s\u00FCpply", "point"));
    AssertThrows<InvalidOperationException>(() => paths.BuildPointSourcePath("endpoint", "bad:point"));
}

static void TemplateRenderer()
{
    var renderer = new ScpiTemplateRenderer();
    var point = CreatePoint();
    AssertEqual("VOLTage?", renderer.RenderRead(point));
    AssertEqual("VOLTage 5.25", renderer.RenderWrite(point, "5.25"));

    point.ReadTemplate = "{name} {value}?";
    AssertThrows<ScpiTemplateException>(() => renderer.RenderRead(point));
    point.ReadTemplate = "{unknown}?";
    AssertThrows<ScpiTemplateException>(() => renderer.RenderRead(point));
    point.ReadTemplate = "{name}?";
    point.WriteTemplate = "{name}";
    AssertThrows<ScpiTemplateException>(() => renderer.RenderWrite(point, "1"));
}

static void NumberConverter()
{
    var converter = new ScpiValueConverter();
    var point = CreatePoint();
    point.NumberType = ScpiNumberTypes.Int;
    var integer = converter.ParseResponse(point, " 42\r\n");
    AssertEqual(42L, integer.Value);
    AssertEqual(JsonValueKind.Number, integer.JsonValue.ValueKind);
    AssertEqual("42", integer.RedisValue);
    AssertThrows<ScpiParseException>(() => converter.ParseResponse(point, "1.5"));
    AssertThrows<ScpiValidationException>(() => converter.ConvertInput(point, JsonSerializer.SerializeToElement(1.5)));

    point.NumberType = ScpiNumberTypes.Double;
    var number = converter.ParseResponse(point, "1.25E+1\n");
    AssertEqual(12.5, number.Value);
    var input = converter.ConvertInput(point, JsonSerializer.SerializeToElement(3.5));
    AssertEqual("3.5", input.ScpiValue);
    AssertEqual(JsonValueKind.Number, input.JsonValue.ValueKind);
    AssertThrows<ScpiParseException>(() => converter.ParseResponse(point, "not-a-number"));
}

static void StringConverter()
{
    var converter = new ScpiValueConverter();
    var point = CreatePoint();
    point.DataType = ScpiDataTypes.String;
    point.NumberType = null;
    point.StringFormat = ScpiStringFormats.Raw;
    var empty = converter.ParseResponse(point, "\n");
    AssertEqual("", empty.Value);
    AssertEqual(JsonValueKind.String, empty.JsonValue.ValueKind);

    point.StringFormat = ScpiStringFormats.Quoted;
    var parsed = converter.ParseResponse(point, "\"A \"\"quoted\"\" value\"\r\n");
    AssertEqual("A \"quoted\" value", parsed.Value);
    var input = converter.ConvertInput(point, JsonSerializer.SerializeToElement("A \"quoted\" value"));
    AssertEqual("\"A \"\"quoted\"\" value\"", input.ScpiValue);
    AssertThrows<ScpiValidationException>(() => converter.ConvertInput(point, JsonSerializer.SerializeToElement(5)));
}

static void EnumConverter()
{
    var converter = new ScpiValueConverter();
    var point = CreatePoint();
    point.DataType = ScpiDataTypes.Enum;
    point.NumberType = null;
    point.StringFormat = null;
    point.EnumOptions =
    [
        new ScpiEnumOption { DisplayName = "Off", Value = "OFF", Code = 0 },
        new ScpiEnumOption { DisplayName = "On", Value = "ON", Code = 1 }
    ];

    point.EnumFormat = ScpiEnumFormats.Value;
    var value = converter.ParseResponse(point, "\"on\"\n");
    AssertEqual("ON", value.Value);
    AssertEqual("ON", value.ScpiValue);
    AssertEqual(JsonValueKind.String, value.JsonValue.ValueKind);
    AssertThrows<ScpiValidationException>(() => converter.ConvertInput(point, JsonSerializer.SerializeToElement("Missing")));

    point.EnumFormat = ScpiEnumFormats.Code;
    var code = converter.ParseResponse(point, "ON\n");
    AssertEqual(1, code.Value);
    AssertEqual("ON", code.ScpiValue);
    AssertEqual(JsonValueKind.Number, code.JsonValue.ValueKind);
    var write = converter.ConvertInput(point, JsonSerializer.SerializeToElement(0));
    AssertEqual("OFF", write.ScpiValue);
    AssertThrows<ScpiValidationException>(() => converter.ConvertInput(point, JsonSerializer.SerializeToElement(99)));
    AssertThrows<ScpiValidationException>(() => converter.ParseResponse(point, "AUTO"));
}

static void ErrorQueueParser()
{
    var service = new ScpiErrorQueueService();
    var success = service.Parse("0,\"No error\"\n");
    AssertTrue(success.Success, "Zero error code must succeed.");
    AssertEqual("No error", success.Message);
    var positiveZero = service.Parse("+0, No error");
    AssertTrue(positiveZero.Success, "+0 must succeed.");
    var failure = service.Parse("-222,\"Data out of range\"");
    AssertFalse(failure.Success, "Non-zero code must fail.");
    AssertThrows<ScpiInstrumentException>(() => ScpiErrorQueueService.ThrowIfError(failure));
    AssertThrows<ScpiParseException>(() => service.Parse("unexpected"));
}

static async Task EndpointSchedulerSerializesAsync()
{
    var scheduler = new EndpointOperationScheduler();
    var active = 0;
    var maximum = 0;
    var operations = Enumerable.Range(0, 12).Select(_ => scheduler.RunAsync(
        "endpoint-a",
        async token =>
        {
            var current = Interlocked.Increment(ref active);
            UpdateMaximum(ref maximum, current);
            await Task.Delay(10, token);
            Interlocked.Decrement(ref active);
            return true;
        }));
    await Task.WhenAll(operations);
    AssertEqual(1, maximum);
}

static async Task TcpTransportAsync()
{
    await using var server = new FakeScpiServer(async (command, _) => command switch
    {
        "MEAS?" => new FakeScpiResponse("12.5\n"),
        "PING?" => new FakeScpiResponse("PONG\n"),
        "LONG?" => new FakeScpiResponse(new string('X', 1100) + "\n"),
        "SLOW?" => await DelayedResponseAsync("TOO-LATE\n", 250),
        _ => new FakeScpiResponse(null)
    });
    await server.StartAsync();

    using var loggerFactory = LoggerFactory.Create(_ => { });
    var runtime = Options.Create(new ScpiRuntimeOptions { DefaultTimeoutMs = 500, MaxResponseBytes = 1024 });
    await using var transport = new TcpScpiTransport(runtime, loggerFactory.CreateLogger<TcpScpiTransport>());
    var endpoint = new ScpiEndpointConfig
    {
        EndpointId = "fake",
        TcpHost = IPAddress.Loopback.ToString(),
        TcpPort = server.Port,
        TimeoutMs = 500,
        CommandTerminator = "\\n",
        ResponseTerminator = "\\n"
    };

    var measured = await transport.QueryAsync(endpoint, "MEAS?");
    AssertEqual("12.5", measured);
    await transport.SendCommandAsync(endpoint, "VOLT 5");
    await server.WaitForCommandCountAsync(2);
    AssertEqual("MEAS?", server.Commands[0]);
    AssertEqual("VOLT 5", server.Commands[1]);

    await AssertThrowsAsync<ScpiParseException>(() => transport.QueryAsync(endpoint, "LONG?"));

    endpoint.TimeoutMs = 50;
    await AssertThrowsAsync<ScpiTimeoutException>(() => transport.QueryAsync(endpoint, "SLOW?"));
    endpoint.TimeoutMs = 500;
    var pong = await transport.QueryAsync(endpoint, "PING?");
    AssertEqual("PONG", pong);
    AssertTrue(server.ConnectionCount >= 2, "Timeout must reset and reconnect the persistent transport.");
}

static async Task ConfigurationCrudAsync()
{
    await using var database = await TestDatabase.CreateAsync();
    var paths = new ScpiSourcePathService();
    var endpoints = new ScpiEndpointService(
        database.Factory,
        paths,
        Options.Create(new RedisScpiOptions { ConverterId = "test-converter", SourceName = "test-source" }));
    var points = new ScpiPointService(database.Factory, paths, new ScpiTemplateRenderer());

    var endpoint = await endpoints.CreateOrUpdateAsync(new ScpiEndpointConfig
    {
        EndpointId = "supply-01",
        DisplayName = "Supply 01",
        Enabled = true,
        TcpHost = "127.0.0.1",
        TcpPort = 5025,
        TimeoutMs = 1000,
        PollingIntervalMs = 500,
        ErrorCheckMode = ScpiErrorCheckModes.AfterWrite,
        ErrorQueueQuery = "SYST:ERR?",
        CommandTerminator = "\\n",
        ResponseTerminator = "\\n"
    });
    AssertEqual("test-converter", endpoint.ConverterId);
    await AssertThrowsAsync<InvalidOperationException>(() => endpoints.CreateOrUpdateAsync(new ScpiEndpointConfig
    {
        EndpointId = "SUPPLY-01",
        DisplayName = "Case duplicate",
        Enabled = true,
        TcpHost = "127.0.0.1",
        TcpPort = 5026,
        TimeoutMs = 1000,
        PollingIntervalMs = 500,
        ErrorCheckMode = ScpiErrorCheckModes.AfterWrite,
        ErrorQueueQuery = "SYST:ERR?",
        CommandTerminator = "\n",
        ResponseTerminator = "\n"
    }));

    var point = await points.CreateOrUpdateAsync(new ScpiPointConfig
    {
        EndpointConfigId = endpoint.Id,
        PointId = "output-state",
        Name = "OUTP",
        DisplayName = "Output state",
        Enabled = false,
        PollingEnabled = false,
        Access = ScpiAccessModes.Readwrite,
        DataType = ScpiDataTypes.Enum,
        EnumFormat = ScpiEnumFormats.Code,
        ReadTemplate = "{name}?",
        WriteTemplate = "{name} {value}",
        EnumOptions =
        [
            new ScpiEnumOption { DisplayName = "Off", Value = "OFF", Code = 0, SortOrder = 0 },
            new ScpiEnumOption { DisplayName = "On", Value = "ON", Code = 1, SortOrder = 1 }
        ]
    });
    AssertEqual("scpi:supply-01/output-state", point.SourcePath);

    await using (var db = await database.Factory.CreateDbContextAsync())
    {
        db.RedisMappings.Add(new RedisMapping
        {
            SourcePath = point.SourcePath,
            RedisKey = "point:lab:supply:output-state"
        });
        await db.SaveChangesAsync();
    }

    point.Enabled = true;
    point.PollingEnabled = true;
    point = await points.CreateOrUpdateAsync(point);
    AssertTrue(point.Enabled && point.PollingEnabled, "Mapped point must be enabled and polling.");

    var firstEditor = await points.GetAsync(point.Id) ?? throw new InvalidOperationException("Point missing.");
    var staleEditor = await points.GetAsync(point.Id) ?? throw new InvalidOperationException("Point missing.");
    firstEditor.DisplayName = "Output state A";
    await points.CreateOrUpdateAsync(firstEditor);
    staleEditor.DisplayName = "Output state stale";
    await AssertThrowsAsync<ScpiConfigurationConcurrencyException>(() => points.CreateOrUpdateAsync(staleEditor));

    var currentEndpoint = await endpoints.GetAsync(endpoint.Id) ?? throw new InvalidOperationException("Endpoint missing.");
    currentEndpoint.EndpointId = "supply-renamed";
    await endpoints.CreateOrUpdateAsync(currentEndpoint);
    await using (var db = await database.Factory.CreateDbContextAsync())
    {
        var renamed = await db.ScpiPointConfigs.SingleAsync();
        var mapping = await db.RedisMappings.SingleAsync();
        AssertEqual("scpi:supply-renamed/output-state", renamed.SourcePath);
        AssertEqual(renamed.SourcePath, mapping.SourcePath);
    }

    using (var loggerFactory = LoggerFactory.Create(_ => { }))
    await using (var unusedRedis = new RedisConnectionFactory(
                     Options.Create(new RedisOptions { Host = "127.0.0.1", Port = 1, ConnectTimeoutMs = 100, SyncTimeoutMs = 100 }),
                     loggerFactory.CreateLogger<RedisConnectionFactory>()))
    {
        var mappings = new RedisMappingValidationService(database.Factory, unusedRedis);
        var currentMapping = (await mappings.ListAsync()).Single();
        await AssertThrowsAsync<InvalidOperationException>(() =>
            mappings.DeleteAsync(currentMapping.Id, currentMapping.ConcurrencyStamp));
    }

    var invalid = await points.GetAsync(point.Id) ?? throw new InvalidOperationException("Point missing.");
    invalid.EnumOptions[1].Code = invalid.EnumOptions[0].Code;
    await AssertThrowsAsync<InvalidOperationException>(() => points.CreateOrUpdateAsync(invalid));
}

static async Task CsvRoundTripAndRollbackAsync()
{
    await using var source = await TestDatabase.CreateAsync();
    await SeedCanonicalConfigurationAsync(source.Factory, "\r\n", "\n");
    var sourceService = new CsvConfigService(source.Factory, TestImportOptions());
    await using var exported = await sourceService.ExportAsync();
    var csvBytes = await ReadAllBytesAsync(exported);
    var csvText = Encoding.UTF8.GetString(csvBytes);
    AssertOrdered(csvText, "endpoint", "point", "enum_option", "mapping");
    AssertTrue(csvText.Contains("\"\\r\\n\"", StringComparison.Ordinal) || csvText.Contains("\"\r\n\"", StringComparison.Ordinal), "Terminator must round-trip through canonical CSV.");

    await using var outOfOrderTarget = await TestDatabase.CreateAsync();
    var outOfOrderService = new CsvConfigService(outOfOrderTarget.Factory, TestImportOptions());
    await using (var input = new MemoryStream(Encoding.UTF8.GetBytes(MoveFinalMappingRowFirst(csvText)), writable: false))
    {
        var outOfOrderResult = await outOfOrderService.ImportAsync(input);
        AssertTrue(outOfOrderResult.Success, string.Join(" | ", outOfOrderResult.Errors));
    }
    await using (var db = await outOfOrderTarget.Factory.CreateDbContextAsync())
    {
        AssertEqual(1, await db.ScpiEndpointConfigs.CountAsync());
        AssertEqual(1, await db.ScpiPointConfigs.CountAsync());
        AssertEqual(2, await db.ScpiEnumOptions.CountAsync());
        AssertEqual(1, await db.RedisMappings.CountAsync());
    }

    await using var target = await TestDatabase.CreateAsync();
    var targetService = new CsvConfigService(target.Factory, TestImportOptions());
    CsvImportResult imported;
    await using (var input = new MemoryStream(csvBytes, writable: false))
    {
        imported = await targetService.ImportAsync(input);
    }
    AssertTrue(imported.Success, string.Join(" | ", imported.Errors));
    int mappingId;
    DateTimeOffset mappingCreatedAt;
    await using (var db = await target.Factory.CreateDbContextAsync())
    {
        AssertEqual(1, await db.ScpiEndpointConfigs.CountAsync());
        AssertEqual(1, await db.ScpiPointConfigs.CountAsync());
        AssertEqual(2, await db.ScpiEnumOptions.CountAsync());
        AssertEqual(1, await db.RedisMappings.CountAsync());
        var endpoint = await db.ScpiEndpointConfigs.SingleAsync();
        AssertEqual("\r\n", endpoint.CommandTerminator);
        AssertEqual("\n", endpoint.ResponseTerminator);
        var mapping = await db.RedisMappings.SingleAsync();
        mappingId = mapping.Id;
        mappingCreatedAt = mapping.CreatedAt;
    }

    var updatedMappingText = csvText.Replace(
        "point:lab:supply:state",
        "point:lab:supply:state-updated",
        StringComparison.Ordinal);
    await using (var input = new MemoryStream(Encoding.UTF8.GetBytes(updatedMappingText), writable: false))
    {
        var updated = await targetService.ImportAsync(input);
        AssertTrue(updated.Success, string.Join(" | ", updated.Errors));
    }
    await using (var db = await target.Factory.CreateDbContextAsync())
    {
        var mapping = await db.RedisMappings.SingleAsync();
        AssertEqual(mappingId, mapping.Id);
        AssertEqual(mappingCreatedAt, mapping.CreatedAt);
        AssertEqual("point:lab:supply:state-updated", mapping.RedisKey);
    }

    var invalidText = csvText.Replace(
        "scpi:supply-01/output-state",
        "scpi:supply-01/missing-point",
        StringComparison.Ordinal);
    await using var rejected = await TestDatabase.CreateAsync();
    var rejectedService = new CsvConfigService(rejected.Factory, TestImportOptions());
    CsvImportResult rejectedResult;
    await using (var input = new MemoryStream(Encoding.UTF8.GetBytes(invalidText), writable: false))
    {
        rejectedResult = await rejectedService.ImportAsync(input);
    }
    AssertFalse(rejectedResult.Success, "Mapping-stage failure must roll back the import transaction.");
    AssertTrue(rejectedResult.Errors.Any(error => error.Contains("line", StringComparison.OrdinalIgnoreCase)), "Import error must retain source line context.");
    await using (var db = await rejected.Factory.CreateDbContextAsync())
    {
        AssertEqual(0, await db.ScpiEndpointConfigs.CountAsync());
        AssertEqual(0, await db.ScpiPointConfigs.CountAsync());
        AssertEqual(0, await db.ScpiEnumOptions.CountAsync());
        AssertEqual(0, await db.RedisMappings.CountAsync());
    }
}

static async Task ZipRoundTripAsync()
{
    await using var source = await TestDatabase.CreateAsync();
    await SeedCanonicalConfigurationAsync(source.Factory, "\n", "\n");
    var sourceCsv = new CsvConfigService(source.Factory, TestImportOptions());
    var sourceZip = new ZipConfigService(sourceCsv, TestImportOptions());
    await using var exported = await sourceZip.ExportAsync();
    var zipBytes = await ReadAllBytesAsync(exported);

    await using var target = await TestDatabase.CreateAsync();
    var targetCsv = new CsvConfigService(target.Factory, TestImportOptions());
    var targetZip = new ZipConfigService(targetCsv, TestImportOptions());
    CsvImportResult result;
    await using (var input = new AsyncOnlyReadStream(zipBytes))
    {
        result = await targetZip.ImportAsync(input);
    }
    AssertTrue(result.Success, string.Join(" | ", result.Errors));
    await using (var db = await target.Factory.CreateDbContextAsync())
    {
        AssertEqual(1, await db.ScpiEndpointConfigs.CountAsync());
        AssertEqual(1, await db.RedisMappings.CountAsync());
    }
}

static async Task ImportSizeLimitsAsync()
{
    await using var database = await TestDatabase.CreateAsync();
    await SeedCanonicalConfigurationAsync(database.Factory, "\n", "\n");

    long canonicalCsvLength;
    var normalCsv = new CsvConfigService(database.Factory, TestImportOptions());
    await using (var exported = await normalCsv.ExportAsync())
    {
        canonicalCsvLength = exported.Length;
    }
    var exportLimitOptions = Options.Create(new ImportExportOptions
    {
        SingleCsvLimitBytes = canonicalCsvLength - 1,
        ZipFileLimitBytes = 2 * 1024 * 1024,
        ZipExtractedLimitBytes = 2 * 1024 * 1024
    });
    var exportLimited = new CsvConfigService(database.Factory, exportLimitOptions);
    await AssertThrowsAsync<InvalidOperationException>(async () =>
    {
        await using var unexpected = await exportLimited.ExportAsync();
    });

    var csvLimitOptions = Options.Create(new ImportExportOptions
    {
        SingleCsvLimitBytes = 16,
        ZipFileLimitBytes = 1024,
        ZipExtractedLimitBytes = 1024
    });
    var csv = new CsvConfigService(database.Factory, csvLimitOptions);
    await using (var input = new AsyncOnlyReadStream(new byte[17]))
    {
        var result = await csv.ImportAsync(input);
        AssertFalse(result.Success, "CSV input exceeding SingleCsvLimitBytes must be rejected.");
        AssertTrue(result.Errors.Any(error => error.Contains("CSV size exceeds 16 bytes", StringComparison.Ordinal)), "CSV limit error must identify the configured limit.");
    }

    var smallZip = CreateZipBytes(
        (ZipConfigService.CsvEntryName, "x"),
        (ZipConfigService.ManifestEntryName, "{}"));
    var zipFileLimitOptions = Options.Create(new ImportExportOptions
    {
        SingleCsvLimitBytes = 1024,
        ZipFileLimitBytes = smallZip.LongLength - 1,
        ZipExtractedLimitBytes = 1024
    });
    var zipFileLimited = new ZipConfigService(
        new CsvConfigService(database.Factory, zipFileLimitOptions),
        zipFileLimitOptions);
    await using (var input = new AsyncOnlyReadStream(smallZip))
    {
        var result = await zipFileLimited.ImportAsync(input);
        AssertFalse(result.Success, "ZIP input exceeding ZipFileLimitBytes must be rejected.");
        AssertTrue(result.Errors.Any(error => error.Contains("ZIP size exceeds", StringComparison.Ordinal)), "ZIP file limit error must be reported.");
    }

    var expandedZip = CreateZipBytes(
        (ZipConfigService.CsvEntryName, "x"),
        (ZipConfigService.ManifestEntryName, new string('a', 512)));
    var extractedLimitOptions = Options.Create(new ImportExportOptions
    {
        SingleCsvLimitBytes = 128,
        ZipFileLimitBytes = expandedZip.LongLength + 1,
        ZipExtractedLimitBytes = 128
    });
    var extractedLimited = new ZipConfigService(
        new CsvConfigService(database.Factory, extractedLimitOptions),
        extractedLimitOptions);
    await using (var input = new AsyncOnlyReadStream(expandedZip))
    {
        var result = await extractedLimited.ImportAsync(input);
        AssertFalse(result.Success, "ZIP input exceeding ZipExtractedLimitBytes must be rejected.");
        AssertTrue(result.Errors.Any(error => error.Contains("ZIP extracted size exceeds 128 bytes", StringComparison.Ordinal)), "ZIP extracted limit error must identify the configured limit.");
    }
}

static async Task BrowserSnapshotSeparationAsync()
{
    await using var database = await TestDatabase.CreateAsync();
    await SeedCanonicalConfigurationAsync(database.Factory, "\n", "\n");
    using var loggerFactory = LoggerFactory.Create(_ => { });
    var runtime = new RuntimeModeService();
    var redisOptions = Options.Create(new RedisOptions());
    var redisScpiOptions = Options.Create(new RedisScpiOptions { ConverterId = "test", SourceName = "test" });
    await using var connection = new RedisConnectionFactory(redisOptions, loggerFactory.CreateLogger<RedisConnectionFactory>());
    var pubSub = new RedisPubSubService(connection, loggerFactory.CreateLogger<RedisPubSubService>());
    var state = new RedisPointStateService(connection, pubSub, redisScpiOptions, runtime);
    var ownership = new RedisPointOwnershipService(connection, redisScpiOptions, runtime, loggerFactory.CreateLogger<RedisPointOwnershipService>());
    var cache = new ScpiValueCache();
    cache.SetGood(
        "scpi:supply-01/output-state",
        "supply-01",
        "output-state",
        JsonSerializer.SerializeToElement(1),
        "1",
        "poll",
        "ON");
    using var browser = new BrowserSnapshotService(database.Factory, cache, state, ownership, runtime);
    var snapshot = await browser.GetSnapshotAsync();
    var row = snapshot.Points.Single();
    AssertTrue(row.LocalValue is not null, "Local SCPI sample must be visible.");
    AssertTrue(row.RedisState is null, "Redis state must remain a separate unavailable source while Redis is disconnected.");
    AssertEqual("point:lab:supply:state", row.RedisKey);
}

static void RuntimeStateDiagnostics()
{
    var runtime = new RuntimeModeService();
    runtime.SetRedisOutput(RuntimeSubsystemStatus.Normal, true, true, "ready");
    runtime.SetTransport(RuntimeSubsystemStatus.Degraded, "timeout");
    runtime.ReportRuntimeDiagnostic("transport", "supply-01", ScpiErrorCodes.Timeout, "timeout");
    var state = runtime.Current;
    AssertEqual(RuntimeMode.Degraded, state.Mode);
    AssertTrue(state.RedisConnected && state.AssetInitialized, "Redis readiness must be visible.");
    AssertEqual(1, state.RuntimeDiagnostics.Count);
    AssertEqual("supply-01", state.RuntimeDiagnostics[0].Scope);
    runtime.ReportRedisOutputDiagnostic("polling", "scpi:supply-01/value", "point:x", "write_failed", "temporary");
    AssertTrue(runtime.IsRedisOutputReady, "A point-level diagnostic must not close the global startup gate or prevent recovery.");
    runtime.SetRedisOutput(RuntimeSubsystemStatus.Degraded, true, true, "startup gate degraded");
    AssertFalse(runtime.IsRedisOutputReady, "A degraded startup gate must block Redis output.");
}

static async Task CommandRetentionAsync()
{
    await using var database = await TestDatabase.CreateAsync();
    await using (var db = await database.Factory.CreateDbContextAsync())
    {
        db.CommandExecutions.AddRange(
            new CommandExecution
            {
                CommandId = "accepted-old",
                RedisKey = "point:x",
                Status = "accepted",
                RequestedPayloadJson = "{}",
                CreatedAt = DateTimeOffset.UtcNow.AddDays(-30)
            },
            new CommandExecution
            {
                CommandId = "completed-old",
                RedisKey = "point:x",
                Status = "completed",
                RequestedPayloadJson = "{}",
                ResultPayloadJson = "{}",
                CompletedAt = DateTimeOffset.UtcNow.AddDays(-30)
            },
            new CommandExecution
            {
                CommandId = "failed-recent",
                RedisKey = "point:x",
                Status = "failed",
                RequestedPayloadJson = "{}",
                ResultPayloadJson = "{}",
                CompletedAt = DateTimeOffset.UtcNow
            });
        await db.SaveChangesAsync();
    }
    using var loggerFactory = LoggerFactory.Create(_ => { });
    var cleanup = new CommandExecutionCleanupHostedService(
        database.Factory,
        Options.Create(new RedisScpiRuntimeOptions { CommandExecutionRetentionDays = 7 }),
        loggerFactory.CreateLogger<CommandExecutionCleanupHostedService>());
    AssertEqual(1, await cleanup.CleanupOnceAsync());
    await using (var db = await database.Factory.CreateDbContextAsync())
    {
        var ids = await db.CommandExecutions.OrderBy(item => item.CommandId).Select(item => item.CommandId).ToListAsync();
        AssertTrue(ids.Contains("accepted-old"), "Accepted command must never be removed by retention cleanup.");
        AssertTrue(ids.Contains("failed-recent"), "Recent terminal command must be retained.");
        AssertFalse(ids.Contains("completed-old"), "Expired terminal command must be removed.");
    }
}

static void CanonicalJsonNormalization()
{
    var first = CanonicalJson.Normalize("{\"b\":2,\"a\":{\"y\":1,\"x\":0}}");
    var second = CanonicalJson.Normalize("{ \"a\" : { \"x\":0, \"y\":1 }, \"b\":2 }");
    AssertEqual(first, second);
}

static async Task RedisOwnershipWriterIntegrationAsync()
{
    using var loggerFactory = LoggerFactory.Create(_ => { });
    var redisOptions = IntegrationRedisOptions();
    var identity = Options.Create(new RedisScpiOptions { ConverterId = "redis-scpi-integration", SourceName = "redis-scpi-test" });
    var runtime = new RuntimeModeService();
    runtime.SetRedisOutput(RuntimeSubsystemStatus.Normal, true, true, "integration ready");
    await using var connection = new RedisConnectionFactory(redisOptions, loggerFactory.CreateLogger<RedisConnectionFactory>());
    var realPubSub = new RedisPubSubService(connection, loggerFactory.CreateLogger<RedisPubSubService>());
    var ownership = new RedisPointOwnershipService(connection, identity, runtime, loggerFactory.CreateLogger<RedisPointOwnershipService>());
    var writer = new RedisPointStateService(connection, realPubSub, identity, runtime);
    var database = await connection.GetDatabaseAsync();
    var numberKey = "point:redis-scpi-test:number";
    var stringKey = "point:redis-scpi-test:string";
    var missingUnitKey = "point:redis-scpi-test:missing-unit";
    var missingQualityKey = "point:redis-scpi-test:missing-quality";
    await database.KeyDeleteAsync([numberKey, stringKey, missingUnitKey, missingQualityKey]);

    try
    {
        await SeedRedisPointAsync(database, numberKey, "double", "readwrite", "V");
        var claim = await ownership.ClaimAsync("scpi:integration/number", numberKey);
        AssertTrue(claim.Acquired, $"Ownership claim failed: {claim.Status}");
        AssertEqual("redis-scpi-integration", (await database.HashGetAsync(numberKey, "owner")).ToString());
        AssertEqual("redis-scpi-test", (await database.HashGetAsync(numberKey, "owner_source")).ToString());

        var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        await realPubSub.SubscribeAsync(
            RedisContractNames.ValueUpdatedChannel,
            payload =>
            {
                using var candidate = JsonDocument.Parse(payload);
                if (candidate.RootElement.TryGetProperty("key", out var key)
                    && key.GetString() == numberKey)
                {
                    received.TrySetResult(payload);
                }
                return Task.CompletedTask;
            });
        var mapping = new RedisMapping { SourcePath = "scpi:integration/number", RedisKey = numberKey };
        var updated = await writer.UpdateDynamicFieldsAsync(
            mapping,
            JsonSerializer.SerializeToElement(5.25),
            ScpiQuality.Good,
            "redis-scpi-test");
        AssertEqual(1L, updated.Version);
        AssertEqual("5.25", (await database.HashGetAsync(numberKey, "value")).ToString());
        var eventJson = await received.Task.WaitAsync(TimeSpan.FromSeconds(3));
        using (var document = JsonDocument.Parse(eventJson))
        {
            AssertEqual(JsonValueKind.Number, document.RootElement.GetProperty("value").ValueKind);
            AssertEqual(1L, document.RootElement.GetProperty("version").GetInt64());
        }
        await realPubSub.UnsubscribeAsync(RedisContractNames.ValueUpdatedChannel);

        var cleared = await writer.UpdateDynamicFieldsAsync(mapping, null, ScpiQuality.Bad, "redis-scpi-test");
        AssertEqual(2L, cleared.Version);
        AssertFalse(await database.HashExistsAsync(numberKey, "value"), "Null must be represented by a missing Hash value field.");

        await SeedRedisPointAsync(database, stringKey, "string", "readwrite", "");
        AssertTrue((await ownership.ClaimAsync("scpi:integration/string", stringKey)).Acquired, "String point ownership failed.");
        await writer.UpdateDynamicFieldsAsync(
            new RedisMapping { SourcePath = "scpi:integration/string", RedisKey = stringKey },
            JsonSerializer.SerializeToElement(""),
            ScpiQuality.Good,
            "redis-scpi-test");
        AssertTrue(await database.HashExistsAsync(stringKey, "value"), "Empty string must preserve the Hash value field.");
        AssertEqual("", (await database.HashGetAsync(stringKey, "value")).ToString());

        await database.HashSetAsync(missingUnitKey,
        [
            new HashEntry("quality", "unset"), new HashEntry("type", "double"),
            new HashEntry("timestamp", "0"), new HashEntry("version", "0"),
            new HashEntry("source", "asset"), new HashEntry("access", "readwrite")
        ]);
        AssertTrue((await ownership.ClaimAsync("scpi:integration/missing-unit", missingUnitKey)).Acquired, "Missing-unit point ownership failed.");
        await AssertThrowsAsync<RedisPointStateException>(() => writer.UpdateDynamicFieldsAsync(
            new RedisMapping { SourcePath = "scpi:integration/missing-unit", RedisKey = missingUnitKey },
            JsonSerializer.SerializeToElement(1.0),
            ScpiQuality.Good,
            "redis-scpi-test"));
        AssertEqual("0", (await database.HashGetAsync(missingUnitKey, "version")).ToString());

        await database.HashSetAsync(missingQualityKey,
        [
            new HashEntry("type", "double"), new HashEntry("timestamp", "0"),
            new HashEntry("version", "0"), new HashEntry("source", "asset"),
            new HashEntry("access", "readwrite"), new HashEntry("unit", "V")
        ]);
        AssertTrue((await ownership.ClaimAsync("scpi:integration/missing-quality", missingQualityKey)).Acquired,
            "Missing-quality point ownership failed.");
        await AssertThrowsAsync<RedisPointStateException>(() => writer.UpdateDynamicFieldsAsync(
            new RedisMapping { SourcePath = "scpi:integration/missing-quality", RedisKey = missingQualityKey },
            JsonSerializer.SerializeToElement(1.0),
            ScpiQuality.Good,
            "redis-scpi-test"));
        AssertFalse(await database.HashExistsAsync(missingQualityKey, "quality"),
            "Writer must not repair a PointState with a missing required quality field.");
        AssertEqual("0", (await database.HashGetAsync(missingQualityKey, "version")).ToString());

        await database.HashSetAsync(numberKey, "owner", "other-converter");
        await AssertThrowsAsync<RedisPointStateException>(() => writer.UpdateDynamicFieldsAsync(
            mapping,
            JsonSerializer.SerializeToElement(9.0),
            ScpiQuality.Good,
            "redis-scpi-test"));
        AssertEqual("2", (await database.HashGetAsync(numberKey, "version")).ToString());
    }
    finally
    {
        await realPubSub.UnsubscribeAsync(RedisContractNames.ValueUpdatedChannel);
        await database.KeyDeleteAsync([numberKey, stringKey, missingUnitKey, missingQualityKey]);
    }
}

static async Task PollingIntegrationAsync()
{
    var localQueries = 0;
    var foreignQueries = 0;
    await using var server = new FakeScpiServer((command, _) =>
    {
        if (command == "LOCAL?")
        {
            Interlocked.Increment(ref localQueries);
            return Task.FromResult(new FakeScpiResponse("12.5\n"));
        }
        if (command == "FOREIGN?")
        {
            Interlocked.Increment(ref foreignQueries);
            return Task.FromResult(new FakeScpiResponse("99\n"));
        }
        if (command == "BAD?")
        {
            return Task.FromResult(new FakeScpiResponse("123\n"));
        }
        if (command == "SYST:ERR?")
        {
            return Task.FromResult(new FakeScpiResponse("-100,\"Simulated error\"\n"));
        }
        return Task.FromResult(new FakeScpiResponse(null));
    });
    await server.StartAsync();

    const string localSource = "scpi:poll-local/value";
    const string foreignSource = "scpi:poll-foreign/value";
    const string errorSource = "scpi:poll-error/value";
    const string localKey = "point:redis-scpi-test:poll-local";
    const string foreignKey = "point:redis-scpi-test:poll-foreign";
    const string errorKey = "point:redis-scpi-test:poll-error";
    await using var database = await FileTestDatabase.CreateAsync();
    await using (var db = await database.Factory.CreateDbContextAsync())
    {
        var localEndpoint = CreatePollingEndpoint("poll-local", "redis-scpi-integration", "LOCAL", localSource, server.Port);
        var foreignEndpoint = CreatePollingEndpoint("poll-foreign", "another-converter", "FOREIGN", foreignSource, server.Port);
        var errorEndpoint = CreatePollingEndpoint(
            "poll-error",
            "redis-scpi-integration",
            "BAD",
            errorSource,
            server.Port,
            ScpiErrorCheckModes.AfterCommand);
        db.ScpiEndpointConfigs.AddRange(localEndpoint, foreignEndpoint, errorEndpoint);
        db.RedisMappings.AddRange(
            new RedisMapping { SourcePath = localSource, RedisKey = localKey },
            new RedisMapping { SourcePath = foreignSource, RedisKey = foreignKey },
            new RedisMapping { SourcePath = errorSource, RedisKey = errorKey });
        await db.SaveChangesAsync();
    }

    using var loggerFactory = LoggerFactory.Create(_ => { });
    var identity = Options.Create(new RedisScpiOptions { ConverterId = "redis-scpi-integration", SourceName = "redis-scpi-test" });
    var runtime = new RuntimeModeService();
    runtime.SetRedisOutput(RuntimeSubsystemStatus.Normal, true, true, "integration ready");
    await using var redis = new RedisConnectionFactory(IntegrationRedisOptions(), loggerFactory.CreateLogger<RedisConnectionFactory>());
    var redisDb = await redis.GetDatabaseAsync();
    await redisDb.KeyDeleteAsync([localKey, foreignKey, errorKey]);
    await SeedRedisPointAsync(redisDb, localKey, "double", "readonly", "V");
    await SeedRedisPointAsync(redisDb, foreignKey, "double", "readonly", "V");
    await SeedRedisPointAsync(redisDb, errorKey, "double", "readonly", "V");

    var captured = new CapturingPubSubService();
    var ownership = new RedisPointOwnershipService(redis, identity, runtime, loggerFactory.CreateLogger<RedisPointOwnershipService>());
    var pointState = new RedisPointStateService(redis, captured, identity, runtime);
    var cache = new ScpiValueCache();
    var reconciliation = new RedisReconciliationService(
        database.Factory,
        cache,
        ownership,
        pointState,
        new PointUpdateIdentity(identity),
        runtime,
        identity,
        loggerFactory.CreateLogger<RedisReconciliationService>());
    var scheduler = new EndpointOperationScheduler();
    var log = new LogService(database.Factory);
    await using var transportFactory = new ScpiTransportFactory(
        Options.Create(new ScpiRuntimeOptions { DefaultTimeoutMs = 1000, MaxResponseBytes = 1024 }),
        loggerFactory);
    var client = new ScpiClientService(
        transportFactory,
        scheduler,
        new ScpiTemplateRenderer(),
        new ScpiValueConverter(),
        new ScpiErrorQueueService(),
        log,
        loggerFactory.CreateLogger<ScpiClientService>());
    var polling = new ScpiPollingHostedService(
        database.Factory,
        client,
        scheduler,
        cache,
        new ScpiQualityPolicy(),
        ownership,
        reconciliation,
        pointState,
        runtime,
        identity,
        loggerFactory.CreateLogger<ScpiPollingHostedService>());

    try
    {
        await reconciliation.StartAsync(CancellationToken.None);
        await polling.StartAsync(CancellationToken.None);
        var deadline = DateTimeOffset.UtcNow.AddSeconds(5);
        PointStateContract? state = null;
        PointStateContract? errorState = null;
        while (DateTimeOffset.UtcNow < deadline)
        {
            state = await pointState.ReadAsync(localKey);
            errorState = await pointState.ReadAsync(errorKey);
            if (state is { Version: > 0 } && errorState is { Version: > 0 }) break;
            await Task.Delay(50);
        }

        AssertTrue(state is { Version: > 0 }, "Local polling point was not committed to Redis.");
        AssertEqual(12.5d, state!.Value!.Value.GetDouble());
        AssertTrue(Volatile.Read(ref localQueries) > 0, "Assigned endpoint must be polled.");
        AssertEqual(0, Volatile.Read(ref foreignQueries));
        var foreignState = await pointState.ReadAsync(foreignKey) ?? throw new InvalidOperationException("Foreign point state missing.");
        AssertEqual(0L, foreignState.Version);
        AssertFalse(await redisDb.HashExistsAsync(foreignKey, "owner"), "Foreign ConverterId point must not be claimed.");
        AssertTrue(errorState is { Version: > 0 }, "Error Queue polling failure was not committed to Redis.");
        AssertEqual(ScpiQuality.Bad, errorState!.Quality);
        AssertFalse(errorState.HasValueField, "A non-zero Error Queue response must discard the point response value.");
        var failedSample = cache.Get(errorSource) ?? throw new InvalidOperationException("Error Queue cache sample missing.");
        AssertEqual(ScpiErrorCodes.InstrumentError, failedSample.ErrorCode);
        AssertTrue(failedSample.RawResponse?.Contains("point: 123", StringComparison.Ordinal) == true,
            "Polling diagnostics must preserve the point raw response.");
        AssertTrue(failedSample.RawResponse?.Contains("error_queue: -100", StringComparison.Ordinal) == true,
            "Polling diagnostics must preserve the Error Queue raw response.");

        var onlineSample = cache.Get(localSource) ?? throw new InvalidOperationException("Local cache sample missing.");
        runtime.SetRedisOutput(RuntimeSubsystemStatus.Degraded, false, false, "integration outage");
        var outageDeadline = DateTimeOffset.UtcNow.AddSeconds(3);
        while (reconciliation.IsReady && DateTimeOffset.UtcNow < outageDeadline)
        {
            await Task.Delay(25);
        }
        var versionAtOutage = (await pointState.ReadAsync(localKey))!.Version;
        while ((cache.Get(localSource)?.Sequence ?? 0) <= onlineSample.Sequence
               && DateTimeOffset.UtcNow < outageDeadline)
        {
            await Task.Delay(25);
        }
        AssertTrue((cache.Get(localSource)?.Sequence ?? 0) > onlineSample.Sequence,
            "SCPI polling must continue updating the local cache while Redis output is unavailable.");
        await Task.Delay(200);
        AssertEqual(versionAtOutage, (await pointState.ReadAsync(localKey))!.Version);
    }
    finally
    {
        await polling.StopAsync(CancellationToken.None);
        await reconciliation.StopAsync(CancellationToken.None);
        polling.Dispose();
        await redisDb.KeyDeleteAsync([localKey, foreignKey, errorKey]);
    }
}

static ScpiEndpointConfig CreatePollingEndpoint(
    string endpointId,
    string converterId,
    string pointName,
    string sourcePath,
    int port,
    string errorCheckMode = ScpiErrorCheckModes.None)
{
    var endpoint = new ScpiEndpointConfig
    {
        EndpointId = endpointId,
        DisplayName = endpointId,
        Enabled = true,
        Transport = "tcp",
        TcpHost = "127.0.0.1",
        TcpPort = port,
        TimeoutMs = 1000,
        PollingIntervalMs = 100,
        ConverterId = converterId,
        ErrorCheckMode = errorCheckMode,
        ErrorQueueQuery = "SYST:ERR?",
        CommandTerminator = "\n",
        ResponseTerminator = "\n"
    };
    endpoint.Points.Add(new ScpiPointConfig
    {
        EndpointConfig = endpoint,
        PointId = "value",
        SourcePath = sourcePath,
        Name = pointName,
        DisplayName = pointName,
        Enabled = true,
        Access = ScpiAccessModes.Readonly,
        DataType = ScpiDataTypes.Number,
        NumberType = ScpiNumberTypes.Double,
        ReadTemplate = "{name}?",
        PollingEnabled = true,
        PollingIntervalMs = 100,
        InitialRead = true
    });
    return endpoint;
}

static async Task CommandFlowIntegrationAsync()
{
    var stateLock = new object();
    var deviceValue = 0d;
    var writeCount = 0;
    var invalidNextRead = false;
    await using var server = new FakeScpiServer((command, _) =>
    {
        lock (stateLock)
        {
            if (command.StartsWith("VOLT ", StringComparison.Ordinal))
            {
                var requested = double.Parse(command[5..], System.Globalization.CultureInfo.InvariantCulture);
                writeCount++;
                if (requested.Equals(7d)) deviceValue = 6d;
                else
                {
                    deviceValue = requested;
                    if (requested.Equals(8d)) invalidNextRead = true;
                }
                return Task.FromResult(new FakeScpiResponse(null));
            }
            if (command == "VOLT?")
            {
                if (invalidNextRead)
                {
                    invalidNextRead = false;
                    return Task.FromResult(new FakeScpiResponse("not-a-number\n"));
                }
                return Task.FromResult(new FakeScpiResponse(deviceValue.ToString("R", System.Globalization.CultureInfo.InvariantCulture) + "\n"));
            }
            return Task.FromResult(new FakeScpiResponse(null));
        }
    });
    await server.StartAsync();

    await using var database = await FileTestDatabase.CreateAsync();
    await using (var db = await database.Factory.CreateDbContextAsync())
    {
        var endpoint = new ScpiEndpointConfig
        {
            EndpointId = "command-device",
            DisplayName = "Command device",
            Enabled = true,
            Transport = "tcp",
            TcpHost = "127.0.0.1",
            TcpPort = server.Port,
            TimeoutMs = 1000,
            PollingIntervalMs = 1000,
            ConverterId = "redis-scpi-integration",
            ErrorCheckMode = ScpiErrorCheckModes.None,
            ErrorQueueQuery = "SYST:ERR?",
            CommandTerminator = "\n",
            ResponseTerminator = "\n"
        };
        var point = new ScpiPointConfig
        {
            EndpointConfig = endpoint,
            PointId = "voltage",
            SourcePath = "scpi:command-device/voltage",
            Name = "VOLT",
            DisplayName = "Voltage",
            Enabled = true,
            Access = ScpiAccessModes.Readwrite,
            DataType = ScpiDataTypes.Number,
            NumberType = ScpiNumberTypes.Double,
            ReadTemplate = "{name}?",
            WriteTemplate = "{name} {value}",
            PollingEnabled = false
        };
        endpoint.Points.Add(point);
        db.ScpiEndpointConfigs.Add(endpoint);
        db.RedisMappings.Add(new RedisMapping { SourcePath = point.SourcePath, RedisKey = "point:redis-scpi-test:command" });
        await db.SaveChangesAsync();
    }

    using var loggerFactory = LoggerFactory.Create(_ => { });
    var redisOptions = IntegrationRedisOptions();
    var identity = Options.Create(new RedisScpiOptions { ConverterId = "redis-scpi-integration", SourceName = "redis-scpi-test" });
    var redisRuntime = Options.Create(new RedisScpiRuntimeOptions { CommandDefaultTimeoutMs = 1000, CommandExecutionRetentionDays = 7 });
    var scpiRuntime = Options.Create(new ScpiRuntimeOptions { DefaultTimeoutMs = 1000, MaxResponseBytes = 1024 });
    var runtime = new RuntimeModeService();
    runtime.SetRedisOutput(RuntimeSubsystemStatus.Normal, true, true, "integration ready");
    await using var redis = new RedisConnectionFactory(redisOptions, loggerFactory.CreateLogger<RedisConnectionFactory>());
    var redisDb = await redis.GetDatabaseAsync();
    var redisKey = "point:redis-scpi-test:command";
    var foreignConverterKey = "point:redis-scpi-test:foreign-command";
    await redisDb.KeyDeleteAsync([redisKey, foreignConverterKey]);
    await SeedRedisPointAsync(redisDb, redisKey, "double", "readwrite", "V");

    var captured = new CapturingPubSubService();
    var ownership = new RedisPointOwnershipService(redis, identity, runtime, loggerFactory.CreateLogger<RedisPointOwnershipService>());
    AssertTrue((await ownership.ClaimAsync("scpi:command-device/voltage", redisKey)).Acquired, "Command point ownership failed.");
    var pointState = new RedisPointStateService(redis, captured, identity, runtime);
    var cache = new ScpiValueCache();
    var reconciliation = new RedisReconciliationService(
        database.Factory,
        cache,
        ownership,
        pointState,
        new PointUpdateIdentity(identity),
        runtime,
        identity,
        loggerFactory.CreateLogger<RedisReconciliationService>());
    var log = new LogService(database.Factory);
    var scheduler = new EndpointOperationScheduler();
    await using var transportFactory = new ScpiTransportFactory(scpiRuntime, loggerFactory);
    var client = new ScpiClientService(
        transportFactory,
        scheduler,
        new ScpiTemplateRenderer(),
        new ScpiValueConverter(),
        new ScpiErrorQueueService(),
        log,
        loggerFactory.CreateLogger<ScpiClientService>());
    var execution = new CommandExecutionService(
        database.Factory,
        captured,
        ownership,
        reconciliation,
        pointState,
        client,
        scheduler,
        cache,
        log,
        runtime,
        identity,
        redisRuntime,
        loggerFactory.CreateLogger<CommandExecutionService>());
    var dispatcher = new CommandDispatcherService(
        execution,
        captured,
        log,
        identity,
        loggerFactory.CreateLogger<CommandDispatcherService>());

    try
    {
        var pendingCommand = DeviceWriteCommandContract.Create(
            redisKey,
            0.5d,
            "integration",
            "test",
            commandId: "command-reconciliation-pending",
            expectedVersion: 0);
        var beforePendingWrite = Volatile.Read(ref writeCount);
        var pendingResult = await dispatcher.DispatchRawAsync(
            JsonSerializer.Serialize(pendingCommand, RedisContractJson.WebOptions));
        AssertEqual("failed", pendingResult.Status);
        AssertEqual(beforePendingWrite, Volatile.Read(ref writeCount));

        await reconciliation.StartAsync(CancellationToken.None);
        var reconciliationDeadline = DateTimeOffset.UtcNow.AddSeconds(3);
        while (!reconciliation.IsReady && DateTimeOffset.UtcNow < reconciliationDeadline)
        {
            await Task.Delay(25);
        }
        AssertTrue(reconciliation.IsReady, "Command integration reconciliation did not become ready.");

        var invalidSchema = DeviceWriteCommandContract.Create(
            redisKey,
            1d,
            "integration",
            "test",
            commandId: "command-invalid-schema",
            expectedVersion: 0);
        invalidSchema.Schema = 2;
        var beforeInvalidSchemaWrite = Volatile.Read(ref writeCount);
        var invalidSchemaResult = await dispatcher.DispatchRawAsync(
            JsonSerializer.Serialize(invalidSchema, RedisContractJson.WebOptions));
        AssertEqual("failed", invalidSchemaResult.Status);
        AssertEqual(beforeInvalidSchemaWrite, Volatile.Read(ref writeCount));
        AssertTrue(
            captured.Objects.OfType<CommandResultEventContract>().Any(result =>
                result.CommandId == invalidSchema.CommandId
                && result.ErrorCode == ScpiErrorCodes.InvalidPayload),
            "A locally routed invalid command payload must persist and publish a terminal invalid_payload result.");

        var first = DeviceWriteCommandContract.Create(
            redisKey,
            5d,
            "integration",
            "test",
            commandId: "command-concurrent",
            expectedVersion: 0);
        var firstPayload = CanonicalJson.Normalize(JsonSerializer.Serialize(first, RedisContractJson.WebOptions));
        var outcomes = await Task.WhenAll(
            execution.AcceptAsync(first, firstPayload),
            execution.AcceptAsync(first, firstPayload));
        AssertEqual(1, Volatile.Read(ref writeCount));
        AssertTrue(outcomes.Any(outcome => outcome.Status == "completed"), "One concurrent command must complete.");
        await using (var db = await database.Factory.CreateDbContextAsync())
        {
            AssertEqual(1, await db.CommandExecutions.CountAsync(item => item.CommandId == first.CommandId));
        }
        var firstState = await pointState.ReadAsync(redisKey) ?? throw new InvalidOperationException("Command point state missing.");
        AssertEqual(1L, firstState.Version);
        AssertEqual(5d, firstState.Value!.Value.GetDouble());
        AssertTrue(captured.Objects.OfType<CommandResultEventContract>().Any(result => result.Success && result.Version == 1), "Successful terminal result must include actualValue/version.");

        var beforeReplayWrites = Volatile.Read(ref writeCount);
        var replay = await execution.AcceptAsync(first, firstPayload);
        AssertEqual("completed", replay.Status);
        AssertEqual(beforeReplayWrites, Volatile.Read(ref writeCount));
        AssertTrue(captured.RawPayloads.Count > 0, "Terminal duplicate must replay the stored raw result.");

        await using (var db = await database.Factory.CreateDbContextAsync())
        {
            var mapping = await db.RedisMappings.SingleAsync(item => item.RedisKey == redisKey);
            db.RedisMappings.Remove(mapping);
            await db.SaveChangesAsync();
        }
        var rawReplayCount = captured.RawPayloads.Count;
        var replayWithoutMapping = await execution.AcceptAsync(first, firstPayload);
        AssertEqual("completed", replayWithoutMapping.Status);
        AssertEqual(beforeReplayWrites, Volatile.Read(ref writeCount));
        AssertTrue(captured.RawPayloads.Count > rawReplayCount, "Terminal duplicate must replay even after its mapping is removed.");
        await using (var db = await database.Factory.CreateDbContextAsync())
        {
            db.RedisMappings.Add(new RedisMapping
            {
                SourcePath = "scpi:command-device/voltage",
                RedisKey = redisKey
            });
            await db.SaveChangesAsync();
        }

        var mismatchPayload = DeviceWriteCommandContract.Create(
            redisKey, 9d, "integration", "test", commandId: first.CommandId, expectedVersion: 1);
        await execution.AcceptAsync(
            mismatchPayload,
            CanonicalJson.Normalize(JsonSerializer.Serialize(mismatchPayload, RedisContractJson.WebOptions)));
        AssertEqual(beforeReplayWrites, Volatile.Read(ref writeCount));
        AssertTrue(runtime.Current.RuntimeDiagnostics.Any(item => item.Status == ScpiErrorCodes.CommandIdPayloadMismatch), "Payload mismatch diagnostic is required.");

        var directService = new ScpiDirectWriteService(
            database.Factory,
            ownership,
            client,
            scheduler,
            log,
            runtime,
            identity,
            loggerFactory.CreateLogger<ScpiDirectWriteService>());
        var eventsBeforeDirect = captured.TotalCount;
        var cacheBeforeDirect = cache.Get("scpi:command-device/voltage");
        var direct = await directService.WriteAsync(
            "scpi:command-device/voltage",
            JsonSerializer.SerializeToElement(4d),
            "integration");
        AssertTrue(direct.Success, direct.Message);
        AssertEqual(2, Volatile.Read(ref writeCount));
        AssertEqual(eventsBeforeDirect, captured.TotalCount);
        AssertEqual(cacheBeforeDirect, cache.Get("scpi:command-device/voltage"));
        var stateAfterDirect = await pointState.ReadAsync(redisKey) ?? throw new InvalidOperationException("State missing after direct write.");
        AssertEqual(1L, stateAfterDirect.Version);
        AssertEqual(5d, stateAfterDirect.Value!.Value.GetDouble());

        var blockerEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseBlocker = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var blocker = scheduler.RunAsync(
            "command-device",
            async _ =>
            {
                blockerEntered.TrySetResult();
                await releaseBlocker.Task;
            });
        await blockerEntered.Task;
        var beforeOwnershipRaceWrite = Volatile.Read(ref writeCount);
        var racedDirectTask = directService.WriteAsync(
            "scpi:command-device/voltage",
            JsonSerializer.SerializeToElement(13d),
            "integration");
        await Task.Delay(50);
        await redisDb.HashSetAsync(redisKey, "owner", "other-converter");
        releaseBlocker.TrySetResult();
        await blocker;
        var racedDirect = await racedDirectTask;
        AssertFalse(racedDirect.Success, "Direct write must revalidate ownership after acquiring the endpoint lock.");
        AssertEqual(ScpiErrorCodes.OwnershipNotAcquired, racedDirect.ErrorCode);
        AssertEqual(beforeOwnershipRaceWrite, Volatile.Read(ref writeCount));
        await redisDb.HashSetAsync(redisKey,
        [
            new HashEntry("owner", "redis-scpi-integration"),
            new HashEntry("owner_source", "redis-scpi-test")
        ]);

        var mismatch = DeviceWriteCommandContract.Create(
            redisKey, 7d, "integration", "test", commandId: "command-mismatch", expectedVersion: 1);
        var mismatchResult = await execution.AcceptAsync(
            mismatch,
            CanonicalJson.Normalize(JsonSerializer.Serialize(mismatch, RedisContractJson.WebOptions)));
        AssertEqual("failed", mismatchResult.Status);
        var mismatchState = await pointState.ReadAsync(redisKey) ?? throw new InvalidOperationException("Mismatch state missing.");
        AssertEqual(2L, mismatchState.Version);
        AssertEqual(6d, mismatchState.Value!.Value.GetDouble());
        AssertTrue(captured.Objects.OfType<CommandResultEventContract>().Any(result => result.CommandId == mismatch.CommandId && result.ErrorCode == ScpiErrorCodes.WriteVerificationFailed), "Mismatch must publish write_verification_failed.");

        var invalid = DeviceWriteCommandContract.Create(
            redisKey, 8d, "integration", "test", commandId: "command-invalid-readback", expectedVersion: 2);
        var invalidResult = await execution.AcceptAsync(
            invalid,
            CanonicalJson.Normalize(JsonSerializer.Serialize(invalid, RedisContractJson.WebOptions)));
        AssertEqual("failed", invalidResult.Status);
        var invalidState = await pointState.ReadAsync(redisKey) ?? throw new InvalidOperationException("Invalid-readback state missing.");
        AssertEqual(3L, invalidState.Version);
        AssertEqual(ScpiQuality.Bad, invalidState.Quality);
        AssertFalse(invalidState.HasValueField, "Failed readback must remove the Redis value field.");

        var beforeVersionRaceWrites = Volatile.Read(ref writeCount);
        var raceA = DeviceWriteCommandContract.Create(
            redisKey, 10d, "integration", "test", commandId: "command-version-race-a", expectedVersion: 3);
        var raceB = DeviceWriteCommandContract.Create(
            redisKey, 11d, "integration", "test", commandId: "command-version-race-b", expectedVersion: 3);
        var versionRace = await Task.WhenAll(
            execution.AcceptAsync(raceA, CanonicalJson.Normalize(JsonSerializer.Serialize(raceA, RedisContractJson.WebOptions))),
            execution.AcceptAsync(raceB, CanonicalJson.Normalize(JsonSerializer.Serialize(raceB, RedisContractJson.WebOptions))));
        AssertEqual(1, versionRace.Count(result => result.Status == "completed"));
        AssertEqual(1, versionRace.Count(result => result.Status == "failed"));
        AssertEqual(beforeVersionRaceWrites + 1, Volatile.Read(ref writeCount));
        var racedState = await pointState.ReadAsync(redisKey) ?? throw new InvalidOperationException("Version-race state missing.");
        AssertEqual(4L, racedState.Version);
        AssertTrue(
            captured.Objects.OfType<CommandResultEventContract>().Any(result =>
                (result.CommandId == raceA.CommandId || result.CommandId == raceB.CommandId)
                && result.ErrorCode == ScpiErrorCodes.ExpectedVersionMismatch),
            "The losing concurrent command must fail expectedVersion validation before writing the device.");

        await using (var db = await database.Factory.CreateDbContextAsync())
        {
            var foreignEndpoint = new ScpiEndpointConfig
            {
                EndpointId = "foreign-command-device",
                DisplayName = "Foreign command device",
                Enabled = true,
                Transport = "tcp",
                TcpHost = "127.0.0.1",
                TcpPort = server.Port,
                TimeoutMs = 1000,
                PollingIntervalMs = 1000,
                ConverterId = "another-converter",
                ErrorCheckMode = ScpiErrorCheckModes.None,
                ErrorQueueQuery = "SYST:ERR?",
                CommandTerminator = "\n",
                ResponseTerminator = "\n"
            };
            var foreignPoint = new ScpiPointConfig
            {
                EndpointConfig = foreignEndpoint,
                PointId = "voltage",
                SourcePath = "scpi:foreign-command-device/voltage",
                Name = "VOLT",
                DisplayName = "Voltage",
                Enabled = true,
                Access = ScpiAccessModes.Readwrite,
                DataType = ScpiDataTypes.Number,
                NumberType = ScpiNumberTypes.Double,
                ReadTemplate = "{name}?",
                WriteTemplate = "{name} {value}",
                PollingEnabled = false
            };
            foreignEndpoint.Points.Add(foreignPoint);
            db.ScpiEndpointConfigs.Add(foreignEndpoint);
            db.RedisMappings.Add(new RedisMapping { SourcePath = foreignPoint.SourcePath, RedisKey = foreignConverterKey });
            await db.SaveChangesAsync();
        }
        await SeedRedisPointAsync(redisDb, foreignConverterKey, "double", "readwrite", "V");
        var foreignCommand = DeviceWriteCommandContract.Create(
            foreignConverterKey, 12d, "integration", "test", commandId: "command-foreign-converter", expectedVersion: 0);
        var beforeForeignWrite = Volatile.Read(ref writeCount);
        var foreignResult = await execution.AcceptAsync(
            foreignCommand,
            CanonicalJson.Normalize(JsonSerializer.Serialize(foreignCommand, RedisContractJson.WebOptions)));
        AssertEqual("ignored", foreignResult.Status);
        AssertEqual(beforeForeignWrite, Volatile.Read(ref writeCount));
        var foreignDirect = await directService.WriteAsync(
            "scpi:foreign-command-device/voltage",
            JsonSerializer.SerializeToElement(12d),
            "integration");
        AssertFalse(foreignDirect.Success, "Direct write must reject an endpoint assigned to another ConverterId.");
        AssertEqual(beforeForeignWrite, Volatile.Read(ref writeCount));

        await redisDb.HashSetAsync(redisKey, "owner", "other-converter");
        var nonOwner = new RedisPointOwnershipService(redis, identity, runtime, loggerFactory.CreateLogger<RedisPointOwnershipService>());
        var ignoredExecution = new CommandExecutionService(
            database.Factory, captured, nonOwner, reconciliation, pointState, client, scheduler, cache, log, runtime,
            identity, redisRuntime, loggerFactory.CreateLogger<CommandExecutionService>());
        var ignored = DeviceWriteCommandContract.Create(
            redisKey, 3d, "integration", "test", commandId: "command-non-owner", expectedVersion: 4);
        var beforeIgnored = Volatile.Read(ref writeCount);
        var ignoredResult = await ignoredExecution.AcceptAsync(
            ignored,
            CanonicalJson.Normalize(JsonSerializer.Serialize(ignored, RedisContractJson.WebOptions)));
        AssertEqual("ignored", ignoredResult.Status);
        AssertEqual(beforeIgnored, Volatile.Read(ref writeCount));
        await using (var db = await database.Factory.CreateDbContextAsync())
        {
            AssertFalse(await db.CommandExecutions.AnyAsync(item => item.CommandId == ignored.CommandId), "Non-owner must not claim or publish a command result.");
        }
        var commandPayloads = captured.Messages
            .Where(item => item.Channel == RedisContractNames.CommandResultChannel)
            .Select(item => item.Json)
            .ToList();
        AssertTrue(commandPayloads.Count > 0, "Command integration must capture terminal command results.");
        AssertTrue(
            commandPayloads.All(payload => Ptlk.SCADA.Interop.Redis.RedisContractValidator.ValidateCommandResult(payload).IsValid),
            "Every SCPI command result must satisfy the shared typed contract.");
    }
    finally
    {
        await reconciliation.StopAsync(CancellationToken.None);
        await redisDb.KeyDeleteAsync([redisKey, foreignConverterKey]);
    }
}

static IOptions<RedisOptions> IntegrationRedisOptions() => Options.Create(new RedisOptions
{
    Host = Environment.GetEnvironmentVariable("REDIS_SCPI_REDIS_HOST") ?? "127.0.0.1",
    Port = int.TryParse(Environment.GetEnvironmentVariable("REDIS_SCPI_REDIS_PORT"), out var port) ? port : 6389,
    DatabaseIndex = int.TryParse(Environment.GetEnvironmentVariable("REDIS_SCPI_REDIS_DB"), out var database) ? database : 15,
    ConnectTimeoutMs = 3000,
    SyncTimeoutMs = 3000,
    AbortConnect = true,
    ConnectRetry = 1,
    KeepAliveSeconds = 30
});

static Task SeedRedisPointAsync(IDatabase database, string key, string type, string access, string unit) =>
    database.HashSetAsync(key,
    [
        new HashEntry("quality", "unset"), new HashEntry("type", type),
        new HashEntry("timestamp", "0"), new HashEntry("version", "0"),
        new HashEntry("source", "asset"), new HashEntry("access", access),
        new HashEntry("unit", unit)
    ]);

static async Task SeedCanonicalConfigurationAsync(
    IDbContextFactory<AppDbContext> factory,
    string commandTerminator,
    string responseTerminator)
{
    await using var db = await factory.CreateDbContextAsync();
    var endpoint = new ScpiEndpointConfig
    {
        EndpointId = "supply-01",
        DisplayName = "Supply, \"Primary\"",
        Enabled = true,
        Transport = "tcp",
        TcpHost = "127.0.0.1",
        TcpPort = 5025,
        TimeoutMs = 1000,
        PollingIntervalMs = 500,
        ConverterId = "test-converter",
        ErrorCheckMode = ScpiErrorCheckModes.AfterWrite,
        ErrorQueueQuery = "SYST:ERR?",
        CommandTerminator = commandTerminator,
        ResponseTerminator = responseTerminator
    };
    var point = new ScpiPointConfig
    {
        EndpointConfig = endpoint,
        PointId = "output-state",
        SourcePath = "scpi:supply-01/output-state",
        Name = "OUTP",
        DisplayName = "Output state",
        Enabled = true,
        Access = ScpiAccessModes.Readwrite,
        DataType = ScpiDataTypes.Enum,
        EnumFormat = ScpiEnumFormats.Code,
        ReadTemplate = "{name}?",
        WriteTemplate = "{name} {value}",
        PollingEnabled = true,
        InitialRead = true,
        EnumOptions =
        [
            new ScpiEnumOption { DisplayName = "Off", Value = "OFF", Code = 0, SortOrder = 0 },
            new ScpiEnumOption { DisplayName = "On", Value = "ON", Code = 1, SortOrder = 1 }
        ]
    };
    endpoint.Points.Add(point);
    db.ScpiEndpointConfigs.Add(endpoint);
    db.RedisMappings.Add(new RedisMapping
    {
        SourcePath = point.SourcePath,
        RedisKey = "point:lab:supply:state"
    });
    await db.SaveChangesAsync();
}

static IOptions<ImportExportOptions> TestImportOptions() => Options.Create(new ImportExportOptions
{
    SingleCsvLimitBytes = 1024 * 1024,
    ZipFileLimitBytes = 2 * 1024 * 1024,
    ZipExtractedLimitBytes = 2 * 1024 * 1024
});

static async Task<byte[]> ReadAllBytesAsync(Stream stream)
{
    if (stream.CanSeek) stream.Position = 0;
    using var buffer = new MemoryStream();
    await stream.CopyToAsync(buffer);
    return buffer.ToArray();
}

static string MoveFinalMappingRowFirst(string csv)
{
    var headerEnd = csv.IndexOf("\r\n", StringComparison.Ordinal);
    var mappingSeparator = csv.LastIndexOf("\r\nmapping,", StringComparison.Ordinal);
    if (headerEnd < 0 || mappingSeparator < headerEnd)
    {
        throw new InvalidOperationException("Canonical CSV does not contain a final mapping row.");
    }

    var mappingStart = mappingSeparator + "\r\n".Length;
    var mappingRow = csv[mappingStart..];
    var precedingRows = csv[(headerEnd + "\r\n".Length)..mappingStart];
    return string.Concat(csv[..(headerEnd + "\r\n".Length)], mappingRow, precedingRows);
}

static byte[] CreateZipBytes(params (string Name, string Content)[] entries)
{
    using var output = new MemoryStream();
    using (var archive = new ZipArchive(output, ZipArchiveMode.Create, leaveOpen: true))
    {
        foreach (var (name, content) in entries)
        {
            var entry = archive.CreateEntry(name, CompressionLevel.Optimal);
            using var entryStream = entry.Open();
            var bytes = Encoding.UTF8.GetBytes(content);
            entryStream.Write(bytes);
        }
    }

    return output.ToArray();
}

static void AssertOrdered(string text, params string[] kinds)
{
    var previous = -1;
    foreach (var kind in kinds)
    {
        var marker = $"\n{kind},";
        var index = text.IndexOf(marker, StringComparison.Ordinal);
        if (index < 0 || index <= previous)
        {
            throw new InvalidOperationException($"CSV kind '{kind}' is missing or out of canonical order.");
        }
        previous = index;
    }
}

static async Task<FakeScpiResponse> DelayedResponseAsync(string payload, int delayMs)
{
    await Task.Delay(delayMs);
    return new FakeScpiResponse(payload);
}

static ScpiPointConfig CreatePoint() => new()
{
    PointId = "voltage",
    SourcePath = "scpi:fake/voltage",
    Name = "VOLTage",
    Enabled = true,
    Access = ScpiAccessModes.Readwrite,
    DataType = ScpiDataTypes.Number,
    NumberType = ScpiNumberTypes.Double,
    ReadTemplate = "{name}?",
    WriteTemplate = "{name} {value}"
};

static void UpdateMaximum(ref int target, int value)
{
    int current;
    do
    {
        current = target;
        if (current >= value) return;
    }
    while (Interlocked.CompareExchange(ref target, value, current) != current);
}

static void AssertEqual<T>(T expected, T actual)
{
    if (!EqualityComparer<T>.Default.Equals(expected, actual))
    {
        throw new InvalidOperationException($"Expected '{expected}', actual '{actual}'.");
    }
}

static void AssertTrue(bool value, string message)
{
    if (!value) throw new InvalidOperationException(message);
}

static void AssertFalse(bool value, string message) => AssertTrue(!value, message);

static void AssertThrows<TException>(Action action) where TException : Exception
{
    try
    {
        action();
    }
    catch (TException)
    {
        return;
    }
    throw new InvalidOperationException($"Expected exception {typeof(TException).Name}.");
}

static async Task AssertThrowsAsync<TException>(Func<Task> action) where TException : Exception
{
    try
    {
        await action();
    }
    catch (TException)
    {
        return;
    }
    throw new InvalidOperationException($"Expected exception {typeof(TException).Name}.");
}

sealed record FakeScpiResponse(string? Payload, bool CloseConnection = false);

sealed class FakeScpiServer(Func<string, int, Task<FakeScpiResponse>> handler) : IAsyncDisposable
{
    private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
    private readonly CancellationTokenSource _stopping = new();
    private readonly ConcurrentQueue<string> _commands = new();
    private readonly List<Task> _clients = [];
    private Task? _acceptLoop;
    private int _connectionCount;

    public int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;
    public int ConnectionCount => Volatile.Read(ref _connectionCount);
    public IReadOnlyList<string> Commands => _commands.ToArray();

    public Task StartAsync()
    {
        _listener.Start();
        _acceptLoop = AcceptLoopAsync(_stopping.Token);
        return Task.CompletedTask;
    }

    public async Task WaitForCommandCountAsync(int count)
    {
        var deadline = DateTimeOffset.UtcNow.AddSeconds(3);
        while (_commands.Count < count && DateTimeOffset.UtcNow < deadline)
        {
            await Task.Delay(10);
        }
        if (_commands.Count < count)
        {
            throw new InvalidOperationException($"Expected {count} fake SCPI commands.");
        }
    }

    public async ValueTask DisposeAsync()
    {
        _stopping.Cancel();
        _listener.Stop();
        if (_acceptLoop is not null)
        {
            try { await _acceptLoop; } catch (OperationCanceledException) { } catch (SocketException) { }
        }
        Task[] clients;
        lock (_clients) clients = _clients.ToArray();
        try { await Task.WhenAll(clients); } catch { }
        _stopping.Dispose();
    }

    private async Task AcceptLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var client = await _listener.AcceptTcpClientAsync(cancellationToken);
            Interlocked.Increment(ref _connectionCount);
            var task = HandleClientAsync(client, cancellationToken);
            lock (_clients) _clients.Add(task);
        }
    }

    private async Task HandleClientAsync(TcpClient client, CancellationToken cancellationToken)
    {
        using (client)
        {
            var stream = client.GetStream();
            var line = new List<byte>();
            var buffer = new byte[1];
            var commandIndex = 0;
            while (!cancellationToken.IsCancellationRequested)
            {
                var read = await stream.ReadAsync(buffer, cancellationToken);
                if (read == 0) return;
                if (buffer[0] != (byte)'\n')
                {
                    if (buffer[0] != (byte)'\r') line.Add(buffer[0]);
                    continue;
                }

                var command = Encoding.UTF8.GetString(line.ToArray());
                line.Clear();
                _commands.Enqueue(command);
                var response = await handler(command, commandIndex++);
                if (response.Payload is not null)
                {
                    var bytes = Encoding.UTF8.GetBytes(response.Payload);
                    await stream.WriteAsync(bytes, cancellationToken);
                    await stream.FlushAsync(cancellationToken);
                }
                if (response.CloseConnection) return;
            }
        }
    }
}

sealed class TestDatabase : IAsyncDisposable
{
    private readonly SqliteConnection _connection;

    private TestDatabase(SqliteConnection connection, TestDbContextFactory factory)
    {
        _connection = connection;
        Factory = factory;
    }

    public TestDbContextFactory Factory { get; }

    public static async Task<TestDatabase> CreateAsync()
    {
        var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();
        var options = new DbContextOptionsBuilder<AppDbContext>()
            .UseSqlite(connection)
            .UseSnakeCaseNamingConvention()
            .Options;
        var factory = new TestDbContextFactory(options);
        await using var db = await factory.CreateDbContextAsync();
        await db.Database.EnsureCreatedAsync();
        return new TestDatabase(connection, factory);
    }

    public async ValueTask DisposeAsync() => await _connection.DisposeAsync();
}

sealed class FileTestDatabase : IAsyncDisposable
{
    private readonly string _path;

    private FileTestDatabase(string path, TestDbContextFactory factory)
    {
        _path = path;
        Factory = factory;
    }

    public TestDbContextFactory Factory { get; }

    public static async Task<FileTestDatabase> CreateAsync()
    {
        var path = Path.Combine(Path.GetTempPath(), $"ptlk-redis-scpi-test-{Guid.NewGuid():N}.db");
        var options = new DbContextOptionsBuilder<AppDbContext>()
            .UseSqlite($"Data Source={path};Cache=Shared")
            .UseSnakeCaseNamingConvention()
            .Options;
        var factory = new TestDbContextFactory(options);
        await using var db = await factory.CreateDbContextAsync();
        await db.Database.EnsureCreatedAsync();
        return new FileTestDatabase(path, factory);
    }

    public ValueTask DisposeAsync()
    {
        foreach (var suffix in new[] { "", "-wal", "-shm" })
        {
            try { File.Delete(_path + suffix); } catch { }
        }
        return ValueTask.CompletedTask;
    }
}

sealed class TestDbContextFactory(DbContextOptions<AppDbContext> options) : IDbContextFactory<AppDbContext>
{
    public AppDbContext CreateDbContext() => new(options);

    public Task<AppDbContext> CreateDbContextAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult(new AppDbContext(options));
}

sealed class AsyncOnlyReadStream(byte[] content) : Stream
{
    private readonly MemoryStream _inner = new(content, writable: false);

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => throw new NotSupportedException();
    public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
    public override void Flush() { }
    public override int Read(byte[] buffer, int offset, int count) => _inner.Read(buffer, offset, count);
    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
        _inner.ReadAsync(buffer, cancellationToken);
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        if (disposing) _inner.Dispose();
        base.Dispose(disposing);
    }
}

sealed record CapturedPublish(string Channel, object? Payload, string Json, bool IsRaw);

sealed class CapturingPubSubService : IRedisPubSubService
{
    private readonly ConcurrentQueue<CapturedPublish> _messages = new();

    public IReadOnlyList<object> Objects => _messages.Where(item => item.Payload is not null).Select(item => item.Payload!).ToList();
    public IReadOnlyList<string> RawPayloads => _messages.Where(item => item.IsRaw).Select(item => item.Json).ToList();
    public IReadOnlyList<CapturedPublish> Messages => _messages.ToList();
    public int TotalCount => _messages.Count;

    public Task PublishAsync(string channel, object payload, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _messages.Enqueue(new CapturedPublish(channel, payload, JsonSerializer.Serialize(payload, RedisContractJson.WebOptions), false));
        return Task.CompletedTask;
    }

    public Task PublishRawAsync(string channel, string payload, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _messages.Enqueue(new CapturedPublish(channel, null, payload, true));
        return Task.CompletedTask;
    }

    public Task<ChannelMessageQueue> SubscribeAsync(
        string channel,
        Func<string, Task> onMessage,
        CancellationToken cancellationToken = default) =>
        Task.FromException<ChannelMessageQueue>(new NotSupportedException("Capturing test PubSub does not subscribe."));

    public Task UnsubscribeAsync(string channel) => Task.CompletedTask;
}
