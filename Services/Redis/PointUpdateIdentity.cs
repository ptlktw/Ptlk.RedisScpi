using Microsoft.Extensions.Options;
using Ptlk.RedisScpi.Configuration;
using Ptlk.SCADA.Interop.Runtime;

namespace Ptlk.RedisScpi.Services.Redis;

public sealed class PointUpdateIdentity(IOptions<RedisScpiOptions> options, EdgeRuntimeIdentity runtimeIdentity)
{
    private long sequence;

    public PointUpdateIdentity(IOptions<RedisScpiOptions> options)
        : this(options, new EdgeRuntimeIdentity())
    {
    }

    public string InstanceId => runtimeIdentity.InstanceId;

    public string Create(string reason, string key, string? commandId = null)
    {
        if (reason == PointUpdateReasons.CommandWrite)
        {
            if (string.IsNullOrWhiteSpace(commandId))
                throw new ArgumentException("commandId is required for command_write.", nameof(commandId));
            return PointOperationId.CommandWrite(options.Value.ConverterId, commandId, key);
        }

        var next = Interlocked.Increment(ref sequence);
        return reason == PointUpdateReasons.AcquisitionFailure
            ? PointOperationId.AcquisitionFailure(InstanceId, next, key)
            : PointOperationId.Acquisition(InstanceId, next, key);
    }
}
