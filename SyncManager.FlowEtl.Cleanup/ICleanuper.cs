using SyncManager.FlowEtl.Common;

namespace SyncManager.FlowEtl.Cleanup
{
    public interface ICleanuper
    {
        void Cleanup(SourceContext sourceContext);
    }
}