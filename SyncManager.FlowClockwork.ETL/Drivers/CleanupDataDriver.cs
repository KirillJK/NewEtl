using SyncManager.FlowEtl.Cleanup;
using SyncManager.FlowEtl.Common;

namespace SyncManager.FlowClockwork.ETL.Drivers
{
    public class CleanupDataDriver : IDataDriver<SourceContext>
    {
        private readonly ICleanuper _cleanuper;

        public CleanupDataDriver(ICleanuper cleanuper)
        {
            _cleanuper = cleanuper;
        }

        public void Dispose()
        {
            
        }

        public void Process(IDataItemWrapper<SourceContext> item)
        {
            _cleanuper.Cleanup(item.Item);
        }

        public void Commit()
        {
            
        }
    }
}