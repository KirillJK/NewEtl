using SyncManager.Etl.Common;

namespace SyncManager.FlowClockwork.ETL
{
    public class BeginEtlDataDriver:IDataDriver<SourceContext>
    {
        public void Dispose()
        {
            
        }

        public void Process(IDataItemWrapper<SourceContext> item)
        {
            item.Item = new SourceContext();
        }

        public void Commit() 
        {
            
        }
    }
}