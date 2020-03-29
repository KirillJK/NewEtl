using SyncManager.Etl.Common;
using SyncManager.Etl.Filtering;

namespace SyncManager.FlowClockwork.ETL
{
    public class FilterDataDriver : IDataDriver<SourceContext>
    {
        private readonly IFilterer _filterer;

        public FilterDataDriver(IFilterer filterer)
        {
            _filterer = filterer;
        }

        public void Dispose()
        {
            
        }

        public void Process(IDataItemWrapper<SourceContext> item)
        {
            _filterer.Filter(item.Item);
        }

        public void Commit()
        {
            
        }
    }
}