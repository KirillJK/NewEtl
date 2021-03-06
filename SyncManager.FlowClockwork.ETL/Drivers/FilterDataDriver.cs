﻿using SyncManager.FlowEtl.Common;
using SyncManager.FlowEtl.Filtering;

namespace SyncManager.FlowClockwork.ETL.Drivers
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