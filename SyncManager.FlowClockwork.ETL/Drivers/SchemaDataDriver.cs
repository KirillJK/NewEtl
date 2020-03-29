using SyncManager.Etl.Common;
using SyncManager.Etl.Schema;

namespace SyncManager.FlowClockwork.ETL.Drivers
{
    public class SchemaDataDriver : IDataDriver<SourceContext>
    {
        private readonly ISchemator _schemator;

        public SchemaDataDriver(ISchemator schemator)
        {
            _schemator = schemator;
        }

        public void Dispose()
        {
            
        }

        public void Process(IDataItemWrapper<SourceContext> item)
        {
            _schemator.Schema(item.Item);
        }

        public void Commit()
        {
        }
    }
}