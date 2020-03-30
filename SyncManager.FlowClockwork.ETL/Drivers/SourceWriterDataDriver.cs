using SyncManager.Etl.Common;
using SyncManager.Readers;

namespace SyncManager.FlowClockwork.ETL.Drivers
{
    public class SourceWriterDataDriver:IDataDriver<SourceContext>
    {
        private readonly ISourceWriter _sourceWriter;

        public SourceWriterDataDriver(ISourceWriter sourceWriter)
        {
            _sourceWriter = sourceWriter;
        }


        public void Dispose()
        {
            _sourceWriter?.Dispose();
        }

        public void Process(IDataItemWrapper<SourceContext> item)
        {
            _sourceWriter.Write(item.Item);
        }

        public void Commit()
        {
            _sourceWriter?.Dispose();
        }
    }
}