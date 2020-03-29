using SyncManager.Etl.Common;
using SyncManager.Readers;

namespace SyncManager.FlowClockwork.ETL.Drivers
{
    public class SourceWriterDataDriver:IDataDriver<SourceContext>
    {
        private readonly IDestinationWriter _destinationWriter;

        public SourceWriterDataDriver(IDestinationWriter destinationWriter)
        {
            _destinationWriter = destinationWriter;
        }


        public void Dispose()
        {
            _destinationWriter?.Dispose();
        }

        public void Process(IDataItemWrapper<SourceContext> item)
        {
            _destinationWriter.Write(item.Item);
        }

        public void Commit()
        {
            _destinationWriter?.Dispose();
        }
    }
}