using SyncManager.FlowEtl.Common;
using SyncManager.Readers;

namespace SyncManager.FlowClockwork.ETL.Drivers
{
    public class SourceReaderDataDriver:IDataDriver<SourceContext>
    {
        private readonly ISourceReader _sourceReader;

        public SourceReaderDataDriver(ISourceReader sourceReader)
        {
            _sourceReader = sourceReader;
        }

        public void Dispose()
        {
            _sourceReader?.Dispose();
        }

        public void Process(IDataItemWrapper<SourceContext> item)
        {
            _sourceReader.Read(item.Item);
            if (_sourceReader.IsEnd)
            {
                item.Stop();
            }
        }

        public void Commit()
        {
            _sourceReader.Dispose();
        }
    }
}
