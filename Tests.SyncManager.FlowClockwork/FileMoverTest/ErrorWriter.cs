using System.IO;
using System.Linq;
using SyncManager.FlowClockwork;

namespace Tests.SyncManager.FlowClockwork.FileMoverTest
{
    public class ErrorWriter : IDataDriver<FileMoverData>
    {

        private string _filePath;

        public ErrorWriter(string filePath)
        {
            _filePath = filePath;
        }

        public void Dispose()
        {
            _streamWriter?.Dispose();
        }

        private StreamWriter _streamWriter;

        public void Process(IDataItemWrapper<FileMoverData> item)
        {
            if (_streamWriter == null)
            {
                _streamWriter = new StreamWriter(new FileStream(_filePath, FileMode.Create, FileAccess.Write));
            }
            if (item.Exceptions.Any())
            {
                _streamWriter.WriteLine(item.Item.StringNumber);
                item.Skip();
            }
            
        }

        public void Commit()
        {

        }
    }
}