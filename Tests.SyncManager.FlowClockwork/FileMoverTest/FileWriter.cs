using System.IO;
using SyncManager.FlowClockwork;

namespace Tests.SyncManager.FlowClockwork.FileMoverTest
{
    public class FileWriter:IDataDriver<FileMoverData>
    {

        private string _filePath;

        public FileWriter(string filePath)
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
            _streamWriter.WriteLine(item.Item.Number);
        }

        public void Commit()
        {

        }
    }
}