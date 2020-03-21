using System.IO;
using NUnit.Framework.Constraints;
using SyncManager.FlowClockwork;

namespace Tests.SyncManager.FlowClockwork.FileMoverTest
{
    public class FileReader:IDataDriver<FileMoverData>
    {
        private string _filePath;

        public FileReader(string filePath)
        {
            _filePath = filePath;
        }

        public void Dispose()
        {
            _streamReader?.Dispose();
        }

        private StreamReader _streamReader;

        public void Process(IDataItemWrapper<FileMoverData> item)
        {
            if (_streamReader == null)
            {
                _streamReader = new StreamReader(new FileStream(_filePath,FileMode.Open, FileAccess.Read));
            }
            var line = _streamReader.ReadLine();
            item.Item = new FileMoverData();
            item.Item.StringNumber = line;
            item.Item.Number = int.Parse(line);
            if (_streamReader.EndOfStream)
            {
                item.Stop();
            }
        }

        public void Commit()
        {
            
        }
    }
}