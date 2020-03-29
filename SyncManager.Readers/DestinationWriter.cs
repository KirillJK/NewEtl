using System.IO;
using SyncManager.Etl.Common;

namespace SyncManager.Readers
{
    public class DestinationWriter: IDestinationWriter
    {
        private StreamWriter _sw;
        private string _path;

        public DestinationWriter(string path)
        {
            _path = path;
        }

        private void TryToInit()
        {
            if (_sw == null)
                _sw = new StreamWriter(_path);
        }

        public void Write(SourceContext context)
        {
            TryToInit();
            var line = CsvParser.ToCsv(context.Destination);
            _sw.WriteLine(line);
        }

        public void Dispose()
        {
            _sw?.Dispose();
        }
    }
}