using System.Collections.Generic;
using System.IO;
using SyncManager.FlowEtl.Common;

namespace SyncManager.Readers
{
    public class CsvReader: ISourceReader
    {
        private StreamReader _sr = null;
        private string _filePath;

        public CsvReader(string filePath)
        {
            _filePath = filePath;
        }

        private List<string> _headers = null;
        public void Read(SourceContext context)
        {
            TryToInit();
         
            if (_headers == null)
            {
                ReadHeaders();
            }

                var line = _sr.ReadLine();
                context.UpdateSource(CsvParser.Parse(line, _headers));
            
        }

        private void ReadHeaders()
        {
            var line = _sr.ReadLine();
            _headers = CsvParser.GetParts(line);
        }


        public bool IsEnd
        {
            get
            {
                if (_sr == null) return false;
                if (_sr.EndOfStream) return true;
                return false;
            }
        }

        private void TryToInit()
        {
            if (_sr == null)
            {
                _sr = new StreamReader(_filePath);
            }
        }

        public void Dispose()
        {
            _sr?.Dispose();
        }
    }
}