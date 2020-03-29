using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SyncManager.Etl.Common;

namespace SyncManager.Readers
{

    public class CsvParser
    {
        public static Dictionary<string, object> Parse(string row, List<string> header)
        {
            Dictionary<string, object> result = new Dictionary<string, object>();
            var parts = GetParts(row);
            var length = parts.Count;
            if (header.Count < length) length = header.Count;
            for (int i = 0; i < length; i++)
            {
                if (parts[i] == "NULL")
                {
                    result[header[i]] = null;
                }
                else
                    result[header[i]] = parts[i];
            }

            return result;
        }

        public static List<string> GetParts(string row)
        {
            var parts = row.Split(',');
            return parts.ToList();
        }

        public static string ToCsv(Dictionary<string, object> item)
        {
            return item.Values.Select(a => a != null ? a.ToString() : "").Aggregate((a, b) => a + "," + b);
        }
    }
    public interface ISourceReader:IDisposable
    {
        void Read(SourceContext context);
    }

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
            var line = _sr.ReadLine();
            if (_headers == null)
            {
                _headers = CsvParser.GetParts(line);
            }
            else
            {
                context.UpdateSource(CsvParser.Parse(line, _headers));
            }
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
