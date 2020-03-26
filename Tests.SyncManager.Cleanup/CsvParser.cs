using System.Collections.Generic;
using System.Linq;

namespace Tests.SyncManager.Cleanup
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
            return item.Values.Select(a=>a.ToString()).Aggregate((a, b) => a + "," + b);
        }
    }
}