using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SyncManager.Etl.Common
{
    public class SourceContext
    {

        public const string ErrorTypeCleanup = "Cleanup";
        public Dictionary<string, object> Source { get; set; } = new Dictionary<string, object>();
        public bool IsDeleted { get; set; }
        public Dictionary<string, List<ContextError>> Errors { get; set; } = new Dictionary<string, List<ContextError>>();

        public void AddErrorForColumn(string columnName, Exception error, string type)
        {
            if (!Errors.ContainsKey(columnName))
            {
                Errors[columnName] = new List<ContextError>();
            }
            Errors[columnName].Add(new ContextError()
            {
                Exception = error, Type = type
            });
        }
    }

    public class ContextError
    {
        public Exception Exception { get; set; }
        public string Message { get; set; }
        public string Type { get; set; }
    }
}
