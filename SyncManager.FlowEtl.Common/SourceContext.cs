using System;
using System.Collections.Generic;

namespace SyncManager.FlowEtl.Common
{
    public class SourceContext
    {

        public const string ErrorTypeCleanup = "Cleanup";
        public const string ErrorTypeFilter = "Filter";
        public const string ErrorTypeSchema = "Schema";
        public const string ErrorTypeTransformation = "Transformation";
        public Dictionary<string, object> Source { get; set; } = new Dictionary<string, object>();
        public Dictionary<string, object> Destination { get; set; } = new Dictionary<string, object>();
        public bool IsDeleted { get; set; }
        public HashSet<string> ListOfSchemaMissedColumns { get; set; } = new HashSet<string>();
        public Dictionary<string, List<ContextError>> SourceCellErrors { get; set; } = new Dictionary<string, List<ContextError>>();
        public List<ContextError> SourceRowErrors { get; set; } = new List<ContextError>();
        public void AddErrorForSourceColumn(string columnName, Exception error, string type)
        {
            if (!SourceCellErrors.ContainsKey(columnName))
            {
                SourceCellErrors[columnName] = new List<ContextError>();
            }
            SourceCellErrors[columnName].Add(new ContextError()
            {
                Exception = error, Type = type
            });
        }
        public void AddErrorForSourceColumn(string columnName, string message, string type)
        {
            if (!SourceCellErrors.ContainsKey(columnName))
            {
                SourceCellErrors[columnName] = new List<ContextError>();
            }
            SourceCellErrors[columnName].Add(new ContextError()
            {
                Message = message,
                Type = type
            });
        }


        public Dictionary<string, List<ContextError>> DestinationCellErrors { get; set; } = new Dictionary<string, List<ContextError>>();
        public List<ContextError> DestinationRowErrors { get; set; } = new List<ContextError>();
        public void AddErrorForDestinationColumn(string columnName, Exception error, string type)
        {
            if (!DestinationCellErrors.ContainsKey(columnName))
            {
                DestinationCellErrors[columnName] = new List<ContextError>();
            }
            DestinationCellErrors[columnName].Add(new ContextError()
            {
                Exception = error,
                Type = type
            });
        }
        public void AddErrorForDestinationColumn(string columnName, string message, string type)
        {
            if (!DestinationCellErrors.ContainsKey(columnName))
            {
                DestinationCellErrors[columnName] = new List<ContextError>();
            }
            DestinationCellErrors[columnName].Add(new ContextError()
            {
                Message = message,
                Type = type
            });
        }

        public void UpdateSource(Dictionary<string, object> data)
        {
            foreach (var o in data)
            {
                Source[o.Key] = o.Value;
            }
        }

        public void AddErrorForRow(Exception e)
        {
            SourceRowErrors.Add(new ContextError(){Exception = e});
        }
    }

    public class ContextError
    {
        public Exception Exception { get; set; }
        public string Message { get; set; }
        public string Type { get; set; }
    }
}
