﻿using System;
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
        public Dictionary<string, List<ContextError>> CellErrors { get; set; } = new Dictionary<string, List<ContextError>>();
        public List<ContextError> RowErrors { get; set; } = new List<ContextError>();
        public void AddErrorForColumn(string columnName, Exception error, string type)
        {
            if (!CellErrors.ContainsKey(columnName))
            {
                CellErrors[columnName] = new List<ContextError>();
            }
            CellErrors[columnName].Add(new ContextError()
            {
                Exception = error, Type = type
            });
        }

        public void AddErrorForRow(Exception e)
        {
            RowErrors.Add(new ContextError(){Exception = e});
        }
    }

    public class ContextError
    {
        public Exception Exception { get; set; }
        public string Message { get; set; }
        public string Type { get; set; }
    }
}