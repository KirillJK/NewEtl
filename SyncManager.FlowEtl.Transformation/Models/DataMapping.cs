using System.Collections.Generic;

namespace SyncManager.FlowEtl.Transformation.Models
{
    public class DataMapping
    {
        public string Function { get; set; }
        public List<DataMappingEntry> Entries { get; set; } = new List<DataMappingEntry>();
    }
}