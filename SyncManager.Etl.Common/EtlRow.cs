using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SyncManager.Etl.Common
{
    public class EtlRow
    {
        public Dictionary<string, object> Source { get; set; } = new Dictionary<string, object>();
        public Dictionary<string, object> Destination { get; set; } = new Dictionary<string, object>();
        public bool IsDeleted { get; set; }
    }
}
