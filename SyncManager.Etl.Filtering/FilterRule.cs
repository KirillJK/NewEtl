using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace SyncManager.Etl.Filtering
{
    public class FilterRule
    {
        public string Name { get; set; }
        public string Expression { get; set; }
        public bool IsEnabled { get; set; }
    }
}

