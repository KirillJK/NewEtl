using System.Collections.Generic;

namespace SyncManager.FlowClockwork
{
    public class NodeDefinition
    {
        public string Name { get; set; }
        public List<string> DependsOn { get; set; } = new List<string>();
        public bool SingleCall { get; set; }
        public bool RunOver { get; set; }
    }
}