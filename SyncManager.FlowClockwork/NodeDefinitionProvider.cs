using System.Collections.Generic;

namespace SyncManager.FlowClockwork
{
    public class NodeDefinitionProvider
    {
        private Dictionary<string, NodeDefinition> _definitions;
        private string _rootName;

        internal NodeDefinitionProvider(Dictionary<string, NodeDefinition> definitions, string rootName)
        {
            _definitions = definitions;
            _rootName = rootName;
        }

        public NodeDefinitionProvider(string rootName):this(new Dictionary<string, NodeDefinition>(), rootName)
        {
        }

        public void Register(string name, NodeDefinition definition)
        {
            _definitions[name] = definition;
        }

        public NodeDefinition GetRoot()
        {
            return _definitions[_rootName];
        }

        public NodeDefinition GetByName(string name)
        {
            return _definitions[name];
        }
    }
}