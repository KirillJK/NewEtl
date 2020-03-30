using System.Collections.Generic;
using System.Linq;

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

        public NodeDefinitionProvider() : this(new Dictionary<string, NodeDefinition>(), null)
        {

        }

        public NodeDefinitionProvider GetSubTree(string rootName)
        {
            return new NodeDefinitionProvider(_definitions, rootName);
        }

        public void SetRoot(string rootName)
        {
            _rootName = rootName;
        }

        public void Register(string name, params string[] dependsOn)
        {
            _definitions[name] = new NodeDefinition()
            {
                Name = name,
                DependsOn = dependsOn.ToList()
            };
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