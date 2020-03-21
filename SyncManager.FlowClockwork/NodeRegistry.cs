using System.Collections.Generic;

namespace SyncManager.FlowClockwork
{
    public class NodeRegistry<TDataItem>: INodeRegistry<TDataItem>
    {
        private Dictionary<string, INode<TDataItem>> _dictionary;

        internal NodeRegistry(Dictionary<string, INode<TDataItem>> dictionary)
        {
            _dictionary = dictionary;
        }

        public NodeRegistry():this(new Dictionary<string, INode<TDataItem>>())
        {
        }

        public void Register(string name, INode<TDataItem> node)
        {
            _dictionary[name] = node;
        }

        public INode<TDataItem> GetNode(string name)
        {
            return _dictionary[name];
        }

        public void Dispose()
        {
            foreach (var node in _dictionary)
            {
                node.Value.Dispose();
            }
        }
    }
}