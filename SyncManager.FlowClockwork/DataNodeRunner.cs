using System;

namespace SyncManager.FlowClockwork
{
    public class DataNodeRunner<TDataItem>:IDisposable
    {
        private NodeDefinitionProvider _provider;
        private INodeRegistry<TDataItem> _registry;
        private IDataItemWrapper<TDataItem> _current;

        public IDataItemWrapper<TDataItem> Current => _current;

        public DataNodeRunner(NodeDefinitionProvider provider, INodeRegistry<TDataItem> registry)
        {
            _provider = provider;
            _registry = registry;
        }

        public bool Step(IDataItemWrapper<TDataItem> dataItemWrapper)
        {
            SetNumber(dataItemWrapper);
            var root = _provider.GetRoot();
            return DoStep(dataItemWrapper, root);
           
        }

        public void Run()
        {
            _current = new DataItemWrapper<TDataItem>();
            int actionCounter = 0;
            while (Step(_current))
            {
                actionCounter++;
                _current.Reset();
            }
        }

        private static void SetNumber(IDataItemWrapper<TDataItem> dataItemWrapper)
        {
            if (dataItemWrapper.Number.HasValue)
            {
                dataItemWrapper.Number++;
            }
            else
            {
                dataItemWrapper.Number = 0;
            }
        }

        private bool DoStep(IDataItemWrapper<TDataItem> dataItemWrapper, NodeDefinition nodeDefinition)
        {
            var node = _registry.GetNode(nodeDefinition.Name);
            bool isOver = false;
            foreach (var dependsOn in nodeDefinition.DependsOn)
            {
                var next = _provider.GetByName(dependsOn);
                isOver |= DoStep(dataItemWrapper, next);
            }
            isOver |= node.Process(dataItemWrapper);
           
            return isOver;
        }

        public void Dispose()
        {
            _registry.Dispose();
        }
    }
}