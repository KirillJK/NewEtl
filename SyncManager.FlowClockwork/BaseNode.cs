using System;

namespace SyncManager.FlowClockwork
{
    public class BaseNode<TDataItem> : INode<TDataItem>
    {
        private IDataDriver<TDataItem> _driver;
        private NodeState<TDataItem> _state = new NodeState<TDataItem>();
        private NodeDefinition _nodeDefinition;

        public BaseNode(IDataDriver<TDataItem> driver, NodeDefinition nodeDefinition)
        {
            _driver = driver;
            _nodeDefinition = nodeDefinition;
        }

        public bool Process(IDataItemWrapper<TDataItem> wrapper)
        {
            
            if (!_state.NeedToContinue(wrapper)) return false;
            Exception exception;
            if (!TrySafeExecute(() =>
            {
                _driver.Process(wrapper);
                if (_nodeDefinition.SingleCall)
                {
                    wrapper.Exclude();
                }
            }, out exception))
            {
                WrapException(wrapper, exception);
            }
            _state.IsExcluded = wrapper.GetAndResetExclude();
            if (wrapper.IsStopped)
            {
                _state.IsStopped = true;
                if (!TrySafeExecute(() => _driver.Commit(), out exception))
                {
                    WrapCommitException(wrapper, exception);
                }
            }
            _state.LastNumber = wrapper.Number;
            return true;
        }

        private void WrapException(IDataItemWrapper<TDataItem> wrapper, Exception exception)
        {
            wrapper.SetException(exception, _nodeDefinition.Name);
        }

        private void WrapCommitException(IDataItemWrapper<TDataItem> wrapper, Exception exception)
        {
            wrapper.SetCommitException(exception, _nodeDefinition.Name);
        }

        bool TrySafeExecute(Action action, out Exception exception)
        {
            try
            {
                action();
                exception = null;
                return true;
            }
            catch (Exception e)
            {
                exception = e;
                return false;
            }
        }

        public void Dispose()
        {
            _driver?.Dispose();
        }
    }
}