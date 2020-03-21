using System;

namespace SyncManager.FlowClockwork
{
    public class BaseNode<TDataItem> : INode<TDataItem>
    {
        private IDataDriver<TDataItem> _driver;
        private NodeState<TDataItem> _state = new NodeState<TDataItem>();
        private string _nodeName;

        public BaseNode(IDataDriver<TDataItem> driver, string nodeName)
        {
            _driver = driver;
            _nodeName = nodeName;
        }

        public bool Process(IDataItemWrapper<TDataItem> wrapper)
        {
            
            if (!_state.NeedToContinue(wrapper)) return false;
            Exception exception;
            if (!TrySafeExecute(() => _driver.Process(wrapper), out exception))
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
            wrapper.SetException(exception, _nodeName);
        }

        private void WrapCommitException(IDataItemWrapper<TDataItem> wrapper, Exception exception)
        {
            wrapper.SetCommitException(exception, _nodeName);
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