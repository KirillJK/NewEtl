using System;
using SyncManager.FlowClockwork;

namespace Tests.SyncManager.FlowClockwork
{
    public class TestActionProcess : IDataDriver<string>
    {
        private readonly Action _commitAction;
        private readonly Action<IDataItemWrapper<string>> _processAction;

        public TestActionProcess(Action<IDataItemWrapper<string>> processAction, Action commitAction)
        {
            _processAction = processAction;
            _commitAction = commitAction;
        }

        public TestActionProcess(Action<IDataItemWrapper<string>> processAction)
        {
            _processAction = processAction;
            _commitAction = () => { };
        }

        public bool IsDisposed { get; set; }

        public void Process(IDataItemWrapper<string> item)
        {
            _processAction(item);
        }

        public void Commit()
        {
            _commitAction();
        }

        public void Dispose()
        {
            IsDisposed = true;
        }
    }
}