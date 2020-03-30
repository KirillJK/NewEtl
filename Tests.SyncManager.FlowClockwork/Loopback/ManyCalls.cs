using System.Collections.Generic;
using SyncManager.FlowClockwork;

namespace Tests.SyncManager.FlowClockwork.Loopback
{
    public class ManyCallsDataDriver:IDataDriver<string>
    {
        private List<string> _messages;
        private int _counter = 0;

        public ManyCallsDataDriver(List<string> messages)
        {
            _messages = messages;
        }

        public void Dispose()
        {

        }

        public void Process(IDataItemWrapper<string> item)
        {
            _messages.Add($"Data{_counter++}");
            if (_counter == 3)
            {
                item.Stop();
            }
        }

        public void Commit()
        {
        }
    }

    public class FirstCallDataDriver : IDataDriver<string>
    {
        private List<string> _messages;

        public FirstCallDataDriver(List<string> messages)
        {
            _messages = messages;
        }

        public void Dispose()
        {
        }

        public void Process(IDataItemWrapper<string> item)
        {
            _messages.Add("FirstCall");
        }

        public void Commit()
        {

        }
    }

    public class LastCallDataDriver : IDataDriver<string>
    {
        private List<string> _messages;

        public LastCallDataDriver(List<string> messages)
        {
            _messages = messages;
        }

        public void Dispose()
        {

        }

        public void Process(IDataItemWrapper<string> item)
        {
            _messages.Add("LastCall");
        }

        public void Commit()
        {

        }
    }
}