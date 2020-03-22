using System;

namespace SyncManager.FlowClockwork
{
    public interface IDataDriver<TDataItem> :IDisposable
    {
        void Process(IDataItemWrapper<TDataItem> item);

        void Commit();
    }
}