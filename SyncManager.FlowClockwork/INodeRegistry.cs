using System;

namespace SyncManager.FlowClockwork
{
    public interface INodeRegistry<TDataItem>:IDisposable
    {
        INode<TDataItem> GetNode(string name);
    }
}