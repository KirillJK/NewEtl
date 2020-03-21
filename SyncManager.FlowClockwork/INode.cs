using System;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace SyncManager.FlowClockwork
{
    public interface INode<TDataItem>:IDisposable
    {
        bool Process(IDataItemWrapper<TDataItem> wrapper);
    }
}
