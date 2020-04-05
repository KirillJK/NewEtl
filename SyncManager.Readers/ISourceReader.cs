using System;
using System.Text;
using System.Threading.Tasks;
using SyncManager.FlowEtl.Common;

namespace SyncManager.Readers
{
    public interface ISourceReader:IDisposable
    {
        void Read(SourceContext context);
        bool IsEnd { get; }
    }
}
