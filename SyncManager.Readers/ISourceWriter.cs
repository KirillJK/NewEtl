using System;
using SyncManager.FlowEtl.Common;

namespace SyncManager.Readers
{
    public interface ISourceWriter:IDisposable
    {
        void Write(SourceContext context);
    }
}