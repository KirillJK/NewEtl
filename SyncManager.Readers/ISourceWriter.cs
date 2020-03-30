using System;
using SyncManager.Etl.Common;

namespace SyncManager.Readers
{
    public interface ISourceWriter:IDisposable
    {
        void Write(SourceContext context);
    }
}