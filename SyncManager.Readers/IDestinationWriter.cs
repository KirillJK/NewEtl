using System;
using SyncManager.Etl.Common;

namespace SyncManager.Readers
{
    public interface IDestinationWriter:IDisposable
    {
        void Write(SourceContext context);
    }
}