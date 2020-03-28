using System;
using System.Linq.Expressions;
using System.Net.Configuration;
using System.Runtime.Serialization.Formatters;
using SyncManager.Etl.Common;

namespace SyncManager.Etl.Cleanup
{
    public interface ICleanuper
    {
        void Cleanup(SourceContext sourceContext);
    }
}