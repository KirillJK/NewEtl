using System;
using System.Collections;
using System.Collections.Generic;

namespace SyncManager.FlowClockwork
{
    public interface IDataItemWrapper<TDataItem>
    {
        TDataItem Item { get; set; }
        bool IsStopped { get; }
        long? Number { get; set; }
        void Stop();
        void Exclude();
        bool GetAndResetExclude();
        void SetException(Exception exception, string nodeName);
        void SetCommitException(Exception exception, string nodeName);
        void Skip();

        bool IsSkipped { get; }
        void Reset();

        IReadOnlyDictionary<string, Exception> Exceptions { get; }
        IReadOnlyDictionary<string, Exception> CommitExceptions { get; }
    }
}