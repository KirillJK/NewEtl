using System;
using System.Collections.Generic;

namespace SyncManager.FlowClockwork
{
    public class DataItemWrapper<TDataItem> : IDataItemWrapper<TDataItem>
    {
        public TDataItem Item { get; set; }
        public bool IsStopped { get; private set; }
        public bool IsFinished { get; private set; }
        private bool IsExcluded { get; set; }
        public bool IsSkipped { get; private set; }
        public long? Number { get; set; } = null;
        
        public void FinishNode()
        {
            IsFinished = true;
        }
        public void Stop()
        {
            IsStopped = true;
        }

        public void Exclude()
        {
            IsExcluded = true;
        }

        public bool GetAndResetExclude()
        {
            var result = IsExcluded;
            IsExcluded = false;
            return result;
        }

        private Dictionary<string, Exception> _exceptions = new Dictionary<string, Exception>();
        private Dictionary<string, Exception> _commitExceptions = new Dictionary<string, Exception>();

        public void SetException(Exception exception, string nodeName)
        {
            _exceptions[nodeName] = exception;
        }


        public void SetCommitException(Exception exception, string nodeName)
        {
            _commitExceptions[nodeName] = exception;
        }


        public IReadOnlyDictionary<string, Exception> Exceptions => _exceptions;
        public IReadOnlyDictionary<string, Exception> CommitExceptions => _commitExceptions;

        public void Skip()
        {
            IsSkipped = true;
        }

        public void Reset()
        {
            IsExcluded = false;
            IsSkipped = false;
        }
    }
}