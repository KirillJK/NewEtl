namespace SyncManager.FlowClockwork
{
    public class NodeState<TDataItem>
    {
        public bool IsStopped { get; set; }
        public long? LastNumber { get; set; }
        public bool IsExcluded { get; set; }

        public bool NeedToContinue(IDataItemWrapper<TDataItem> wrapper)
        {
            return (!IsStopped && LastNumber!= wrapper.Number && !IsExcluded && !wrapper.IsSkipped);
        }
    }
}