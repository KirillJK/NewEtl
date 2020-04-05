namespace SyncManager.FlowEtl.Cleanup
{
    public enum CleanupCondition
    {
        Empty = 1,
        Equal = 2,
        StartsWith = 3,
        EndsWith = 4,
        Contains = 5,
        Regex = 6,
        DateTime = 7,
        Expression = 8
    }
}