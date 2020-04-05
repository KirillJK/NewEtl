namespace SyncManager.FlowEtl.Cleanup
{
    public enum CleanupAction
    {
        Replace = 1,
        ReplaceMatched = 2,
        StopLoad = 3,
        StartLoad = 4,
        StartLoadExclude = 5,
        //  StopLoadExclude = 6,
        GetPrevious = 7,
        Remove = 8,
        Validate = 9
    }
}