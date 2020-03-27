namespace SyncManager.Etl.Cleanup
{
    public class FrameState
    {
        private bool _isCaptured;
        private bool _isLocked = true;
        public void StartLoad()
        {
            _isLocked = false;
        }

        public void Capture()
        {
            _isCaptured = true;
        }

        public bool IsLocked()
        {
            if (!_isCaptured) return false;
            return _isLocked;
        }

        public void StopLoad()
        {
            _isLocked = true;
        }

        public void MakeOpenedByDefault()
        {
            _isLocked = false;
        }

        public void StartLoadExclude()
        {

        }

        public bool Ignore()
        {
            return false;
        }
    }
}