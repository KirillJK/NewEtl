namespace SyncManager.FlowEtl.Cleanup
{
    public class FrameState
    {
        private bool _isCaptured;
        private bool _isLocked = true;
        private bool? _oneTimeLocked = null;
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
            if (_oneTimeLocked.HasValue && _oneTimeLocked.Value)
            {
                _oneTimeLocked = false;
                return true;
            }
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
            if (!_oneTimeLocked.HasValue)
            _oneTimeLocked = true;
            _isLocked = false;
        }

    }
}