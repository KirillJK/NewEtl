namespace SyncManager.Etl.Common
{
    public interface IExpressionEvaluator
    {
        void EnrichContext(string key, object value);
        object Evaluate(string expression);
        bool ConditionalEvaluate(string expression);
    }
}