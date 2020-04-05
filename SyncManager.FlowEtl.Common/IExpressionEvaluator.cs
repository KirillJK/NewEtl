using System.Collections.Generic;

namespace SyncManager.FlowEtl.Common
{
    public interface IExpressionEvaluator
    {
        void EnrichContext(string key, object value);
        object Evaluate(string expression);
        bool ConditionalEvaluate(string expression);
    }

    public interface IDataMappingEvaluator
    {
        void Evaluate(Dictionary<string, object> value);
    }
}