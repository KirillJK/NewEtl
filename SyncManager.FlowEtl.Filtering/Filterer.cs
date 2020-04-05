using System;
using System.Collections.Generic;
using System.Linq;
using SyncManager.FlowEtl.Common;

namespace SyncManager.FlowEtl.Filtering
{
    public interface IFilterer
    {
        void Filter(SourceContext sourceContext);
    }

    public class Filterer : IFilterer
    {
        private Dictionary<string, FilterRule> _filters;
        private IExpressionEvaluator _expressionEvaluator;
        public Filterer(List<FilterRule> filterRules, IExpressionEvaluator expressionEvaluator)
        {
            _expressionEvaluator = expressionEvaluator;
            _filters = filterRules.Where(a=>a.IsEnabled).ToDictionary(a => a.Name, a => a);
        }

        public void Filter(SourceContext sourceContext)
        {
           _expressionEvaluator.EnrichContext("source",sourceContext.Source);
            foreach (var filter in _filters)
            {
                try
                {
                    var result =_expressionEvaluator.ConditionalEvaluate(filter.Value.Expression);
                    sourceContext.IsDeleted = !result;
                }
                catch (Exception e)
                {
                    sourceContext.AddErrorForRow(e);
                }
            }
        }
    }
}