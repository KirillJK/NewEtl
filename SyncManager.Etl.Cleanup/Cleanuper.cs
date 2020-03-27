using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using SyncManager.Etl.Common;

namespace SyncManager.Etl.Cleanup
{
    public class Cleanuper : ICleanuper
    {
        private readonly Dictionary<string, List<CleanupRule>> _cleanupRulesIndex =
            new Dictionary<string, List<CleanupRule>>();

        private readonly FrameState _frameState = new FrameState();
        private IExpressionEvaluator _expressionEvaluator;
        private bool _isInitialized;
        private Dictionary<string, object> _previousNonEmptyValue = new Dictionary<string, object>();
        private Dictionary<string, string> _matched = new Dictionary<string, string>(); 
        public Cleanuper(List<CleanupRule> rules, IExpressionEvaluator _expressionEvaluator)
        {
            foreach (var cleanupRule in rules)
            {
                if (!_cleanupRulesIndex.ContainsKey(cleanupRule.ColumnName))
                    _cleanupRulesIndex[cleanupRule.ColumnName] = new List<CleanupRule>();
                _cleanupRulesIndex[cleanupRule.ColumnName].Add(cleanupRule);
            }
        }

        public void Cleanup(EtlRow etlRow)
        {
            TryToInit();
            var result = new SingleCleanupRuleResult();
            foreach (var cleanupRuleList in _cleanupRulesIndex.Values)
            foreach (var cleanupRule in cleanupRuleList.Where(a => a.IsEnabled))
                Apply(cleanupRule, etlRow, result);
            Post(etlRow, result);
            etlRow.IsDeleted = result.IsDeleted;
        }

        private void Apply(CleanupRule rule, EtlRow etlRow, SingleCleanupRuleResult result)
        {
            var capturedValue = etlRow.Source[rule.ColumnName];
            var stringValue = capturedValue != null ? capturedValue.ToString() : "";
            if (CheckCondition(rule, etlRow, capturedValue, stringValue))
            {
                ApplyConditionalCleanup(rule, etlRow, stringValue, result);
            }
            ApplyNonConditionalCleanup(rule, etlRow, stringValue, result);

        }

        private void TryToInit()
        {
            if (_isInitialized) return;
            var values = _cleanupRulesIndex.SelectMany(a => a.Value).Where(a=>a.IsEnabled).ToList();
            if (values
                .Any(a=>(a.Action == CleanupAction.StartLoad|| a.Action == CleanupAction.StartLoadExclude || a.Action == CleanupAction.StopLoad)))
            {
                if (values.All(a => a.Action != CleanupAction.StartLoad && a.Action != CleanupAction.StartLoadExclude))
                {
                    _frameState.MakeOpenedByDefault();
                }
                _frameState.Capture();
                ;
            }
       


            _isInitialized = true;
        }

        private void Post(EtlRow etlRow, SingleCleanupRuleResult result)
        {
            if (_frameState.IsLocked()) result.IsDeleted = true;
        }

        private void ApplyNonConditionalCleanup(CleanupRule rule, EtlRow etlRow, string stringValue,
            SingleCleanupRuleResult result)
        {
            switch (rule.Action)
            {
                case CleanupAction.GetPrevious:
                {
                    if (string.IsNullOrEmpty(stringValue) && _previousNonEmptyValue.ContainsKey(rule.ColumnName))
                        etlRow.Source[rule.ColumnName] = _previousNonEmptyValue[rule.ColumnName];
                    else
                        _previousNonEmptyValue[rule.ColumnName] = stringValue;
                }
                    break;
            }

            if (_frameState.IsLocked())
            {
                result.IsDeleted = true;
            }
        }

        private void ApplyConditionalCleanup(CleanupRule rule, EtlRow etlRow, string stringValue,
            SingleCleanupRuleResult result)
        {
            switch (rule.Action)
            {
                case CleanupAction.Replace:
                {
                    etlRow.Source[rule.ColumnName] = rule.Value;
                }
                    break;
                case CleanupAction.ReplaceMatched:
                {
                    if (_matched.ContainsKey(rule.ColumnName) && !string.IsNullOrEmpty(_matched[rule.ColumnName]))
                        if (string.IsNullOrEmpty(stringValue))
                        {
                            etlRow.Source[rule.ColumnName] = rule.Value;
                        }
                        else
                        {
                            etlRow.Source[rule.ColumnName] = stringValue.Replace(_matched[rule.ColumnName], rule.Value);
                        }
                    
                }
                    break;
                case CleanupAction.Remove:
                {
                    result.IsDeleted = true;
                }
                    break;
                case CleanupAction.StartLoad:
                {
                    _frameState.StartLoad();
                }
                    break;
                case CleanupAction.StartLoadExclude:
                {
                    _frameState.StartLoadExclude();
                }
                    break;
                case CleanupAction.StopLoad:
                {
                    _frameState.StopLoad();
                }
                    break;
                }
        }


        private bool CheckCondition(CleanupRule rule, EtlRow etlRow, object capturedValue, string stringValue)
        {
            switch (rule.Condition)
            {
                case CleanupCondition.Empty:
                {
                    _matched[rule.ColumnName] = rule.Value;
                        return capturedValue == null || capturedValue as string == "";
                }
                case CleanupCondition.StartsWith:
                {
                    _matched[rule.ColumnName] = rule.ConditionArgument;
                    return stringValue.StartsWith(rule.ConditionArgument);
                }
                case CleanupCondition.EndsWith:
                {
                    _matched[rule.ColumnName] = rule.ConditionArgument;
                    return stringValue.EndsWith(rule.ConditionArgument);
                }
                case CleanupCondition.Equal:
                {
                    _matched[rule.ColumnName] = rule.ConditionArgument;
                        return stringValue == rule.ConditionArgument;
                }
                case CleanupCondition.Regex:
                {
                    var matched = Regex.Match(stringValue, rule.ConditionArgument);
                    if (matched.Length > 0)
                    {
                         _matched[rule.ColumnName] = matched.Value;
                         return true;
                    }

                    return false;
                }
                case CleanupCondition.Contains:
                {
                    _matched[rule.ColumnName] = rule.ConditionArgument;
                        return stringValue.Contains(rule.ConditionArgument);
                }
                default:
                    return false;
            }
        }
    }
}

public interface IExpressionEvaluator
{
    void EnrichContext(string key, object value);
    object Evaluate(string expression);
}