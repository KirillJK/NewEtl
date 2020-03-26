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

        private bool _isInitialized;
        private string _previousNonEmptyValue;

        public Cleanuper(List<CleanupRule> rules)
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
                ApplyConditionalCleanup(rule, etlRow, stringValue, result);
        }

        private void TryToInit()
        {
            if (_isInitialized) return;
            if (_cleanupRulesIndex.SelectMany(a => a.Value)
                .Any(a => a.IsEnabled && a.Action == CleanupAction.StartLoad))
            {
                _frameState.Capture();
                ;
            }

            _isInitialized = true;
        }

        private void Post(EtlRow etlRow, SingleCleanupRuleResult result)
        {
            if (_frameState.Ignore()) result.IsDeleted = true;
        }

        private void ApplyNonConditionalCleanup(CleanupRule rule, EtlRow etlRow, string stringValue,
            SingleCleanupRuleResult result)
        {
            switch (rule.Action)
            {
                case CleanupAction.GetPrevious:
                {
                    if (string.IsNullOrEmpty(stringValue))
                        etlRow.Source[rule.ColumnName] = _previousNonEmptyValue;
                    else
                        _previousNonEmptyValue = stringValue;
                }
                    break;
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
                    etlRow.Source[rule.ColumnName] = stringValue.Replace(rule.ConditionArgument, rule.Value);
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
                    return capturedValue == null || capturedValue as string == "";
                }
                case CleanupCondition.StartsWith:
                {
                    return stringValue.StartsWith(rule.ConditionArgument);
                }
                case CleanupCondition.EndsWith:
                {
                    return stringValue.EndsWith(rule.ConditionArgument);
                }
                case CleanupCondition.Equal:
                {
                    return stringValue == rule.ConditionArgument;
                }
                case CleanupCondition.Regex:
                {
                    return Regex.IsMatch(stringValue, rule.ConditionArgument);
                }
                case CleanupCondition.Contains:
                {
                    return stringValue.Contains(rule.ConditionArgument);
                }
                default:
                    return false;
            }
        }
    }
}