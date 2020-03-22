using System.Collections.Generic;
using System.Linq;
using System.Net.Configuration;
using System.Runtime.Serialization.Formatters;
using System.Text.RegularExpressions;
using SyncManager.Etl.Common;

namespace SyncManager.Etl.Cleanup
{
    public interface ICleanuper
    {
        SingleCleanupRuleResult Cleanup(EtlRow etlRow);
    }

    public class Cleanuper: ICleanuper
    {
        private Dictionary<string, List<CleanupRule>> _cleanupRulesIndex;

        public bool Ignore { get; set; }

        public SingleCleanupRuleResult Cleanup(EtlRow etlRow)
        {
            SingleCleanupRuleResult result = new SingleCleanupRuleResult();
            foreach (var cleanupRuleList in _cleanupRulesIndex.Values)
            {
                foreach (var cleanupRule in cleanupRuleList.Where(a=>a.IsEnabled))
                {
                    Apply(cleanupRule, etlRow, result);
                }
            }
            return result;
        }

        private void Apply(CleanupRule rule, EtlRow etlRow, SingleCleanupRuleResult result)
        {

            object capturedValue = etlRow.Source[rule.ColumnName];
            var stringValue = capturedValue != null ? capturedValue.ToString() : "";
            if (CheckCondition(rule, etlRow, capturedValue, stringValue))
            {
                ApplyCleanup(rule, etlRow, stringValue, result);
            }
        }

        private SingleCleanupRuleResult ApplyCleanup(CleanupRule rule, EtlRow etlRow, string stringValue, SingleCleanupRuleResult result)
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
            }
            return result;
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
                default:
                    return false;
            }
        }
    }

    public class SingleCleanupRuleResult
    {
        public bool IsDeleted { get; set; }
    }
}