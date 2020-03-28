using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
        public Cleanuper(List<CleanupRule> rules, IExpressionEvaluator expressionEvaluator)
        {
            _expressionEvaluator = expressionEvaluator;
            foreach (var cleanupRule in rules)
            {
                if (!_cleanupRulesIndex.ContainsKey(cleanupRule.ColumnName))
                    _cleanupRulesIndex[cleanupRule.ColumnName] = new List<CleanupRule>();
                _cleanupRulesIndex[cleanupRule.ColumnName].Add(cleanupRule);
            }
        }

        public void Cleanup(SourceContext sourceContext)
        {
            TryToInit();
            var result = new SingleCleanupRuleResult();
            foreach (var cleanupRuleList in _cleanupRulesIndex.Values)
            foreach (var cleanupRule in cleanupRuleList.Where(a => a.IsEnabled))
            {
                SafeRun(() =>
                {
                    Apply(cleanupRule, sourceContext, result);
                }, sourceContext, cleanupRule);
              
            }
              
            Post(sourceContext, result);
            sourceContext.IsDeleted = result.IsDeleted;
        }

        private void SafeRun(Action action, SourceContext sourceContext, CleanupRule rule)
        {
            try
            {
                action();
            }
            catch (Exception e)
            {
                sourceContext.AddErrorForColumn(rule.ColumnName, e, SourceContext.ErrorTypeCleanup);
            }
        }

        private void Apply(CleanupRule rule, SourceContext sourceContext, SingleCleanupRuleResult result)
        {
            var capturedValue = sourceContext.Source[rule.ColumnName];
            var stringValue = capturedValue != null ? capturedValue.ToString() : "";
            EnrichContextByRow(sourceContext);
            Lazy<string> evaluation = new Lazy<string>(() =>
            {
             
                var evaluatedResult = _expressionEvaluator.Evaluate(rule.Expression).ToString();
                
                
                return evaluatedResult;
            });
            EvaluatedValue evaluatedValue = new EvaluatedValue(evaluation);
            if (CheckCondition(rule, sourceContext, capturedValue, stringValue, evaluatedValue))
            {
                ApplyConditionalCleanup(rule, sourceContext, stringValue, result, evaluatedValue);
            }
            ApplyNonConditionalCleanup(rule, sourceContext, stringValue, result, evaluatedValue);

        }

        private void EnrichContextByRow(SourceContext sourceContext)
        {
            _expressionEvaluator.EnrichContext("source", sourceContext.Source);
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

        private void Post(SourceContext sourceContext, SingleCleanupRuleResult result)
        {
            if (_frameState.IsLocked()) result.IsDeleted = true;
        }

        private void ApplyNonConditionalCleanup(CleanupRule rule, SourceContext sourceContext, string stringValue,
            SingleCleanupRuleResult result, EvaluatedValue evaluation)
        {
            switch (rule.Action)
            {
                case CleanupAction.GetPrevious:
                {
                    if (string.IsNullOrEmpty(stringValue) && _previousNonEmptyValue.ContainsKey(rule.ColumnName))
                        sourceContext.Source[rule.ColumnName] = _previousNonEmptyValue[rule.ColumnName];
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

        private void ApplyConditionalCleanup(CleanupRule rule, SourceContext sourceContext, string stringValue,
            SingleCleanupRuleResult result, EvaluatedValue evaluation)
        {
            
            switch (rule.Action)
            {
                case CleanupAction.Replace:
                {
                    sourceContext.Source[rule.ColumnName] = evaluation.Value;
                }
                    break;
                case CleanupAction.ReplaceMatched:
                {
                    if (_matched.ContainsKey(rule.ColumnName) && !string.IsNullOrEmpty(_matched[rule.ColumnName]))
                        if (string.IsNullOrEmpty(stringValue))
                        {
                            sourceContext.Source[rule.ColumnName] = evaluation.Value;
                            }
                        else
                        {
                            sourceContext.Source[rule.ColumnName] = stringValue.Replace(_matched[rule.ColumnName], evaluation.Value);
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


        private bool CheckCondition(CleanupRule rule, SourceContext sourceContext, object capturedValue, string stringValue,
            EvaluatedValue evaluation)
        {

            switch (rule.Condition)
            {
                case CleanupCondition.Empty:
                {
                    _matched[rule.ColumnName] = evaluation.Value;
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
                case CleanupCondition.Expression:
                {
                    _matched[rule.ColumnName] = stringValue;
                    return (bool)_expressionEvaluator.Evaluate(rule.ConditionArgument);
                    }
                default:
                    return false;
            }
        }
    }
}

public class EvaluatedValue
{
    private Lazy<string> _lazyGetter;
    private string _value;
    public EvaluatedValue(Lazy<string> lazyGetter)
    {
        _lazyGetter = lazyGetter;
    }

    public string Value
    {
        get
        {
            if (string.IsNullOrEmpty(_value))
            {
                _value = _lazyGetter.Value;
            }
            return _value;
        }
    }
}


public interface IExpressionEvaluator
{
    void EnrichContext(string key, object value);
    object Evaluate(string expression);
}