﻿using System;
using System.Collections.Generic;

namespace Tests.SyncManager.Cleanup
{
    public class TestExpressionEvaluator : IExpressionEvaluator
    {
        public TestExpressionEvaluator()
        {
            Evaluations["source['Col1'] == '4'"] = () =>
            {
                var source = (Variables["source"] as Dictionary<string, object>);
                var result = source["Col1"] as string == "4";
                return result;
            };
        }

        public Dictionary<string, object> Variables { get; set; } = new Dictionary<string, object>();
        public Dictionary<string, Func<object>> Evaluations { get; set; } = new Dictionary<string, Func<object>>();
        public void EnrichContext(string key, object value)
        {
            Variables[key] = value;
        }

        public object Evaluate(string expression)
        {
            if (string.IsNullOrEmpty(expression)) return expression;
            if (expression.StartsWith("'") && (expression.EndsWith("'")))
            {
                return expression.Replace("'", "");
            }
            var result = Evaluations[expression]();
            return result;
        }
    }
}