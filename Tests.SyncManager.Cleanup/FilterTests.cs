using System;
using System.Collections.Generic;
using NUnit.Framework;
using SyncManager.Etl.Common;
using SyncManager.Etl.Filtering;

namespace Tests.SyncManager.NewSteps
{
    [TestFixture]
    public class FilterTests
    {
        [Test]
        public void FilterOne()
        {
            FilterRule filterRule = new FilterRule()
            {
                Name = "Rule1",
                Expression = "source['a'] == 3"
                ,IsEnabled = true
            }; 
            var expressionEvaluator = new TestExpressionEvaluator();
            expressionEvaluator.Evaluations["source['a'] == 3"] = () => true;
            var headerList = CsvParser.GetParts("a,b,c");
            var row1 = CsvParser.Parse("1,2,3", headerList);
            Filterer filterer = new Filterer(new List<FilterRule>(){ filterRule}, expressionEvaluator );
            var rowCtx = new SourceContext()
            {
                Source = row1
            };
            filterer.Filter(rowCtx);
            Assert.IsFalse(rowCtx.IsDeleted);

        }

        [Test]
        public void FilterWhenFilterReturnsFalse()
        {
            FilterRule filterRule = new FilterRule()
            {
                Name = "Rule1",
                Expression = "source['a'] == 3"
                ,
                IsEnabled = true
            };
            var expressionEvaluator = new TestExpressionEvaluator();
            expressionEvaluator.Evaluations["source['a'] == 3"] = () => false;
            var headerList = CsvParser.GetParts("a,b,c");
            var row1 = CsvParser.Parse("1,2,3", headerList);
            Filterer filterer = new Filterer(new List<FilterRule>() { filterRule }, expressionEvaluator);
            var rowCtx = new SourceContext()
            {
                Source = row1
            };
            filterer.Filter(rowCtx);
            Assert.IsTrue(rowCtx.IsDeleted);

        }


        [Test]
        public void FilterWhenExpressionThrows()
        {
            FilterRule filterRule = new FilterRule()
            {
                Name = "Rule1",
                Expression = "source['a'] == 3"
                ,
                IsEnabled = true
            };
            var expressionEvaluator = new TestExpressionEvaluator();
            expressionEvaluator.Evaluations["source['a'] == 3"] = () => throw new Exception("Some error");
            var headerList = CsvParser.GetParts("a,b,c");
            var row1 = CsvParser.Parse("1,2,3", headerList);
            Filterer filterer = new Filterer(new List<FilterRule>() { filterRule }, expressionEvaluator);
            var rowCtx = new SourceContext()
            {
                Source = row1
            };
            filterer.Filter(rowCtx);
            Assert.IsFalse(rowCtx.IsDeleted);
            Assert.AreEqual("Some error", rowCtx.SourceRowErrors[0].Exception.Message);

        }
    }
}