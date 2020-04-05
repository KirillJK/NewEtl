using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using SyncManager.FlowEtl.Common;
using SyncManager.FlowEtl.Transformation;
using SyncManager.FlowEtl.Transformation.Models;

namespace Tests.SyncManager.NewSteps
{
    [TestFixture]
    public class TransformerTests
    {
        [Test]
        public void SimpleMapping()
        {
            TestExpressionEvaluator expressionEvaluator = new TestExpressionEvaluator();
            List<TransformationMap> maps = new List<TransformationMap>();
            maps.Add(new TransformationMap()
            {
                KeyExpression = "Col0", MappingType = MappingType.Field, Output = "Key1"
            });
            Transformer transformer = new Transformer(maps, expressionEvaluator);
            var header = "Col0,Col1,Col2";
            var row1 = "1,2,3";
            var row2 = "4,5,6";
            var row3 = "7,8,9";
            var headerList = CsvParser.GetParts(header);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new SourceContext() { Source = a }).ToList();
            foreach (var sourceContext in rows)
            {
                transformer.Transform(sourceContext);
            }

            Assert.AreEqual("1", rows[0].Destination["Key1"]);
            Assert.AreEqual("4", rows[1].Destination["Key1"]);
            Assert.AreEqual("7", rows[2].Destination["Key1"]);
        }

        [Test]
        public void SimpleMappingWithNull()
        {
            TestExpressionEvaluator expressionEvaluator = new TestExpressionEvaluator();
            List<TransformationMap> maps = new List<TransformationMap>();
            maps.Add(new TransformationMap()
            {
                KeyExpression = "Col0",
                MappingType = MappingType.Field,
                Output = "Key1"
            });
            Transformer transformer = new Transformer(maps, expressionEvaluator);
            var header = "Col0,Col1,Col2";
            var row1 = "1,2,3";
            var row2 = "4,5,6";
            var row3 = "NULL,8,9";
            var headerList = CsvParser.GetParts(header);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new SourceContext() { Source = a }).ToList();
            foreach (var sourceContext in rows)
            {
                transformer.Transform(sourceContext);
            }

            Assert.AreEqual("1", rows[0].Destination["Key1"]);
            Assert.AreEqual("4", rows[1].Destination["Key1"]);
            Assert.AreEqual(null, rows[2].Destination["Key1"]);
        }

        [Test]
        public void SimpleMappingWithMissedSourceKey()
        {
            TestExpressionEvaluator expressionEvaluator = new TestExpressionEvaluator();
            List<TransformationMap> maps = new List<TransformationMap>();
            maps.Add(new TransformationMap()
            {
                KeyExpression = "WrongCol",
                MappingType = MappingType.Field,
                Output = "Key1"
            });
            Transformer transformer = new Transformer(maps, expressionEvaluator);
            var header = "Col0,Col1,Col2";
            var row1 = "1,2,3";
            var row2 = "4,5,6";
            var row3 = "4,8,9";
            var headerList = CsvParser.GetParts(header);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new SourceContext() { Source = a }).ToList();
            foreach (var sourceContext in rows)
            {
                transformer.Transform(sourceContext);
            }

            Assert.True(rows[0].DestinationCellErrors["Key1"][0].Message.Contains("missed"));
            Assert.True(rows[1].DestinationCellErrors["Key1"][0].Message.Contains("missed"));
            Assert.True(rows[2].DestinationCellErrors["Key1"][0].Message.Contains("missed"));
        }

        [Test]
        public void TransformationMapping()
        {
            TestExpressionEvaluator expressionEvaluator = new TestExpressionEvaluator();
            expressionEvaluator.Evaluations["source['Col0']+'AAA'"] =
                () => expressionEvaluator.Variables["source['Col0']"] + "AAA";
            List<TransformationMap> maps = new List<TransformationMap>();
            maps.Add(new TransformationMap()
            {
                Expression = "source['Col0']+'AAA'",
                MappingType = MappingType.Expression,
                Output = "Key1"
            });
            Transformer transformer = new Transformer(maps, expressionEvaluator);
            var header = "Col0,Col1,Col2";
            var row1 = "1,2,3";
            var row2 = "4,5,6";
            var row3 = "7,8,9";
            var headerList = CsvParser.GetParts(header);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new SourceContext() { Source = a }).ToList();
            foreach (var sourceContext in rows)
            {
                expressionEvaluator.EnrichContext("source['Col0']", sourceContext.Source["Col0"]);
                transformer.Transform(sourceContext);
            }

            Assert.AreEqual("1AAA", rows[0].Destination["Key1"]);
            Assert.AreEqual("4AAA", rows[1].Destination["Key1"]);
            Assert.AreEqual("7AAA", rows[2].Destination["Key1"]);
        }


        [Test]
        public void TransformationMappingWhenExpressionThrows()
        {
            TestExpressionEvaluator expressionEvaluator = new TestExpressionEvaluator();
            expressionEvaluator.Evaluations["source['Col0']+'AAA'"] =
                () => throw new Exception("Some error");
            List<TransformationMap> maps = new List<TransformationMap>();
            maps.Add(new TransformationMap()
            {
                Expression = "source['Col0']+'AAA'",
                MappingType = MappingType.Expression,
                Output = "Key1"
            });
            Transformer transformer = new Transformer(maps, expressionEvaluator);
            var header = "Col0,Col1,Col2";
            var row1 = "1,2,3";
            var row2 = "4,5,6";
            var row3 = "7,8,9";
            var headerList = CsvParser.GetParts(header);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new SourceContext() { Source = a }).ToList();
            foreach (var sourceContext in rows)
            {
                expressionEvaluator.EnrichContext("source['Col0']", sourceContext.Source["Col0"]);
                transformer.Transform(sourceContext);
            }

            Assert.True(rows[0].DestinationCellErrors["Key1"][0].Exception.Message.Contains("Some error"));
            Assert.True(rows[1].DestinationCellErrors["Key1"][0].Exception.Message.Contains("Some error"));
            Assert.True(rows[2].DestinationCellErrors["Key1"][0].Exception.Message.Contains("Some error"));

            Assert.IsNull(rows[0].Destination["Key1"]);
            Assert.IsNull(rows[1].Destination["Key1"]);
            Assert.IsNull(rows[2].Destination["Key1"]);
        }

        [Test]
        public void SimpleMappingWhenSourceIsEmptyAndTransformOptionIsTrue()
        {
            TestExpressionEvaluator expressionEvaluator = new TestExpressionEvaluator();
            List<TransformationMap> maps = new List<TransformationMap>();
            maps.Add(new TransformationMap()
            {
                KeyExpression = "Col0",
                MappingType = MappingType.Field,
                Output = "Key1"
                ,TransformOnlyIfColumnExists = true
            });
            Transformer transformer = new Transformer(maps, expressionEvaluator);
            var header = "Col0,Col1,Col2";
            var row1 = "NULL,2,3";
            var row2 = "4,5,6";
            var row3 = ",8,9";
            var headerList = CsvParser.GetParts(header);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new SourceContext() { Source = a }).ToList();
            rows[0].ListOfSchemaMissedColumns.Add("Col0");
            rows[2].ListOfSchemaMissedColumns.Add("Col0");
            foreach (var sourceContext in rows)
            {
                transformer.Transform(sourceContext);
            }

            Assert.IsFalse(rows[0].Destination.ContainsKey("Key1"));
            Assert.AreEqual("4", rows[1].Destination["Key1"]);
            Assert.IsFalse(rows[2].Destination.ContainsKey("Key1"));
        }

    }
}