using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using SyncManager.FlowClockwork;
using SyncManager.FlowClockwork.ETL;
using SyncManager.FlowEtl.Cleanup;
using SyncManager.FlowEtl.Schema;
using ValueType = SyncManager.Common.ValueType;

namespace Tests.SyncManager.NewSteps
{
    [TestFixture]
    public class EtlBuilderTests
    {
        [Test]
        public void CheckDependenciesTest()
        {
            var expressionEvaluator = new TestExpressionEvaluator();
            //expressionEvaluator.Evaluations["source['Col1'] != ccc"] =
            //    () => (string) expressionEvaluator.Variables["source['Col1']"] != "ccc";
            expressionEvaluator.Evaluations["'0'"] =
                () =>
                {
                    return "0";
                };
            var globalPath = AppDomain.CurrentDomain.SetupInformation.ApplicationBase;
            var filePath = "TestData\\test.csv";
            filePath = Path.Combine(globalPath, filePath);
            //using (StreamWriter streamWriter = new StreamWriter(filePath))
            //{
            //    streamWriter.WriteLine("Field1,Field2,Field3");
            //    for (int i = 0; i < 1000000; i++)
            //    {
            //        streamWriter.WriteLine($"line#{i},{i},{(decimal)((decimal)12 / (decimal)(i+1))}");
            //    }
            //}
               
            IEtlNodeBuilder etlNodeBuilder = new EtlNodeBuilder();
            etlNodeBuilder.AddExpressionEvaluator(expressionEvaluator);
            etlNodeBuilder.AddStart();
            etlNodeBuilder.AddDataReader(filePath);
            etlNodeBuilder.AddCleanup(new List<CleanupRule>(){new CleanupRule()
            {
                IsEnabled = true, Action = CleanupAction.Replace, ColumnName = "Field3", Condition = CleanupCondition.Equal, ConditionArgument = "NaN", Expression = "'0'"
            }});
            etlNodeBuilder.AddSchema(new List<DataSourceSchemaItem>()
            {
                new DataSourceSchemaItem()
                {
                    Alias = "Col1", ColumnName = "Field1", Type = ValueType.String
                },
                new DataSourceSchemaItem()
                {
                    Alias = "Col2", ColumnName = "Field2", Type = ValueType.Int
                },
                new DataSourceSchemaItem()
                {
                    Alias = "Col3", ColumnName = "Field3", Type = ValueType.Decimal
                }
            });
            //etlNodeBuilder.AddFilter(new List<FilterRule>()
            //{
            //    new FilterRule()
            //    {
            //        Expression = "source['Col1'] != ccc", IsEnabled = true, Name = "One rule"
            //    }
            //});
            etlNodeBuilder.AddDataWriter(Path.Combine(globalPath, "result.csv"));
       
            var result = etlNodeBuilder.Get();
            //var filterNode = result.NodeDefinitionProvider.GetByName("Filter");
            var schemaNode = result.NodeDefinitionProvider.GetByName("Schema");
            var cleanupNode = result.NodeDefinitionProvider.GetByName("Cleanup");
            var readerNode = result.NodeDefinitionProvider.GetByName("Reader");
            var startNode = result.NodeDefinitionProvider.GetByName("Start");
            var action0 = result.NodeDefinitionProvider.GetByName("Action0");
            Assert.AreEqual("Schema", action0.DependsOn[0]);
            Assert.AreEqual("Cleanup", schemaNode.DependsOn[0]);
            Assert.AreEqual("Reader", cleanupNode.DependsOn[0]);
            Assert.AreEqual("Start", readerNode.DependsOn[0]);
            Assert.AreEqual(0, startNode.DependsOn.Count);


            //var runner = new DataNodeRunner<SourceContext>(result.NodeDefinitionProvider, result.Registry);
            //runner.Run();
        }
    }
}