using System.Collections.Generic;
using NUnit.Framework;
using SyncManager.Common;
using SyncManager.FlowEtl.Common;
using SyncManager.FlowEtl.Schema;

namespace Tests.SyncManager.NewSteps
{
    [TestFixture]
    public class SchematorTests
    {
        [Test]
        public void TransformToIntOnlyAliasWasAppliedAndOnlyOneKey()
        {
            Schemator schemator = new Schemator(new List<DataSourceSchemaItem>()
            {
                new DataSourceSchemaItem()
                {
                    Alias = "MOMO",
                    ColumnName = "Col1",
                    Type = ValueType.Int
                }
            });
            var sourceContext = new SourceContext();
            sourceContext.Source["Col1"] = "1";
            schemator.Schema( sourceContext);
            Assert.AreEqual(1,sourceContext.Source["MOMO"]);
            Assert.AreEqual(1,sourceContext.Source.Keys.Count);
        }

        [Test]
        public void TransformToIntIsRequired()
        {
            Schemator schemator = new Schemator(new List<DataSourceSchemaItem>()
            {
                new DataSourceSchemaItem()
                {
                    Alias = "MOMO",
                    ColumnName = "Col1",
                    Type = ValueType.Int,
                    IsRequired = true
                }
            });
            var sourceContext = new SourceContext();
            sourceContext.Source["Col1"] = null;
            schemator.Schema(sourceContext);
            Assert.IsTrue(sourceContext.SourceCellErrors["Col1"][0].Message.ToLower().Contains("missed"));
        }

        [Test]
        public void TransformToIntOnlyAliasWasnotAppliedAndOnlyOneKey()
        {
            Schemator schemator = new Schemator(new List<DataSourceSchemaItem>()
            {
                new DataSourceSchemaItem()
                {
                    //Alias = "MOMO",
                    ColumnName = "Col1",
                    Type = ValueType.Int
                }
            });
            var sourceContext = new SourceContext();
            sourceContext.Source["Col1"] = "1";
            schemator.Schema(sourceContext);
            Assert.AreEqual(1, sourceContext.Source["Col1"]);
            Assert.AreEqual(1, sourceContext.Source.Keys.Count);
        }

        [Test]
        [TestCase("1", ValueType.String, "1")]
        [TestCase("1", ValueType.Int, 1)]
        [TestCase(1, ValueType.Int, 1)]
        [TestCase(0.34, ValueType.Int, 0 )]
        public void TransformCases(object from, ValueType type, object result)
        {
            Schemator schemator = new Schemator(new List<DataSourceSchemaItem>()
            {
                new DataSourceSchemaItem()
                {
                    Alias = "MOMO",
                    ColumnName = "Col1",
                    Type = type
                }
            });
            var sourceContext = new SourceContext();
            sourceContext.Source["Col1"] = from;
            schemator.Schema(sourceContext);
            Assert.AreEqual(result, sourceContext.Source["MOMO"]);
            Assert.AreEqual(1, sourceContext.Source.Keys.Count);
        }
    }
}