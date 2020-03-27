﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;
using SyncManager.Etl.Cleanup;
using SyncManager.Etl.Common;

namespace Tests.SyncManager.Cleanup
{
    [TestFixture]
    public class CleanuperTests
    {
        [Test]
        public void EmptyRulesAndNothingChanged()
        {
            var expressionEvaluator = GetMocked();
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                //new CleanupRule(){Action = CleanupAction.Remove, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.Equal, ConditionArgument = "4"}
            }, expressionEvaluator);

            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            var header = "Col1,Col2,Col3";
            var row1 = "1,2,3";
            var row2 = "4,5,6";
            var row3 = "7,8,9";
            var headerList = CsvParser.GetParts(header);
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));
            var rows = list.Select(a => new EtlRow() {Source = a}).ToList();
            foreach (var row in rows)
            {
                cleanuper.Cleanup(row);
            }

            foreach (var etlRow in rows)
            {
                Assert.IsFalse(etlRow.IsDeleted);
            }
            Assert.AreEqual(row1, CsvParser.ToCsv(rows[0].Source));
            Assert.AreEqual(row2, CsvParser.ToCsv(rows[1].Source));
            Assert.AreEqual(row3, CsvParser.ToCsv(rows[2].Source));
        

        }

        [Test]
        public void OneRuleRemoveSecondRow()
        {
            var expressionEvaluator = GetMocked();
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.Remove, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.Equal, ConditionArgument = "4"}
            }, expressionEvaluator);

            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            var header = "Col1,Col2,Col3";
            var row1 = "1,2,3";
            var row2 = "4,5,6";
            var row3 = "7,8,9";
            var headerList = CsvParser.GetParts(header);
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));
            var rows = list.Select(a => new EtlRow() { Source = a }).ToList();
            foreach (var row in rows)
            {
                cleanuper.Cleanup(row);
            }


            Assert.IsFalse(rows[0].IsDeleted);
            Assert.IsTrue(rows[1].IsDeleted);
            Assert.IsFalse(rows[2].IsDeleted);
            Assert.AreEqual(row1, CsvParser.ToCsv(rows[0].Source));
            Assert.AreEqual(row2, CsvParser.ToCsv(rows[1].Source));
            Assert.AreEqual(row3, CsvParser.ToCsv(rows[2].Source));


        }

        [Test]
        public void OneRuleReplace()
        {
            var expressionEvaluator = GetMocked();
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.Replace, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.Equal, ConditionArgument = "444", Expression =  "'AAA'"}
            }, expressionEvaluator);

            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            var header = "Col1,Col2,Col3";
            var row1 = "1,2,3";
            var row2 = "444,5,6";
            var row3 = "7,8,9";
            var headerList = CsvParser.GetParts(header);
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));
            var rows = list.Select(a => new EtlRow() { Source = a }).ToList();
            foreach (var row in rows)
            {
                cleanuper.Cleanup(row);
            }


            Assert.IsFalse(rows[0].IsDeleted);
            Assert.IsFalse(rows[1].IsDeleted);
            Assert.IsFalse(rows[2].IsDeleted);
            Assert.AreEqual(row1, CsvParser.ToCsv(rows[0].Source));
            Assert.AreEqual("AAA,5,6", CsvParser.ToCsv(rows[1].Source));
            Assert.AreEqual(row3, CsvParser.ToCsv(rows[2].Source));
        }

        [Test]
        public void OneRuleReplaceMatched()
        {
            var expressionEvaluator = GetMocked();
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.ReplaceMatched, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.Contains, ConditionArgument = "44", Expression =  "'AAA'"}
            }, expressionEvaluator);

            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            var header = "Col1,Col2,Col3";
            var row1 = "1,2,3";
            var row2 = "444,5,6";
            var row3 = "7,8,9";
            var headerList = CsvParser.GetParts(header);
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));
            var rows = list.Select(a => new EtlRow() { Source = a }).ToList();
            foreach (var row in rows)
            {
                cleanuper.Cleanup(row);
            }


            Assert.IsFalse(rows[0].IsDeleted);
            Assert.IsFalse(rows[1].IsDeleted);
            Assert.IsFalse(rows[2].IsDeleted);
            Assert.AreEqual(row1, CsvParser.ToCsv(rows[0].Source));
            Assert.AreEqual("AAA4,5,6", CsvParser.ToCsv(rows[1].Source));
            Assert.AreEqual(row3, CsvParser.ToCsv(rows[2].Source));
        }

        [Test]
        public void OneRuleReplaceMatchedFull()
        {
            var expressionEvaluator = GetMocked();
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.Replace, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.Contains, ConditionArgument = "44", Expression =  "'AAA'"}
            }, expressionEvaluator);

            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            var header = "Col1,Col2,Col3";
            var row1 = "1,2,3";
            var row2 = "444,5,6";
            var row3 = "7,8,9";
            var headerList = CsvParser.GetParts(header);
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));
            var rows = list.Select(a => new EtlRow() { Source = a }).ToList();
            foreach (var row in rows)
            {
                cleanuper.Cleanup(row);
            }


            Assert.IsFalse(rows[0].IsDeleted);
            Assert.IsFalse(rows[1].IsDeleted);
            Assert.IsFalse(rows[2].IsDeleted);
            Assert.AreEqual(row1, CsvParser.ToCsv(rows[0].Source));
            Assert.AreEqual("AAA,5,6", CsvParser.ToCsv(rows[1].Source));
            Assert.AreEqual(row3, CsvParser.ToCsv(rows[2].Source));
        }

        [Test]
        public void EmptyPlusReplace()
        {
            var expressionEvaluator = GetMocked();
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.Replace, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.Empty, ConditionArgument = "", Expression =  "'AAA'"}
            }, expressionEvaluator);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            var header = "Col1,Col2,Col3";
            var row1 = "1,2,3";
            var row2 = ",5,6";
            var row3 = "7,8,9";

            var headerList = CsvParser.GetParts(header);
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new EtlRow() { Source = a }).ToList();
            foreach (var row in rows)
            {
                cleanuper.Cleanup(row);
            }


            Assert.IsFalse(rows[0].IsDeleted);
            Assert.IsFalse(rows[1].IsDeleted);
            Assert.IsFalse(rows[2].IsDeleted);

            Assert.AreEqual(row1, CsvParser.ToCsv(rows[0].Source));
            Assert.AreEqual("AAA,5,6", CsvParser.ToCsv(rows[1].Source));
            Assert.AreEqual(row3, CsvParser.ToCsv(rows[2].Source));
        }

        [Test]
        public void NullPlusReplace()
        {
            var expressionEvaluator = GetMocked();
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.Replace, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.Empty, ConditionArgument = "", Expression =  "'AAA'"}
            }, expressionEvaluator);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            var header = "Col1,Col2,Col3";
            var row1 = "1,2,3";
            var row2 = "NULL,5,6";
            var row3 = "7,8,9";

            var headerList = CsvParser.GetParts(header);
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new EtlRow() { Source = a }).ToList();
            foreach (var row in rows)
            {
                cleanuper.Cleanup(row);
            }


            Assert.IsFalse(rows[0].IsDeleted);
            Assert.IsFalse(rows[1].IsDeleted);
            Assert.IsFalse(rows[2].IsDeleted);

            Assert.AreEqual(row1, CsvParser.ToCsv(rows[0].Source));
            Assert.AreEqual("AAA,5,6", CsvParser.ToCsv(rows[1].Source));
            Assert.AreEqual(row3, CsvParser.ToCsv(rows[2].Source));
        }


        [Test]
        public void EqualPlusReplace()
        {
            var expressionEvaluator = GetMocked();
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.Replace, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.Equal, ConditionArgument = "4", Expression =  "'AAA'"}
            }, expressionEvaluator);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            var header = "Col1,Col2,Col3";
            var row1 = "1,2,3";
            var row2 = "4,5,6";
            var row3 = "7,8,9";

            var headerList = CsvParser.GetParts(header);
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new EtlRow() { Source = a }).ToList();
            foreach (var row in rows)
            {
                cleanuper.Cleanup(row);
            }


            Assert.IsFalse(rows[0].IsDeleted);
            Assert.IsFalse(rows[1].IsDeleted);
            Assert.IsFalse(rows[2].IsDeleted);

            Assert.AreEqual(row1, CsvParser.ToCsv(rows[0].Source));
            Assert.AreEqual("AAA,5,6", CsvParser.ToCsv(rows[1].Source));
            Assert.AreEqual(row3, CsvParser.ToCsv(rows[2].Source));
        }

        [Test]
        public void StartsWithPlusReplace()
        {
            var expressionEvaluator = GetMocked();
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.Replace, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.StartsWith, ConditionArgument = "4BBB", Expression =  "'AAA'"}
            }, expressionEvaluator);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            var header = "Col1,Col2,Col3";
            var row1 = "1,2,3";
            var row2 = "4BBBBOBOR,5,6";
            var row3 = "7,8,9";

            var headerList = CsvParser.GetParts(header);
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new EtlRow() { Source = a }).ToList();
            foreach (var row in rows)
            {
                cleanuper.Cleanup(row);
            }


            Assert.IsFalse(rows[0].IsDeleted);
            Assert.IsFalse(rows[1].IsDeleted);
            Assert.IsFalse(rows[2].IsDeleted);

            Assert.AreEqual(row1, CsvParser.ToCsv(rows[0].Source));
            Assert.AreEqual("AAA,5,6", CsvParser.ToCsv(rows[1].Source));
            Assert.AreEqual(row3, CsvParser.ToCsv(rows[2].Source));
        }

        [Test]
        public void EndsWithPlusReplace()
        {
            var expressionEvaluator = GetMocked();
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.Replace, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.EndsWith, ConditionArgument = "BOBOR", Expression =  "'AAA'"}
            }, expressionEvaluator);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            var header = "Col1,Col2,Col3";
            var row1 = "1,2,3";
            var row2 = "4BBBBOBOR,5,6";
            var row3 = "7,8,9";

            var headerList = CsvParser.GetParts(header);
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new EtlRow() { Source = a }).ToList();
            foreach (var row in rows)
            {
                cleanuper.Cleanup(row);
            }


            Assert.IsFalse(rows[0].IsDeleted);
            Assert.IsFalse(rows[1].IsDeleted);
            Assert.IsFalse(rows[2].IsDeleted);

            Assert.AreEqual(row1, CsvParser.ToCsv(rows[0].Source));
            Assert.AreEqual("AAA,5,6", CsvParser.ToCsv(rows[1].Source));
            Assert.AreEqual(row3, CsvParser.ToCsv(rows[2].Source));
        }

        [Test]
        public void RegexPlusReplace()
        {
            var expressionEvaluator = GetMocked();
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.Replace, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.Regex, ConditionArgument = "(?<=This is)(.*)(?=sentence)", Expression =  "'AAA'"}
            }, expressionEvaluator);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            var header = "Col1,Col2,Col3";
            var row1 = "1,2,3";
            var row2 = "This is just a simple sentence,5,6";
            var row3 = "7,8,9";

            var headerList = CsvParser.GetParts(header);
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new EtlRow() { Source = a }).ToList();
            foreach (var row in rows)
            {
                cleanuper.Cleanup(row);
            }


            Assert.IsFalse(rows[0].IsDeleted);
            Assert.IsFalse(rows[1].IsDeleted);
            Assert.IsFalse(rows[2].IsDeleted);

            Assert.AreEqual(row1, CsvParser.ToCsv(rows[0].Source));
            Assert.AreEqual("AAA,5,6", CsvParser.ToCsv(rows[1].Source));
            Assert.AreEqual(row3, CsvParser.ToCsv(rows[2].Source));
        }



        [Test]
        public void EmptyPlusReplaceMatched()
        {
            var expressionEvaluator = GetMocked();
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.ReplaceMatched, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.Empty, ConditionArgument = "", Expression =  "'AAA'"}
            }, expressionEvaluator);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            var header = "Col1,Col2,Col3";
            var row1 = "1,2,3";
            var row2 = ",5,6";
            var row3 = "7,8,9";

            var headerList = CsvParser.GetParts(header);
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new EtlRow() { Source = a }).ToList();
            foreach (var row in rows)
            {
                cleanuper.Cleanup(row);
            }


            Assert.IsFalse(rows[0].IsDeleted);
            Assert.IsFalse(rows[1].IsDeleted);
            Assert.IsFalse(rows[2].IsDeleted);

            Assert.AreEqual(row1, CsvParser.ToCsv(rows[0].Source));
            Assert.AreEqual("AAA,5,6", CsvParser.ToCsv(rows[1].Source));
            Assert.AreEqual(row3, CsvParser.ToCsv(rows[2].Source));
        }

        [Test]
        public void NullPlusReplaceMatched()
        {
            var expressionEvaluator = GetMocked();
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.ReplaceMatched, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.Empty, ConditionArgument = "", Expression =  "'AAA'"}
            }, expressionEvaluator);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            var header = "Col1,Col2,Col3";
            var row1 = "1,2,3";
            var row2 = "NULL,5,6";
            var row3 = "7,8,9";

            var headerList = CsvParser.GetParts(header);
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new EtlRow() { Source = a }).ToList();
            foreach (var row in rows)
            {
                cleanuper.Cleanup(row);
            }


            Assert.IsFalse(rows[0].IsDeleted);
            Assert.IsFalse(rows[1].IsDeleted);
            Assert.IsFalse(rows[2].IsDeleted);

            Assert.AreEqual(row1, CsvParser.ToCsv(rows[0].Source));
            Assert.AreEqual("AAA,5,6", CsvParser.ToCsv(rows[1].Source));
            Assert.AreEqual(row3, CsvParser.ToCsv(rows[2].Source));
        }


        [Test]
        public void EqualPlusReplaceMatched()
        {
            var expressionEvaluator = GetMocked();
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.ReplaceMatched, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.Equal, ConditionArgument = "4", Expression =  "'AAA'"}
            }, expressionEvaluator);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            var header = "Col1,Col2,Col3";
            var row1 = "1,2,3";
            var row2 = "4,5,6";
            var row3 = "7,8,9";

            var headerList = CsvParser.GetParts(header);
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new EtlRow() { Source = a }).ToList();
            foreach (var row in rows)
            {
                cleanuper.Cleanup(row);
            }


            Assert.IsFalse(rows[0].IsDeleted);
            Assert.IsFalse(rows[1].IsDeleted);
            Assert.IsFalse(rows[2].IsDeleted);

            Assert.AreEqual(row1, CsvParser.ToCsv(rows[0].Source));
            Assert.AreEqual("AAA,5,6", CsvParser.ToCsv(rows[1].Source));
            Assert.AreEqual(row3, CsvParser.ToCsv(rows[2].Source));
        }


        [Test]
        public void StartsWithPlusReplaceMatched()
        {
            var expressionEvaluator = GetMocked();
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.ReplaceMatched, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.StartsWith, ConditionArgument = "4BBB", Expression =  "'AAA'"}
            }, expressionEvaluator);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            var header = "Col1,Col2,Col3";
            var row1 = "1,2,3";
            var row2 = "4BBBBOBOR,5,6";
            var row3 = "7,8,9";

            var headerList = CsvParser.GetParts(header);
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new EtlRow() { Source = a }).ToList();
            foreach (var row in rows)
            {
                cleanuper.Cleanup(row);
            }


            Assert.IsFalse(rows[0].IsDeleted);
            Assert.IsFalse(rows[1].IsDeleted);
            Assert.IsFalse(rows[2].IsDeleted);

            Assert.AreEqual(row1, CsvParser.ToCsv(rows[0].Source));
            Assert.AreEqual("AAABOBOR,5,6", CsvParser.ToCsv(rows[1].Source));
            Assert.AreEqual(row3, CsvParser.ToCsv(rows[2].Source));
        }

        [Test]
        public void EndsWithPlusReplaceMatched()
        {
            var expressionEvaluator = GetMocked();
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.ReplaceMatched, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.EndsWith, ConditionArgument = "BOBOR", Expression =  "'AAA'"}
            }, expressionEvaluator);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            var header = "Col1,Col2,Col3";
            var row1 = "1,2,3";
            var row2 = "4BBBBOBOR,5,6";
            var row3 = "7,8,9";

            var headerList = CsvParser.GetParts(header);
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new EtlRow() { Source = a }).ToList();
            foreach (var row in rows)
            {
                cleanuper.Cleanup(row);
            }


            Assert.IsFalse(rows[0].IsDeleted);
            Assert.IsFalse(rows[1].IsDeleted);
            Assert.IsFalse(rows[2].IsDeleted);

            Assert.AreEqual(row1, CsvParser.ToCsv(rows[0].Source));
            Assert.AreEqual("4BBBAAA,5,6", CsvParser.ToCsv(rows[1].Source));
            Assert.AreEqual(row3, CsvParser.ToCsv(rows[2].Source));
        }


        [Test]
        public void RegexPlusReplaceMatched()
        {
            var expressionEvaluator = GetMocked();
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.ReplaceMatched, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.Regex, ConditionArgument = "(?<=This is)(.*)(?=sentence)", Expression =  "'AAA'"}
            }, expressionEvaluator);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            var header = "Col1,Col2,Col3";
            var row1 = "1,2,3";
            var row2 = "This is just a simple sentence,5,6";
            var row3 = "7,8,9";

            var headerList = CsvParser.GetParts(header);
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new EtlRow() { Source = a }).ToList();
            foreach (var row in rows)
            {
                cleanuper.Cleanup(row);
            }


            Assert.IsFalse(rows[0].IsDeleted);
            Assert.IsFalse(rows[1].IsDeleted);
            Assert.IsFalse(rows[2].IsDeleted);

            Assert.AreEqual(row1, CsvParser.ToCsv(rows[0].Source));
            Assert.AreEqual("This isAAAsentence,5,6", CsvParser.ToCsv(rows[1].Source));
            Assert.AreEqual(row3, CsvParser.ToCsv(rows[2].Source));
        }


        [Test]
        public void EmptyPlusRemove()
        {
            var expressionEvaluator = GetMocked();
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.Remove, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.Empty, ConditionArgument = "", Expression =  "'AAA'"}
            }, expressionEvaluator);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            var header = "Col1,Col2,Col3";
            var row1 = "1,2,3";
            var row2 = ",5,6";
            var row3 = "7,8,9";

            var headerList = CsvParser.GetParts(header);
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new EtlRow() { Source = a }).ToList();
            foreach (var row in rows)
            {
                cleanuper.Cleanup(row);
            }


            Assert.IsFalse(rows[0].IsDeleted);
            Assert.IsTrue(rows[1].IsDeleted);
            Assert.IsFalse(rows[2].IsDeleted);

            Assert.AreEqual(row1, CsvParser.ToCsv(rows[0].Source));
            Assert.AreEqual(",5,6", CsvParser.ToCsv(rows[1].Source));
            Assert.AreEqual(row3, CsvParser.ToCsv(rows[2].Source));
        }

        [Test]
        public void EqualPlusRemove()
        {
            var expressionEvaluator = GetMocked();
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.Remove, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.Equal, ConditionArgument = "4", Expression =  "AAA"}
            }, expressionEvaluator);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            var header = "Col1,Col2,Col3";
            var row1 = "1,2,3";
            var row2 = "4,5,6";
            var row3 = "7,8,9";

            var headerList = CsvParser.GetParts(header);
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new EtlRow() { Source = a }).ToList();
            foreach (var row in rows)
            {
                cleanuper.Cleanup(row);
            }


            Assert.IsFalse(rows[0].IsDeleted);
            Assert.IsTrue(rows[1].IsDeleted);
            Assert.IsFalse(rows[2].IsDeleted);

            Assert.AreEqual(row1, CsvParser.ToCsv(rows[0].Source));
            Assert.AreEqual("4,5,6", CsvParser.ToCsv(rows[1].Source));
            Assert.AreEqual(row3, CsvParser.ToCsv(rows[2].Source));
        }


        [Test]
        public void StartsWithPlusRemove()
        {
            var expressionEvaluator = GetMocked();
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.Remove, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.StartsWith, ConditionArgument = "4A", Expression =  "'AAA'"}
            }, expressionEvaluator);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            var header = "Col1,Col2,Col3";
            var row1 = "1,2,3";
            var row2 = "4AAA,5,6";
            var row3 = "7,8,9";

            var headerList = CsvParser.GetParts(header);
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new EtlRow() { Source = a }).ToList();
            foreach (var row in rows)
            {
                cleanuper.Cleanup(row);
            }


            Assert.IsFalse(rows[0].IsDeleted);
            Assert.IsTrue(rows[1].IsDeleted);
            Assert.IsFalse(rows[2].IsDeleted);

            Assert.AreEqual(row1, CsvParser.ToCsv(rows[0].Source));
            Assert.AreEqual("4AAA,5,6", CsvParser.ToCsv(rows[1].Source));
            Assert.AreEqual(row3, CsvParser.ToCsv(rows[2].Source));
        }

        [Test]
        public void EndsWithPlusRemove()
        {
            var expressionEvaluator = GetMocked();
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.Remove, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.EndsWith, ConditionArgument = "AAB", Expression =  "'AAA'"}
            }, expressionEvaluator);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            var header = "Col1,Col2,Col3";
            var row1 = "1,2,3";
            var row2 = "4AAAB,5,6";
            var row3 = "7,8,9";

            var headerList = CsvParser.GetParts(header);
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new EtlRow() { Source = a }).ToList();
            foreach (var row in rows)
            {
                cleanuper.Cleanup(row);
            }


            Assert.IsFalse(rows[0].IsDeleted);
            Assert.IsTrue(rows[1].IsDeleted);
            Assert.IsFalse(rows[2].IsDeleted);

            Assert.AreEqual(row1, CsvParser.ToCsv(rows[0].Source));
            Assert.AreEqual("4AAAB,5,6", CsvParser.ToCsv(rows[1].Source));
            Assert.AreEqual(row3, CsvParser.ToCsv(rows[2].Source));
        }


        [Test]
        public void RegexPlusRemove()
        {
            var expressionEvaluator = GetMocked();
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.Remove, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.Regex, ConditionArgument = "(?<=This is)(.*)(?=sentence)", Expression =  "AAA"}
            }, expressionEvaluator);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            var header = "Col1,Col2,Col3";
            var row1 = "1,2,3";
            var row2 = "This is just a simple sentence,5,6";
            var row3 = "7,8,9";

            var headerList = CsvParser.GetParts(header);
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new EtlRow() { Source = a }).ToList();
            foreach (var row in rows)
            {
                cleanuper.Cleanup(row);
            }


            Assert.IsFalse(rows[0].IsDeleted);
            Assert.IsTrue(rows[1].IsDeleted);
            Assert.IsFalse(rows[2].IsDeleted);

            Assert.AreEqual(row1, CsvParser.ToCsv(rows[0].Source));
            Assert.AreEqual("This is just a simple sentence,5,6", CsvParser.ToCsv(rows[1].Source));
            Assert.AreEqual(row3, CsvParser.ToCsv(rows[2].Source));
        }

        [Test]
        public void GetPreviousTest()
        {
            var expressionEvaluator = GetMocked();
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.GetPrevious, ColumnName = "Col2", IsEnabled = true, Condition = CleanupCondition.Empty, ConditionArgument = "", Expression =  ""},
                new CleanupRule(){Action = CleanupAction.GetPrevious, ColumnName = "Col3", IsEnabled = true, Condition = CleanupCondition.Empty, ConditionArgument = "", Expression =  ""}
            }, expressionEvaluator);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            var header = "Col1,Col2,Col3";
            var row1 = "1,2,3";
            var row2 = "4,,6";
            var row3 = "7,8,";

            var headerList = CsvParser.GetParts(header);
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new EtlRow() { Source = a }).ToList();
            foreach (var row in rows)
            {
                cleanuper.Cleanup(row);
            }

            Assert.IsFalse(rows[0].IsDeleted);
            Assert.IsFalse(rows[1].IsDeleted);
            Assert.IsFalse(rows[2].IsDeleted);

            Assert.AreEqual("1,2,3", CsvParser.ToCsv(rows[0].Source));
            Assert.AreEqual("4,2,6", CsvParser.ToCsv(rows[1].Source));
            Assert.AreEqual("7,8,6", CsvParser.ToCsv(rows[2].Source));
        }


        [Test]
        public void EmptyPlusGetPrevious()
        {
            var expressionEvaluator = GetMocked();
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.GetPrevious, ColumnName = "Col2", IsEnabled = true, Condition = CleanupCondition.Empty, ConditionArgument = "", Expression =  ""},
                new CleanupRule(){Action = CleanupAction.GetPrevious, ColumnName = "Col3", IsEnabled = true, Condition = CleanupCondition.Empty, ConditionArgument = "3", Expression =  ""}
            }, expressionEvaluator);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            var header = "Col1,Col2,Col3";
            var row1 = "1,2,3";
            var row2 = "4,,6";
            var row3 = "7,8,";

            var headerList = CsvParser.GetParts(header);
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new EtlRow() { Source = a }).ToList();
            foreach (var row in rows)
            {
                cleanuper.Cleanup(row);
            }

            Assert.IsFalse(rows[0].IsDeleted);
            Assert.IsFalse(rows[1].IsDeleted);
            Assert.IsFalse(rows[2].IsDeleted);

            Assert.AreEqual("1,2,3", CsvParser.ToCsv(rows[0].Source));
            Assert.AreEqual("4,2,6", CsvParser.ToCsv(rows[1].Source));
            Assert.AreEqual("7,8,6", CsvParser.ToCsv(rows[2].Source));
        }

        [Test]
        public void EqualsPlusGetPrevious()
        {
            var expressionEvaluator = GetMocked();
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.GetPrevious, ColumnName = "Col2", IsEnabled = true, Condition = CleanupCondition.Equal, ConditionArgument = "", Expression =  ""},
                new CleanupRule(){Action = CleanupAction.GetPrevious, ColumnName = "Col3", IsEnabled = true, Condition = CleanupCondition.Equal, ConditionArgument = "3", Expression =  ""}
            }, expressionEvaluator);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            var header = "Col1,Col2,Col3";
            var row1 = "1,2,3";
            var row2 = "4,,6";
            var row3 = "7,8,";

            var headerList = CsvParser.GetParts(header);
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new EtlRow() { Source = a }).ToList();
            foreach (var row in rows)
            {
                cleanuper.Cleanup(row);
            }

            Assert.IsFalse(rows[0].IsDeleted);
            Assert.IsFalse(rows[1].IsDeleted);
            Assert.IsFalse(rows[2].IsDeleted);

            Assert.AreEqual("1,2,3", CsvParser.ToCsv(rows[0].Source));
            Assert.AreEqual("4,2,6", CsvParser.ToCsv(rows[1].Source));
            Assert.AreEqual("7,8,6", CsvParser.ToCsv(rows[2].Source));
        }

        [Test]
        public void StartsWithPlusGetPrevious()
        {
            var expressionEvaluator = GetMocked();
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.GetPrevious, ColumnName = "Col2", IsEnabled = true, Condition = CleanupCondition.StartsWith, ConditionArgument = "", Expression =  ""},
                new CleanupRule(){Action = CleanupAction.GetPrevious, ColumnName = "Col3", IsEnabled = true, Condition = CleanupCondition.StartsWith, ConditionArgument = "3", Expression =  ""}
            }, expressionEvaluator);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            var header = "Col1,Col2,Col3";
            var row1 = "1,2,3";
            var row2 = "4,,6";
            var row3 = "7,8,";

            var headerList = CsvParser.GetParts(header);
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new EtlRow() { Source = a }).ToList();
            foreach (var row in rows)
            {
                cleanuper.Cleanup(row);
            }

            Assert.IsFalse(rows[0].IsDeleted);
            Assert.IsFalse(rows[1].IsDeleted);
            Assert.IsFalse(rows[2].IsDeleted);

            Assert.AreEqual("1,2,3", CsvParser.ToCsv(rows[0].Source));
            Assert.AreEqual("4,2,6", CsvParser.ToCsv(rows[1].Source));
            Assert.AreEqual("7,8,6", CsvParser.ToCsv(rows[2].Source));
        }

        [Test]
        public void EndsWithPlusGetPrevious()
        {
            var expressionEvaluator = GetMocked();
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.GetPrevious, ColumnName = "Col2", IsEnabled = true, Condition = CleanupCondition.EndsWith, ConditionArgument = "", Expression =  ""},
                new CleanupRule(){Action = CleanupAction.GetPrevious, ColumnName = "Col3", IsEnabled = true, Condition = CleanupCondition.EndsWith, ConditionArgument = "3", Expression =  ""}
            }, expressionEvaluator);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            var header = "Col1,Col2,Col3";
            var row1 = "1,2,3";
            var row2 = "4,,6";
            var row3 = "7,8,";

            var headerList = CsvParser.GetParts(header);
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new EtlRow() { Source = a }).ToList();
            foreach (var row in rows)
            {
                cleanuper.Cleanup(row);
            }

            Assert.IsFalse(rows[0].IsDeleted);
            Assert.IsFalse(rows[1].IsDeleted);
            Assert.IsFalse(rows[2].IsDeleted);

            Assert.AreEqual("1,2,3", CsvParser.ToCsv(rows[0].Source));
            Assert.AreEqual("4,2,6", CsvParser.ToCsv(rows[1].Source));
            Assert.AreEqual("7,8,6", CsvParser.ToCsv(rows[2].Source));
        }


        [Test]
        public void RegexPlusGetPrevious()
        {
            var expressionEvaluator = GetMocked();
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.GetPrevious, ColumnName = "Col2", IsEnabled = true, Condition = CleanupCondition.Regex, ConditionArgument = "", Expression =  ""},
                new CleanupRule(){Action = CleanupAction.GetPrevious, ColumnName = "Col3", IsEnabled = true, Condition = CleanupCondition.Regex, ConditionArgument = "3", Expression =  ""}
            }, expressionEvaluator);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            var header = "Col1,Col2,Col3";
            var row1 = "1,2,3";
            var row2 = "4,,6";
            var row3 = "7,8,";

            var headerList = CsvParser.GetParts(header);
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new EtlRow() { Source = a }).ToList();
            foreach (var row in rows)
            {
                cleanuper.Cleanup(row);
            }

            Assert.IsFalse(rows[0].IsDeleted);
            Assert.IsFalse(rows[1].IsDeleted);
            Assert.IsFalse(rows[2].IsDeleted);

            Assert.AreEqual("1,2,3", CsvParser.ToCsv(rows[0].Source));
            Assert.AreEqual("4,2,6", CsvParser.ToCsv(rows[1].Source));
            Assert.AreEqual("7,8,6", CsvParser.ToCsv(rows[2].Source));
        }
        [Test]
        public void EmptyPlusStartLoad()
        {
            var expressionEvaluator = GetMocked();
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.StartLoad, ColumnName = "Col3", IsEnabled = true, Condition = CleanupCondition.Empty, ConditionArgument = "", Expression =  ""},
            },expressionEvaluator);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            var header = "Col1,Col2,Col3";
            var row1 = "1,2,3";
            var row2 = "4,5,";
            var row3 = "7,8,9";

            var headerList = CsvParser.GetParts(header);
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new EtlRow() { Source = a }).ToList();
            foreach (var row in rows)
            {
                cleanuper.Cleanup(row);
            }

            Assert.IsTrue(rows[0].IsDeleted);
            Assert.IsFalse(rows[1].IsDeleted);
            Assert.IsFalse(rows[2].IsDeleted);

            Assert.AreEqual("1,2,3", CsvParser.ToCsv(rows[0].Source));
            Assert.AreEqual("4,5,", CsvParser.ToCsv(rows[1].Source));
            Assert.AreEqual("7,8,9", CsvParser.ToCsv(rows[2].Source));
        }

        [Test]
        public void EqualsPlusStartLoad()
        {
            var expressionEvaluator = GetMocked();
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.StartLoad, ColumnName = "Col3", IsEnabled = true, Condition = CleanupCondition.Equal, ConditionArgument = "6", Expression =  ""},
            }, expressionEvaluator);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            var header = "Col1,Col2,Col3";
            var row1 = "1,2,3";
            var row2 = "4,5,6";
            var row3 = "7,8,";

            var headerList = CsvParser.GetParts(header);
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new EtlRow() { Source = a }).ToList();
            foreach (var row in rows)
            {
                cleanuper.Cleanup(row);
            }

            Assert.IsTrue(rows[0].IsDeleted);
            Assert.IsFalse(rows[1].IsDeleted);
            Assert.IsFalse(rows[2].IsDeleted);

            Assert.AreEqual("1,2,3", CsvParser.ToCsv(rows[0].Source));
            Assert.AreEqual("4,5,6", CsvParser.ToCsv(rows[1].Source));
            Assert.AreEqual("7,8,", CsvParser.ToCsv(rows[2].Source));
        }

        [Test]
        public void StartsWithPlusStartLoad()
        {
            var expressionEvaluator = GetMocked();
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.StartLoad, ColumnName = "Col3", IsEnabled = true, Condition = CleanupCondition.StartsWith, ConditionArgument = "6", Expression =  ""},
            }, expressionEvaluator);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            var header = "Col1,Col2,Col3";
            var row1 = "1,2,3";
            var row2 = "4,5,6a";
            var row3 = "7,8,";

            var headerList = CsvParser.GetParts(header);
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new EtlRow() { Source = a }).ToList();
            foreach (var row in rows)
            {
                cleanuper.Cleanup(row);
            }

            Assert.IsTrue(rows[0].IsDeleted);
            Assert.IsFalse(rows[1].IsDeleted);
            Assert.IsFalse(rows[2].IsDeleted);

            Assert.AreEqual("1,2,3", CsvParser.ToCsv(rows[0].Source));
            Assert.AreEqual("4,5,6a", CsvParser.ToCsv(rows[1].Source));
            Assert.AreEqual("7,8,", CsvParser.ToCsv(rows[2].Source));
        }

        [Test]
        public void EndsWithPlusStartLoad()
        {
            var expressionEvaluator = GetMocked();
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.StartLoad, ColumnName = "Col3", IsEnabled = true, Condition = CleanupCondition.EndsWith, ConditionArgument = "a", Expression =  ""},
            }, expressionEvaluator);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            var header = "Col1,Col2,Col3";
            var row1 = "1,2,3";
            var row2 = "4,5,6a";
            var row3 = "7,8,";

            var headerList = CsvParser.GetParts(header);
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new EtlRow() { Source = a }).ToList();
            foreach (var row in rows)
            {
                cleanuper.Cleanup(row);
            }

            Assert.IsTrue(rows[0].IsDeleted);
            Assert.IsFalse(rows[1].IsDeleted);
            Assert.IsFalse(rows[2].IsDeleted);

            Assert.AreEqual("1,2,3", CsvParser.ToCsv(rows[0].Source));
            Assert.AreEqual("4,5,6a", CsvParser.ToCsv(rows[1].Source));
            Assert.AreEqual("7,8,", CsvParser.ToCsv(rows[2].Source));
        }


        [Test]
        public void RegexPlusStartLoad()
        {
            var expressionEvaluator = GetMocked();
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.StartLoad, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.Regex, ConditionArgument = "(?<=This is)(.*)(?=sentence)", Expression =  "AAA"}
            }, expressionEvaluator);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            var header = "Col1,Col2,Col3";
            var row1 = "1,2,3";
            var row2 = "This is just a simple sentence,5,6";
            var row3 = "7,8,9";

            var headerList = CsvParser.GetParts(header);
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new EtlRow() { Source = a }).ToList();
            foreach (var row in rows)
            {
                cleanuper.Cleanup(row);
            }


            Assert.IsTrue(rows[0].IsDeleted);
            Assert.IsFalse(rows[1].IsDeleted);
            Assert.IsFalse(rows[2].IsDeleted);

            Assert.AreEqual(row1, CsvParser.ToCsv(rows[0].Source));
            Assert.AreEqual("This is just a simple sentence,5,6", CsvParser.ToCsv(rows[1].Source));
            Assert.AreEqual(row3, CsvParser.ToCsv(rows[2].Source));
        }



        [Test]
        public void EmptyPlusStopLoad()
        {
            var expressionEvaluator = GetMocked();
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.StopLoad, ColumnName = "Col3", IsEnabled = true, Condition = CleanupCondition.Empty, ConditionArgument = "", Expression =  ""},
            }, expressionEvaluator);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            var header = "Col1,Col2,Col3";
            var row1 = "1,2,3";
            var row2 = "4,5,";
            var row3 = "7,8,9";

            var headerList = CsvParser.GetParts(header);
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new EtlRow() { Source = a }).ToList();
            foreach (var row in rows)
            {
                cleanuper.Cleanup(row);
            }

            Assert.IsFalse(rows[0].IsDeleted);
            Assert.IsTrue(rows[1].IsDeleted);
            Assert.IsTrue(rows[2].IsDeleted);

            Assert.AreEqual("1,2,3", CsvParser.ToCsv(rows[0].Source));
            Assert.AreEqual("4,5,", CsvParser.ToCsv(rows[1].Source));
            Assert.AreEqual("7,8,9", CsvParser.ToCsv(rows[2].Source));
        }

        [Test]
        public void EqualPlusStopLoad()
        {
            var expressionEvaluator = GetMocked();
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.StopLoad, ColumnName = "Col3", IsEnabled = true, Condition = CleanupCondition.Equal, ConditionArgument = "6", Expression =  ""},
            }, expressionEvaluator);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            var header = "Col1,Col2,Col3";
            var row1 = "1,2,3";
            var row2 = "4,5,6";
            var row3 = "7,8,9";

            var headerList = CsvParser.GetParts(header);
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new EtlRow() { Source = a }).ToList();
            foreach (var row in rows)
            {
                cleanuper.Cleanup(row);
            }

            Assert.IsFalse(rows[0].IsDeleted);
            Assert.IsTrue(rows[1].IsDeleted);
            Assert.IsTrue(rows[2].IsDeleted);

            Assert.AreEqual("1,2,3", CsvParser.ToCsv(rows[0].Source));
            Assert.AreEqual("4,5,6", CsvParser.ToCsv(rows[1].Source));
            Assert.AreEqual("7,8,9", CsvParser.ToCsv(rows[2].Source));
        }

        [Test]
        public void StartsWithPlusStopLoad()
        {
            var expressionEvaluator = GetMocked();
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.StopLoad, ColumnName = "Col3", IsEnabled = true, Condition = CleanupCondition.StartsWith, ConditionArgument = "6", Expression =  ""},
            }, expressionEvaluator);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            var header = "Col1,Col2,Col3";
            var row1 = "1,2,3";
            var row2 = "4,5,6A";
            var row3 = "7,8,9";

            var headerList = CsvParser.GetParts(header);
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new EtlRow() { Source = a }).ToList();
            foreach (var row in rows)
            {
                cleanuper.Cleanup(row);
            }

            Assert.IsFalse(rows[0].IsDeleted);
            Assert.IsTrue(rows[1].IsDeleted);
            Assert.IsTrue(rows[2].IsDeleted);

            Assert.AreEqual("1,2,3", CsvParser.ToCsv(rows[0].Source));
            Assert.AreEqual("4,5,6A", CsvParser.ToCsv(rows[1].Source));
            Assert.AreEqual("7,8,9", CsvParser.ToCsv(rows[2].Source));
        }

        [Test]
        public void EndsWithPlusStopLoad()
        {
            var expressionEvaluator = GetMocked();
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.StopLoad, ColumnName = "Col3", IsEnabled = true, Condition = CleanupCondition.EndsWith, ConditionArgument = "A", Expression =  ""},
            }, expressionEvaluator);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            var header = "Col1,Col2,Col3";
            var row1 = "1,2,3";
            var row2 = "4,5,6A";
            var row3 = "7,8,9";

            var headerList = CsvParser.GetParts(header);
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new EtlRow() { Source = a }).ToList();
            foreach (var row in rows)
            {
                cleanuper.Cleanup(row);
            }

            Assert.IsFalse(rows[0].IsDeleted);
            Assert.IsTrue(rows[1].IsDeleted);
            Assert.IsTrue(rows[2].IsDeleted);

            Assert.AreEqual("1,2,3", CsvParser.ToCsv(rows[0].Source));
            Assert.AreEqual("4,5,6A", CsvParser.ToCsv(rows[1].Source));
            Assert.AreEqual("7,8,9", CsvParser.ToCsv(rows[2].Source));
        }



        [Test]
        public void RegexPlusStopLoad()
        {
            var expressionEvaluator = GetMocked();
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.StopLoad, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.Regex, ConditionArgument = "(?<=This is)(.*)(?=sentence)", Expression =  "AAA"}
            }, expressionEvaluator);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            var header = "Col1,Col2,Col3";
            var row1 = "1,2,3";
            var row2 = "This is just a simple sentence,5,6";
            var row3 = "7,8,9";

            var headerList = CsvParser.GetParts(header);
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new EtlRow() { Source = a }).ToList();
            foreach (var row in rows)
            {
                cleanuper.Cleanup(row);
            }


            Assert.IsFalse(rows[0].IsDeleted);
            Assert.IsTrue(rows[1].IsDeleted);
            Assert.IsTrue(rows[2].IsDeleted);

            Assert.AreEqual(row1, CsvParser.ToCsv(rows[0].Source));
            Assert.AreEqual("This is just a simple sentence,5,6", CsvParser.ToCsv(rows[1].Source));
            Assert.AreEqual(row3, CsvParser.ToCsv(rows[2].Source));
        }


        [Test]
        public void EmptyPlusStartLoadExclude()
        {
            var expressionEvaluator = GetMocked();
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.StartLoadExclude, ColumnName = "Col3", IsEnabled = true, Condition = CleanupCondition.Empty, ConditionArgument = "", Expression =  ""},
            }, expressionEvaluator);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            var header = "Col1,Col2,Col3";
            var row1 = "1,2,3";
            var row2 = "4,5,";
            var row3 = "7,8,9";

            var headerList = CsvParser.GetParts(header);
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new EtlRow() { Source = a }).ToList();
            foreach (var row in rows)
            {
                cleanuper.Cleanup(row);
            }

            Assert.IsTrue(rows[0].IsDeleted);
            Assert.IsTrue(rows[1].IsDeleted);
            Assert.IsFalse(rows[2].IsDeleted);

            Assert.AreEqual("1,2,3", CsvParser.ToCsv(rows[0].Source));
            Assert.AreEqual("4,5,", CsvParser.ToCsv(rows[1].Source));
            Assert.AreEqual("7,8,9", CsvParser.ToCsv(rows[2].Source));
        }

        [Test]
        public void EqualsPlusStartLoadExclude()
        {
            var expressionEvaluator = GetMocked();
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.StartLoadExclude, ColumnName = "Col3", IsEnabled = true, Condition = CleanupCondition.Equal, ConditionArgument = "6", Expression =  ""},
            }, expressionEvaluator);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            var header = "Col1,Col2,Col3";
            var row1 = "1,2,3";
            var row2 = "4,5,6";
            var row3 = "7,8,";

            var headerList = CsvParser.GetParts(header);
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new EtlRow() { Source = a }).ToList();
            foreach (var row in rows)
            {
                cleanuper.Cleanup(row);
            }

            Assert.IsTrue(rows[0].IsDeleted);
            Assert.IsTrue(rows[1].IsDeleted);
            Assert.IsFalse(rows[2].IsDeleted);

            Assert.AreEqual("1,2,3", CsvParser.ToCsv(rows[0].Source));
            Assert.AreEqual("4,5,6", CsvParser.ToCsv(rows[1].Source));
            Assert.AreEqual("7,8,", CsvParser.ToCsv(rows[2].Source));
        }

        [Test]
        public void StartsWithPlusStartLoadExclude()
        {
            var expressionEvaluator = GetMocked();
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.StartLoadExclude, ColumnName = "Col3", IsEnabled = true, Condition = CleanupCondition.StartsWith, ConditionArgument = "6", Expression =  ""},
            }, expressionEvaluator);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            var header = "Col1,Col2,Col3";
            var row1 = "1,2,3";
            var row2 = "4,5,6a";
            var row3 = "7,8,";

            var headerList = CsvParser.GetParts(header);
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new EtlRow() { Source = a }).ToList();
            foreach (var row in rows)
            {
                cleanuper.Cleanup(row);
            }

            Assert.IsTrue(rows[0].IsDeleted);
            Assert.IsTrue(rows[1].IsDeleted);
            Assert.IsFalse(rows[2].IsDeleted);

            Assert.AreEqual("1,2,3", CsvParser.ToCsv(rows[0].Source));
            Assert.AreEqual("4,5,6a", CsvParser.ToCsv(rows[1].Source));
            Assert.AreEqual("7,8,", CsvParser.ToCsv(rows[2].Source));
        }

        [Test]
        public void EndsWithPlusStartLoadExclude()
        {
            var expressionEvaluator = GetMocked();
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.StartLoadExclude, ColumnName = "Col3", IsEnabled = true, Condition = CleanupCondition.EndsWith, ConditionArgument = "a", Expression =  ""},
            }, expressionEvaluator);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            var header = "Col1,Col2,Col3";
            var row1 = "1,2,3";
            var row2 = "4,5,6a";
            var row3 = "7,8,";

            var headerList = CsvParser.GetParts(header);
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new EtlRow() { Source = a }).ToList();
            foreach (var row in rows)
            {
                cleanuper.Cleanup(row);
            }

            Assert.IsTrue(rows[0].IsDeleted);
            Assert.IsTrue(rows[1].IsDeleted);
            Assert.IsFalse(rows[2].IsDeleted);

            Assert.AreEqual("1,2,3", CsvParser.ToCsv(rows[0].Source));
            Assert.AreEqual("4,5,6a", CsvParser.ToCsv(rows[1].Source));
            Assert.AreEqual("7,8,", CsvParser.ToCsv(rows[2].Source));
        }


        [Test]
        public void RegexPlusStartLoadExclude()
        {
            var expressionEvaluator = GetMocked();
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.StartLoadExclude, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.Regex, ConditionArgument = "(?<=This is)(.*)(?=sentence)", Expression =  "AAA"}
            }, expressionEvaluator);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            var header = "Col1,Col2,Col3";
            var row1 = "1,2,3";
            var row2 = "This is just a simple sentence,5,6";
            var row3 = "7,8,9";

            var headerList = CsvParser.GetParts(header);
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new EtlRow() { Source = a }).ToList();
            foreach (var row in rows)
            {
                cleanuper.Cleanup(row);
            }


            Assert.IsTrue(rows[0].IsDeleted);
            Assert.IsTrue(rows[1].IsDeleted);
            Assert.IsFalse(rows[2].IsDeleted);

            Assert.AreEqual(row1, CsvParser.ToCsv(rows[0].Source));
            Assert.AreEqual("This is just a simple sentence,5,6", CsvParser.ToCsv(rows[1].Source));
            Assert.AreEqual(row3, CsvParser.ToCsv(rows[2].Source));
        }

        [Test]
        public void StartLoadAndStopLoadForTheSingleColumn()
        {
            var expressionEvaluator = GetMocked();
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.StartLoad, ColumnName = "Col3", IsEnabled = true, Condition = CleanupCondition.Equal, ConditionArgument = "3", Expression =  ""},
                new CleanupRule(){Action = CleanupAction.StopLoad, ColumnName = "Col3", IsEnabled = true, Condition = CleanupCondition.Empty, ConditionArgument = "", Expression =  ""},
            }, expressionEvaluator);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            var header = "Col1,Col2,Col3";
            var row1 = "1,2,3";
            var row2 = "4,5,";
            var row3 = "7,8,9";

            var headerList = CsvParser.GetParts(header);
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new EtlRow() { Source = a }).ToList();
            foreach (var row in rows)
            {
                cleanuper.Cleanup(row);
            }

            Assert.IsFalse(rows[0].IsDeleted);
            Assert.IsTrue(rows[1].IsDeleted);
            Assert.IsTrue(rows[2].IsDeleted);

            Assert.AreEqual("1,2,3", CsvParser.ToCsv(rows[0].Source));
            Assert.AreEqual("4,5,", CsvParser.ToCsv(rows[1].Source));
            Assert.AreEqual("7,8,9", CsvParser.ToCsv(rows[2].Source));
        }

        //TODO come up with a warning
        [Test]
        public void StartLoadAndStopLoadForTheSingleColumnOneAndSingleRow()
        {
            var expressionEvaluator = GetMocked();
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.StartLoad, ColumnName = "Col3", IsEnabled = true, Condition = CleanupCondition.Equal, ConditionArgument = "3", Expression = ""},
                new CleanupRule(){Action = CleanupAction.StopLoad, ColumnName = "Col3", IsEnabled = true, Condition = CleanupCondition.Equal, ConditionArgument = "3", Expression = ""},
            }, expressionEvaluator);
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            var header = "Col1,Col2,Col3";
            var row1 = "1,2,3";
            var row2 = "4,5,";
            var row3 = "7,8,9";

            var headerList = CsvParser.GetParts(header);
            list.Add(CsvParser.Parse(row1, headerList));
            list.Add(CsvParser.Parse(row2, headerList));
            list.Add(CsvParser.Parse(row3, headerList));

            var rows = list.Select(a => new EtlRow() { Source = a }).ToList();
            foreach (var row in rows)
            {
                cleanuper.Cleanup(row);
            }

            Assert.IsTrue(rows[0].IsDeleted);
            Assert.IsTrue(rows[1].IsDeleted);
            Assert.IsTrue(rows[2].IsDeleted);

            Assert.AreEqual("1,2,3", CsvParser.ToCsv(rows[0].Source));
            Assert.AreEqual("4,5,", CsvParser.ToCsv(rows[1].Source));
            Assert.AreEqual("7,8,9", CsvParser.ToCsv(rows[2].Source));
        }


        private static IExpressionEvaluator GetMocked()
        {
            return new TestExpressionEvaluator();
        }

        public class TestExpressionEvaluator: IExpressionEvaluator
        {
            public Dictionary<string, object> Variables { get; set; } = new Dictionary<string, object>();
            public Dictionary<string, object> Evaluations { get; set; } = new Dictionary<string, object>();
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
                return Evaluations[expression];
            }
        }
    }
}
