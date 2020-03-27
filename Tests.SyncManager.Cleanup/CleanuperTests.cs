using System;
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
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                //new CleanupRule(){Action = CleanupAction.Remove, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.Equal, ConditionArgument = "4"}
            });

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
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.Remove, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.Equal, ConditionArgument = "4"}
            });

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
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.Replace, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.Equal, ConditionArgument = "444", Value = "AAA"}
            });

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
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.ReplaceMatched, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.Contains, ConditionArgument = "44", Value = "AAA"}
            });

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
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.Replace, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.Contains, ConditionArgument = "44", Value = "AAA"}
            });

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
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.Replace, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.Empty, ConditionArgument = "", Value = "AAA"}
            });
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
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.Replace, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.Empty, ConditionArgument = "", Value = "AAA"}
            });
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
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.Replace, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.Equal, ConditionArgument = "4", Value = "AAA"}
            });
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
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.Replace, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.StartsWith, ConditionArgument = "4BBB", Value = "AAA"}
            });
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
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.Replace, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.EndsWith, ConditionArgument = "BOBOR", Value = "AAA"}
            });
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
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.Replace, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.Regex, ConditionArgument = "(?<=This is)(.*)(?=sentence)", Value = "AAA"}
            });
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
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.ReplaceMatched, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.Empty, ConditionArgument = "", Value = "AAA"}
            });
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
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.ReplaceMatched, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.Empty, ConditionArgument = "", Value = "AAA"}
            });
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
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.ReplaceMatched, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.Equal, ConditionArgument = "4", Value = "AAA"}
            });
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
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.ReplaceMatched, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.StartsWith, ConditionArgument = "4BBB", Value = "AAA"}
            });
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
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.ReplaceMatched, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.EndsWith, ConditionArgument = "BOBOR", Value = "AAA"}
            });
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
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.ReplaceMatched, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.Regex, ConditionArgument = "(?<=This is)(.*)(?=sentence)", Value = "AAA"}
            });
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
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.Remove, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.Empty, ConditionArgument = "", Value = "AAA"}
            });
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
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.Remove, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.Equal, ConditionArgument = "4", Value = "AAA"}
            });
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
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.Remove, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.StartsWith, ConditionArgument = "4A", Value = "AAA"}
            });
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
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.Remove, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.EndsWith, ConditionArgument = "AAB", Value = "AAA"}
            });
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
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.Remove, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.Regex, ConditionArgument = "(?<=This is)(.*)(?=sentence)", Value = "AAA"}
            });
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
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.GetPrevious, ColumnName = "Col2", IsEnabled = true, Condition = CleanupCondition.Empty, ConditionArgument = "", Value = ""},
                new CleanupRule(){Action = CleanupAction.GetPrevious, ColumnName = "Col3", IsEnabled = true, Condition = CleanupCondition.Empty, ConditionArgument = "", Value = ""}
            });
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
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.GetPrevious, ColumnName = "Col2", IsEnabled = true, Condition = CleanupCondition.Empty, ConditionArgument = "", Value = ""},
                new CleanupRule(){Action = CleanupAction.GetPrevious, ColumnName = "Col3", IsEnabled = true, Condition = CleanupCondition.Empty, ConditionArgument = "3", Value = ""}
            });
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
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.GetPrevious, ColumnName = "Col2", IsEnabled = true, Condition = CleanupCondition.Equal, ConditionArgument = "", Value = ""},
                new CleanupRule(){Action = CleanupAction.GetPrevious, ColumnName = "Col3", IsEnabled = true, Condition = CleanupCondition.Equal, ConditionArgument = "3", Value = ""}
            });
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
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.GetPrevious, ColumnName = "Col2", IsEnabled = true, Condition = CleanupCondition.StartsWith, ConditionArgument = "", Value = ""},
                new CleanupRule(){Action = CleanupAction.GetPrevious, ColumnName = "Col3", IsEnabled = true, Condition = CleanupCondition.StartsWith, ConditionArgument = "3", Value = ""}
            });
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
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.GetPrevious, ColumnName = "Col2", IsEnabled = true, Condition = CleanupCondition.EndsWith, ConditionArgument = "", Value = ""},
                new CleanupRule(){Action = CleanupAction.GetPrevious, ColumnName = "Col3", IsEnabled = true, Condition = CleanupCondition.EndsWith, ConditionArgument = "3", Value = ""}
            });
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
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.GetPrevious, ColumnName = "Col2", IsEnabled = true, Condition = CleanupCondition.Regex, ConditionArgument = "", Value = ""},
                new CleanupRule(){Action = CleanupAction.GetPrevious, ColumnName = "Col3", IsEnabled = true, Condition = CleanupCondition.Regex, ConditionArgument = "3", Value = ""}
            });
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
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.StartLoad, ColumnName = "Col3", IsEnabled = true, Condition = CleanupCondition.Empty, ConditionArgument = "", Value = ""},
            });
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
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.StartLoad, ColumnName = "Col3", IsEnabled = true, Condition = CleanupCondition.Equal, ConditionArgument = "6", Value = ""},
            });
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
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.StartLoad, ColumnName = "Col3", IsEnabled = true, Condition = CleanupCondition.StartsWith, ConditionArgument = "6", Value = ""},
            });
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
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.StartLoad, ColumnName = "Col3", IsEnabled = true, Condition = CleanupCondition.EndsWith, ConditionArgument = "a", Value = ""},
            });
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
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.StartLoad, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.Regex, ConditionArgument = "(?<=This is)(.*)(?=sentence)", Value = "AAA"}
            });
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
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.StopLoad, ColumnName = "Col3", IsEnabled = true, Condition = CleanupCondition.Empty, ConditionArgument = "", Value = ""},
            });
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
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.StopLoad, ColumnName = "Col3", IsEnabled = true, Condition = CleanupCondition.Equal, ConditionArgument = "6", Value = ""},
            });
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
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.StopLoad, ColumnName = "Col3", IsEnabled = true, Condition = CleanupCondition.StartsWith, ConditionArgument = "6", Value = ""},
            });
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
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.StopLoad, ColumnName = "Col3", IsEnabled = true, Condition = CleanupCondition.EndsWith, ConditionArgument = "A", Value = ""},
            });
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
            Cleanuper cleanuper = new Cleanuper(new List<CleanupRule>()
            {
                new CleanupRule(){Action = CleanupAction.StopLoad, ColumnName = "Col1", IsEnabled = true, Condition = CleanupCondition.Regex, ConditionArgument = "(?<=This is)(.*)(?=sentence)", Value = "AAA"}
            });
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




    }
}
