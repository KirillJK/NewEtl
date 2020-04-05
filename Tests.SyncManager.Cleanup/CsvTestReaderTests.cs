using System;
using System.IO;
using NUnit.Framework;
using SyncManager.FlowEtl.Common;
using SyncManager.Readers;

namespace Tests.SyncManager.NewSteps
{
    [TestFixture]
    public class CsvTestReaderTests
    {
        [Test]
        [Ignore("Just to test csv reader")]
        public void Do()
        {
            var path = @"D:\Temp\data.csv";
            var context = new SourceContext();
            using (var reader = new CsvReader(path))
            {
                while (!reader.IsEnd)
                {
                    reader.Read(context);
                }
            }
            
        }
    }
}