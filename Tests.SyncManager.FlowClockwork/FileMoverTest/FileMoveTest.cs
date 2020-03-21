using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using SyncManager.FlowClockwork;

namespace Tests.SyncManager.FlowClockwork.FileMoverTest
{
    [TestFixture]
    public class FileMoveTest
    {
        [Test]
        public void Run()
        {
            var input = @"C:\Temp\My\1.txt";
            var output = @"C:\Temp\My\2.txt";
            var error = @"C:\Temp\My\errors.txt";
            using (StreamWriter sw = new StreamWriter(input))
            {
                for (int i = 0; i < 100000; i++)
                {
                    if (i % 3000 == 0)
                    {
                        sw.WriteLine($"POOP{i}");
                    }
                    else
                    {
                        sw.WriteLine(i);
                    }
               
                }
            }
            var nodeRegistry = new NodeRegistry<FileMoverData>();
            nodeRegistry.Register("FileReader", new BaseNode<FileMoverData>(new FileReader(input), "FileReader"));
            nodeRegistry.Register("FileWriter", new BaseNode<FileMoverData>(new FileWriter(output), "FileWriter"));
            nodeRegistry.Register("ErrorWriter", new BaseNode<FileMoverData>(new ErrorWriter(error), "ErrorWriter"));

            var provider = new NodeDefinitionProvider("FileWriter");
            provider.Register("FileReader", new NodeDefinition(){Name = "FileReader"});
            provider.Register("FileWriter", new NodeDefinition(){
                Name = "FileWriter" , DependsOn = new List<string>()
            {
                "ErrorWriter"
            }
            });
            provider.Register("ErrorWriter", new NodeDefinition()
            {
                Name = "ErrorWriter",
                DependsOn = new List<string>()
                {
                    "FileReader"
                }
            });
            var runner = new DataNodeRunner<FileMoverData>(provider, nodeRegistry);
            runner.Run();
            runner.Dispose();
        }
    }
}