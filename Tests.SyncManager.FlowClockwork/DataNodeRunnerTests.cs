using System;
using System.Collections.Generic;
using NUnit.Framework;
using SyncManager.FlowClockwork;

namespace Tests.SyncManager.FlowClockwork
{
    [TestFixture]
    public class DataNodeRunnerTests
    {
        private class TestCounterWrapper
        {
            public int Counter { get; set; }
        }


        private class TestFlag
        {
            public bool Flag;
        }

        [Test]
        public void ABBCSimpleWhenANodeShouldntBeCalledTwice()
        {
            var nodeRegistry = new NodeRegistry<string>();
            nodeRegistry.Register("A", new BaseNode<string>(new TestActionProcess(p => { p.Item = "AAA"; }), "A"));
            nodeRegistry.Register("B",
                new BaseNode<string>(new TestActionProcess(p => { p.Item = p.Item += "BBB"; }), "B"));
            nodeRegistry.Register("C",
                new BaseNode<string>(new TestActionProcess(p => { p.Item = p.Item += "CCC"; }), "C"));
            var definitionC = new NodeDefinition
            {
                Name = "C",
                DependsOn = new List<string>
                {
                    "B",
                    "A"
                }
            };

            var definitionB = new NodeDefinition
            {
                Name = "B",
                DependsOn = new List<string>
                {
                    "A"
                }
            };

            var definitionA = new NodeDefinition
            {
                Name = "A"
            };
            var provider = new NodeDefinitionProvider("C");
            provider.Register("C", definitionC);
            provider.Register("B", definitionB);
            provider.Register("A", definitionA);

            var runner = new DataNodeRunner<string>(provider, nodeRegistry);
            var wrapper = new DataItemWrapper<string>();
            runner.Step(wrapper);
            Assert.AreEqual("AAABBBCCC", wrapper.Item);
        }

        [Test]
        public void ABCSimple()
        {
            var nodeRegistry = new NodeRegistry<string>();
            nodeRegistry.Register("A", new BaseNode<string>(new TestActionProcess(p => { p.Item = "AAA"; }), "A"));
            nodeRegistry.Register("B",
                new BaseNode<string>(new TestActionProcess(p => { p.Item = p.Item += "BBB"; }), "B"));
            nodeRegistry.Register("C",
                new BaseNode<string>(new TestActionProcess(p => { p.Item = p.Item += "CCC"; }), "C"));
            var definitionC = new NodeDefinition
            {
                Name = "C",
                DependsOn = new List<string>
                {
                    "B"
                }
            };

            var definitionB = new NodeDefinition
            {
                Name = "B",
                DependsOn = new List<string>
                {
                    "A"
                }
            };

            var definitionA = new NodeDefinition
            {
                Name = "A"
            };
            var provider = new NodeDefinitionProvider("C");
            provider.Register("C", definitionC);
            provider.Register("B", definitionB);
            provider.Register("A", definitionA);

            var runner = new DataNodeRunner<string>(provider, nodeRegistry);
            var wrapper = new DataItemWrapper<string>();
            runner.Step(wrapper);
            Assert.AreEqual("AAABBBCCC", wrapper.Item);
        }

        [Test]
        public void CommitTest()
        {
            var nodeRegistry = new NodeRegistry<string>();
            var flag = new TestFlag();
            nodeRegistry.Register("A", new BaseNode<string>(new TestActionProcess(p =>
            {
                if (p.Number < 3)
                    p.Item = $"AAA{p.Number}";
                if (p.Number == 2)
                    p.Stop();
            }, () => { flag.Flag = true; }), "A"));
            nodeRegistry.Register("B",
                new BaseNode<string>(new TestActionProcess(p => { p.Item = p.Item += "BBB"; }), "B"));
            nodeRegistry.Register("C",
                new BaseNode<string>(new TestActionProcess(p => { p.Item = p.Item += "CCC"; }), "C"));
            var definitionC = new NodeDefinition
            {
                Name = "C",
                DependsOn = new List<string>
                {
                    "B"
                }
            };

            var definitionB = new NodeDefinition
            {
                Name = "B",
                DependsOn = new List<string>
                {
                    "A"
                }
            };

            var definitionA = new NodeDefinition
            {
                Name = "A"
            };
            var provider = new NodeDefinitionProvider("C");
            provider.Register("C", definitionC);
            provider.Register("B", definitionB);
            provider.Register("A", definitionA);

            var runner = new DataNodeRunner<string>(provider, nodeRegistry);
            runner.Run();
            Assert.IsTrue(flag.Flag);
        }


        [Test]
        public void CommitThrowsTest()
        {
            var nodeRegistry = new NodeRegistry<string>();
            nodeRegistry.Register("A", new BaseNode<string>(new TestActionProcess(p =>
            {
                if (p.Number < 3)
                    p.Item = $"AAA{p.Number}";
                if (p.Number == 2)
                    p.Stop();
            }, () => throw new Exception("Houston we have a problem")), "A"));
            nodeRegistry.Register("B",
                new BaseNode<string>(new TestActionProcess(p => { p.Item = p.Item += "BBB"; }), "B"));
            nodeRegistry.Register("C",
                new BaseNode<string>(new TestActionProcess(p => { p.Item = p.Item += "CCC"; }), "C"));
            var definitionC = new NodeDefinition
            {
                Name = "C",
                DependsOn = new List<string>
                {
                    "B"
                }
            };

            var definitionB = new NodeDefinition
            {
                Name = "B",
                DependsOn = new List<string>
                {
                    "A"
                }
            };

            var definitionA = new NodeDefinition
            {
                Name = "A"
            };
            var provider = new NodeDefinitionProvider("C");
            provider.Register("C", definitionC);
            provider.Register("B", definitionB);
            provider.Register("A", definitionA);

            var runner = new DataNodeRunner<string>(provider, nodeRegistry);
            runner.Run();
            var current = runner.Current;
            Assert.AreEqual(1, current.CommitExceptions.Count);
        }

        [Test]
        public void ThreeRowsAndBThrowsErrorOnTheSecond()
        {
            var counterWrapper = new TestCounterWrapper();
            var nodeRegistry = new NodeRegistry<string>();
            nodeRegistry.Register("A", new BaseNode<string>(new TestActionProcess(p =>
            {
                if (p.Number < 3)
                    p.Item = $"AAA{counterWrapper.Counter++}";
                if (p.Number == 2)
                    p.Stop();
            }), "A"));
            nodeRegistry.Register("B", new BaseNode<string>(new TestActionProcess(p =>
            {
                if (p.Number == 1)
                    throw new Exception("Houston we have a problem");
                p.Item += "BBB";
            }), "B"));
            nodeRegistry.Register("C",
                new BaseNode<string>(new TestActionProcess(p => { p.Item = p.Item += "CCC"; }), "C"));
            var definitionC = new NodeDefinition
            {
                Name = "C",
                DependsOn = new List<string>
                {
                    "B"
                }
            };

            var definitionB = new NodeDefinition
            {
                Name = "B",
                DependsOn = new List<string>
                {
                    "A"
                }
            };

            var definitionA = new NodeDefinition
            {
                Name = "A"
            };
            var provider = new NodeDefinitionProvider("C");
            provider.Register("C", definitionC);
            provider.Register("B", definitionB);
            provider.Register("A", definitionA);

            var runner = new DataNodeRunner<string>(provider, nodeRegistry);
            runner.Run();
            var result = runner.Current;
            Assert.AreEqual("AAA2BBBCCC", result.Item);
        }

        [Test]
        public void ThreeRowsAndNodeBShouldWorkOnce()
        {
            var counterWrapper = new TestCounterWrapper();
            var nodeRegistry = new NodeRegistry<string>();
            nodeRegistry.Register("A", new BaseNode<string>(new TestActionProcess(p =>
            {
                if (p.Number < 3)
                    p.Item = $"AAA{counterWrapper.Counter++}";
                else
                    p.Stop();
            }), "A"));
            nodeRegistry.Register("B", new BaseNode<string>(new TestActionProcess(p =>
            {
                p.Item = p.Item += "BBB";
                p.Exclude();
            }), "A"));
            nodeRegistry.Register("C",
                new BaseNode<string>(new TestActionProcess(p => { p.Item = p.Item += "CCC"; }), "C"));
            var definitionC = new NodeDefinition
            {
                Name = "C",
                DependsOn = new List<string>
                {
                    "B"
                }
            };

            var definitionB = new NodeDefinition
            {
                Name = "B",
                DependsOn = new List<string>
                {
                    "A"
                }
            };

            var definitionA = new NodeDefinition
            {
                Name = "A"
            };
            var provider = new NodeDefinitionProvider("C");
            provider.Register("C", definitionC);
            provider.Register("B", definitionB);
            provider.Register("A", definitionA);

            var runner = new DataNodeRunner<string>(provider, nodeRegistry);
            var wrapper = new DataItemWrapper<string>();
            runner.Step(wrapper);
            Assert.AreEqual("AAA0BBBCCC", wrapper.Item);
            runner.Step(wrapper);
            Assert.AreEqual("AAA1CCC", wrapper.Item);
            runner.Step(wrapper);
            Assert.AreEqual("AAA2CCC", wrapper.Item);
        }


        [Test]
        public void ThreeRowsAndOneMustBeIgnoredSimple()
        {
            var counterWrapper = new TestCounterWrapper();
            var nodeRegistry = new NodeRegistry<string>();
            nodeRegistry.Register("A", new BaseNode<string>(new TestActionProcess(p =>
            {
                if (p.Number < 3)
                    p.Item = $"AAA{counterWrapper.Counter++}";
                if (p.Number == 2)
                    p.Stop();
            }), "A"));
            nodeRegistry.Register("B",
                new BaseNode<string>(new TestActionProcess(p => { p.Item = p.Item += "BBB"; }), "B"));
            nodeRegistry.Register("C",
                new BaseNode<string>(new TestActionProcess(p => { p.Item = p.Item += "CCC"; }), "C"));
            var definitionC = new NodeDefinition
            {
                Name = "C",
                DependsOn = new List<string>
                {
                    "B"
                }
            };

            var definitionB = new NodeDefinition
            {
                Name = "B",
                DependsOn = new List<string>
                {
                    "A"
                }
            };

            var definitionA = new NodeDefinition
            {
                Name = "A"
            };
            var provider = new NodeDefinitionProvider("C");
            provider.Register("C", definitionC);
            provider.Register("B", definitionB);
            provider.Register("A", definitionA);

            var runner = new DataNodeRunner<string>(provider, nodeRegistry);
            var wrapper = new DataItemWrapper<string>();
            runner.Step(wrapper);
            Assert.AreEqual("AAA0BBBCCC", wrapper.Item);
            runner.Step(wrapper);
            Assert.AreEqual("AAA1BBBCCC", wrapper.Item);
            runner.Step(wrapper);
            Assert.AreEqual("AAA2BBBCCC", wrapper.Item);
            runner.Step(wrapper);
            Assert.AreEqual("AAA2BBBCCC", wrapper.Item);
        }

        [Test]
        public void ThreeRowsSimple()
        {
            var counterWrapper = new TestCounterWrapper();
            var nodeRegistry = new NodeRegistry<string>();
            nodeRegistry.Register("A", new BaseNode<string>(new TestActionProcess(p =>
            {
                if (p.Number < 3)
                    p.Item = $"AAA{counterWrapper.Counter++}";
                else
                    p.Stop();
            }), "A"));
            nodeRegistry.Register("B",
                new BaseNode<string>(new TestActionProcess(p => { p.Item = p.Item += "BBB"; }), "B"));
            nodeRegistry.Register("C",
                new BaseNode<string>(new TestActionProcess(p => { p.Item = p.Item += "CCC"; }), "C"));
            var definitionC = new NodeDefinition
            {
                Name = "C",
                DependsOn = new List<string>
                {
                    "B"
                }
            };

            var definitionB = new NodeDefinition
            {
                Name = "B",
                DependsOn = new List<string>
                {
                    "A"
                }
            };

            var definitionA = new NodeDefinition
            {
                Name = "A"
            };
            var provider = new NodeDefinitionProvider("C");
            provider.Register("C", definitionC);
            provider.Register("B", definitionB);
            provider.Register("A", definitionA);

            var runner = new DataNodeRunner<string>(provider, nodeRegistry);
            var wrapper = new DataItemWrapper<string>();
            runner.Step(wrapper);
            Assert.AreEqual("AAA0BBBCCC", wrapper.Item);
            runner.Step(wrapper);
            Assert.AreEqual("AAA1BBBCCC", wrapper.Item);
            runner.Step(wrapper);
            Assert.AreEqual("AAA2BBBCCC", wrapper.Item);
        }


        [Test]
        public void ThreeRowsSimpleRun()
        {
            var counterWrapper = new TestCounterWrapper();
            var nodeRegistry = new NodeRegistry<string>();
            nodeRegistry.Register("A", new BaseNode<string>(new TestActionProcess(p =>
            {
                if (p.Number < 3)
                    p.Item = $"AAA{counterWrapper.Counter++}";
                if (p.Number == 2)
                    p.Stop();
            }), "A"));
            nodeRegistry.Register("B",
                new BaseNode<string>(new TestActionProcess(p => { p.Item = p.Item += "BBB"; }), "B"));
            nodeRegistry.Register("C",
                new BaseNode<string>(new TestActionProcess(p => { p.Item = p.Item += "CCC"; }), "C"));
            var definitionC = new NodeDefinition
            {
                Name = "C",
                DependsOn = new List<string>
                {
                    "B"
                }
            };

            var definitionB = new NodeDefinition
            {
                Name = "B",
                DependsOn = new List<string>
                {
                    "A"
                }
            };

            var definitionA = new NodeDefinition
            {
                Name = "A"
            };
            var provider = new NodeDefinitionProvider("C");
            provider.Register("C", definitionC);
            provider.Register("B", definitionB);
            provider.Register("A", definitionA);

            var runner = new DataNodeRunner<string>(provider, nodeRegistry);
            runner.Run();
            var result = runner.Current;
            Assert.AreEqual("AAA2BBBCCC", result.Item);
        }


        [Test]
        public void ThreeRowsSimpleRunAllNodesMustBeDisposed()
        {
            var counterWrapper = new TestCounterWrapper();
            var nodeRegistry = new NodeRegistry<string>();
            var a = new TestActionProcess(p =>
            {
                if (p.Number < 3)
                    p.Item = $"AAA{counterWrapper.Counter++}";
                if (p.Number == 2)
                    p.Stop();
            });
            var b = new TestActionProcess(p => { p.Item = p.Item += "BBB"; });
            var c = new TestActionProcess(p => { p.Item = p.Item += "CCC"; });
            nodeRegistry.Register("A", new BaseNode<string>(a, "A"));
            nodeRegistry.Register("B", new BaseNode<string>(b, "B"));
            nodeRegistry.Register("C", new BaseNode<string>(c, "C"));
            var definitionC = new NodeDefinition
            {
                Name = "C",
                DependsOn = new List<string>
                {
                    "B"
                }
            };

            var definitionB = new NodeDefinition
            {
                Name = "B",
                DependsOn = new List<string>
                {
                    "A"
                }
            };

            var definitionA = new NodeDefinition
            {
                Name = "A"
            };
            var provider = new NodeDefinitionProvider("C");
            provider.Register("C", definitionC);
            provider.Register("B", definitionB);
            provider.Register("A", definitionA);

            var runner = new DataNodeRunner<string>(provider, nodeRegistry);
            runner.Run();
            runner.Dispose();
            Assert.IsTrue(a.IsDisposed);
            Assert.IsTrue(b.IsDisposed);
            Assert.IsTrue(c.IsDisposed);
        }


        //AAA0BBBCCCAAA1AAA2BBBCCC

        [Test]
        public void ThreeRowsTheSecondSkip()
        {
            var counterWrapper = new TestCounterWrapper();
            var nodeRegistry = new NodeRegistry<string>();
            nodeRegistry.Register("A", new BaseNode<string>(new TestActionProcess(p =>
            {
                if (p.Number < 3)
                {
                    if (p.Item == null) p.Item = "";
                    p.Item = p.Item + $"AAA{counterWrapper.Counter++}";
                }
                if (p.Number == 2)
                    p.Stop();
            }), "A"));
            nodeRegistry.Register("B", new BaseNode<string>(new TestActionProcess(p =>
            {
                if (p.Number == 1)
                    p.Skip();
                else
                    p.Item = p.Item += "BBB";
            }), "B"));
            nodeRegistry.Register("C",
                new BaseNode<string>(new TestActionProcess(p => { p.Item = p.Item += "CCC"; }), "C"));
            var definitionC = new NodeDefinition
            {
                Name = "C",
                DependsOn = new List<string>
                {
                    "B"
                }
            };

            var definitionB = new NodeDefinition
            {
                Name = "B",
                DependsOn = new List<string>
                {
                    "A"
                }
            };

            var definitionA = new NodeDefinition
            {
                Name = "A"
            };
            var provider = new NodeDefinitionProvider("C");
            provider.Register("C", definitionC);
            provider.Register("B", definitionB);
            provider.Register("A", definitionA);

            var runner = new DataNodeRunner<string>(provider, nodeRegistry);
            runner.Run();
            var result = runner.Current;
            Assert.AreEqual("AAA0BBBCCCAAA1AAA2BBBCCC", result.Item);
        }
    }
}