using System.Collections.Generic;
using System.Diagnostics;
using NUnit.Framework;
using SyncManager.FlowClockwork;
using Tests.SyncManager.FlowClockwork.FileMoverTest;

namespace Tests.SyncManager.FlowClockwork.Loopback
{
    [TestFixture]
    public class LoopbackTests
    {
        [Test]
        public void StepByStepNoOptions()
        {
            var provider = new NodeDefinitionProvider();
            provider.Register("FirstCall");
            provider.Register("ManyCalls", "FirstCall");
            provider.Register("LastCall", "ManyCalls");
            provider.SetRoot("LastCall");
            List<string> messages = new List<string>();
            var nodeRegistry = new NodeRegistry<string>();
            nodeRegistry.Register("FirstCall", new BaseNode<string>(new FirstCallDataDriver(messages), provider.GetByName("FirstCall")));
            nodeRegistry.Register("ManyCalls", new BaseNode<string>(new ManyCallsDataDriver(messages),  provider.GetByName("ManyCalls")));
            nodeRegistry.Register("LastCall", new BaseNode<string>(new LastCallDataDriver(messages), provider.GetByName("LastCall")));

            var runner = new DataNodeRunner<string>(provider, nodeRegistry);
            runner.Run();
            Assert.AreEqual("FirstCall", messages[0]);
            Assert.AreEqual("Data0", messages[1]);
            Assert.AreEqual("LastCall", messages[2]);
            Assert.AreEqual("FirstCall", messages[3]);
            Assert.AreEqual("Data1", messages[4]);
            Assert.AreEqual("LastCall", messages[5]);
            Assert.AreEqual("FirstCall", messages[6]);
            Assert.AreEqual("Data2", messages[7]);
            Assert.AreEqual("LastCall", messages[8]);
        }

        [Test]
        public void StepByStepLastCallIsSingleCall()
        {
            var provider = new NodeDefinitionProvider();
            provider.Register("FirstCall");
            provider.Register("ManyCalls", "FirstCall");
            provider.Register("LastCall", "ManyCalls");
            provider.SetRoot("LastCall");
            var node = provider.GetByName("LastCall");
            node.SingleCall = true;
            List<string> messages = new List<string>();
            var nodeRegistry = new NodeRegistry<string>();
            nodeRegistry.Register("FirstCall", new BaseNode<string>(new FirstCallDataDriver(messages), provider.GetByName("FirstCall")));
            nodeRegistry.Register("ManyCalls", new BaseNode<string>(new ManyCallsDataDriver(messages), provider.GetByName("ManyCalls")));
            nodeRegistry.Register("LastCall", new BaseNode<string>(new LastCallDataDriver(messages), provider.GetByName("LastCall")));

            var runner = new DataNodeRunner<string>(provider, nodeRegistry);
            runner.Run();
            Assert.AreEqual("FirstCall", messages[0]);
            Assert.AreEqual("Data0", messages[1]);
            Assert.AreEqual("LastCall", messages[2]);
            Assert.AreEqual("FirstCall", messages[3]);
            Assert.AreEqual("Data1", messages[4]);
            Assert.AreEqual("FirstCall", messages[5]);
            Assert.AreEqual("Data2", messages[6]);
        }

        [Test]
        public void StepByStepFirstAndLastCallIsSingle()
        {
            var provider = new NodeDefinitionProvider();
            provider.Register("FirstCall");
            provider.Register("ManyCalls", "FirstCall");
            provider.Register("LastCall", "ManyCalls");
            provider.SetRoot("LastCall");
            var node = provider.GetByName("LastCall");
            node.SingleCall = true;
            node = provider.GetByName("FirstCall");
            node.SingleCall = true;
            List<string> messages = new List<string>();
            var nodeRegistry = new NodeRegistry<string>();
            nodeRegistry.Register("FirstCall", new BaseNode<string>(new FirstCallDataDriver(messages), provider.GetByName("FirstCall")));
            nodeRegistry.Register("ManyCalls", new BaseNode<string>(new ManyCallsDataDriver(messages), provider.GetByName("ManyCalls")));
            nodeRegistry.Register("LastCall", new BaseNode<string>(new LastCallDataDriver(messages), provider.GetByName("LastCall")));

            var runner = new DataNodeRunner<string>(provider, nodeRegistry);
            runner.Run();
            Assert.AreEqual("FirstCall", messages[0]);
            Assert.AreEqual("Data0", messages[1]);
            Assert.AreEqual("LastCall", messages[2]);
            Assert.AreEqual("Data1", messages[3]);
            Assert.AreEqual("Data2", messages[4]);
        }

        [Test]
        //[Ignore("Under construction")]
        public void FirstAndLastAreSingleCallSecondHasRunOverOption()
        {
            var provider = new NodeDefinitionProvider();
            provider.Register("FirstCall");
            provider.Register("ManyCalls", "FirstCall");
            provider.Register("LastCall", "ManyCalls");
            provider.SetRoot("LastCall");
            var node = provider.GetByName("LastCall");
            node.SingleCall = true;
            node = provider.GetByName("FirstCall");
            node.SingleCall = true;
            node = provider.GetByName("ManyCalls");
            node.RunOver = true;
            List<string> messages = new List<string>();
            var nodeRegistry = new NodeRegistry<string>();
            nodeRegistry.Register("FirstCall", new BaseNode<string>(new FirstCallDataDriver(messages), provider.GetByName("FirstCall")));
            nodeRegistry.Register("ManyCalls", new BaseNode<string>(new ManyCallsDataDriver(messages), provider.GetByName("ManyCalls")));
            nodeRegistry.Register("LastCall", new BaseNode<string>(new LastCallDataDriver(messages), provider.GetByName("LastCall")));

            var runner = new DataNodeRunner<string>(provider, nodeRegistry);
            runner.Run();
            Assert.AreEqual("FirstCall", messages[0]);
            Assert.AreEqual("Data0", messages[1]);
            Assert.AreEqual("Data1", messages[2]);
            Assert.AreEqual("Data2", messages[3]);
            Assert.AreEqual("LastCall", messages[4]);
        }

    }
}