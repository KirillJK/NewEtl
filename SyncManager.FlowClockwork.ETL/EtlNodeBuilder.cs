using System.Collections.Generic;
using SyncManager.Etl.Cleanup;
using SyncManager.Etl.Common;
using SyncManager.Etl.Filtering;
using SyncManager.Etl.Schema;
using SyncManager.Etl.Transformation;
using SyncManager.Etl.Transformation.Models;
using SyncManager.FlowClockwork.ETL.Drivers;
using SyncManager.Readers;

namespace SyncManager.FlowClockwork.ETL
{
    public class EtlNodeBuilder: IEtlNodeBuilder
    {
        private NodeDefinitionProvider _nodeDefinitionProvider = new NodeDefinitionProvider();
        private NodeRegistry<SourceContext> _nodeRegistry = new NodeRegistry<SourceContext>();
        private IExpressionEvaluator _expressionEvaluator = null;
        private NodeDefinition _current;
        private int _transformationBlockCounter = 0;
        private int _actionCounter = 0;
        private NodeDefinition Attach(NodeDefinition nodeDefinition)
        {
            if (_current != null)
            {
                nodeDefinition.DependsOn.Add(_current.Name);

            }
          
                _current = nodeDefinition;
            
            _nodeDefinitionProvider.SetRoot(nodeDefinition.Name);
            return nodeDefinition;
        }

        public IEtlNodeBuilder AddExpressionEvaluator(IExpressionEvaluator expressionEvaluator)
        {
            _expressionEvaluator = expressionEvaluator;
            return this;
        }

        public IEtlNodeBuilder AddStart()
        {
            Register("Start", new BeginEtlDataDriver());
            return this;
        }

        private void Register(string name, IDataDriver<SourceContext> dataDriver)
        {
            var nodeDefinition = Attach(new NodeDefinition() { Name = name });
            _nodeDefinitionProvider.Register(name, nodeDefinition);
            _nodeRegistry.Register(name, new BaseNode<SourceContext>(dataDriver, name));
        }

        public IEtlNodeBuilder AddCleanup(List<CleanupRule> rules)
        {
            Register("Cleanup", new CleanupDataDriver(new Cleanuper(rules, _expressionEvaluator)));
            return this;
        }

        public IEtlNodeBuilder AddFilter(List<FilterRule> rules)
        {

            Register("Filter", new FilterDataDriver(new Filterer(rules, _expressionEvaluator)));
            return this;
        }

        public IEtlNodeBuilder AddSchema(List<DataSourceSchemaItem> schemaItems)
        {
            Register("Schema", new SchemaDataDriver(new Schemator(schemaItems)));
            return this;
        }

        public IEtlNodeBuilder AddTransformations(List<TransformationMap> transformations)
        {
            Register($"Transform{_transformationBlockCounter++}", new TransformDataDriver(new Transformer(transformations, _expressionEvaluator)));
            return this;
        }

        public IEtlNodeBuilder AddDataWriter(string to)
        {
            Register($"Action{_actionCounter++}", new SourceWriterDataDriver(new SourceWriter(to)));
            return this;
        }

        public IEtlNodeBuilder AddDataReader(string @from)
        {
            Register("Reader", new SourceReaderDataDriver(new CsvReader(@from)));
            return this;
        }

        public EtlBuilderPack Get()
        {
            return new EtlBuilderPack()
            {
                NodeDefinitionProvider = _nodeDefinitionProvider,
                Registry = _nodeRegistry
            };
        }
    }

    public class EtlBuilderPack
    {
        public NodeDefinitionProvider NodeDefinitionProvider { get; set; }
        public NodeRegistry<SourceContext> Registry { get; set; }
    }
}