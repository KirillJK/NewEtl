using System.Collections.Generic;
using SyncManager.FlowEtl.Cleanup;
using SyncManager.FlowEtl.Common;
using SyncManager.FlowEtl.Filtering;
using SyncManager.FlowEtl.Schema;
using SyncManager.FlowEtl.Transformation.Models;

namespace SyncManager.FlowClockwork.ETL
{
    public interface IEtlNodeBuilder
    {
        IEtlNodeBuilder AddExpressionEvaluator(IExpressionEvaluator expressionEvaluator);
        IEtlNodeBuilder AddStart();
        IEtlNodeBuilder AddCleanup(List<CleanupRule> rules);
        IEtlNodeBuilder AddFilter(List<FilterRule> rules);
        IEtlNodeBuilder AddSchema(List<DataSourceSchemaItem> schemaItems);
        IEtlNodeBuilder AddTransformations(List<TransformationMap> transformations);
        IEtlNodeBuilder AddDataWriter(string to);
        IEtlNodeBuilder AddDataReader(string from);
        EtlBuilderPack Get();
    }
}