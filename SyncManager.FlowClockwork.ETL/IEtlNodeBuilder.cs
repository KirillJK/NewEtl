using System.Collections.Generic;
using SyncManager.Etl.Cleanup;
using SyncManager.Etl.Common;
using SyncManager.Etl.Filtering;
using SyncManager.Etl.Schema;
using SyncManager.Etl.Transformation.Models;

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