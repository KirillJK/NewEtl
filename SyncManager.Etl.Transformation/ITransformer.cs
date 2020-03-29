using System;
using System.Collections.Generic;
using SyncManager.Etl.Common;
using SyncManager.Etl.Transformation.Models;

namespace SyncManager.Etl.Transformation
{
    public interface ITransformer
    {
        void Transform(SourceContext sourceContext);
    }

    public class Transformer: ITransformer
    {
        private List<TransformationMap> _transformationMaps;
        private IExpressionEvaluator _expressionEvaluator;

        public Transformer(List<TransformationMap> transformationMaps, IExpressionEvaluator expressionEvaluator)
        {
            _transformationMaps = transformationMaps;
            _expressionEvaluator = expressionEvaluator;
        }

        public void Transform(SourceContext sourceContext)
        {
            foreach (var transformationMap in _transformationMaps)
            {
                SafeRun(()=>Transform(sourceContext, transformationMap), sourceContext, transformationMap);
            }
        }

        private void SetEmptyOutput(SourceContext context, TransformationMap map)
        {
            context.Destination[map.Output] = null;
        }

        private void Transform(SourceContext sourceContext, TransformationMap transformationMap)
        {
            if (transformationMap.MappingType == MappingType.Field)
            {
                if (!sourceContext.Source.ContainsKey(transformationMap.KeyExpression))
                {
                    SetEmptyOutput(sourceContext, transformationMap);
                    sourceContext.AddErrorForDestinationColumn(transformationMap.Output, $"Source key {transformationMap.KeyExpression} is missed", SourceContext.ErrorTypeTransformation);
                }
                else
                {
                    if (transformationMap.TransformOnlyIfColumnExists && sourceContext.ListOfSchemaMissedColumns.Contains(transformationMap.KeyExpression))
                    {
                        sourceContext.Destination.Remove(transformationMap.Output);
                    }
                    else
                    {
                        sourceContext.Destination[transformationMap.Output] =
                            sourceContext.Source[transformationMap.KeyExpression];
                    }

                }

             
        
            }
            else if (transformationMap.MappingType == MappingType.Expression)
            {
                sourceContext.Destination[transformationMap.Output] =
                    _expressionEvaluator.Evaluate(transformationMap.Expression);
            }
            else if (transformationMap.MappingType == MappingType.DataMapping)
            {

            }
        }


        private void SafeRun(Action action, SourceContext sourceContext, TransformationMap map)
        {
            try
            {
                action();
            }
            catch (Exception e)
            {
                SetEmptyOutput(sourceContext, map);
                sourceContext.AddErrorForDestinationColumn(map.Output, e, SourceContext.ErrorTypeCleanup);
            }
        }
    }
}