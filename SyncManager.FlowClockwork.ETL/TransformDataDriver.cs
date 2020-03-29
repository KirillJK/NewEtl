using SyncManager.Etl.Common;
using SyncManager.Etl.Transformation;

namespace SyncManager.FlowClockwork.ETL
{
    public class TransformDataDriver : IDataDriver<SourceContext>
    {
        private readonly ITransformer _transformer;

        public TransformDataDriver(ITransformer transformer)
        {
            _transformer = transformer;
        }

        public void Dispose()
        {
           
        }

        public void Process(IDataItemWrapper<SourceContext> item)
        {
            _transformer.Transform(item.Item);
        }

        public void Commit()
        {
            
        }
    }
}