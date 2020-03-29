﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SyncManager.Etl.Common;
using SyncManager.Readers;

namespace SyncManager.FlowClockwork.ETL
{
    public class SourceReaderDataDriver:IDataDriver<SourceContext>
    {
        private readonly ISourceReader _sourceReader;

        public SourceReaderDataDriver(ISourceReader sourceReader)
        {
            _sourceReader = sourceReader;
        }

        public void Dispose()
        {
            _sourceReader?.Dispose();
        }

        public void Process(IDataItemWrapper<SourceContext> item)
        {
            _sourceReader.Read(item.Item);
        }

        public void Commit()
        {
            _sourceReader.Dispose();
        }
    }
}
