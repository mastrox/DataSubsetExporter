using DataSubset.DbDependencyGraph.Core.Configurations;
using DataSubset.DbDependencyGraph.Core.DependencyGraph;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataSubset.Exporters.Common.BinaryExporter
{
    internal class BinaryExporter: ExporterBase<byte[]>
    {
        public BinaryExporter(IDbExporterEngine dbExporterEngine, ILogger? logger = null) : base(dbExporterEngine, logger)
        {
        }

        protected async override Task<byte[]> GenerateCurrentRowExportItem(TableNode currentNode, (string column, object? value)[] row, IEnumerable<TableExportConfig> tableExportConfig)
        {
            throw new NotImplementedException();
        }

        protected override async Task<IAsyncEnumerable<byte[]>> GenerateMetadata(DatabaseGraph databaseGraph, IEnumerable<TableExportConfig> tableExportConfig)
        {
            MessagePack.serrial
            yield return new ExportMetadata("1", DbExporterEngine.GetDbType(), DateTimeOffset.Now);
            await foreach (var item in GetTablesMetadata(databaseGraph))
            {
                yield return item;
            }
        }

        private IAsyncEnumerable<object> GetTablesMetadata(DatabaseGraph databaseGraph)
        {
            throw new NotImplementedException();
        }
    }
}
