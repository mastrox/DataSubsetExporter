using DataSubset.DbDependencyGraph.Core.Configurations;
using DataSubset.DbDependencyGraph.Core.DependencyGraph;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataSubset.Exporters.Common
{
    public class InsertStatementExporter(IDbExporterEngine dbExporterEngine, ILogger? logger = null) : ExporterBase<string>(dbExporterEngine, logger)
    {
        protected async override Task<string> GenerateCurrentRowExportItem(TableNode currentNode, (string column, object? value)[] row, IEnumerable<TableExportConfig> tableExportConfig)
        {
            return await dbExporterEngine.GenerateInsertStatement(currentNode, row, tableExportConfig);
        }
    }
}
