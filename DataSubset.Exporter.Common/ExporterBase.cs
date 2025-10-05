using DataSubset.Core.Configurations;
using DataSubset.Core.DependencyGraph;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;

namespace DataSubset.Exporter.Common
{
    public class ExporterBase(IDatabaseService databaseService, ILogger? logger = null)
    {

        public async Task<IAsyncEnumerable<T>> GetRowsToExportInInsertOrder<T>(IEnumerable<TableExportConfig> TableExportConfig, DatabaseGraph databaseGraph)
        {
            foreach (var rootTables in TableExportConfig)
            {
                //get node
                var tableNode = databaseGraph.FindTable(rootTables.Schema, rootTables.TableName);
                if (tableNode == null)
                {
                    logger?.LogWarning("Table {0}.{1} not found in graph", rootTables.Schema, rootTables.TableName);
                    continue;
                }
                List<Dictionary<string, object>> dataRows = new List<Dictionary<string, object>>();

                var relations = databaseGraph.GetOutgoingEdges(tableNode);
                //process FK dependencies
                var fkRelations = relations.Where(a => a is FkTableDependencyEdge);

                foreach (var fkRelation in fkRelations)
                {
                    ProcessDependenciy(fkRelation)

                //return current node values

                foreach (var item in await GetRowsToExport<T>(tableNode, dataRows))
                {

                }
            }
        }
    }
}
