using DataSubset.DbDependencyGraph.Core.Configurations;
using DataSubset.DbDependencyGraph.Core.DependencyGraph;
using Microsoft.Extensions.Logging;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataSubset.Exporters.Common
{
    public abstract class ExporterBase<T>(IDbExporterEngine dbExporterEngine, ILogger? logger = null)
    {
        HashSet<string> exportedRows = new HashSet<string>();

        public async IAsyncEnumerable<T> GetItemsToExportInInsertOrder(IEnumerable<TableExportConfig> tableExportConfig, DatabaseGraph databaseGraph)
        {
            dbExporterEngine.InitExport();
            foreach (var rootTables in tableExportConfig)
            {
                //get node
                var currentTable = databaseGraph.FindTable(rootTables.Schema, rootTables.TableName);
                if (currentTable == null)
                {
                    logger?.LogWarning("Table {0}.{1} not found in graph", rootTables.Schema, rootTables.TableName);
                    continue;
                }
                var rootEdge = new GraphEdge<TableNode, ITableDependencyEdgeData>(source: null, target: currentTable, data: null);
                SelectionCondition? whereValue = new SelectionCondition(null, rootTables.WhereClause, rootTables.PrimaryKeyValue);
                
                await foreach (var item in GetRelationItemsToExportInInsertOrder(rootEdge, whereValue, databaseGraph, tableExportConfig))
                {
                    yield return item;
                }
            }
        }

        private async IAsyncEnumerable<T> GetRelationItemsToExportInInsertOrder(GraphEdge<TableNode, ITableDependencyEdgeData> parentToCurrentEdge, SelectionCondition selectionCondition, DatabaseGraph databaseGraph, IEnumerable<TableExportConfig> tableExportConfig)
        {
            var currentNode = parentToCurrentEdge.Target;
            

            await foreach (var rowData in dbExporterEngine.GetCurrentNodeRows(currentNode, parentToCurrentEdge.Data, selectionCondition))
            {
                StringBuilder rowKey = new StringBuilder();
                rowKey.Append(currentNode.FullName);
                rowKey.Append('^');
                foreach (var data in rowData)
                {
                    if (currentNode.PrimaryKeyColumnsSet.Contains(data.column))
                        rowKey.Append(dbExporterEngine.ValueToString(data.value));
                        rowKey.Append('^');
                }

                var rorId = rowKey.ToString();
                //find in rowData the primaryey value
                if (exportedRows.Contains(rorId))
                {
                    continue;
                }
                else exportedRows.Add(rorId);

                List<Dictionary<string, object>> dataRows = new List<Dictionary<string, object>>();

                var relations = databaseGraph.GetOutgoingEdges(currentNode);

                //process FK dependencies
                var fkRelations = relations.Where(a => a.Data is FkTableDependencyEdgeData);
                foreach (var fkRelation in fkRelations)
                {

                    await foreach (var item in GetRelationItemsToExportInInsertOrder(fkRelation, new SelectionCondition(rowData,null, null), databaseGraph, tableExportConfig))
                    {
                        yield return item;
                    }
                }

                
                exportedRows.Add(currentNode.FullName + string.Join("_", rowData.Select(r => r.value?.ToString() ?? "NULL")));
                //process current node value
                yield return await GenerateCurrentRowExportItem(currentNode, rowData, tableExportConfig);

                //process implicit relation
                var implicitRelations = relations.Where(a => a.Data is ImplicitRelTableDependencyEdgeData);
                foreach (var implicitRelation in implicitRelations)
                {
                    await foreach (var item in GetRelationItemsToExportInInsertOrder(implicitRelation, new SelectionCondition(rowData,  null, null), databaseGraph, tableExportConfig))
                    {
                        yield return item;
                    }
                }
            }

        }

        protected abstract Task<T> GenerateCurrentRowExportItem(TableNode currentNode, (string column, object? value)[] row, IEnumerable<TableExportConfig> tableExportConfig);


    }
}
