using DataSubset.DbDependencyGraph.Core.Configurations;
using DataSubset.DbDependencyGraph.Core.DependencyGraph;
using System.Data;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace DataSubset.Exporters.Common
{
    public abstract class DbExporterEngineBase : IDbExporterEngine
    {
        Dictionary<string, string> selectQueryByTable = new ();
        Dictionary<string, string> insertQueryByTable = new();

        public async Task<string> GenerateInsertStatement(TableNode currentNode, (string column, object? value)[] rowData, IEnumerable<TableExportConfig> tableExportConfig)
        {
            if (!insertQueryByTable.TryGetValue(currentNode.FullName, out var query))
            {
                //build query
                query = await GenerateInsertStatement(currentNode, rowData);
                //store query
                insertQueryByTable.Add(currentNode.FullName, query);
            }

            return string.Format(query, rowData);
            
        }

        public async IAsyncEnumerable<(string column, object? value)[]> GetCurrentNodeRows(TableNode currentNode, ITableDependencyEdgeData? edgeData, (string column, object? value)[]? parentValue)
        {

            if (!selectQueryByTable.TryGetValue(currentNode.FullName, out var query))
            {
                //build query
                query = await GenerateSelectQuery(currentNode, edgeData);
                //store query
                selectQueryByTable.Add(currentNode.FullName, query);
            }

            var queryWithValues = string.Format(query, parentValue);
            
            yield return ExecuteGetRowQuery(queryWithValues);

        }

        protected abstract (string column, object? value)[] ExecuteGetRowQuery(string queryWithValues);
        protected abstract Task<IDbConnection> OpenConnetionAsync();
        protected abstract Task<string?> GenerateInsertStatement(TableNode currentNode, (string column, object? value)[] rowData);

        protected abstract Task<string?> GenerateSelectQuery(TableNode currentNode, ITableDependencyEdgeData? edgeData);

        public void InitExport()
        {
            selectQueryByTable.Clear();
            insertQueryByTable.Clear();
        }
    }
}