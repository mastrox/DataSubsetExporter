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

            //add row data to query
            return AddValuesToQuery(query, rowData);
            
        }

        protected abstract string AddValuesToQuery(string query, (string column, object? value)[]? values);
       

        public async IAsyncEnumerable<(string column, object? value)[]> GetCurrentNodeRows(TableNode currentNode, ITableDependencyEdgeData? edgeData, SelectionCondition selectionCondition)
        {
            var parentValueConverted = selectionCondition.parentValue?.Select(a => (edgeData?.GetTargetColumnFromBindings(a.column) ?? a.column, a.value)).ToArray();
            if (!selectQueryByTable.TryGetValue(currentNode.FullName, out string query))
            {
                //build query
                query = await GenerateSelectQuery(currentNode, edgeData, selectionCondition.whereCondition);
                //store query
                selectQueryByTable.Add(currentNode.FullName, query);
            }

            if (selectionCondition.parentValue != null)
            {
                //add parent value to query
                query = AddValuesToQuery(query, selectionCondition.parentValue);
            }

            yield return await ExecuteGetRowQuery(query);

        }

        protected abstract Task<(string column, object? value)[]> ExecuteGetRowQuery(string queryWithValues);
        protected abstract ValueTask<string> GenerateInsertStatement(TableNode currentNode, (string column, object? value)[] rowData);

        protected abstract ValueTask<string> GenerateSelectQuery(TableNode currentNode, ITableDependencyEdgeData? edgeData, string? whereCondition);

        public void InitExport()
        {
            selectQueryByTable.Clear();
            insertQueryByTable.Clear();
        }
    }
}