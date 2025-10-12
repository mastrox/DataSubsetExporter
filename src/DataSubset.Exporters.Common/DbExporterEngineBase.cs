using DataSubset.DbDependencyGraph.Core.Configurations;
using DataSubset.DbDependencyGraph.Core.DependencyGraph;
using System.Data;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace DataSubset.Exporters.Common
{
    public abstract class DbExporterEngineBase : IDbExporterEngine
    {
        Dictionary<string, string> selectQueryByTable = new();
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
            List<(string column, object? value)> selectData = new();
            foreach (var item in selectionCondition?.parentValue ?? Enumerable.Empty<(string column, object? value)>())
            {
                var targetColumn = edgeData?.GetTargetColumnFromBindings(item.column);
                if (targetColumn != null)
                {
                    selectData.Add((targetColumn, item.value));
                }
            }


            if (!selectQueryByTable.TryGetValue(currentNode.FullName, out string query))
            {
                //build query
                query = await GenerateSelectQuery(currentNode, edgeData, selectionCondition?.whereCondition, selectionCondition?.PrimaryKeyValue);
                //store query
                selectQueryByTable.Add(currentNode.FullName, query);
            }

            if (selectData.Count > 0)
            {
                //add parent value to query
                query = AddValuesToQuery(query, selectData.ToArray());
            }

            await foreach(var row in ExecuteGetRowQuery(query))
            {
                yield return row;
            }
        }

        protected abstract IAsyncEnumerable<(string column, object? value)[]> ExecuteGetRowQuery(string queryWithValues);
        protected abstract ValueTask<string> GenerateInsertStatement(TableNode currentNode, (string column, object? value)[] rowData);

        protected abstract ValueTask<string> GenerateSelectQuery(TableNode currentNode, ITableDependencyEdgeData? edgeData, string? whereCondition, PrimaryKeyValue[]? primaryKeyValue);

        public void InitExport()
        {
            selectQueryByTable.Clear();
            insertQueryByTable.Clear();
        }

        public abstract string ValueToString(object? value);
    }
}