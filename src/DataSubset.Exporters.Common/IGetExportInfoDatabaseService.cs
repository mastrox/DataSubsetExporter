using DataSubset.DbDependencyGraph.Core.DependencyGraph;
using System.Data;

namespace DataSubset.Exporters.Common
{
    public interface IGetExportInfoDatabaseService
    {
        (string column, object? value)[] ExecuteGetRowsQuery(string queryWithValues, IDbConnection connection);
        Task<Dictionary<string, object>> FetchRowByPrimaryKey(string schema, string tableName, IEnumerable<string> primaryKeyColumns, IDictionary<string, string>? primaryKeyValues);
        Task<List<Dictionary<string, object>>> FetchRowsByWhereClause(string schema, string tableName, string whereClause);
        Task<IDictionary<(string schema, string table), ColumnInfo>> GetColumnMetadata(string schema, string tableName);
        Task<string> GenerateInsertStatement(TableNode currentNode, (string column, object? value)[] rowData);
        Task<List<string>> GetPrimaryKeyColumns(string schema, string tableName);
        Task<string> GetSelectQuery(TableNode currentNode, ITableDependencyEdgeData? data);
        Task<IDbConnection> OpenConnetionAsync();
    }
}