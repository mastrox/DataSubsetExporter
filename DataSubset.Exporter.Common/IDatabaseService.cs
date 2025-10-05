namespace DataSubset.Exporter.Common
{
    public interface IDatabaseService
    {
        Task<Dictionary<string, object>> FetchRowByPrimaryKey(string schema, string tableName, IEnumerable<string> primaryKeyColumns, IDictionary<string, string>? primaryKeyValues);
        Task<List<Dictionary<string, object>>> FetchRowsByWhereClause(string schema, string tableName, string whereClause);
        Task<List<ColumnInfo>> GetColumnMetadata(string schema, string tableName);
        Task<List<string>> GetPrimaryKeyColumns(string schema, string tableName);
    }
}