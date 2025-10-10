using DataSubset.DbDependencyGraph.Core.Configurations;
using DataSubset.DbDependencyGraph.Core.DependencyGraph;

namespace DataSubset.Exporters.Common
{
    public interface IDbExporterEngine
    {
        Task<string> GenerateInsertStatement(TableNode currentNode, (string column, object? value)[] rowData, IEnumerable<TableExportConfig> tableExportConfig);
        IAsyncEnumerable<(string column, object? value)[]> GetCurrentNodeRows(TableNode currentNode, ITableDependencyEdgeData? data, (string column, object? value)[]? parentValue);
        void InitExport();
    }
}