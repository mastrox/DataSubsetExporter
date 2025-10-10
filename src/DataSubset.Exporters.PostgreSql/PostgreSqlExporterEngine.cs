using DataSubset.DbDependencyGraph.Core.DependencyGraph;
using DataSubset.Exporters.Common;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataSubset.Exporters.PostgreSql
{
    public class PostgreSqlExporterEngine : DbExporterEngineBase
    {
        protected override (string column, object? value)[] ExecuteGetRowQuery(string queryWithValues)
        {
            throw new NotImplementedException();
        }

        protected override Task<string?> GenerateInsertStatement(TableNode currentNode, (string column, object? value)[] rowData)
        {
            throw new NotImplementedException();
        }

        protected override Task<string?> GenerateSelectQuery(TableNode currentNode, ITableDependencyEdgeData? edgeData)
        {
            
            return Task.FromResult<string?>($"SELECT * FROM {currentNode.Schema}.\"{currentNode.Name}\" WHERE  {{0}}");
        }

        protected override Task<IDbConnection> OpenConnetionAsync()
        {
            throw new NotImplementedException();
        }
    }
}
