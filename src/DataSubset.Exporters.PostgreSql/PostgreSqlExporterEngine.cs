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
    public class PostgreSqlExporterEngine(string connectionString) : DbExporterEngineBase
    {
        // Use Npgsql for PostgreSQL database connections and commands
        protected override async Task<(string column, object? value)[]> ExecuteGetRowQuery(string queryWithValues)
        {
            using var connection = new Npgsql.NpgsqlConnection(connectionString);
            await connection.OpenAsync();
            using var command = new Npgsql.NpgsqlCommand(queryWithValues, connection);
            using var reader = await command.ExecuteReaderAsync();
            var results = new List<(string column, object? value)[]>();
            while (await reader.ReadAsync())
            {
                var row = new (string column, object? value)[reader.FieldCount];
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    row[i] = (reader.GetName(i), reader.IsDBNull(i) ? null : reader.GetValue(i));
                }
                results.Add(row);
            }
            if (results.Count > 1)
                throw new Exception("More than one row returned for query: " + queryWithValues);
            return results.FirstOrDefault() ?? Array.Empty<(string column, object? value)>();
        }

        protected override ValueTask<string> GenerateInsertStatement(TableNode currentNode, (string column, object? value)[] rowData)
        {
            StringBuilder query = new StringBuilder();
            query.Append($"INSERT INTO {currentNode.Schema}.\"{currentNode.Name}\" (");
            query.Append(string.Join(", ", rowData.Select(a => a.column)));
            query.Append(") VALUES (");
            query.Append(string.Join(", ", rowData.Select((a, index) => $"{{{index}}}")));
            query.Append(");");
            return ValueTask.FromResult<string>(query.ToString());

            //build insert query
        }

        protected override ValueTask<string> GenerateSelectQuery(TableNode currentNode, ITableDependencyEdgeData? edgeData, string? whereCondition)
        {
            StringBuilder query = new StringBuilder();
            query.Append($"SELECT * FROM {currentNode.Schema}.\"{currentNode.Name}\" ");

            if (edgeData != null)
            {
                
                int count = 0;
                foreach (var binding in edgeData.ColumnBindings)
                {
                    if(count == 0)
                        query.Append(" WHERE ");
                    else query.Append(" AND ");

                    query.Append($"{binding.TargetColumn} = {{{count}}}");
                    count++;
                }
            }

            if(whereCondition!= null)
                {
                if (edgeData != null && edgeData.ColumnBindings.Any())
                    query.Append(" AND ");
                else
                    query.Append(" WHERE ");

                query.Append("(");
                query.Append(whereCondition);
                query.Append(")");
            }

            return ValueTask.FromResult<string>(query.ToString());
        }

  
    }
}
