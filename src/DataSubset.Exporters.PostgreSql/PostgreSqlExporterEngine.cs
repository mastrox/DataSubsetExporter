using DataSubset.DbDependencyGraph.Core.Configurations;
using DataSubset.DbDependencyGraph.Core.DependencyGraph;
using DataSubset.Exporters.Common;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataSubset.Exporters.PostgreSql
{
    public class PostgreSqlExporterEngine(string connectionString) : DbExporterEngineBase
    {
        // Use Npgsql for PostgreSQL database connections and commands
        protected override async IAsyncEnumerable<(string column, object? value)[]> ExecuteGetRowQuery(string queryWithValues)
        {
            using var connection = new Npgsql.NpgsqlConnection(connectionString);
            await connection.OpenAsync();
            using var command = new Npgsql.NpgsqlCommand(queryWithValues, connection);
            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var row = new (string column, object? value)[reader.FieldCount];
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    row[i] = (reader.GetName(i), reader.IsDBNull(i) ? null : reader.GetValue(i));
                }
                yield return row;
            }
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

        protected override ValueTask<string> GenerateSelectQuery(TableNode currentNode, ITableDependencyEdgeData? edgeData, string? whereCondition, PrimaryKeyValue[]? primaryKeyValue)
        {
            StringBuilder query = new StringBuilder();
            query.Append($"SELECT * FROM {currentNode.Schema}.\"{currentNode.Name}\" ");
            bool hasWhereCondition = false;
            if (edgeData != null)
            {

                int count = 0;
                foreach (var binding in edgeData.ColumnBindings)
                {

                    if (count == 0)
                    {
                        query.Append(" WHERE ");
                        hasWhereCondition = true;
                    }
                    else query.Append(" AND ");

                    query.Append($"{binding.TargetColumn} = {{{count}}}");
                    count++;
                }
            }

            if (whereCondition != null)
            {
                if (hasWhereCondition)
                    query.Append(" AND ");
                else
                {
                    query.Append(" WHERE ");
                    hasWhereCondition = true;
                }

                query.Append("(");
                query.Append(whereCondition);
                query.Append(")");
            }

            if (primaryKeyValue != null && primaryKeyValue.Length > 0)
            {
                if (hasWhereCondition)
                    query.Append(" AND ");
                else
                {
                    query.Append(" WHERE ");
                    hasWhereCondition = true;
                }
                
                bool first = true;
                foreach (var pk in primaryKeyValue)
                {
                    if (!first)
                        query.Append(" AND ");
                    else first = false;
                    query.Append($"{pk.ColumnName} = {pk.Value}");
                }

            }

            return ValueTask.FromResult<string>(query.ToString());
        }

        protected override string AddValuesToQuery(string query, (string column, object? value)[]? values)
        {
            for (int i = 0; i < (values?.Length ?? 0); i++)
            {
                var value = values[i].value;
                query = query.Replace($"{{{i}}}", FormatSqlValue(value));
            }
            return query;
        }

        /// <summary>
        /// Formats a value for SQL.
        /// </summary>
        /// <param name="value">The value to format</param>
        /// <returns>A SQL-formatted string representation of the value</returns>
        public static string FormatSqlValue(object value)
        {
            if (value == null)
                return "NULL";

            if (value is DateTime dt)
                return $"'{dt:yyyy-MM-dd HH:mm:ss}'::timestamp";

            if (value is DateTimeOffset dto)
                return $"'{dto:yyyy-MM-dd HH:mm:ss zzz}'::timestamptz";

            if (value is DateOnly dateOnly)
                return $"'{dateOnly:yyyy-MM-dd}'::date";

            if (value is TimeOnly timeOnly)
                return $"'{timeOnly:HH:mm:ss}'::time";

            if (value is bool b)
                return b ? "TRUE" : "FALSE";

            if (value is string str)
            {
                // Check if this looks like a PostgreSQL array string representation
                if (str.StartsWith("{") && str.EndsWith("}"))
                {
                    // This is likely an array - return it as-is but properly escaped
                    return $"'{str.Replace("'", "''")}'";
                }
                else
                {
                    // Regular string
                    return $"'{str.Replace("'", "''")}'";
                }
            }

            if (value is char c)
                return $"'{c.ToString().Replace("'", "''")}'";

            if (value is Guid guid)
                return $"'{guid}'::uuid";

            if (value is byte[] bytes)
                return $@"'\x{BitConverter.ToString(bytes).Replace("-", "")}'::bytea";

            // Handle decimal/numeric types with proper formatting
            if (value is decimal dec)
                return dec.ToString(CultureInfo.InvariantCulture);

            if (value is double dbl)
            {
                if (double.IsNaN(dbl))
                    return "'NaN'::double precision";
                if (double.IsPositiveInfinity(dbl))
                    return "'Infinity'::double precision";
                if (double.IsNegativeInfinity(dbl))
                    return "'-Infinity'::double precision";
                return dbl.ToString(CultureInfo.InvariantCulture);
            }

            if (value is float flt)
            {
                if (float.IsNaN(flt))
                    return "'NaN'::real";
                if (float.IsPositiveInfinity(flt))
                    return "'Infinity'::real";
                if (float.IsNegativeInfinity(flt))
                    return "'-Infinity'::real";
                return flt.ToString(CultureInfo.InvariantCulture);
            }

            // Handle other numeric types that might have decimal places
            if (value is IFormattable formattable && IsNumericType(value))
                return formattable.ToString(null, CultureInfo.InvariantCulture);

            // Handle arrays and other complex types
            if (value.GetType().IsArray)
            {
                // Convert array to PostgreSQL array format
                var array = (Array)value;
                var elements = new List<string>();
                for (int i = 0; i < array.Length; i++)
                {
                    var element = array.GetValue(i);
                    if (element == null)
                    {
                        elements.Add("NULL");
                    }
                    else
                    {
                        // For array elements, we need to handle escaping differently
                        var elementValue = FormatSqlValue(element);
                        // Remove outer quotes and type casting for array elements
                        if (elementValue.StartsWith("'") && elementValue.Contains("'::"))
                        {
                            var endQuote = elementValue.LastIndexOf("'", elementValue.LastIndexOf("::") - 1);
                            elementValue = elementValue.Substring(1, endQuote - 1);
                        }
                        else if (elementValue.StartsWith("'") && elementValue.EndsWith("'"))
                        {
                            elementValue = elementValue.Substring(1, elementValue.Length - 2);
                        }
                        elements.Add($"\"{elementValue}\"");
                    }
                }
                return $"'{{{string.Join(",", elements)}}}'";
            }

            // Handle JSON types (if using Npgsql's JsonDocument or similar)
            if (value.ToString().StartsWith("{") || value.ToString().StartsWith("["))
            {
                return $"'{value.ToString().Replace("'", "''")}'::json";
            }

            // Default case - convert to string and escape
            return $"'{value.ToString().Replace("'", "''")}'";
        }

        /// <summary>
        /// Checks if a type is numeric.
        /// </summary>
        /// <param name="value">The value to check</param>
        /// <returns>True if the value is a numeric type</returns>
        private static bool IsNumericType(object value)
        {
            return value is sbyte || value is byte || value is short || value is ushort ||
                   value is int || value is uint || value is long || value is ulong ||
                   value is float || value is double || value is decimal;
        }

        /// <summary>
        /// Properly escapes PostgreSQL identifiers.
        /// </summary>
        /// <param name="identifier">The identifier to escape</param>
        /// <returns>The escaped identifier</returns>
        public static string EscapeIdentifier(string identifier)
        {
            if (string.IsNullOrEmpty(identifier))
                throw new ArgumentException("Identifier cannot be null or empty", nameof(identifier));

            // Double quotes are used to escape identifiers in PostgreSQL
            return $"\"{identifier.Replace("\"", "\"\"")}\"";
        }


    }
}
