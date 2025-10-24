using DataSubset.DbDependencyGraph.Core.Configurations;
using DataSubset.DbDependencyGraph.Core.DependencyGraph;
using DataSubset.Exporters.Common;
using DataSubset.Exporters.Common.BinaryExporter;
using NpgsqlTypes;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO.Hashing;
using System.Linq;
using System.Reflection.Metadata;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace DataSubset.Exporters.PostgreSql
{
    public class PostgreSqlExporterEngine(string connectionString) : DbExporterEngineBase
    {
        Dictionary<(string schema, string table), Dictionary<string, ColumnMetadata>> ColumnInfoByFullName = new();
        XxHash32 hash = new XxHash32();

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

        protected override async ValueTask<string> AddValuesToQuery(string schema, string table, string query, (string column, object? value)[]? values)
        {
            for (int i = 0; i < (values?.Length ?? 0); i++)
            {
                var value = values?[i].value;
                query = query.Replace($"{{{i}}}", await ValueToString(schema, table, values[i].column, value));
            }
            return query;
        }

        public async ValueTask<string> ValueToString(string schema, string table, string columnName, object? value)
        {
            if (value == null)
                return "NULL";

            var key = (schema, table);

            //search for the column in 
            if (!ColumnInfoByFullName.TryGetValue(key, out var columns))
            {
                var cols = await GetTableMetadata(schema, table);
                columns = cols.ColumnMetadata.ToDictionary(c => c.Name);
                ColumnInfoByFullName.Add(key, columns);


            }
            if (columns.TryGetValue(columnName, out var columnInfo))
            {
                //use Npgsql to convert value to string
                return GetValueString(columnName, ref value, columnInfo.DataType);
            }

            return $"'{value.ToString()}'";
        }

        private string GetValueString(string columnName, ref object value, string dataType)
        {
            // Normalize the data type to lowercase for case-insensitive comparison
            var normalizedDataType = dataType.ToLowerInvariant();

            // Check if this is an array type (ends with [])
            if (normalizedDataType.Equals("array", StringComparison.OrdinalIgnoreCase) ||
                normalizedDataType.StartsWith("_") ||
                value.GetType().IsArray)
            {
                // Convert array to PostgreSQL array format
                var array = (Array)value;
                var elements = new List<string>();

                // Get the element type by removing array indicators
                var elementType = normalizedDataType.TrimStart('_');

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
                        var elementValue = GetSqlValue(element, elementType);
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

            return GetSqlValue(value, normalizedDataType);
        }

        private static string GetSqlValue(object value, string dataType)
        {
            if (value == null)
                return "NULL";

            // Normalize for case-insensitive comparison
            dataType = dataType.ToLowerInvariant();

            // Numeric types
            if (dataType == "bigint" || dataType == "int8")
                return Convert.ToInt64(value).ToString(CultureInfo.InvariantCulture);

            if (dataType == "double precision" || dataType == "float8")
            {
                value = Convert.ToDouble(value);
                if (double.IsNaN((double)value))
                    return "'NaN'::double precision";
                if (double.IsPositiveInfinity((double)value))
                    return "'Infinity'::double precision";
                if (double.IsNegativeInfinity((double)value))
                    return "'-Infinity'::double precision";
                return ((double)value).ToString(CultureInfo.InvariantCulture);
            }

            if (dataType == "integer" || dataType == "int" || dataType == "int4")
                return Convert.ToInt32(value).ToString(CultureInfo.InvariantCulture);

            if (dataType == "numeric" || dataType == "decimal")
                return Convert.ToDecimal(value).ToString(CultureInfo.InvariantCulture);

            if (dataType == "real" || dataType == "float4")
                return Convert.ToSingle(value).ToString(CultureInfo.InvariantCulture);

            if (dataType == "smallint" || dataType == "int2")
                return Convert.ToInt16(value).ToString(CultureInfo.InvariantCulture);

            if (dataType == "money")
                return Convert.ToDecimal(value).ToString(CultureInfo.InvariantCulture);

            // Boolean
            if (dataType == "boolean" || dataType == "bool")
                return Convert.ToBoolean(value) ? "TRUE" : "FALSE";

            // Character types
            if (dataType == "character" || dataType == "char")
                return $"'{Convert.ToChar(value).ToString().Replace("'", "''")}'";

            if (dataType == "text" || dataType == "character varying" || dataType == "varchar" || dataType == "citext")
                return $"'{value.ToString()?.Replace("'", "''")}'";

            // Binary
            if (dataType == "bytea")
                return $@"'\x{BitConverter.ToString((byte[])value).Replace("-", "")}'::bytea";

            // Date/Time types
            if (dataType == "date")
            {
                var dateOnly = Convert.ToDateTime(value);
                return $"'{dateOnly:yyyy-MM-dd}'::date";
            }

            if (dataType == "time" || dataType == "time without time zone")
            {
                var timeOnly = (TimeSpan)value;
                return $"'{timeOnly}'::time";
            }

            if (dataType == "timestamp" || dataType == "timestamp without time zone")
            {
                var dateTime = Convert.ToDateTime(value);
                return $"'{dateTime:yyyy-MM-dd HH:mm:ss}'::timestamp";
            }

            if (dataType == "timestamp with time zone" || dataType == "timestamptz")
            {
                var dateTimeOffset = value as DateTimeOffset?;
                if (dateTimeOffset == null)
                    dateTimeOffset = new DateTimeOffset(Convert.ToDateTime(value));
                return $"'{dateTimeOffset:yyyy-MM-dd HH:mm:ss zzz}'::timestamptz";
            }

            if (dataType == "interval")
                return $"'{value.ToString()}'::interval";

            if (dataType == "time with time zone" || dataType == "timetz")
            {
                var timeTz = value as DateTimeOffset?;
                if (timeTz == null)
                    timeTz = new DateTimeOffset(Convert.ToDateTime(value));
                return $"'{timeTz:HH:mm:ss zzz}'::timetz";
            }

            // Bit strings
            if (dataType == "bit" || dataType == "bit varying" || dataType == "varbit")
            {
                switch (value)
                {
                    case bool boleanValue:
                        return boleanValue ? "B'1'" : "B'0'";
                    case BitArray bitArr:
                        var sb = new StringBuilder("B'");

                        for (int i = 0; i < bitArr.Count; i++)
                        {
                            char c = bitArr[i] ? '1' : '0';
                            sb.Append(c);
                        }
                        sb.Append("'");
                        return sb.ToString();
                }
            }

            // UUID
            if (dataType == "uuid")
                return $"'{value.ToString()}'::uuid";

            // XML
            if (dataType == "xml")
                return $"'{value.ToString()?.Replace("'", "''")}'::xml";

            // JSON types
            if (dataType == "json")
                return $"'{value.ToString()?.Replace("'", "''")}'::json";

            if (dataType == "jsonb")
                return $"'{value.ToString()?.Replace("'", "''")}'::jsonb";

            if (dataType == "jsonpath")
                return $"'{value.ToString()?.Replace("'", "''")}'::jsonpath";


            // Range types
            if (dataType == "tsrange" || dataType == "tstzrange" || dataType == "daterange" ||
                dataType == "int4range" || dataType == "int8range" || dataType == "numrange" ||
                dataType == "tsmultirange" || dataType == "tstzmultirange" || dataType == "datemultirange" ||
                dataType == "int4multirange" || dataType == "int8multirange" || dataType == "nummultirange")
            {
                // Check if value is NpgsqlRange type
                var valueT = value.GetType();
                if (valueT.IsGenericType && (valueT.GetGenericTypeDefinition().Name.Contains("NpgsqlRange") || 
                                              valueT.GetGenericTypeDefinition().Name.Contains("NpgsqlMultiRange")))
                {
                    // For daterange, format without time component
                    if (dataType == "daterange" || dataType == "datemultirange")
                    {
                        string quote= dataType == "daterange" ? "'" : "";
                        var rangeVal = (NpgsqlRange<DateTime>)value;
                        
                        var lowerBound = rangeVal.LowerBoundInfinite ? "" : rangeVal.LowerBound.ToString("yyyy-MM-dd");
                        var upperBound = rangeVal.UpperBoundInfinite ? "" : rangeVal.UpperBound.ToString("yyyy-MM-dd");
                        var lowerSymbol = rangeVal.LowerBoundIsInclusive ? "[" : "(";
                        var upperSymbol = rangeVal.UpperBoundIsInclusive ? "]" : ")";
                        return $"{quote}{lowerSymbol}{lowerBound},{upperBound}{upperSymbol}{quote}";
                    }

                    // Use ToString which formats as [lower,upper) or (lower,upper] etc.
                    return $"'{value.ToString()}'";
                }

                // Check if value is NpgsqlRange type (duplicate check removed, kept for safety)
                var valueType = value.GetType();
                if (valueType.IsGenericType && (valueType.GetGenericTypeDefinition().Name.Contains("NpgsqlRange") || 
                                                 valueType.GetGenericTypeDefinition().Name.Contains("NpgsqlMultiRange")))
                {
                    // Use ToString which formats appropriately for range and multirange types
                    return $"'{value.ToString()}'";
                }

                // Fallback for string representation
                return $"'{value.ToString()}'";
            }

            // Default case - convert to string and escape
            return $"'{value.ToString()?.Replace("'", "''")}'";
        }

        /// <summary>
        /// Formats a value for SQL.
        /// </summary>
        /// <param name="value">The value to format</param>
        /// <returns>A SQL-formatted string representation of the value</returns>
        public override string ValueToString(object? value)
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
                        var elementValue = ValueToString(element);
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
                return $"'{value.ToString()?.Replace("'", "''")}'::json";
            }

            // Default case - convert to string and escape
            return $"'{value.ToString()?.Replace("'", "''")}'";
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

        public override DbTypes GetDbType()
        {
            return DbTypes.Postgres;
        }

        public override async Task<TableMetadata> GetTableMetadata(string schema, string tableName)
        {
            var columns = new List<ColumnMetadata>();

            using var connection = new Npgsql.NpgsqlConnection(connectionString);
            await connection.OpenAsync();

            var query = @"
                SELECT 
                    column_name,
                    data_type,
                    udt_name
                FROM 
                    information_schema.columns
                WHERE 
                    table_schema = @schema 
                    AND table_name = @tableName
                ORDER BY 
                    ordinal_position";

            using var command = new Npgsql.NpgsqlCommand(query, connection);
            command.Parameters.AddWithValue("@schema", schema);
            command.Parameters.AddWithValue("@tableName", tableName);

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var columnName = reader.GetString(0);
                var dataType = reader.GetString(1);
                var udtName = reader.GetString(2);

                // Use udt_name for more specific type information (e.g., for user-defined types)
                var finalDataType = udtName;

                columns.Add(new ColumnMetadata
                {
                    Name = columnName,
                    DataType = finalDataType
                });
            }
            hash.Append(Encoding.UTF8.GetBytes($"{schema}.{tableName}"));
            var key = BitConverter.ToInt32(hash.GetCurrentHash(), 0);
            return new TableMetadata()
            {
                TableKey = key,
                Schema = schema,
                Table = tableName,
                ColumnMetadata = columns.ToArray(),
            };
        }
    }
}
