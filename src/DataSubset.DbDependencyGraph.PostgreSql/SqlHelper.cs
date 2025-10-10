using DataSubset.Exporters.Common;
using Npgsql;
using NpgsqlTypes;
using System.Globalization;
using System.Text;

namespace DataSubset.PostgreSql
{
    /// <summary>
    /// Represents a parameterized SQL statement with its parameters.
    /// </summary>
    public class ParameterizedSql
    {
        public string Sql { get; set; }
        public List<NpgsqlParameter> Parameters { get; set; } = new();

        public ParameterizedSql(string sql, List<NpgsqlParameter>? parameters = null)
        {
            Sql = sql;
            Parameters = parameters ?? new List<NpgsqlParameter>();
        }

        /// <summary>
        /// Converts the parameterized SQL to a string with inline values for display/logging purposes.
        /// </summary>
        public string ToInlineString()
        {
            var result = Sql;
            for (int i = 0; i < Parameters.Count; i++)
            {
                var param = Parameters[i];
                var placeholder = $"${i + 1}";
                var value = SqlHelper.FormatSqlValue(param.Value);
                result = result.Replace(placeholder, value);
            }
            return result;
        }

        public override string ToString()
        {
            return ToInlineString();
        }
    }

    /// <summary>
    /// Represents a batch of INSERT statements for the same table.
    /// </summary>
    public class BatchInsertResult
    {
        public required string TableKey { get; set; }
        public required string Schema { get; set; }
        public required string TableName { get; set; }
        public List<string> InsertStatements { get; set; } = new();
        public int RowCount { get; set; }

        public override string ToString()
        {
            return $"Batch for {Schema}.{TableName}: {RowCount} rows, {InsertStatements.Count} statements";
        }
    }

    /// <summary>
    /// Configuration for batch insert operations.
    /// </summary>
    public class BatchInsertConfig
    {
        /// <summary>
        /// Maximum number of rows per batch INSERT statement. Default: 100
        /// </summary>
        public int MaxRowsPerBatch { get; set; } = 100;

        /// <summary>
        /// Maximum number of rows to collect before flushing the batch. Default: 1000
        /// </summary>
        public int MaxRowsToBuffer { get; set; } = 1000;

        /// <summary>
        /// Whether to use multi-row VALUES syntax. Default: true
        /// </summary>
        public bool UseMultiRowValues { get; set; } = true;

        /// <summary>
        /// Whether to add ON CONFLICT DO NOTHING clause. Default: false
        /// </summary>
        public bool AddOnConflictDoNothing { get; set; } = false;
    }

    /// <summary>
    /// Utility class for SQL operations, including statement generation and value formatting.
    /// </summary>
    public static class SqlHelper
    {
        /// <summary>
        /// Generates an INSERT statement for a row.
        /// </summary>
        /// <param name="schema">The schema name</param>
        /// <param name="tableName">The table name</param>
        /// <param name="columns">The column metadata</param>
        /// <param name="rowData">The row data</param>
        /// <returns>A SQL INSERT statement</returns>
        public static string GenerateInsertStatement(
            string schema, string tableName, List<ColumnInfo> columns, Dictionary<string, object> rowData)
        {
            var columnNames = new List<string>();
            var values = new List<string>();

            foreach (var column in columns)
            {
                if (rowData.TryGetValue(column.Name, out var value))
                {
                    columnNames.Add(EscapeIdentifier(column.Name));
                    values.Add(FormatSqlValue(value));
                }
            }

            var sb = new StringBuilder();
            sb.Append($"INSERT INTO {EscapeIdentifier(schema)}.{EscapeIdentifier(tableName)} (");
            sb.Append(string.Join(", ", columnNames));
            sb.Append(") VALUES (");
            sb.Append(string.Join(", ", values));
            sb.Append(");");

            return sb.ToString();
        }

        /// <summary>
        /// Generates a batch INSERT statement for multiple rows.
        /// </summary>
        /// <param name="schema">The schema name</param>
        /// <param name="tableName">The table name</param>
        /// <param name="columns">The column metadata</param>
        /// <param name="rowsData">The collection of row data</param>
        /// <param name="config">Batch configuration</param>
        /// <returns>A list of batch INSERT statements</returns>
        public static List<string> GenerateBatchInsertStatements(
            string schema, string tableName, List<ColumnInfo> columns,
            IEnumerable<Dictionary<string, object>> rowsData, BatchInsertConfig? config = null)
        {
            config ??= new BatchInsertConfig();
            var statements = new List<string>();
            var rows = rowsData.ToList();

            if (!rows.Any())
                return statements;

            // Get consistent column ordering based on the first row and available columns
            var firstRow = rows.First();
            var orderedColumns = columns
                .Where(col => firstRow.ContainsKey(col.Name))
                .ToList();

            var columnNames = orderedColumns.Select(col => EscapeIdentifier(col.Name)).ToList();
            var columnNamesString = string.Join(", ", columnNames);

            // Process rows in batches
            for (int i = 0; i < rows.Count; i += config.MaxRowsPerBatch)
            {
                var batchRows = rows.Skip(i).Take(config.MaxRowsPerBatch);
                var valueRows = new List<string>();

                foreach (var rowData in batchRows)
                {
                    var values = new List<string>();
                    foreach (var column in orderedColumns)
                    {
                        var value = rowData.TryGetValue(column.Name, out var val) ? val : null;
                        values.Add(FormatSqlValue(value));
                    }
                    valueRows.Add($"({string.Join(", ", values)})");
                }

                var sb = new StringBuilder();
                sb.Append($"INSERT INTO {EscapeIdentifier(schema)}.{EscapeIdentifier(tableName)} (");
                sb.Append(columnNamesString);
                sb.Append(") VALUES ");
                sb.Append(string.Join(", ", valueRows));

                if (config.AddOnConflictDoNothing)
                {
                    sb.Append(" ON CONFLICT DO NOTHING");
                }

                sb.Append(';');
                statements.Add(sb.ToString());
            }

            return statements;
        }

        /// <summary>
        /// Generates a batch parameterized INSERT statement for multiple rows.
        /// </summary>
        /// <param name="schema">The schema name</param>
        /// <param name="tableName">The table name</param>
        /// <param name="columns">The column metadata</param>
        /// <param name="rowsData">The collection of row data</param>
        /// <param name="config">Batch configuration</param>
        /// <returns>A list of parameterized batch INSERT statements</returns>
        public static List<ParameterizedSql> GenerateBatchParameterizedInsertStatements(
            string schema, string tableName, List<ColumnInfo> columns,
            IEnumerable<Dictionary<string, object>> rowsData, BatchInsertConfig? config = null)
        {
            config ??= new BatchInsertConfig();
            var statements = new List<ParameterizedSql>();
            var rows = rowsData.ToList();

            if (!rows.Any())
                return statements;

            // Get consistent column ordering based on the first row and available columns
            var firstRow = rows.First();
            var orderedColumns = columns
                .Where(col => firstRow.ContainsKey(col.Name))
                .ToList();

            var columnNames = orderedColumns.Select(col => EscapeIdentifier(col.Name)).ToList();
            var columnNamesString = string.Join(", ", columnNames);

            // Process rows in batches
            for (int i = 0; i < rows.Count; i += config.MaxRowsPerBatch)
            {
                var batchRows = rows.Skip(i).Take(config.MaxRowsPerBatch).ToList();
                var parameters = new List<NpgsqlParameter>();
                var valueRows = new List<string>();
                int paramIndex = 1;

                foreach (var rowData in batchRows)
                {
                    var rowParams = new List<string>();
                    foreach (var column in orderedColumns)
                    {
                        var value = rowData.TryGetValue(column.Name, out var val) ? val : null;
                        var parameter = CreateNpgsqlParameter($"p{paramIndex}", value, column);
                        parameters.Add(parameter);
                        rowParams.Add($"${paramIndex}");
                        paramIndex++;
                    }
                    valueRows.Add($"({string.Join(", ", rowParams)})");
                }

                var sb = new StringBuilder();
                sb.Append($"INSERT INTO {EscapeIdentifier(schema)}.{EscapeIdentifier(tableName)} (");
                sb.Append(columnNamesString);
                sb.Append(") VALUES ");
                sb.Append(string.Join(", ", valueRows));

                if (config.AddOnConflictDoNothing)
                {
                    sb.Append(" ON CONFLICT DO NOTHING");
                }

                sb.Append(';');
                statements.Add(new ParameterizedSql(sb.ToString(), parameters));
            }

            return statements;
        }

        /// <summary>
        /// Generates a parameterized INSERT statement for a row.
        /// </summary>
        /// <param name="schema">The schema name</param>
        /// <param name="tableName">The table name</param>
        /// <param name="columns">The column metadata</param>
        /// <param name="rowData">The row data</param>
        /// <returns>A parameterized SQL INSERT statement</returns>
        public static ParameterizedSql GenerateParameterizedInsertStatement(
            string schema, string tableName, List<ColumnInfo> columns, Dictionary<string, object> rowData)
        {
            var columnNames = new List<string>();
            var parameters = new List<NpgsqlParameter>();
            var parameterPlaceholders = new List<string>();
            int paramIndex = 1;

            foreach (var column in columns)
            {
                if (rowData.TryGetValue(column.Name, out var value))
                {
                    columnNames.Add(EscapeIdentifier(column.Name));
                    parameterPlaceholders.Add($"${paramIndex}");

                    // Create parameter with appropriate type
                    var parameter = CreateNpgsqlParameter($"p{paramIndex}", value, column);
                    parameters.Add(parameter);

                    paramIndex++;
                }
            }

            var sb = new StringBuilder();
            sb.Append($"INSERT INTO {EscapeIdentifier(schema)}.{EscapeIdentifier(tableName)} (");
            sb.Append(string.Join(", ", columnNames));
            sb.Append(") VALUES (");
            sb.Append(string.Join(", ", parameterPlaceholders));
            sb.Append(");");

            return new ParameterizedSql(sb.ToString(), parameters);
        }

        /// <summary>
        /// Generates a template for parameterized INSERT statements that can be cached per table.
        /// This template can be reused with different parameter values.
        /// </summary>
        /// <param name="schema">The schema name</param>
        /// <param name="tableName">The table name</param>
        /// <param name="columns">The column metadata</param>
        /// <returns>A SQL template with parameter placeholders and column information</returns>
        public static InsertStatementTemplate GenerateInsertStatementTemplate(
            string schema, string tableName, List<ColumnInfo> columns)
        {
            var columnNames = new List<string>();
            var parameterPlaceholders = new List<string>();
            var columnInfos = new List<ColumnInfo>();
            int paramIndex = 1;

            foreach (var column in columns)
            {
                columnNames.Add(EscapeIdentifier(column.Name));
                parameterPlaceholders.Add($"${paramIndex}");
                columnInfos.Add(column);
                paramIndex++;
            }

            var sb = new StringBuilder();
            sb.Append($"INSERT INTO {EscapeIdentifier(schema)}.{EscapeIdentifier(tableName)} (");
            sb.Append(string.Join(", ", columnNames));
            sb.Append(") VALUES (");
            sb.Append(string.Join(", ", parameterPlaceholders));
            sb.Append(");");

            return new InsertStatementTemplate
            {
                SqlTemplate = sb.ToString(),
                Columns = columnInfos,
                Schema = schema,
                TableName = tableName
            };
        }

        /// <summary>
        /// Creates parameters for a template based on row data.
        /// </summary>
        /// <param name="template">The insert statement template</param>
        /// <param name="rowData">The row data</param>
        /// <returns>A parameterized SQL statement</returns>
        public static ParameterizedSql CreateParameterizedStatementFromTemplate(
            InsertStatementTemplate template, Dictionary<string, object> rowData)
        {
            var parameters = new List<NpgsqlParameter>();
            int paramIndex = 1;

            foreach (var column in template.Columns)
            {
                if (rowData.TryGetValue(column.Name, out var value))
                {
                    var parameter = CreateNpgsqlParameter($"p{paramIndex}", value, column);
                    parameters.Add(parameter);
                    paramIndex++;
                }
            }

            return new ParameterizedSql(template.SqlTemplate, parameters);
        }

        /// <summary>
        /// Creates an NpgsqlParameter with appropriate type mapping.
        /// </summary>
        private static NpgsqlParameter CreateNpgsqlParameter(string parameterName, object? value, ColumnInfo column)
        {
            var parameter = new NpgsqlParameter(parameterName, value ?? DBNull.Value);

            // Set appropriate NpgsqlDbType based on column data type
            if (!string.IsNullOrEmpty(column.DataType))
            {
                parameter.NpgsqlDbType = MapPostgreSqlTypeToNpgsqlDbType(column.DataType);
            }

            return parameter;
        }

        /// <summary>
        /// Maps PostgreSQL data types to NpgsqlDbType enumeration.
        /// </summary>
        private static NpgsqlDbType MapPostgreSqlTypeToNpgsqlDbType(string postgresType)
        {
            // Handle array types
            if (postgresType.EndsWith("[]"))
            {
                var baseType = postgresType.Substring(0, postgresType.Length - 2);
                var baseNpgsqlType = MapPostgreSqlTypeToNpgsqlDbType(baseType);
                return baseNpgsqlType | NpgsqlDbType.Array;
            }

            return postgresType.ToLowerInvariant() switch
            {
                "integer" or "int4" => NpgsqlDbType.Integer,
                "bigint" or "int8" => NpgsqlDbType.Bigint,
                "smallint" or "int2" => NpgsqlDbType.Smallint,
                "decimal" or "numeric" => NpgsqlDbType.Numeric,
                "real" or "float4" => NpgsqlDbType.Real,
                "double precision" or "float8" => NpgsqlDbType.Double,
                "boolean" or "bool" => NpgsqlDbType.Boolean,
                "character varying" or "varchar" => NpgsqlDbType.Varchar,
                "character" or "char" => NpgsqlDbType.Char,
                "text" => NpgsqlDbType.Text,
                "timestamp without time zone" or "timestamp" => NpgsqlDbType.Timestamp,
                "timestamp with time zone" or "timestamptz" => NpgsqlDbType.TimestampTz,
                "date" => NpgsqlDbType.Date,
                "time" or "time without time zone" => NpgsqlDbType.Time,
                "time with time zone" or "timetz" => NpgsqlDbType.TimeTz,
                "uuid" => NpgsqlDbType.Uuid,
                "bytea" => NpgsqlDbType.Bytea,
                "json" => NpgsqlDbType.Json,
                "jsonb" => NpgsqlDbType.Jsonb,
                _ => NpgsqlDbType.Unknown
            };
        }

        /// <summary>
        /// Formats a value for SQL.
        /// </summary>
        /// <param name="value">The value to format</param>
        /// <returns>A SQL-formatted string representation of the value</returns>
        public static string FormatSqlValue(object? value)
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
                if (str.StartsWith('{') && str.EndsWith('}'))
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
            if (value != null && (value.ToString()!.StartsWith("{") || value.ToString()!.StartsWith("[")))
            {
                return $"'{value!.ToString()!.Replace("'", "''")}'::json";
            }

            // Default case - convert to string and escape
            return $"'{value?.ToString()?.Replace("'", "''")}'";
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

        /// <summary>
        /// Generates a SELECT statement with optional WHERE clause.
        /// </summary>
        /// <param name="schema">The schema name</param>
        /// <param name="tableName">The table name</param>
        /// <param name="columns">The columns to select (null for all columns)</param>
        /// <param name="whereClause">Optional WHERE clause</param>
        /// <returns>A SQL SELECT statement</returns>
        public static string GenerateSelectStatement(string schema, string tableName,
            IEnumerable<string>? columns = null, string? whereClause = null)
        {
            var sb = new StringBuilder();
            sb.Append("SELECT ");

            if (columns?.Any() == true)
            {
                sb.Append(string.Join(", ", columns.Select(EscapeIdentifier)));
            }
            else
            {
                sb.Append('*');
            }

            sb.Append($" FROM {EscapeIdentifier(schema)}.{EscapeIdentifier(tableName)}");

            if (!string.IsNullOrWhiteSpace(whereClause))
            {
                sb.Append($" WHERE {whereClause}");
            }

            return sb.ToString();
        }

        /// <summary>
        /// Generates an UPDATE statement.
        /// </summary>
        /// <param name="schema">The schema name</param>
        /// <param name="tableName">The table name</param>
        /// <param name="updates">Dictionary of column names and new values</param>
        /// <param name="whereClause">WHERE clause for the update</param>
        /// <returns>A SQL UPDATE statement</returns>
        public static string GenerateUpdateStatement(string schema, string tableName,
            Dictionary<string, object> updates, string whereClause)
        {
            if (updates == null || !updates.Any())
                throw new ArgumentException("Updates cannot be null or empty", nameof(updates));

            if (string.IsNullOrWhiteSpace(whereClause))
                throw new ArgumentException("WHERE clause is required for UPDATE statements", nameof(whereClause));

            var sb = new StringBuilder();
            sb.Append($"UPDATE {EscapeIdentifier(schema)}.{EscapeIdentifier(tableName)} SET ");

            var setParts = updates.Select(kvp => $"{EscapeIdentifier(kvp.Key)} = {FormatSqlValue(kvp.Value)}");
            sb.Append(string.Join(", ", setParts));

            sb.Append($" WHERE {whereClause}");

            return sb.ToString();
        }

        /// <summary>
        /// Generates a DELETE statement.
        /// </summary>
        /// <param name="schema">The schema name</param>
        /// <param name="tableName">The table name</param>
        /// <param name="whereClause">WHERE clause for the delete</param>
        /// <returns>A SQL DELETE statement</returns>
        public static string GenerateDeleteStatement(string schema, string tableName, string whereClause)
        {
            if (string.IsNullOrWhiteSpace(whereClause))
                throw new ArgumentException("WHERE clause is required for DELETE statements", nameof(whereClause));

            return $"DELETE FROM {EscapeIdentifier(schema)}.{EscapeIdentifier(tableName)} WHERE {whereClause}";
        }
    }

    /// <summary>
    /// Represents a cached INSERT statement template that can be reused with different parameter values.
    /// </summary>
    public class InsertStatementTemplate
    {
        public required string SqlTemplate { get; set; }
        public List<ColumnInfo> Columns { get; set; } = new();
        public required string Schema { get; set; }
        public required string TableName { get; set; }

        public string TableKey => $"{Schema}.{TableName}";
    }
}