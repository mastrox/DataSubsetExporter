using DataSubset.DbDependencyGraph.Core.DependencyGraph;
using DataSubset.Exporters.Common;
using DataSubset.PostgreSql;
using Npgsql;
using NpgsqlTypes;
using System.Data;


namespace DataSubset.Exporters.PostgreSql
{
    /// <summary>
    /// Service class for database operations, providing methods to fetch data and metadata from PostgreSQL.
    /// </summary>
    public class GetExportInfoDatabaseService : IGetExportInfoDatabaseService
    {
        private readonly string connectionString;

        public GetExportInfoDatabaseService(string connectionString)
        {
            this.connectionString = connectionString;
        }

        /// <summary>
        /// Fetches a row by its primary key value.
        /// </summary>
        /// <param name="schema">The schema name</param>
        /// <param name="tableName">The table name</param>
        /// <param name="primaryKeyColumn">The primary key column name</param>
        /// <param name="primaryKeyValue">The primary key value</param>
        /// <returns>A dictionary containing column names and their values</returns>
        public async Task<Dictionary<string, object>> FetchRowByPrimaryKey(
            string schema, string tableName, string primaryKeyColumn, string primaryKeyValue)
        {
            var result = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);

            var query = $"SELECT * FROM {SqlHelper.EscapeIdentifier(schema)}.{SqlHelper.EscapeIdentifier(tableName)} WHERE {SqlHelper.EscapeIdentifier(primaryKeyColumn)} = $1";

            await using var cmd = new NpgsqlCommand(query); //TODO
            cmd.Parameters.AddWithValue(NpgsqlDbType.Unknown, primaryKeyValue);

            try
            {
                await using var reader = await cmd.ExecuteReaderAsync();
                if (await reader.ReadAsync())
                {
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        string columnName = reader.GetName(i);
                        object? value = null;

                        if (!reader.IsDBNull(i))
                        {
                            // Handle PostgreSQL arrays that contain nulls
                            var fieldType = reader.GetFieldType(i);
                            if (fieldType.Name == "Array")
                            {
                                var elementType = fieldType.GetElementType();
                                if (elementType != null)
                                {
                                    // Handle specific array types with potential nulls
                                    if (elementType == typeof(double))
                                    {
                                        value = reader.GetFieldValue<double?[]>(i);
                                    }
                                    else if (elementType == typeof(float))
                                    {
                                        value = reader.GetFieldValue<float?[]>(i);
                                    }
                                    else if (elementType == typeof(int))
                                    {
                                        value = reader.GetFieldValue<int?[]>(i);
                                    }
                                    else if (elementType == typeof(long))
                                    {
                                        value = reader.GetFieldValue<long?[]>(i);
                                    }
                                    else if (elementType == typeof(string))
                                    {
                                        value = reader.GetFieldValue<string[]>(i);
                                    }
                                    else
                                    {
                                        // For other array types, fall back to string representation
                                        value = reader.GetString(i);
                                    }
                                }
                                else if (reader.GetDataTypeName(i).StartsWith("double"))
                                {
                                    value = reader.GetFieldValue<double?[]>(i);
                                }
                                else throw new InvalidCastException();
                            }
                            else
                            {
                                value = reader.GetValue(i);
                            }
                        }

                        result[columnName] = value!;
                    }
                }
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Error fetching row from {schema}.{tableName} with {primaryKeyColumn}={primaryKeyValue}: {ex.Message}", ex);
            }

            return result;
        }

        /// <summary>
        /// Gets metadata for all columns in a table, including foreign key relationships.
        /// </summary>
        /// <param name="schema">The schema name</param>
        /// <param name="tableName">The table name</param>
        /// <returns>A list of column metadata</returns>
        public async Task<List<ColumnInfo>> GetColumnMetadata(string schema, string tableName)
        {
            using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync();
            var columns = new List<ColumnInfo>();

            // Query for column information
            var columnQuery = @"
                SELECT 
                    a.attname AS column_name,
                    pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
                    CASE  
                        WHEN a.attgenerated = 's'  or a.attgenerated = 'v' THEN true
                        ELSE false;
                    END as isgenerated,
                    a.attnotnull AS not_null
                FROM 
                    pg_catalog.pg_attribute a
                    JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
                    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE 
                    n.nspname = $1
                    AND c.relname = $2
                    AND a.attnum > 0 
                    AND NOT a.attisdropped
                ORDER BY a.attnum;
            ";

            await using (var cmd = new NpgsqlCommand(columnQuery, connection))
            {
                cmd.Parameters.AddWithValue(NpgsqlDbType.Text, schema);
                cmd.Parameters.AddWithValue(NpgsqlDbType.Text, tableName);

                await using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    columns.Add(new ColumnInfo
                    {
                        Name = reader.GetString("column_name"),
                        DataType = reader.GetString("data_type"),
                        IsNullable = !reader.GetBoolean("not_null"),
                        IsGenerated = reader.GetBoolean("isgenerated"),
                    });
                }
            }

            // Get primary key information
            var pkQuery = @"
                SELECT a.attname as column_name
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                JOIN pg_class c ON c.oid = i.indrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = $1 AND c.relname = $2
                AND i.indisprimary;
            ";

            await using (var cmd = new NpgsqlCommand(pkQuery, connection))
            {
                cmd.Parameters.AddWithValue(NpgsqlDbType.Text, schema);
                cmd.Parameters.AddWithValue(NpgsqlDbType.Text, tableName);

                await using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    string pkColumn = reader.GetString("column_name");
                    var column = columns.FirstOrDefault(c => c.Name == pkColumn);
                    if (column != null)
                    {
                        column.IsPrimaryKey = true;
                    }
                }
            }

            // Get foreign key information using PostgreSQL system catalogs
            var fkQuery = @"
                SELECT
                    a.attname AS column_name,
                    fn.nspname AS foreign_schema,
                    fc.relname AS foreign_table_name,
                    fa.attname AS foreign_column_name
                FROM
                    pg_constraint con
                    JOIN pg_class c ON con.conrelid = c.oid
                    JOIN pg_namespace n ON c.relnamespace = n.oid
                    JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(con.conkey)
                    JOIN pg_class fc ON con.confrelid = fc.oid
                    JOIN pg_namespace fn ON fc.relnamespace = fn.oid
                    JOIN pg_attribute fa ON fa.attrelid = fc.oid AND fa.attnum = ANY(con.confkey)
                WHERE
                    con.contype = 'f'
                    AND n.nspname = $1
                    AND c.relname = $2
                ORDER BY a.attnum;
            ";

            await using (var cmd = new NpgsqlCommand(fkQuery, connection))
            {
                cmd.Parameters.AddWithValue(NpgsqlDbType.Text, schema);
                cmd.Parameters.AddWithValue(NpgsqlDbType.Text, tableName);

                await using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    string fkColumn = reader.GetString("column_name");
                    var column = columns.FirstOrDefault(c => c.Name == fkColumn);
                    if (column != null)
                    {
                        column.ReferencedSchema = reader.GetString("foreign_schema");
                        column.ReferencedTable = reader.GetString("foreign_table_name");
                        column.ReferencedColumn = reader.GetString("foreign_column_name");
                    }
                }
            }

            return columns;
        }

        /// <summary>
        /// Gets row IDs using a WHERE clause.
        /// </summary>
        /// <param name="schema">The schema name</param>
        /// <param name="tableName">The table name</param>
        /// <param name="whereClause">The WHERE clause to apply</param>
        /// <returns>A list of primary key values that match the criteria</returns>
        public async Task<List<string>> GetRowIdsByWhereClause(string schema, string tableName, string whereClause)
        {
            var rowIds = new List<string>();

            // Get the primary key column
            var pkColumn = await GetPrimaryKeyColumns(schema, tableName);
            if (pkColumn.Count == 0)
            {
                throw new InvalidOperationException($"Could not find primary key for table {schema}.{tableName}");
            }
            var a = string.Join(',', pkColumn.Select(a => SqlHelper.EscapeIdentifier(a)));

            // Build and execute the query)
            var query = $"SELECT {string.Join(", ", pkColumn.Select(a => SqlHelper.EscapeIdentifier(a)))} FROM {SqlHelper.EscapeIdentifier(schema)}.{SqlHelper.EscapeIdentifier(tableName)} WHERE {whereClause}";

            try
            {
                using var connection = new NpgsqlConnection(connectionString);
                await connection.OpenAsync();
                await using var cmd = new NpgsqlCommand(query, connection);
                await using var reader = await cmd.ExecuteReaderAsync();

                while (await reader.ReadAsync())
                {
                    var value = reader.GetValue(0);
                    if (value != null && value != DBNull.Value)
                    {
                        rowIds.Add(value.ToString()!);
                    }
                }
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Error executing WHERE clause for {schema}.{tableName}: {ex.Message}. Query: {query}", ex);
            }

            return rowIds;
        }


        public async Task<List<Dictionary<string, object>>> FetchRowsByWhereClause(string schema, string tableName, string whereClause)
        {
            var result = new List<Dictionary<string, object>>();

            var query = $"SELECT * FROM {SqlHelper.EscapeIdentifier(schema)}.{SqlHelper.EscapeIdentifier(tableName)} WHERE {whereClause}";
            using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync();
            await using var cmd = new NpgsqlCommand(query, connection);

            try
            {
                await using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    var row = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        string columnName = reader.GetName(i);
                        object? value = null;

                        if (!reader.IsDBNull(i))
                        {
                            // Handle PostgreSQL arrays that contain nulls
                            var fieldType = reader.GetFieldType(i);
                            if (fieldType.Name == "Array")
                            {
                                var elementType = fieldType.GetElementType();
                                if (elementType != null)
                                {
                                    // Handle specific array types with potential nulls
                                    if (elementType == typeof(double))
                                    {
                                        value = reader.GetFieldValue<double?[]>(i);
                                    }
                                    else if (elementType == typeof(float))
                                    {
                                        value = reader.GetFieldValue<float?[]>(i);
                                    }
                                    else if (elementType == typeof(int))
                                    {
                                        value = reader.GetFieldValue<int?[]>(i);
                                    }
                                    else if (elementType == typeof(long))
                                    {
                                        value = reader.GetFieldValue<long?[]>(i);
                                    }
                                    else if (elementType == typeof(string))
                                    {
                                        value = reader.GetFieldValue<string[]>(i);
                                    }
                                    else
                                    {
                                        // For other array types, fall back to string representation
                                        value = reader.GetString(i);
                                    }
                                }
                                else if (reader.GetDataTypeName(i).StartsWith("double"))
                                {
                                    value = reader.GetFieldValue<double?[]>(i);
                                }
                                else throw new InvalidCastException();
                            }
                            else
                            {
                                value = reader.GetValue(i);
                            }
                        }

                        row[columnName] = value!;
                    }
                    result.Add(row);
                }
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Error fetching row from {schema}.{tableName} with WHERE clause '{whereClause}': {ex.Message}", ex);
            }

            return result;
        }

        /// <summary>
        /// Gets the primary key columns for a table (supports composite keys).
        /// </summary>
        /// <param name="schema">The schema name</param>
        /// <param name="tableName">The table name</param>
        /// <returns>A list of primary key column names</returns>
        public async Task<List<string>> GetPrimaryKeyColumns(string schema, string tableName)
        {
            var pkColumns = new List<string>();

            var query = @"
                SELECT a.attname as column_name
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                JOIN pg_class c ON c.oid = i.indrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = $1 AND c.relname = $2
                AND i.indisprimary
                ORDER BY array_position(i.indkey, a.attnum);
            ";
            using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync();
            await using var cmd = new NpgsqlCommand(query, connection);
            cmd.Parameters.AddWithValue(NpgsqlDbType.Text, schema);
            cmd.Parameters.AddWithValue(NpgsqlDbType.Text, tableName);

            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                pkColumns.Add(reader.GetString("column_name"));
            }

            return pkColumns;
        }

        public Task<Dictionary<string, object>> FetchRowByPrimaryKey(string schema, string tableName, IEnumerable<string> primaryKeyColumns, IDictionary<string, string>? primaryKeyValues)
        {
            throw new NotImplementedException();
        }

        public (string column, object? value)[] ExecuteGetRowsQuery(string queryWithValues, IDbConnection connection)
        {
            throw new NotImplementedException();
        }

        public Task<string> GenerateInsertStatement(TableNode currentNode, (string column, object? value)[] rowData)
        {
            throw new NotImplementedException();
        }

        public Task<string> GetSelectQuery(TableNode currentNode, ITableDependencyEdgeData? data)
        {
            throw new NotImplementedException();
        }

        public async Task<IDbConnection> OpenConnetionAsync()
        {
            NpgsqlConnection connection = new NpgsqlConnection("");
            await connection.OpenAsync();
            return connection;

        }
    }
}