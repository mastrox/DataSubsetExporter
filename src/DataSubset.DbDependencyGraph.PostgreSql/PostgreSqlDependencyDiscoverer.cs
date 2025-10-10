using DataSubset.DbDependencyGraph.Core.Configurations;
using DataSubset.DbDependencyGraph.Core.DependencyGraph;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;

namespace DataSubset.PostgreSql
{
    public class PostgreSqlDependencyDiscoverer(string dbConnectionString): IDatabaseDependencyDiscoverer
    {
        // Discover all tables in the schema and get their primary keys
        public async Task DiscoverTablesAsync(DatabaseGraph graph, string[] schemas, HashSet<string> ignoredTables)
        {
            var query = @"
                SELECT 
                    n.nspname AS schema_name,
                    c.relname AS table_name
                FROM 
                    pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE 
                    (c.relkind = 'r' or c.relkind = 'p')  -- Only regular tables and partitioned table
                    AND n.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                    AND ($1::text[] IS NULL OR n.nspname = ANY($1))
                    -- Exclude partitioned tables
                    AND NOT EXISTS (
                        SELECT 1 FROM pg_inherits i 
                        WHERE i.inhrelid = c.oid
                    )   
                ORDER BY n.nspname, c.relname;
            ";
            var discoveredTables = new List<(string Schema, string TableName)>();
            using (var connection = new NpgsqlConnection(dbConnectionString))
            {
                await connection.OpenAsync();
                await using var cmd = new NpgsqlCommand(query, connection);
                cmd.Parameters.AddWithValue(NpgsqlDbType.Array | NpgsqlDbType.Text, schemas ?? (object)DBNull.Value);

                await using var reader = await cmd.ExecuteReaderAsync();
               
                while (await reader.ReadAsync())
                {
                    var tableSchema = reader.GetString("schema_name");
                    var tableName = reader.GetString("table_name");
                    var fullName = $"{tableSchema}.{tableName}";

                    // Skip ignored tables
                    if (ignoredTables.Contains(fullName))
                    {
                        continue;
                    }

                    discoveredTables.Add((tableSchema, tableName));
                }

            }

            // Now get primary keys for each discovered table
            foreach (var (tableSchema, tableName) in discoveredTables)
            {
                var node = graph.GetOrCreateNode(tableSchema, tableName);
                node.PrimaryKeyColumns = await GetPrimaryKeyColumnsAsync(tableSchema, tableName);

                //if (verbose)
                //{
                //    var pkInfo = node.PrimaryKeyColumns.Any()
                //        ? $"PK: [{string.Join(", ", node.PrimaryKeyColumns)}]"
                //        : "No primary key";
                //    Console.WriteLine($"  Discovered: {node.FullName} - {pkInfo}");
                //}
            }
        }

        // Build foreign key relationships between tables
        public async Task BuildForeignKeyRelationshipsAsync(DatabaseGraph graph, string[] schemas, HashSet<string> ignoredTables)
        {
            var query = @"
                SELECT
                    n_child.nspname AS child_schema,
                    child.relname AS child_table,
                    n_parent.nspname AS parent_schema,
                    parent.relname AS parent_table,
                    a.attname AS child_column,
                    fa.attname AS parent_column,
                    con.conname AS constraint_name
                FROM
                    pg_constraint con
                    JOIN pg_class child ON con.conrelid = child.oid
                    JOIN pg_namespace n_child ON child.relnamespace = n_child.oid
                    JOIN pg_class parent ON con.confrelid = parent.oid
                    JOIN pg_namespace n_parent ON parent.relnamespace = n_parent.oid
                    JOIN pg_attribute a ON a.attrelid = child.oid AND a.attnum = ANY(con.conkey)
                    JOIN pg_attribute fa ON fa.attrelid = parent.oid AND fa.attnum = ANY(con.confkey)
                WHERE 
                    con.contype = 'f'
                    AND ($1::text[] IS NULL OR n_child.nspname = ANY($1) OR n_parent.nspname = ANY($1))
                    -- Exclude partitioned tables
                    AND NOT EXISTS (
                        SELECT 1 FROM pg_inherits i 
                        WHERE i.inhrelid = child.oid OR i.inhrelid = parent.oid
                    )
                ORDER BY n_child.nspname, child.relname, con.conname;
            ";

            // Group by constraint to handle composite foreign keys
            var constraintGroups = new Dictionary<string, List<(string childSchema, string childTable, string parentSchema, string parentTable, string childColumn, string parentColumn, string constraintName)>>();

            using (var connection = new NpgsqlConnection(dbConnectionString))
            {
                await connection.OpenAsync();
                await using var cmd = new NpgsqlCommand(query, connection);
                cmd.Parameters.AddWithValue(NpgsqlDbType.Array | NpgsqlDbType.Text, schemas ?? (object)DBNull.Value);
                
                await using var reader = await cmd.ExecuteReaderAsync();

                while (await reader.ReadAsync())
                {
                    var childSchema = reader.GetString("child_schema");
                    var childTable = reader.GetString("child_table");
                    var parentSchema = reader.GetString("parent_schema");
                    var parentTable = reader.GetString("parent_table");
                    var childColumn = reader.GetString("child_column");
                    var parentColumn = reader.GetString("parent_column");
                    var constraintName = reader.GetString("constraint_name");

                    var key = $"{childSchema}.{childTable}.{constraintName}";
                    if (!constraintGroups.ContainsKey(key))
                    {
                        constraintGroups[key] = new List<(string, string, string, string, string, string, string)>();
                    }

                    constraintGroups[key].Add((childSchema, childTable, parentSchema, parentTable, childColumn, parentColumn, constraintName));
                }
            }

            // Process each constraint as a single edge
            foreach (var group in constraintGroups.Values)
            {
                var first = group.First();
                var childFullName = $"{first.childSchema}.{first.childTable}";
                var parentFullName = $"{first.parentSchema}.{first.parentTable}";

                // Skip if either table is ignored
                if (ignoredTables.Contains(childFullName) || ignoredTables.Contains(parentFullName))
                {
                    //if (verbose)
                    //{
                    //    Console.WriteLine($"  Skipping FK relationship (ignored table): {childFullName} -> {parentFullName}");
                    //}
                    continue;
                }

                // Get or create nodes
                var childNode = graph.GetOrCreateNode(first.childSchema, first.childTable);
                var parentNode = graph.GetOrCreateNode(first.parentSchema, first.parentTable);

                // Create column bindings for the constraint
                var columnBindings = group.Select(g => new ColumnBinding
                {
                    SourceColumn = g.childColumn,
                    TargetColumn = g.parentColumn
                }).ToList();

                // Create edge data with all column bindings
                var edgeData = new FkTableDependencyEdgeData(first.childSchema, first.childTable, columnBindings, first.constraintName);

                // Add single edge to graph for the entire constraint
                graph.AddEdge(childNode, parentNode, edgeData);

                //if (verbose)
                //{
                //    var columnInfo = string.Join(", ", columnBindings.Select(cb => $"{cb.SourceColumn}->{cb.TargetColumn}"));
                //    Console.WriteLine($"  FK: {childFullName}({columnInfo}) -> {parentFullName} (Constraint: {first.constraintName})");
                //}
            }
        }

        // Get primary key columns for a specific table
        private async Task<List<string>> GetPrimaryKeyColumnsAsync(string schema, string tableName)
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
            using (var connection = new NpgsqlConnection(dbConnectionString))
            {

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
        }

    }
}
