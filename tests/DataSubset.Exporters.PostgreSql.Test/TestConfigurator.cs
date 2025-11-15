using DataSubset.DbDependencyGraph.Core.Configurations;
using DataSubset.DbDependencyGraph.Core.DependencyGraph;
using DataSubset.Exporters.Common.InsetStatementExporter;
using DataSubset.PostgreSql;
using DependencyTreeApp;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataSubset.Exporters.PostgreSql.Test
{
    internal class TestConfigurator
    {
        internal static async Task<(string firstTableName, int firtsIdToExport, string secondTableName, int secondIdToExport, string exportSchema, string importSchema, DatabaseGraph graph, TableExportConfig[] tableExportConfig)> Multiple_RootTable_Overlapping_Hierarchy_Overlapping_DataConfig(string connString, NpgsqlCommand command)
        {
            string firstTableName = "table2";
            int firtsIdToExport = 1;
            string secondTableName = "table5";
            int secondIdToExport = 1;

            string exportSchema = "export";
            string importSchema = "import";

            await DropSchema(command, exportSchema);
            await DropSchema(command, importSchema);

            // 1) Ensure schemas exist
            command.CommandText = $"CREATE SCHEMA IF NOT EXISTS {exportSchema};";
            await command.ExecuteNonQueryAsync();
            command.CommandText = $"CREATE SCHEMA IF NOT EXISTS {importSchema};";
            await command.ExecuteNonQueryAsync();

            await CreateTable1(command, exportSchema);
            await CreateTable2(command, exportSchema);
            await CreateTable5(command, exportSchema);

            await CreateTable1(command, importSchema);
            await CreateTable2(command, importSchema);
            await CreateTable5(command, importSchema);

            // 5) Insert sample rows into export.table1
            command.CommandText = "INSERT INTO export.table1 (id, name) VALUES (1, 'Alice') ON CONFLICT (id) DO NOTHING;";
            await command.ExecuteNonQueryAsync();
            command.CommandText = "INSERT INTO export.table1 (id, name) VALUES (2, 'John') ON CONFLICT (id) DO NOTHING;";
            await command.ExecuteNonQueryAsync();

            // 6) Insert sample rows into export.table2 referencing table1
            command.CommandText = "INSERT INTO export.table2 (id, table1_id, data) VALUES (1, 1, 'Bob') ON CONFLICT (id) DO NOTHING;";
            await command.ExecuteNonQueryAsync();
            command.CommandText = "INSERT INTO export.table2 (id, table1_id, data) VALUES (2, 2, 'Useless') ON CONFLICT (id) DO NOTHING;";
            await command.ExecuteNonQueryAsync();


            // 6) Insert sample rows into export.table5 referencing table1
            command.CommandText = "INSERT INTO export.table5 (id, table1_id, data) VALUES (1, 1, 'Mike') ON CONFLICT (id) DO NOTHING;";
            await command.ExecuteNonQueryAsync();
            command.CommandText = "INSERT INTO export.table5 (id, table1_id, data) VALUES (2, 2, 'Useless') ON CONFLICT (id) DO NOTHING;";
            await command.ExecuteNonQueryAsync();

            // 7) Build dependency graph for schema 'export'
            var postgreSqlDependencyDiscoverer = new PostgreSqlDependencyDiscoverer(connString);
            var tableDependencyGraphBuilder = new TableDependencyGraphBuilder(postgreSqlDependencyDiscoverer);
            var graph = await tableDependencyGraphBuilder.BuildDependencyGraphAsync(new[] { "export" });


            // 9) Configure the table export to use schema 'export' and table 'table2' with PK id=1
            TableExportConfig[] tableExportConfig = new[]
            {
                    new TableExportConfig
                    {
                        TableName = firstTableName,
                        Schema = "export",
                        PrimaryKeyValue = new PrimaryKeyValue[]
                        {
                            new PrimaryKeyValue
                            {
                                ColumnName = "id",
                                Value = firtsIdToExport.ToString()
                            }
                        }
                    },
                    new TableExportConfig
                    {
                        TableName = secondTableName,
                        Schema = "export",
                        PrimaryKeyValue = new PrimaryKeyValue[]
                        {
                            new PrimaryKeyValue
                            {
                                ColumnName = "id",
                                Value = secondIdToExport.ToString()
                            }
                        }
                    }
                };
            return (firstTableName, firtsIdToExport, secondTableName, secondIdToExport, exportSchema, importSchema, graph, tableExportConfig);
        }

        internal static async Task<(string firstTableName, int firtsIdToExport, string secondTableName, int secondIdToExport, string exportSchema, string importSchema, DatabaseGraph graph, TableExportConfig[] tableExportConfig)> Multiple_RootTable_Overlapping_Hierarchy_Different_DataConfig(string connectionString)
        {
            string firstTableName = "table2";
            int firtsIdToExport = 1;
            string secondTableName = "table5";
            int secondIdToExport = 2;

            using NpgsqlConnection connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync();
            // Use a single command object and execute sequential SQL statements
            await using var command = connection.CreateCommand();

            string exportSchema = "export";
            string importSchema = "import";

            await DropSchema(command, exportSchema);
            await DropSchema(command, importSchema);

            // 1) Ensure schemas exist
            command.CommandText = $"CREATE SCHEMA IF NOT EXISTS {exportSchema};";
            await command.ExecuteNonQueryAsync();
            command.CommandText = $"CREATE SCHEMA IF NOT EXISTS {importSchema};";
            await command.ExecuteNonQueryAsync();

            await CreateTable1(command, exportSchema);
            await CreateTable2(command, exportSchema);

            await CreateTable1(command, importSchema);
            await CreateTable2(command, importSchema);

            await CreateTable5(command, exportSchema);
            await CreateTable5(command, importSchema);

            // 5) Insert sample rows into export.table1
            command.CommandText = "INSERT INTO export.table1 (id, name) VALUES (1, 'Alice') ON CONFLICT (id) DO NOTHING;";
            await command.ExecuteNonQueryAsync();
            command.CommandText = "INSERT INTO export.table1 (id, name) VALUES (2, 'John') ON CONFLICT (id) DO NOTHING;";
            await command.ExecuteNonQueryAsync();

            // 6) Insert sample rows into export.table2 referencing table1
            command.CommandText = "INSERT INTO export.table2 (id, table1_id, data) VALUES (1, 1, 'Bob') ON CONFLICT (id) DO NOTHING;";
            await command.ExecuteNonQueryAsync();
            command.CommandText = "INSERT INTO export.table2 (id, table1_id, data) VALUES (2, 2, 'Useless') ON CONFLICT (id) DO NOTHING;";
            await command.ExecuteNonQueryAsync();


            // 6) Insert sample rows into export.table5 referencing table1
            command.CommandText = "INSERT INTO export.table5 (id, table1_id, data) VALUES (1, 1, 'Useless') ON CONFLICT (id) DO NOTHING;";
            await command.ExecuteNonQueryAsync();
            command.CommandText = "INSERT INTO export.table5 (id, table1_id, data) VALUES (2, 2, 'Mike') ON CONFLICT (id) DO NOTHING;";
            await command.ExecuteNonQueryAsync();

            // 7) Build dependency graph for schema 'export'
            var postgreSqlDependencyDiscoverer = new PostgreSqlDependencyDiscoverer(connectionString);
            var tableDependencyGraphBuilder = new TableDependencyGraphBuilder(postgreSqlDependencyDiscoverer);
            var graph = await tableDependencyGraphBuilder.BuildDependencyGraphAsync(new[] { "export" });

            // 9) Configure the table export to use schema 'export' and table 'table2' with PK id=1
            TableExportConfig[] tableExportConfig = new[]
            {
                    new TableExportConfig
                    {
                        TableName = firstTableName,
                        Schema = "export",
                        PrimaryKeyValue = new PrimaryKeyValue[]
                        {
                            new PrimaryKeyValue
                            {
                                ColumnName = "id",
                                Value = firtsIdToExport.ToString()
                            }
                        }
                    },
                    new TableExportConfig
                    {
                        TableName = secondTableName,
                        Schema = "export",
                        PrimaryKeyValue = new PrimaryKeyValue[]
                        {
                            new PrimaryKeyValue
                            {
                                ColumnName = "id",
                                Value = secondIdToExport.ToString()
                            }
                        }
                    }
                };
            return (firstTableName, firtsIdToExport, secondTableName, secondIdToExport, exportSchema, importSchema, graph, tableExportConfig);
        }

        internal static async Task<(string tableName, int idToExport, string exportSchema, string importSchema, DatabaseGraph graph, TableExportConfig tableExportConfig)> Single_RootTableConfig(string connString, NpgsqlCommand command)
        {
            string tableName = "table2";
            int idToExport = 1;


            string exportSchema = "export";
            string importSchema = "import";

            await DropSchema(command, exportSchema);
            await DropSchema(command, importSchema);

            // 1) Ensure schemas exist
            command.CommandText = $"CREATE SCHEMA IF NOT EXISTS {exportSchema};";
            await command.ExecuteNonQueryAsync();
            command.CommandText = $"CREATE SCHEMA IF NOT EXISTS {importSchema};";
            await command.ExecuteNonQueryAsync();

            // 2) Drop and create tables for hierarchy 1 in both schemas
            await CreateTable1(command, exportSchema);
            await CreateTable2(command, exportSchema);

            await CreateTable1(command, importSchema);
            await CreateTable2(command, importSchema);

            // 5) Insert sample rows into export.table1
            command.CommandText = "INSERT INTO export.table1 (id, name) VALUES (1, 'Alice') ON CONFLICT (id) DO NOTHING;";
            await command.ExecuteNonQueryAsync();
            command.CommandText = "INSERT INTO export.table1 (id, name) VALUES (2, 'Useless') ON CONFLICT (id) DO NOTHING;";
            await command.ExecuteNonQueryAsync();

            // 6) Insert sample rows into export.table2 referencing table1
            command.CommandText = "INSERT INTO export.table2 (id, table1_id, data) VALUES (1, 1, 'Bob') ON CONFLICT (id) DO NOTHING;";
            await command.ExecuteNonQueryAsync();
            command.CommandText = "INSERT INTO export.table2 (id, table1_id, data) VALUES (2, 2, 'Useless') ON CONFLICT (id) DO NOTHING;";
            await command.ExecuteNonQueryAsync();

            // 7) Build dependency graph for schema 'export'
            var postgreSqlDependencyDiscoverer = new PostgreSqlDependencyDiscoverer(connString);
            var tableDependencyGraphBuilder = new TableDependencyGraphBuilder(postgreSqlDependencyDiscoverer);
            var graph = await tableDependencyGraphBuilder.BuildDependencyGraphAsync(new[] { "export" });
          

            // 9) Configure the table export to use schema 'export' and table 'table2' with PK id=1
            var tableExportConfig = new TableExportConfig
            {
                TableName = tableName,
                Schema = "export",
                PrimaryKeyValue = new PrimaryKeyValue[]
                {
                            new PrimaryKeyValue
                            {
                                ColumnName = "id",
                                Value = idToExport.ToString()
                            }
                }
            };
            return (tableName, idToExport, exportSchema, importSchema, graph, tableExportConfig);
        }

        internal static async Task<(string firstTableName, int firtsIdToExport, string secondTableName, int secondIdToExport, string exportSchema, string importSchema, DatabaseGraph graph, TableExportConfig[] tableExportConfig)> Multiple_RootTable_Separate_HierarchyConfig(string connString, NpgsqlCommand command)
        {
            string firstTableName = "table2";
            int firtsIdToExport = 1;
            string secondTableName = "table4";
            int secondIdToExport = 2;


            string exportSchema = "export";
            string importSchema = "import";

            await DropSchema(command, exportSchema);
            await DropSchema(command, importSchema);

            // 1) Ensure schemas exist
            command.CommandText = $"CREATE SCHEMA IF NOT EXISTS {exportSchema};";
            await command.ExecuteNonQueryAsync();
            command.CommandText = $"CREATE SCHEMA IF NOT EXISTS {importSchema};";
            await command.ExecuteNonQueryAsync();

            // H1 tables (table1, table2) in both schemas
            await CreateTable1(command, exportSchema);
            await CreateTable2(command, exportSchema);

            await CreateTable1(command, importSchema);
            await CreateTable2(command, importSchema);

            // H2 tables (table3, table4) in both schemas
            await CreateTable3(command, exportSchema);
            await CreateTable4(command, exportSchema);

            await CreateTable3(command, importSchema);
            await CreateTable4(command, importSchema);

            // 5) Insert sample rows into export.table1
            command.CommandText = "INSERT INTO export.table1 (id, name) VALUES (1, 'Alice') ON CONFLICT (id) DO NOTHING;";
            await command.ExecuteNonQueryAsync();
            command.CommandText = "INSERT INTO export.table1 (id, name) VALUES (2, 'Useless') ON CONFLICT (id) DO NOTHING;";
            await command.ExecuteNonQueryAsync();

            // 6) Insert sample rows into export.table2 referencing table1
            command.CommandText = "INSERT INTO export.table2 (id, table1_id, data) VALUES (1, 1, 'Bob') ON CONFLICT (id) DO NOTHING;";
            await command.ExecuteNonQueryAsync();
            command.CommandText = "INSERT INTO export.table2 (id, table1_id, data) VALUES (2, 2, 'Useless') ON CONFLICT (id) DO NOTHING;";
            await command.ExecuteNonQueryAsync();

            // 5) Insert sample rows into export.table3
            command.CommandText = "INSERT INTO export.table3 (id, name) VALUES (1, 'Useless') ON CONFLICT (id) DO NOTHING;";
            await command.ExecuteNonQueryAsync();
            command.CommandText = "INSERT INTO export.table3 (id, name) VALUES (2, 'John') ON CONFLICT (id) DO NOTHING;";
            await command.ExecuteNonQueryAsync();

            // 6) Insert sample rows into export.table4 referencing table3
            command.CommandText = "INSERT INTO export.table4 (id, table1_id, data) VALUES (1, 1, 'Useless') ON CONFLICT (id) DO NOTHING;";
            await command.ExecuteNonQueryAsync();
            command.CommandText = "INSERT INTO export.table4 (id, table1_id, data) VALUES (2, 2, 'Mike') ON CONFLICT (id) DO NOTHING;";
            await command.ExecuteNonQueryAsync();

            // 7) Build dependency graph for schema 'export'
            var postgreSqlDependencyDiscoverer = new PostgreSqlDependencyDiscoverer(connString);
            var tableDependencyGraphBuilder = new TableDependencyGraphBuilder(postgreSqlDependencyDiscoverer);
            var graph = await tableDependencyGraphBuilder.BuildDependencyGraphAsync(new[] { "export" });

            // 9) Configure the table export to use schema 'export' and table 'table2' with PK id=1
            TableExportConfig[] tableExportConfig = [ new TableExportConfig
                {
                    TableName = firstTableName,
                    Schema = "export",
                    PrimaryKeyValue = new PrimaryKeyValue[]
                    {
                            new PrimaryKeyValue
                            {
                                ColumnName = "id",
                                Value = firtsIdToExport.ToString()
                            }
                    }
                },
                new TableExportConfig
                {
                    TableName = secondTableName,
                    Schema = "export",
                    PrimaryKeyValue = new PrimaryKeyValue[]
                    {
                            new PrimaryKeyValue
                            {
                                ColumnName = "id",
                                Value = secondIdToExport.ToString()
                            }
                    }
                }];
            return (firstTableName, firtsIdToExport, secondTableName, secondIdToExport, exportSchema, importSchema, graph, tableExportConfig);
        }

        internal static async Task<(string connString, NpgsqlConnection connection)> GetConnection()
        {
            var connString = "Host=localhost;Username=postgres;Password=ciao;Database=postgres;Include Error Detail=True";

            var connection = new NpgsqlConnection(connString);
            await connection.OpenAsync();
            return (connString, connection);
        }

        private static async Task DropSchema(NpgsqlCommand command, string exportSchema)
        {
            command.CommandText = $"DROP SCHEMA IF EXISTS {exportSchema} CASCADE;";
            await command.ExecuteNonQueryAsync();
        }


        private static async Task CreateTable1(NpgsqlCommand command, string schema)
        {
            command.CommandText = $"CREATE TABLE IF NOT EXISTS {schema}.table1 (id INT PRIMARY KEY, name TEXT);";
            await command.ExecuteNonQueryAsync();
        }


        private static async Task CreateTable2(NpgsqlCommand command, string schema)
        {
            command.CommandText = @$"
                        CREATE TABLE IF NOT EXISTS {schema}.table2 (
                            id INT PRIMARY KEY,
                            table1_id INT NOT NULL,
                            data TEXT,
                            CONSTRAINT fk_table2_table1 FOREIGN KEY (table1_id) REFERENCES {schema}.table1(id)
                        );";
            await command.ExecuteNonQueryAsync();
        }

        private static async Task CreateTable3(NpgsqlCommand command, string schema)
        {
            command.CommandText = $"CREATE TABLE IF NOT EXISTS {schema}.table3 (id INT PRIMARY KEY, name TEXT);";
            await command.ExecuteNonQueryAsync();
        }

        private static async Task CreateTable4(NpgsqlCommand command, string schema)
        {
            command.CommandText = @$"
                        CREATE TABLE IF NOT EXISTS {schema}.table4 (
                            id INT PRIMARY KEY,
                            table1_id INT NOT NULL,
                            data TEXT,
                            CONSTRAINT fk_tabl4_table3 FOREIGN KEY (table1_id) REFERENCES {schema}.table3(id)
                        );";
            await command.ExecuteNonQueryAsync();
        }

        private static async Task CreateTable5(NpgsqlCommand command, string schema)
        {
            command.CommandText = @$"
                        CREATE TABLE IF NOT EXISTS {schema}.table5 (
                            id INT PRIMARY KEY,
                            table1_id INT NOT NULL,
                            data TEXT,
                            CONSTRAINT fk_table5_table1 FOREIGN KEY (table1_id) REFERENCES {schema}.table1(id)
                        );";
            await command.ExecuteNonQueryAsync();
        }

        
    }
}
