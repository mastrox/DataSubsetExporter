using DataSubset.DbDependencyGraph.Core.Configurations;
using DataSubset.Exporters.Common;
using DataSubset.PostgreSql;
using DependencyTreeApp;
using Npgsql;

namespace DataSubset.Exporters.PostgreSql
{
    public class UnitTest1
    {
        [Fact]
        public async Task Single_RootTable_Test()
        {
            var connString = "Host=localhost;Username=postgres;Password=ciao;Database=postgres;Include Error Detail=True";

            await using var connection = new NpgsqlConnection(connString);
            await connection.OpenAsync();

            // Use a single command object and execute sequential SQL statements
            await using var command = connection.CreateCommand();

            string exportSchema = "export";
            string importSchema = "import";
            // 1) Ensure schemas exist
            command.CommandText = $"CREATE SCHEMA IF NOT EXISTS {exportSchema};";
            await command.ExecuteNonQueryAsync();
            command.CommandText = $"CREATE SCHEMA IF NOT EXISTS {importSchema};";
            await command.ExecuteNonQueryAsync();

            // 2) Drop tables if they exist in the export schema (idempotent)
            command.CommandText = "DROP TABLE IF EXISTS export.table2;";
            await command.ExecuteNonQueryAsync();
            command.CommandText = "DROP TABLE IF EXISTS export.table1;";
            await command.ExecuteNonQueryAsync();

            await CreteTablesH1(command,exportSchema);
            await CreteTablesH1(command,importSchema);


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

            // 8) Create exporter engine and exporter
            var postgreSqlExporterEngine = new PostgreSqlExporterEngine(connString);
            var exp = new InserStatementExporter(postgreSqlExporterEngine);

            // 9) Configure the table export to use schema 'export' and table 'table2' with PK id=1
            var tableExportConfig = new TableExportConfig
            {
                TableName = "table2",
                Schema = "export",
                PrimaryKeyValue = new PrimaryKeyValue[]
                {
                        new PrimaryKeyValue
                        {
                            ColumnName = "id",
                            Value = "1"
                        }
                }
            };

            // 10) Collect results from the exporter asynchronously
            var results = new List<string>();
            await foreach (var item in exp.GetItemsToExportInInsertOrder(new[] { tableExportConfig }, graph))
            {
                results.Add(item);
            }

            // For debug / simple check
            var res = results.ToArray();
            // Optionally assert something about results, e.g. at least one insert statement produced
            Assert.True(res.Length == 2);
            Assert.Contains("'Alice'", res[0]);
            Assert.DoesNotContain("Useless", res[0]);
            Assert.Contains("'Bob'", res[1]);
            Assert.DoesNotContain("Useless", res[1]);

            foreach (var r in res)
            {
                command.CommandText = r.Replace(exportSchema, importSchema);
                await command.ExecuteNonQueryAsync();
            }
        }

        [Fact]
        public async Task Multiple_RootTable_Separate_Hierarchy_Test()
        {
            var connString = "Host=localhost;Username=postgres;Password=ciao;Database=postgres;Include Error Detail=True";

            await using var connection = new NpgsqlConnection(connString);
            await connection.OpenAsync();

            // Use a single command object and execute sequential SQL statements
            await using var command = connection.CreateCommand();

            string exportSchema = "export";
            string importSchema = "import";
            // 1) Ensure schemas exist
            command.CommandText = $"CREATE SCHEMA IF NOT EXISTS {exportSchema};";
            await command.ExecuteNonQueryAsync();
            command.CommandText = $"CREATE SCHEMA IF NOT EXISTS {importSchema};";
            await command.ExecuteNonQueryAsync();


            await CreteTablesH1(command, exportSchema);
            await CreteTablesH1(command, importSchema);
            await CreteTablesH2(command, exportSchema);
            await CreteTablesH2(command, importSchema);


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

            // 5) Insert sample rows into export.table1
            command.CommandText = "INSERT INTO export.table3 (id, name) VALUES (1, 'Useless') ON CONFLICT (id) DO NOTHING;";
            await command.ExecuteNonQueryAsync();
            command.CommandText = "INSERT INTO export.table3 (id, name) VALUES (2, 'John') ON CONFLICT (id) DO NOTHING;";
            await command.ExecuteNonQueryAsync();

            // 6) Insert sample rows into export.table2 referencing table1
            command.CommandText = "INSERT INTO export.table4 (id, table1_id, data) VALUES (1, 1, 'Useless') ON CONFLICT (id) DO NOTHING;";
            await command.ExecuteNonQueryAsync();
            command.CommandText = "INSERT INTO export.table4 (id, table1_id, data) VALUES (2, 2, 'Doe') ON CONFLICT (id) DO NOTHING;";
            await command.ExecuteNonQueryAsync();

            // 7) Build dependency graph for schema 'export'
            var postgreSqlDependencyDiscoverer = new PostgreSqlDependencyDiscoverer(connString);
            var tableDependencyGraphBuilder = new TableDependencyGraphBuilder(postgreSqlDependencyDiscoverer);
            var graph = await tableDependencyGraphBuilder.BuildDependencyGraphAsync(new[] { "export" });

            // 8) Create exporter engine and exporter
            var postgreSqlExporterEngine = new PostgreSqlExporterEngine(connString);
            var exp = new InserStatementExporter(postgreSqlExporterEngine);

            // 9) Configure the table export to use schema 'export' and table 'table2' with PK id=1
            TableExportConfig[] tableExportConfig =[ new TableExportConfig
            {
                TableName = "table2",
                Schema = "export",
                PrimaryKeyValue = new PrimaryKeyValue[]
                {
                        new PrimaryKeyValue
                        {
                            ColumnName = "id",
                            Value = "1"
                        }
                }
            },
            new TableExportConfig
            {
                TableName = "table4",
                Schema = "export",
                PrimaryKeyValue = new PrimaryKeyValue[]
                {
                        new PrimaryKeyValue
                        {
                            ColumnName = "id",
                            Value = "2"
                        }
                }
            }];

            // 10) Collect results from the exporter asynchronously
            var results = new List<string>();
            await foreach (var item in exp.GetItemsToExportInInsertOrder(tableExportConfig, graph))
            {
                results.Add(item);
            }

            // For debug / simple check
            var res = results.ToArray();
            // Optionally assert something about results, e.g. at least one insert statement produced
            Assert.True(res.Length == 4);
            Assert.Contains("'Alice'", res[0]);
            Assert.DoesNotContain("Useless", res[0]);
            Assert.Contains("'Bob'", res[1]);
            Assert.DoesNotContain("Useless", res[1]);
            Assert.Contains("'John'", res[2]);
            Assert.DoesNotContain("Useless", res[2]);
            Assert.Contains("'Doe'", res[3]);
            Assert.DoesNotContain("Useless", res[3]);

            foreach (var r in res)
            {
                command.CommandText = r.Replace(exportSchema, importSchema);
                await command.ExecuteNonQueryAsync();
            }
        }

        [Fact]
        public async Task Multiple_RootTable_Overlapping_Hierarchy_Different_Data_Test()
        {
            var connString = "Host=localhost;Username=postgres;Password=ciao;Database=postgres;Include Error Detail=True";

            await using var connection = new NpgsqlConnection(connString);
            await connection.OpenAsync();

            // Use a single command object and execute sequential SQL statements
            await using var command = connection.CreateCommand();

            string exportSchema = "export";
            string importSchema = "import";
            // 1) Ensure schemas exist
            command.CommandText = $"CREATE SCHEMA IF NOT EXISTS {exportSchema};";
            await command.ExecuteNonQueryAsync();
            command.CommandText = $"CREATE SCHEMA IF NOT EXISTS {importSchema};";
            await command.ExecuteNonQueryAsync();

            // 2) Drop tables if they exist in the export schema (idempotent)
            command.CommandText = $"DROP TABLE IF EXISTS {importSchema}.table5;";
            await command.ExecuteNonQueryAsync();
            command.CommandText = $"DROP TABLE IF EXISTS {exportSchema}.table5;";
            await command.ExecuteNonQueryAsync();

            await CreteTablesH1(command, exportSchema);
            await CreteTablesH1(command, importSchema);
            await CreteTablesH3(command, exportSchema);
            await CreteTablesH3(command, importSchema);


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


            // 6) Insert sample rows into export.table2 referencing table1
            command.CommandText = "INSERT INTO export.table5 (id, table1_id, data) VALUES (1, 1, 'Useless') ON CONFLICT (id) DO NOTHING;";
            await command.ExecuteNonQueryAsync();
            command.CommandText = "INSERT INTO export.table5 (id, table1_id, data) VALUES (2, 2, 'Doe') ON CONFLICT (id) DO NOTHING;";
            await command.ExecuteNonQueryAsync();

            // 7) Build dependency graph for schema 'export'
            var postgreSqlDependencyDiscoverer = new PostgreSqlDependencyDiscoverer(connString);
            var tableDependencyGraphBuilder = new TableDependencyGraphBuilder(postgreSqlDependencyDiscoverer);
            var graph = await tableDependencyGraphBuilder.BuildDependencyGraphAsync(new[] { "export" });

            // 8) Create exporter engine and exporter
            var postgreSqlExporterEngine = new PostgreSqlExporterEngine(connString);
            var exp = new InserStatementExporter(postgreSqlExporterEngine);

            // 9) Configure the table export to use schema 'export' and table 'table2' with PK id=1
            TableExportConfig[] tableExportConfig = [ new TableExportConfig
            {
                TableName = "table2",
                Schema = "export",
                PrimaryKeyValue = new PrimaryKeyValue[]
                {
                        new PrimaryKeyValue
                        {
                            ColumnName = "id",
                            Value = "1"
                        }
                }
            },
            new TableExportConfig
            {
                TableName = "table5",
                Schema = "export",
                PrimaryKeyValue = new PrimaryKeyValue[]
                {
                        new PrimaryKeyValue
                        {
                            ColumnName = "id",
                            Value = "2"
                        }
                }
            }];

            // 10) Collect results from the exporter asynchronously
            var results = new List<string>();
            await foreach (var item in exp.GetItemsToExportInInsertOrder(tableExportConfig, graph))
            {
                results.Add(item);
            }

            // For debug / simple check
            var res = results.ToArray();
            // Optionally assert something about results, e.g. at least one insert statement produced
            Assert.True(res.Length == 4);
            Assert.Contains("'Alice'", res[0]);
            Assert.DoesNotContain("Useless", res[0]);
            Assert.Contains("'Bob'", res[1]);
            Assert.DoesNotContain("Useless", res[1]);
            Assert.Contains("'John'", res[2]);
            Assert.DoesNotContain("Useless", res[2]);
            Assert.Contains("'Doe'", res[3]);
            Assert.DoesNotContain("Useless", res[3]);

            foreach (var r in res)
            {
                command.CommandText = r.Replace(exportSchema, importSchema);
                await command.ExecuteNonQueryAsync();
            }
        }

        [Fact]
        public async Task Multiple_RootTable_Overlapping_Hierarchy_Overlapping_Data_Test()
        {
            var connString = "Host=localhost;Username=postgres;Password=ciao;Database=postgres;Include Error Detail=True";

            await using var connection = new NpgsqlConnection(connString);
            await connection.OpenAsync();

            // Use a single command object and execute sequential SQL statements
            await using var command = connection.CreateCommand();

            string exportSchema = "export";
            string importSchema = "import";
            // 1) Ensure schemas exist
            command.CommandText = $"CREATE SCHEMA IF NOT EXISTS {exportSchema};";
            await command.ExecuteNonQueryAsync();
            command.CommandText = $"CREATE SCHEMA IF NOT EXISTS {importSchema};";
            await command.ExecuteNonQueryAsync();

            // 2) Drop tables if they exist in the export schema (idempotent)
            command.CommandText = $"DROP TABLE IF EXISTS {importSchema}.table5;";
            await command.ExecuteNonQueryAsync();
            command.CommandText = $"DROP TABLE IF EXISTS {exportSchema}.table5;";
            await command.ExecuteNonQueryAsync();

            await CreteTablesH1(command, exportSchema);
            await CreteTablesH1(command, importSchema);
            await CreteTablesH3(command, exportSchema);
            await CreteTablesH3(command, importSchema);


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


            // 6) Insert sample rows into export.table2 referencing table1
            command.CommandText = "INSERT INTO export.table5 (id, table1_id, data) VALUES (1, 1, 'Doe') ON CONFLICT (id) DO NOTHING;";
            await command.ExecuteNonQueryAsync();
            command.CommandText = "INSERT INTO export.table5 (id, table1_id, data) VALUES (2, 2, 'Useless') ON CONFLICT (id) DO NOTHING;";
            await command.ExecuteNonQueryAsync();

            // 7) Build dependency graph for schema 'export'
            var postgreSqlDependencyDiscoverer = new PostgreSqlDependencyDiscoverer(connString);
            var tableDependencyGraphBuilder = new TableDependencyGraphBuilder(postgreSqlDependencyDiscoverer);
            var graph = await tableDependencyGraphBuilder.BuildDependencyGraphAsync(new[] { "export" });

            // 8) Create exporter engine and exporter
            var postgreSqlExporterEngine = new PostgreSqlExporterEngine(connString);
            var exp = new InserStatementExporter(postgreSqlExporterEngine);

            // 9) Configure the table export to use schema 'export' and table 'table2' with PK id=1
            TableExportConfig[] tableExportConfig = [ new TableExportConfig
            {
                TableName = "table2",
                Schema = "export",
                PrimaryKeyValue = new PrimaryKeyValue[]
                {
                        new PrimaryKeyValue
                        {
                            ColumnName = "id",
                            Value = "1"
                        }
                }
            },
            new TableExportConfig
            {
                TableName = "table5",
                Schema = "export",
                PrimaryKeyValue = new PrimaryKeyValue[]
                {
                        new PrimaryKeyValue
                        {
                            ColumnName = "id",
                            Value = "1"
                        }
                }
            }];

            // 10) Collect results from the exporter asynchronously
            var results = new List<string>();
            await foreach (var item in exp.GetItemsToExportInInsertOrder(tableExportConfig, graph))
            {
                results.Add(item);
            }

            // For debug / simple check
            var res = results.ToArray();
            // Optionally assert something about results, e.g. at least one insert statement produced
            Assert.True(res.Length == 3);
            Assert.Contains("'Alice'", res[0]);
            Assert.DoesNotContain("Useless", res[0]);
            Assert.Contains("'Bob'", res[1]);
            Assert.DoesNotContain("Useless", res[1]);
            Assert.Contains("'Doe'", res[2]);
            Assert.DoesNotContain("Useless", res[2]);

            foreach (var r in res)
            {
                command.CommandText = r.Replace(exportSchema, importSchema);
                await command.ExecuteNonQueryAsync();
            }
        }

        private static async Task CreteTablesH1(NpgsqlCommand command, string schema)
        {
            // 2) Drop tables if they exist in the export schema (idempotent)
            command.CommandText = $"DROP TABLE IF EXISTS {schema}.table2;";
            await command.ExecuteNonQueryAsync();
            command.CommandText = $"DROP TABLE IF EXISTS {schema}.table1;";
            await command.ExecuteNonQueryAsync();
            // 3) Create table1 in schema 'export'
            command.CommandText = $"CREATE TABLE IF NOT EXISTS {schema}.table1 (id INT PRIMARY KEY, name TEXT);";
            await command.ExecuteNonQueryAsync();

            // 4) Create table2 in schema 'export' with FK to export.table1
            command.CommandText = @$"
                    CREATE TABLE IF NOT EXISTS {schema}.table2 (
                        id INT PRIMARY KEY,
                        table1_id INT NOT NULL,
                        data TEXT,
                        CONSTRAINT fk_table2_table1 FOREIGN KEY (table1_id) REFERENCES {schema}.table1(id)
                    );";
            await command.ExecuteNonQueryAsync();
        }


        private static async Task CreteTablesH2(NpgsqlCommand command, string schema)
        {
            // 2) Drop tables if they exist in the export schema (idempotent)
            command.CommandText = $"DROP TABLE IF EXISTS {schema}.table4;";
            await command.ExecuteNonQueryAsync();
            command.CommandText = $"DROP TABLE IF EXISTS {schema}.table3;";
            await command.ExecuteNonQueryAsync();
            // 3) Create table1 in schema 'export'
            command.CommandText = $"CREATE TABLE IF NOT EXISTS {schema}.table3 (id INT PRIMARY KEY, name TEXT);";
            await command.ExecuteNonQueryAsync();

            // 4) Create table2 in schema 'export' with FK to export.table1
            command.CommandText = @$"
                    CREATE TABLE IF NOT EXISTS {schema}.table4 (
                        id INT PRIMARY KEY,
                        table1_id INT NOT NULL,
                        data TEXT,
                        CONSTRAINT fk_tabl4_table3 FOREIGN KEY (table1_id) REFERENCES {schema}.table3(id)
                    );";
            await command.ExecuteNonQueryAsync();
        }

        private static async Task CreteTablesH3(NpgsqlCommand command, string schema)
        {
            // 3) Create table1 in schema 'export'
            command.CommandText = $"CREATE TABLE IF NOT EXISTS {schema}.table1 (id INT PRIMARY KEY, name TEXT);";
            await command.ExecuteNonQueryAsync();

            // 4) Create table2 in schema 'export' with FK to export.table1
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