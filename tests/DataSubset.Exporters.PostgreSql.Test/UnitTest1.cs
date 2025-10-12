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
        public async Task Test1()
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

            await CreteTable(command,exportSchema);
            await CreteTable(command,importSchema);


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
            Assert.True(res.Length == 2, "Expected at least one insert statement to be generated.");
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

        private static async Task CreteTable(NpgsqlCommand command, string schema)
        {
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
    }
}