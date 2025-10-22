using DataSubset.DbDependencyGraph.Core.Configurations;
using DataSubset.Exporters.Common;
using DataSubset.PostgreSql;
using DependencyTreeApp;
using Npgsql;

namespace DataSubset.Exporters.PostgreSql.Test
{
    public class InsertStatementExporterTest
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

            // 2) Drop and create tables for hierarchy 1 in both schemas
            await DropTable2(command, exportSchema);
            await DropTable1(command, exportSchema);
            await CreateTable1(command, exportSchema);
            await CreateTable2(command, exportSchema);

            await DropTable2(command, importSchema);
            await DropTable1(command, importSchema);
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

            // 8) Create exporter engine and exporter
            var postgreSqlExporterEngine = new PostgreSqlExporterEngine(connString);
            var exp = new InsertStatementExporter(postgreSqlExporterEngine);

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


            await DropTable2(command, exportSchema);
            await DropTable1(command, exportSchema);
            await DropTable2(command, importSchema);
            await DropTable1(command, importSchema);
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

            // H1 tables (table1, table2) in both schemas
            await DropTable2(command, exportSchema);
            await DropTable1(command, exportSchema);
            await CreateTable1(command, exportSchema);
            await CreateTable2(command, exportSchema);

            await DropTable2(command, importSchema);
            await DropTable1(command, importSchema);
            await CreateTable1(command, importSchema);
            await CreateTable2(command, importSchema);

            // H2 tables (table3, table4) in both schemas
            await DropTable4(command, exportSchema);
            await DropTable3(command, exportSchema);
            await CreateTable3(command, exportSchema);
            await CreateTable4(command, exportSchema);

            await DropTable4(command, importSchema);
            await DropTable3(command, importSchema);
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

            // 8) Create exporter engine and exporter
            var postgreSqlExporterEngine = new PostgreSqlExporterEngine(connString);
            var exp = new InsertStatementExporter(postgreSqlExporterEngine);

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
            Assert.Contains("'Mike'", res[3]);
            Assert.DoesNotContain("Useless", res[3]);

            foreach (var r in res)
            {
                command.CommandText = r.Replace(exportSchema, importSchema);
                await command.ExecuteNonQueryAsync();
            }

            await DropTable4(command, exportSchema);
            await DropTable3(command, exportSchema);
            await DropTable4(command, importSchema);
            await DropTable3(command, importSchema);

            await DropTable2(command, exportSchema);
            await DropTable1(command, exportSchema);
            await DropTable2(command, importSchema);
            await DropTable1(command, importSchema);
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
            await DropTable5(command, importSchema);
            await DropTable5(command, exportSchema);

            // H1 and H3 (table1 + table5) in both schemas
            await DropTable2(command, exportSchema);
            await DropTable1(command, exportSchema);
            await DropTable2(command, importSchema);
            await DropTable1(command, importSchema);

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
            var postgreSqlDependencyDiscoverer = new PostgreSqlDependencyDiscoverer(connString);
            var tableDependencyGraphBuilder = new TableDependencyGraphBuilder(postgreSqlDependencyDiscoverer);
            var graph = await tableDependencyGraphBuilder.BuildDependencyGraphAsync(new[] { "export" });

            // 8) Create exporter engine and exporter
            var postgreSqlExporterEngine = new PostgreSqlExporterEngine(connString);
            var exp = new InsertStatementExporter(postgreSqlExporterEngine);

            // 9) Configure the table export to use schema 'export' and table 'table2' with PK id=1
            TableExportConfig[] tableExportConfig = new[]
            {
                    new TableExportConfig
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
                    }
                };

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
            Assert.Contains("'Mike'", res[3]);
            Assert.DoesNotContain("Useless", res[3]);

            foreach (var r in res)
            {
                command.CommandText = r.Replace(exportSchema, importSchema);
                await command.ExecuteNonQueryAsync();
            }
            await DropTable5(command, importSchema);
            await DropTable5(command, exportSchema);

            // H1 and H3 (table1 + table5) in both schemas
            await DropTable2(command, exportSchema);
            await DropTable1(command, exportSchema);
            await DropTable2(command, importSchema);
            await DropTable1(command, importSchema);
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
            await DropTable5(command, importSchema);
            await DropTable5(command, exportSchema);

            // Ensure H1 and H3 are created
            await DropTable2(command, exportSchema);
            await DropTable5(command, exportSchema);
            await DropTable1(command, exportSchema);
            await DropTable2(command, importSchema);
            await DropTable5(command, importSchema);
            await DropTable1(command, importSchema);

            
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

            // 8) Create exporter engine and exporter
            var postgreSqlExporterEngine = new PostgreSqlExporterEngine(connString);
            var exp = new InsertStatementExporter(postgreSqlExporterEngine);

            // 9) Configure the table export to use schema 'export' and table 'table2' with PK id=1
            TableExportConfig[] tableExportConfig = new[]
            {
                    new TableExportConfig
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
                    }
                };

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
            Assert.Contains("'Mike'", res[2]);
            Assert.DoesNotContain("Useless", res[2]);

            foreach (var r in res)
            {
                command.CommandText = r.Replace(exportSchema, importSchema);
                await command.ExecuteNonQueryAsync();
            }

            await DropTable2(command, exportSchema);
            await DropTable5(command, exportSchema);
            await DropTable1(command, exportSchema);
            await DropTable2(command, importSchema);
            await DropTable5(command, importSchema);
            await DropTable1(command, importSchema);
        }

        [Fact]
        public async Task BooleanTypesTest()
        {
            int idToExport = 1;
            string createTableQuery = PostgresqlScripts.createBooleanTable;
            string insertDataQuery = PostgresqlScripts.insertBooleanTable;
            string tableName = "typetest";
            await PerformTypesTest(idToExport, createTableQuery, insertDataQuery, tableName);
        }

        [Fact]
        public async Task IntegerTypesTest()
        {
            int idToExport = 1;
            string createTableQuery = PostgresqlScripts.createIntegerTable;
            string insertDataQuery = PostgresqlScripts.insertIntegerTable;
            string tableName = "typetest";
            await PerformTypesTest(idToExport, createTableQuery, insertDataQuery, tableName);
        }

        [Fact]
        public async Task BitTypesTest()
        {
            int idToExport = 1;
            string createTableQuery = PostgresqlScripts.createBitTable;
            string insertDataQuery = PostgresqlScripts.insertBitTable;
            string tableName = "typetest";
            await PerformTypesTest(idToExport, createTableQuery, insertDataQuery, tableName);
        }

        [Fact]
        public async Task GeometricTypesTest()
        {
            int idToExport = 1;
            string createTableQuery = PostgresqlScripts.createGeometricTable;
            string insertDataQuery = PostgresqlScripts.insertGeometricTable;
            string tableName = "typetest";
            await PerformTypesTest(idToExport, createTableQuery, insertDataQuery, tableName);
        }

        [Fact]
        public async Task CharacterTypesTest()
        {
            int idToExport = 1;
            string createTableQuery = PostgresqlScripts.createCharacterTable;
            string insertDataQuery = PostgresqlScripts.insertCharacterTable;
            string tableName = "typetest";
            await PerformTypesTest(idToExport, createTableQuery, insertDataQuery, tableName);
        }

        [Fact]
        public async Task DateTimeTypesTest()
        {
            int idToExport = 1;
            string createTableQuery = PostgresqlScripts.createDateTimeTable;
            string insertDataQuery = PostgresqlScripts.insertDateTimeTable;
            string tableName = "typetest";
            await PerformTypesTest(idToExport, createTableQuery, insertDataQuery, tableName);
        }


        private static async Task PerformTypesTest(int idToExport, string createTableQuery, string insertDataQuery, string tableName)
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
            command.CommandText = $"DROP TABLE IF EXISTS {importSchema}.{tableName};";
            await command.ExecuteNonQueryAsync();
            command.CommandText = $"DROP TABLE IF EXISTS {exportSchema}.{tableName};";
            await command.ExecuteNonQueryAsync();

            command.CommandText = string.Format(createTableQuery, importSchema, tableName);
            await command.ExecuteNonQueryAsync();
            command.CommandText = string.Format(createTableQuery, exportSchema, tableName);
            await command.ExecuteNonQueryAsync();

            // 5) Insert sample rows into export
            command.CommandText = string.Format(insertDataQuery, exportSchema, tableName);
            await command.ExecuteNonQueryAsync();

            // 7) Build dependency graph for schema 'export'
            var postgreSqlDependencyDiscoverer = new PostgreSqlDependencyDiscoverer(connString);
            var tableDependencyGraphBuilder = new TableDependencyGraphBuilder(postgreSqlDependencyDiscoverer);
            var graph = await tableDependencyGraphBuilder.BuildDependencyGraphAsync(new[] { "export" });

            // 8) Create exporter engine and exporter
            var postgreSqlExporterEngine = new PostgreSqlExporterEngine(connString);
            var exp = new InsertStatementExporter(postgreSqlExporterEngine);



            // 9) Configure the table export to use schema 'export' and table 'table2' with PK id=1
            var tableExportConfig = new TableExportConfig
            {
                TableName = tableName,
                Schema = exportSchema,
                PrimaryKeyValue = new PrimaryKeyValue[]
                {
                            new PrimaryKeyValue
                            {
                                ColumnName = "id",
                                Value = idToExport.ToString()
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

            Assert.True(res.Length == 1);

            // test generated insert statements
            foreach (var r in res)
            {
                command.CommandText = r.Replace(exportSchema, importSchema);
                await command.ExecuteNonQueryAsync();
            }

            // Verify data was correctly inserted by comparing export and import tables
            await CompareImportedValue(idToExport, tableName, exportSchema, importSchema, connection);

            Assert.True(true);
        }

        private static async Task CompareImportedValue(int idToExport, string tableName,string exportSchema, string importSchema, NpgsqlConnection connection)
        {
            using var command = connection.CreateCommand();
            command.CommandText = $"SELECT * FROM {exportSchema}.{tableName} WHERE id = {idToExport}";
            await using var exportReader = await command.ExecuteReaderAsync();
            var exportRows = new List<Dictionary<string, object?>>();
            while (await exportReader.ReadAsync())
            {
                var row = new Dictionary<string, object?>();
                for (int i = 0; i < exportReader.FieldCount; i++)
                {
                    row[exportReader.GetName(i)] = exportReader.IsDBNull(i) ? null : exportReader.GetValue(i);
                }
                exportRows.Add(row);
            }
            await exportReader.CloseAsync();

            command.CommandText = $"SELECT * FROM {importSchema}.{tableName} WHERE id = {idToExport}";
            await using var importReader = await command.ExecuteReaderAsync();
            var importRows = new List<Dictionary<string, object?>>();
            while (await importReader.ReadAsync())
            {
                var row = new Dictionary<string, object?>();
                for (int i = 0; i < importReader.FieldCount; i++)
                {
                    row[importReader.GetName(i)] = importReader.IsDBNull(i) ? null : importReader.GetValue(i);
                }
                importRows.Add(row);
            }
            await importReader.CloseAsync();

            Assert.Equal(exportRows.Count, importRows.Count);
            for (int i = 0; i < exportRows.Count; i++)
            {
                foreach (var key in exportRows[i].Keys)
                {
                    Assert.Equal(exportRows[i][key], importRows[i][key]);
                }
            }
        }

        private static async Task DropTable1(NpgsqlCommand command, string schema)
        {
            command.CommandText = $"DROP TABLE IF EXISTS {schema}.table1;";
            await command.ExecuteNonQueryAsync();
        }

        private static async Task CreateTable1(NpgsqlCommand command, string schema)
        {
            command.CommandText = $"CREATE TABLE IF NOT EXISTS {schema}.table1 (id INT PRIMARY KEY, name TEXT);";
            await command.ExecuteNonQueryAsync();
        }

        private static async Task DropTable2(NpgsqlCommand command, string schema)
        {
            command.CommandText = $"DROP TABLE IF EXISTS {schema}.table2;";
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

        private static async Task DropTable3(NpgsqlCommand command, string schema)
        {
            command.CommandText = $"DROP TABLE IF EXISTS {schema}.table3;";
            await command.ExecuteNonQueryAsync();
        }

        private static async Task CreateTable3(NpgsqlCommand command, string schema)
        {
            command.CommandText = $"CREATE TABLE IF NOT EXISTS {schema}.table3 (id INT PRIMARY KEY, name TEXT);";
            await command.ExecuteNonQueryAsync();
        }

        private static async Task DropTable4(NpgsqlCommand command, string schema)
        {
            command.CommandText = $"DROP TABLE IF EXISTS {schema}.table4;";
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

        private static async Task DropTable5(NpgsqlCommand command, string schema)
        {
            command.CommandText = $"DROP TABLE IF EXISTS {schema}.table5;";
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