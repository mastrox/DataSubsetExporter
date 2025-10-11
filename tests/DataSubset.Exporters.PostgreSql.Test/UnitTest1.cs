using DataSubset.DbDependencyGraph.Core.Configurations;
using DataSubset.Exporters.Common;
using DataSubset.PostgreSql;
using DependencyTreeApp;

namespace DataSubset.Exporters.PostgreSql
{
    public class UnitTest1
    {
        [Fact]
        public async Task Test1()
        {
            var connString = "Host=localhost;Username=postgres;Password=ciao;Database=postgres;Include Error Detail=True";
            using Npgsql.NpgsqlConnection connection = new Npgsql.NpgsqlConnection(connString);
            connection.Open();
            Npgsql.NpgsqlCommand command = new Npgsql.NpgsqlCommand("DROP TABLE IF EXISTS public.table2;", connection);
            var result = command.ExecuteNonQuery();
            command.CommandText = "DROP TABLE IF EXISTS public.table1;";
             result = command.ExecuteNonQuery();
            command.CommandText = "CREATE TABLE IF NOT EXISTS public.table1 (id INT PRIMARY KEY, name TEXT);";
            result = command.ExecuteNonQuery();

            // Create table2 with a foreign key to table1
            command.CommandText = "DROP TABLE IF EXISTS public.table2;";
            result = command.ExecuteNonQuery();
            command.CommandText = @"
                        CREATE TABLE IF NOT EXISTS public.table2 (
                            id INT PRIMARY KEY,
                            table1_id INT NOT NULL,
                            CONSTRAINT fk_table2_table1 FOREIGN KEY (table1_id) REFERENCES public.table1(id)
                        );";
            result = command.ExecuteNonQuery();

            // Insert one row into table1 (id = 1, name = 'Alice')
            command.CommandText = "INSERT INTO public.table1 (id, name) VALUES (1, 'Alice') ON CONFLICT (id) DO NOTHING;";
            result = command.ExecuteNonQuery();

            // Insert one row into table2 referencing table1 (id = 1, table1_id = 1)
            command.CommandText = "INSERT INTO public.table2 (id, table1_id) VALUES (1, 1) ON CONFLICT (id) DO NOTHING;";
            result = command.ExecuteNonQuery();
            PostgreSqlDependencyDiscoverer postgreSqlDependencyDiscoverer = new PostgreSqlDependencyDiscoverer(connString);
            TableDependencyGraphBuilder tableDependencyGraphBuilder = new TableDependencyGraphBuilder(postgreSqlDependencyDiscoverer);
            var graph= await tableDependencyGraphBuilder.BuildDependencyGraphAsync(["public"]);
            PostgreSqlExporterEngine postgreSqlExporterEngine = new PostgreSqlExporterEngine(connString);
            InserStatementExporter exp = new InserStatementExporter(postgreSqlExporterEngine);
            TableExportConfig tableExportConfig = new TableExportConfig
            {
                TableName = "table1",
                Schema = "public",
                PrimaryKeyValue = new PrimaryKeyValue[]
                {
                    new PrimaryKeyValue
                    {
                        ColumnName = "id",
                        Value = "1"
                    }
                }
            };

            // The call below was incomplete in the original file. Commented out to preserve compilation.
            var res=exp.GetItemsToExportInInsertOrder([tableExportConfig], graph).ToBlockingEnumerable().ToArray();
            int a = 0;
        }
    }
}