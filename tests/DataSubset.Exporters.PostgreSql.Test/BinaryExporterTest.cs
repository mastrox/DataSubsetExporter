using DataSubset.DbDependencyGraph.Core.Configurations;
using DataSubset.DbDependencyGraph.Core.DependencyGraph;
using DataSubset.Exporters.Common.BinaryExporter;
using DataSubset.Exporters.Common.InsetStatementExporter;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection.Metadata;
using System.Text;
using System.Threading.Tasks;

namespace DataSubset.Exporters.PostgreSql.Test
{
    public class BinaryExporterTest
    {

        [Fact]
        public async Task Single_RootTable_Test()
        {
            (string connString, NpgsqlConnection connection) = await TestConfigurator.GetConnection();
            using var command = connection.CreateCommand();

            (string tableName, int idToExport, string exportSchema, string importSchema, DatabaseGraph graph, TableExportConfig tableExportConfig) = await TestConfigurator.Single_RootTableConfig(connString, command);


            // Create exporter engine and exporter
            var postgreSqlExporterEngine = new PostgreSqlExporterEngine(connString);
            var exp = new BinaryExporter(postgreSqlExporterEngine);

            // Collect results from the exporter asynchronously
            int row = 0;
            int table2Key = 0;
            int table1Key = 0;
            await foreach (var item in exp.GetItemsToExportInInsertOrder(new[] { tableExportConfig }, graph))
            {
                switch (row)
                {
                    case 0: // metadata
                        var metadata = MessagePack.MessagePackSerializer.Deserialize<ExportMetadata>(item);
                        Assert.Equal("1", metadata.Version);
                        Assert.Equal(Common.DbTypes.Postgres, metadata.DbType);
                        var tableMetadata = metadata.TablesMetadata[0];
                        table2Key = tableMetadata.TableKey;
                        Assert.NotEqual(0, table2Key);
                        Assert.Equal("export", tableMetadata.Schema);
                        Assert.Equal("table2", tableMetadata.Table);
                        Assert.Equal(3, tableMetadata.ColumnMetadata.Count());
                        Assert.Equal("id", tableMetadata.ColumnMetadata[0].Name);
                        Assert.Equal("int4", tableMetadata.ColumnMetadata[0].DataType);
                        Assert.Equal("table1_id", tableMetadata.ColumnMetadata[1].Name);
                        Assert.Equal("int4", tableMetadata.ColumnMetadata[1].DataType);
                        Assert.Equal("data", tableMetadata.ColumnMetadata[2].Name);
                        Assert.Equal("text", tableMetadata.ColumnMetadata[2].DataType);

                        tableMetadata = metadata.TablesMetadata[1];
                        table1Key = tableMetadata.TableKey;
                        Assert.NotEqual(0, table1Key);
                        Assert.Equal("export", tableMetadata.Schema);
                        Assert.Equal("table1", tableMetadata.Table);
                        Assert.Equal(2, tableMetadata.ColumnMetadata.Count());
                        Assert.Equal("id", tableMetadata.ColumnMetadata[0].Name);
                        Assert.Equal("int4", tableMetadata.ColumnMetadata[0].DataType);
                        Assert.Equal("name", tableMetadata.ColumnMetadata[1].Name);
                        Assert.Equal("text", tableMetadata.ColumnMetadata[1].DataType);
                        break;
                    case 1:
                        var rowData = MessagePack.MessagePackSerializer.Deserialize<RowData>(item);
                        Assert.Equal(table1Key, rowData.TableKey);
                        Assert.Equal(2, rowData.ColumnValues.Count());
                        Assert.Equal(1, rowData.ColumnValues[0]);
                        Assert.Equal("Alice", rowData.ColumnValues[1]);
                        break;
                    case 2:
                        rowData = MessagePack.MessagePackSerializer.Deserialize<RowData>(item);
                        Assert.Equal(table2Key, rowData.TableKey);
                        Assert.Equal(3, rowData.ColumnValues.Count());
                        Assert.Equal(1, rowData.ColumnValues[0]);
                        Assert.Equal(1, rowData.ColumnValues[1]);
                        Assert.Equal("Bob", rowData.ColumnValues[2]);
                        break;
                    default:
                        Assert.Fail("too manay row returned");
                        break;
                }

                row++;
            }
        }
    }
}
