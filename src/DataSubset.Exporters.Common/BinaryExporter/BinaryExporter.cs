using DataSubset.DbDependencyGraph.Core.Configurations;
using DataSubset.DbDependencyGraph.Core.DependencyGraph;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataSubset.Exporters.Common.BinaryExporter
{
    internal class BinaryExporter: ExporterBase<byte[]>
    {
        private Dictionary<(string schema, string table), int> tableKeyMapping = new Dictionary<(string schema, string table), int>();
        public BinaryExporter(IDbExporterEngine dbExporterEngine, ILogger? logger = null) : base(dbExporterEngine, logger)
        {
        }

        protected async override Task<byte[]> GenerateCurrentRowExportItem(TableNode currentNode, (string column, object? value)[] row, IEnumerable<TableExportConfig> tableExportConfig)
        {
            return MessagePack.MessagePackSerializer.Serialize(
                new RowData() 
                { 
                    TableKey = tableKeyMapping[(currentNode.Schema,currentNode.Name)] , 
                    ColumnValues = row.Select(r=> r.value).ToArray() 
                });
        }

        protected override async IAsyncEnumerable<byte[]> GenerateMetadata(DatabaseGraph databaseGraph, IEnumerable<TableExportConfig> tableExportConfig)
        {

            yield return MessagePack.MessagePackSerializer.Serialize(new ExportMetadata("1", DbExporterEngine.GetDbType(), DateTimeOffset.Now));

            foreach (var rootTables in tableExportConfig)
            {
                yield return await GetTableMetadata(rootTables);
            }

        }

        private async Task<byte[]> GetTableMetadata(TableExportConfig rootTables)
        {
            var tm = await DbExporterEngine.GetTableMetadata(rootTables.Schema, rootTables.TableName);
            tableKeyMapping.Add((rootTables.Schema, rootTables.TableName), tm.TableKey);
            return MessagePack.MessagePackSerializer.Serialize(tm);
        }

    }
}
