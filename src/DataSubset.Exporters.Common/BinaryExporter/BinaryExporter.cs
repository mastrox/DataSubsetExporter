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
    public class BinaryExporter: ExporterBase<byte[]>
    {
        TableMetadataVisitor? tableMetadataVisitor = null;
        public BinaryExporter(IDbExporterEngine dbExporterEngine, ILogger? logger = null) : base(dbExporterEngine, logger)
        {
        }

        protected async override Task<byte[]> GenerateCurrentRowExportItem(TableNode currentNode, (string column, object? value)[] row, IEnumerable<TableExportConfig> tableExportConfig)
        {
            return MessagePack.MessagePackSerializer.Serialize(
                new RowData() 
                { 
                    TableKey = tableMetadataVisitor!.TablesMetadata[(currentNode.Schema,currentNode.Name)].TableKey, 
                    ColumnValues = row.Select(r=> r.value).ToArray() 
                });
        }

        protected override byte[]? GenerateMetadata(DatabaseGraph databaseGraph, IEnumerable<TableExportConfig> tableExportConfig)
        {
            var exportMetadata = new ExportMetadata("1", DbExporterEngine.GetDbType(), DateTimeOffset.Now);

            tableMetadataVisitor = new TableMetadataVisitor(databaseGraph, DbExporterEngine);
            tableMetadataVisitor.VisitTablePreOrder(tableExportConfig.Select(t => (t.Schema, t.TableName)));

            exportMetadata.TablesMetadata = tableMetadataVisitor.TablesMetadata.Values.ToArray();
            return MessagePack.MessagePackSerializer.Serialize(exportMetadata);
        }

    }
}
