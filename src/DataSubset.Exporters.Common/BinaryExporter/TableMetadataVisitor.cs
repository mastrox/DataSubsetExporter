using DataSubset.DbDependencyGraph.Core.DependencyGraph;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataSubset.Exporters.Common.BinaryExporter
{
    internal class TableMetadataVisitor : DepthFirsVisitorBase<bool?>
    {
        public IDbExporterEngine dbExporterEngine;

        public Dictionary<(string schema, string table), TableMetadata> TablesMetadata { get; set; } = new Dictionary<(string schema, string table), TableMetadata>();

        public Dictionary<string, string?> ReceivedContext { get; } = new(StringComparer.OrdinalIgnoreCase);

        public TableMetadataVisitor(DatabaseGraph graph, IDbExporterEngine dbExporterEngine) : base(graph)
        {
            this.dbExporterEngine = dbExporterEngine;
        }

        protected override bool? GetNodeContext(TableNode currentTable, ITableDependencyEdgeData? data, int depth)
        {
            return null;
        }

        protected override bool GetSkipCurrentNode(TableNode currentTable, ITableDependencyEdgeData? data, int depth)
        {
            return false;
        }

        protected override void ProcessCurrentNode(TableNode currentTable, ITableDependencyEdgeData? data, int depth, bool? currentNodeContext)
        {
            var key = (currentTable.Schema, currentTable.Name);
            if (!TablesMetadata.ContainsKey(key))
            {
                var metadata = dbExporterEngine.GetTableMetadata(currentTable.Schema, currentTable.Name).Result;
                TablesMetadata.TryAdd(key, metadata);
            }
        }


    }
}
