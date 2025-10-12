using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataSubset.Exporters.Common.BinaryExporter
{
    internal class TableMetadata()
    {
        [MessagePack.Key(0)]
        public required string Schema { get; set; }
        [MessagePack.Key(1)]
        public required string Table { get; set; }
        [MessagePack.Key(2)]
        public required ColumnMetadata[] ColumnMetadata { get; set; }
    }
}
