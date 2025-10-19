using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataSubset.Exporters.Common.BinaryExporter
{
    public struct ColumnMetadata
    {
        [MessagePack.Key(0)]
        public required string Name { get; set; }

        [MessagePack.Key(1)]
        public required string DataType { get; set; }
    }
}
