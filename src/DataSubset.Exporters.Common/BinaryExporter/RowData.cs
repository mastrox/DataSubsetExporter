using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataSubset.Exporters.Common.BinaryExporter
{
    [MessagePack.MessagePackObject]
    public struct RowData
    {
        [MessagePack.Key(0)]
        public int TableKey { get; set; }
        [MessagePack.Key(1)]
        public object?[] ColumnValues { get; set; }
    }
}
