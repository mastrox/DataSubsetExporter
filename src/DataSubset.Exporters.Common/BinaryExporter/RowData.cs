using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataSubset.Exporters.Common.BinaryExporter
{
    internal struct RowData
    {
        public int TableKey { get; set; }
        public object?[] ColumnValues { get; set; }
    }
}
