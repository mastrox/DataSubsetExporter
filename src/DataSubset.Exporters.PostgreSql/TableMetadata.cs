using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataSubset.Exporters.PostgreSql
{
    public struct TableMetadata()
    {
        public int TableKey { get; set; }
        public required string Schema { get; set; }

        public required string Table { get; set; }

        public required ColumnMetadata[] ColumnMetadata { get; set; }
    }
}
