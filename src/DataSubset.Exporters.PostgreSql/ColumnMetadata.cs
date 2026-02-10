using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataSubset.Exporters.PostgreSql
{
    public struct ColumnMetadata
    {
        public required string Name { get; set; }

        public required string DataType { get; set; }
    }
}
