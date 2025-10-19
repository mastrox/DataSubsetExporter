using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataSubset.Exporters.Common.BinaryExporter
{
    public struct ExportMetadata
    {
        [MessagePack.Key(0)]
        public string Version { get; set; }
        [MessagePack.Key(1)]
        public DbTypes DbType { get; set; }
        [MessagePack.Key(2)]
        public DateTimeOffset ExportTimestamp { get; set; }

        public ExportMetadata(string version, DbTypes dbType, DateTimeOffset exportTimestamp)
        {
            Version = version;
            DbType = dbType;
            ExportTimestamp = exportTimestamp;
        }

        public ExportMetadata()
        {
        }
    }
}
