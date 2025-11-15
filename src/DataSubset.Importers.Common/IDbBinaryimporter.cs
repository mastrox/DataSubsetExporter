using DataSubset.Exporters.Common.BinaryExporter;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataSubset.Importers.Common
{
    public interface IDbBinaryImporter
    {
        Task ImportRowAsync(ExportMetadata exportMetadata, RowData row);
    }
}
