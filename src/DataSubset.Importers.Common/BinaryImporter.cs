using DataSubset.Exporters.Common.BinaryExporter;

namespace DataSubset.Importers.Common
{
    public class BinaryImporter(IDbBinaryImporter dbBinaryImporter)
    {
        public async Task ImportAsync(IAsyncEnumerable<byte[]> rows)
        {
            await using var enumerator = rows.GetAsyncEnumerator();
            ExportMetadata exportMetadata;

            if (await enumerator.MoveNextAsync())
            {
                exportMetadata = MessagePack.MessagePackSerializer.Deserialize<ExportMetadata>(enumerator.Current);


                while (await enumerator.MoveNextAsync())
                {
                    // Process each row
                    RowData row = MessagePack.MessagePackSerializer.Deserialize<RowData>(enumerator.Current);
                    await dbBinaryImporter.ImportRowAsync(exportMetadata, row);
                }
            }
        }
    }
}
