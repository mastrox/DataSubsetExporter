namespace DataSubset.PostgreSql
{
    public class ColumnInfo
    {
        public required string Name { get; set; }
        public required string DataType { get; set; }
        public bool IsNullable { get; set; }
        public bool IsPrimaryKey { get; set; }
        public string? ReferencedSchema { get; set; }
        public string? ReferencedTable { get; set; }
        public string? ReferencedColumn { get; set; }
    }
}
