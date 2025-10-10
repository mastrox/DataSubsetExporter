namespace DataSubset.DbDependencyGraph.Core.Configurations
{
    /// <summary>
    /// Represents a pairing of a primary key column name and its corresponding value,
    /// used to uniquely identify a row when subsetting, seeding, or filtering data.
    /// </summary>
    /// <remarks>
    /// Both properties are required. Values are represented as strings to simplify configuration and
    /// serialization; consumers are responsible for converting them to the appropriate CLR/database types.
    /// </remarks>
    public class PrimaryKeyValue
    {
        /// <summary>
        /// Gets or sets the name of the primary key column.
        /// </summary>
        /// <remarks>
        /// Provide the column name exactly as defined in the database schema.
        /// This value must not be null or empty.
        /// </remarks>
        public required string ColumnName { get; set; }

        /// <summary>
        /// Gets or sets the value for the specified primary key column.
        /// </summary>
        /// <remarks>
        /// The value is stored as a string (e.g., "42", "2024-01-01T00:00:00Z", "3F2504E0-4F89-11D3-9A0C-0305E82C3301").
        /// Convert this to the appropriate CLR/database type as needed during query or command execution.
        /// This value must not be null or empty.
        /// </remarks>
        public required string Value { get; set; }
    }
}