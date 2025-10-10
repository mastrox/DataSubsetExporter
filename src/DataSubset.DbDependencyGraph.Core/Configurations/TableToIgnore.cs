using System.ComponentModel.DataAnnotations;
using System.Text.Json.Serialization;

namespace DataSubset.DbDependencyGraph.Core.Configurations
{
    /// <summary>
    /// Represents a table that should be ignored during processing operations.
    /// </summary>
    public class TableToIgnore
    {
        /// <summary>
        /// The database schema name.
        /// </summary>
        public string Schema { get; set; } = string.Empty;

        /// <summary>
        /// The table name to ignore.
        /// </summary>
        public string TableName { get; set; } = string.Empty;

        /// <summary>
        /// Gets the full qualified name of the table (schema.tablename).
        /// </summary>
        public string FullName => $"{Schema}.{TableName}";

        public TableToIgnore()
        {
        }

        public TableToIgnore(string schema, string tableName)
        {
            Schema = schema ?? throw new ArgumentNullException(nameof(schema));
            TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
        }

        /// <summary>
        /// Checks if this ignore entry matches the given schema and table name.
        /// </summary>
        /// <param name="schema">The schema name to match</param>
        /// <param name="tableName">The table name to match</param>
        /// <returns>True if matches; otherwise, false</returns>
        public bool Matches(string schema, string tableName)
        {
            return Schema.Equals(schema, StringComparison.OrdinalIgnoreCase) &&
                   TableName.Equals(tableName, StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Validates this table ignore entry.
        /// </summary>
        /// <returns>A list of validation error messages</returns>
        public List<string> Validate()
        {
            var errors = new List<string>();

            if (string.IsNullOrWhiteSpace(Schema))
            {
                errors.Add("Schema name is required");
            }

            if (string.IsNullOrWhiteSpace(TableName))
            {
                errors.Add("Table name is required");
            }

            return errors;
        }

        public override bool Equals(object? obj)
        {
            if (obj is not TableToIgnore other) return false;
            return Matches(other.Schema, other.TableName);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(
                Schema.ToLowerInvariant(),
                TableName.ToLowerInvariant());
        }

        public override string ToString()
        {
            return FullName;
        }
    }
}