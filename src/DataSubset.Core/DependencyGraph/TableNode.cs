namespace DataSubsetCore.DependencyGraph
{
    /// <summary>
    /// Represents a database table within the dependency graph.
    /// </summary>
    /// <remarks>
    /// Equality and hash code are computed case-insensitively from <see cref="FullName"/>.
    /// </remarks>
    public class TableNode
    {
        /// <summary>
        /// Gets the schema name the table belongs to.
        /// </summary>
        public string Schema { get; }

        /// <summary>
        /// Gets the unqualified table name.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Gets the fully qualified table name in the form "Schema.Name".
        /// </summary>
        public string FullName => $"{Schema}.{Name}";

        /// <summary>
        /// Gets or sets the ordered list of primary key column names for this table.
        /// Empty if the table has no primary key or it is not known.
        /// </summary>
        public List<string> PrimaryKeyColumns { get; set; } = new();

        /// <summary>
        /// Initializes a new instance of the <see cref="TableNode"/> class.
        /// </summary>
        /// <param name="schema">The schema name (e.g., "dbo").</param>
        /// <param name="name">The table name (e.g., "Users").</param>
        public TableNode(string schema, string name)
        {
            Schema = schema;
            Name = name;
        }

        /// <summary>
        /// Returns a hash code for this instance that is consistent with <see cref="Equals(object)"/>.
        /// </summary>
        /// <returns>A case-insensitive hash code derived from <see cref="FullName"/>.</returns>
        public override int GetHashCode()
        {
            return StringComparer.OrdinalIgnoreCase.GetHashCode(FullName);
        }

        /// <summary>
        /// Determines whether the specified object is equal to the current object.
        /// Two <see cref="TableNode"/> instances are considered equal if their <see cref="FullName"/> values are equal, ignoring case.
        /// </summary>
        /// <param name="obj">The object to compare with the current object.</param>
        /// <returns><c>true</c> if the specified object is a <see cref="TableNode"/> with the same <see cref="FullName"/> (case-insensitive); otherwise, <c>false</c>.</returns>
        public override bool Equals(object obj)
        {
            return obj is TableNode other &&
                   StringComparer.OrdinalIgnoreCase.Equals(FullName, other.FullName);
        }

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        /// <remarks>
        /// The format is "Schema.Name [PK: col1, col2]" when primary key columns are present or "Schema.Name [No PK]" otherwise.
        /// </remarks>
        /// <returns>A readable representation including the fully qualified name and primary key information.</returns>
        public override string ToString()
        {
            var pkInfo = PrimaryKeyColumns.Any() ? $" [PK: {string.Join(", ", PrimaryKeyColumns)}]" : " [No PK]";
            return $"{FullName}{pkInfo}";
        }
    }
}
