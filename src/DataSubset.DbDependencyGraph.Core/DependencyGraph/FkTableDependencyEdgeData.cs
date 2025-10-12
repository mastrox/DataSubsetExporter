using DataSubset.DbDependencyGraph.Core.Configurations;

namespace DataSubset.DbDependencyGraph.Core.DependencyGraph
{
    /// <summary>
    /// Represents a foreign key-based dependency edge originating from a specific source table.
    /// </summary>
    /// <remarks>
    /// This implementation of <see cref="ITableDependencyEdgeData"/> captures the source schema/table and the
    /// set of column-level bindings that participate in the relationship, along with the backing database
    /// foreign key constraint name.
    /// </remarks>
    public class FkTableDependencyEdgeData : ITableDependencyEdgeData
    {
        private IEnumerable<ColumnBinding> columnBindings;
        private Dictionary<string, string> columnTargetBySource;

        /// <summary>
        /// Gets or sets the database constraint name (foreign key) that defines this dependency.
        /// </summary>
        /// <value>
        /// A database object name (for example, <c>"FK_Order_OrderLine"</c>). May be <see langword="null"/> or empty if not applicable.
        /// </value>
        public string ConstraintName { get; set; }

        /// <summary>
        /// Gets the schema name that contains the source table for this dependency edge.
        /// </summary>
        /// <value>
        /// A non-empty schema identifier (for example, <c>"dbo"</c>).
        /// </value>
        public string SourceSchema { get; set; }

        /// <summary>
        /// Gets the unqualified name of the source table for this dependency edge.
        /// </summary>
        /// <value>
        /// A non-empty table identifier without schema qualification.
        /// </value>
        public string SourceTable { get; set; }

        /// <summary>
        /// Gets the collection of column bindings that describe how source columns relate to corresponding target columns.
        /// </summary>
        /// <value>
        /// A sequence of <see cref="ColumnBinding"/> instances. The sequence should not be <see langword="null"/>;
        /// it may be empty if no column-level mapping is required.
        /// </value>
        /// <remarks>
        /// Each binding pairs a source column name with a target column name to express the dependency at the column level.
        /// </remarks>
        public IEnumerable<ColumnBinding> ColumnBindings
        {
            get => columnBindings;

            private set
            {
                columnBindings = value;
                columnTargetBySource = columnBindings.ToDictionary(cb => cb.SourceColumn, cb => cb.TargetColumn, StringComparer.OrdinalIgnoreCase);
            }
        }

        public string? GetTargetColumnFromBindings(string sourceColumn)
        {
            columnTargetBySource.TryGetValue(sourceColumn, out var targetColumn);
            return targetColumn;
        }


        /// <summary>
        /// Initializes a new instance of the <see cref="FkTableDependencyEdgeData"/> class.
        /// </summary>
        /// <param name="sourceSchema">The schema that contains the source table (for example, <c>"dbo"</c>).</param>
        /// <param name="sourceTable">The unqualified name of the source table.</param>
        /// <param name="columnBindings">The set of column bindings that participate in the relationship. Should not be <see langword="null"/>.</param>
        /// <param name="constraintName">The database foreign key constraint name that backs this edge.</param>
        public FkTableDependencyEdgeData(string sourceSchema, string sourceTable, IEnumerable<ColumnBinding> columnBindings, string constraintName)
        {
            ConstraintName = constraintName;
            SourceSchema = sourceSchema;
            SourceTable = sourceTable;
            ColumnBindings = columnBindings;
        }

        /// <summary>
        /// Returns a textual representation of this dependency edge, including column mappings and the source table.
        /// </summary>
        /// <returns>
        /// A string in the form "<c>SrcCol -> DstCol, ... -> Schema.Table</c>".
        /// </returns>
        public override string ToString()
        {
            var bindings = string.Join(", ", ColumnBindings.Select(cb => $"{cb.SourceColumn} -> {cb.TargetColumn}"));
            return $"{bindings} -> {SourceSchema}.{SourceTable}";
        }
    }
}
