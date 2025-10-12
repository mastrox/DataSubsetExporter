using DataSubset.DbDependencyGraph.Core.Configurations;

namespace DataSubset.DbDependencyGraph.Core.DependencyGraph
{
    /// <summary>
    /// Represents a dependency edge between tables inferred from relational column bindings
    /// (for example, a foreign key–like relationship).
    /// </summary>
    /// <remarks>
    /// The edge points to <see cref="SourceSchema"/>.<see cref="SourceTable"/> using the set of
    /// <see cref="ColumnBindings"/>. An optional <see cref="WhereClause"/> can further restrict
    /// the target rows.
    /// </remarks>
    public class ImplicitRelTableDependencyEdgeData : ITableDependencyEdgeData
    {
        private IEnumerable<ColumnBinding> columnBindings;
        private IDictionary<string,string> columnTargetBySource;

        /// <summary>
        /// Schema name of the table at the head of the edge (the table being depended on).
        /// </summary>
        public string SourceSchema { get; set; }

        /// <summary>
        /// Table name at the head of the edge (the table being depended on).
        /// </summary>
        public string SourceTable { get; set; }

        /// <summary>
        /// Optional SQL predicate applied to the target table side of the relationship.
        /// Do not include the WHERE keyword; it is added where appropriate.
        /// </summary>
        public string? WhereClause { get; set; }

        /// <summary>
        /// Column pairs that define the join between the dependent table (source columns)
        /// and the referenced table (target columns).
        /// </summary>
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
        /// Initializes a new instance of the <see cref="ImplicitRelTableDependencyEdgeData"/> class.
        /// </summary>
        /// <param name="referencedSchema">Schema of the referenced (target) table.</param>
        /// <param name="referencedTable">Name of the referenced (target) table.</param>
        /// <param name="columnBindings">Column mappings used to join from the dependent table to the referenced table.</param>
        /// <param name="whereClause">Optional filter predicate applied to the referenced table; omit the WHERE keyword.</param>
        public ImplicitRelTableDependencyEdgeData(string referencedSchema, string referencedTable, IEnumerable<ColumnBinding> columnBindings, string? whereClause = null)
        {
            SourceSchema = referencedSchema;
            SourceTable = referencedTable;
            WhereClause = whereClause;
            ColumnBindings = columnBindings;
        }

        /// <summary>
        /// Returns a concise, readable representation of the dependency edge.
        /// </summary>
        /// <returns>
        /// A string like "ColA -> RefColA, ColB -> RefColB -> Schema.Table WHERE ...".
        /// </returns>
        public override string ToString()
        {
            var wc = WhereClause != null ? $" WHERE {WhereClause}" : string.Empty;
            var bindings = string.Join(", ", ColumnBindings.Select(cb => $"{cb.SourceColumn} -> {cb.TargetColumn}"));
            return $"{bindings} -> {SourceSchema}.{SourceTable}{wc}";
        }
    }
}
