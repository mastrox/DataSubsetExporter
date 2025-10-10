using DataSubset.DbDependencyGraph.Core.Configurations;

namespace DataSubset.DbDependencyGraph.Core.DependencyGraph
{

    /// <summary>
    /// Represents a directed dependency edge originating from a specific source table within a table dependency graph.
    /// </summary>
    /// <remarks>
    /// The edge describes how a source table participates in a relationship via one or more column-level bindings.
    /// Implementations typically pair this edge with target-side metadata elsewhere in the model to complete the relationship.
    /// </remarks>
    public interface ITableDependencyEdgeData
    {
        /// <summary>
        /// Gets the schema name that contains the source table for this dependency edge.
        /// </summary>
        /// <value>
        /// A non-empty schema identifier (for example, <c>"dbo"</c>).
        /// </value>
        string SourceSchema { get; }

        /// <summary>
        /// Gets the unqualified name of the source table for this dependency edge.
        /// </summary>
        /// <value>
        /// A non-empty table identifier without schema qualification.
        /// </value>
        string SourceTable { get; }

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
        /// <seealso cref="ColumnBinding"/>
        IEnumerable<ColumnBinding> ColumnBindings { get; }

        string GetTargetColumnFromBindings(string sourceColumn);

        }
}