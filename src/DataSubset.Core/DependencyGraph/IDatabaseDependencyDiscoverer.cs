namespace DataSubsetCore.DependencyGraph
{
    /// <summary>
    /// Defines operations for discovering database tables and constructing foreign key dependencies,
    /// projecting the results into a <see cref="DatabaseGraph"/>.
    /// </summary>
    /// <remarks>
    /// Typical usage:
    /// <list type="number">
    ///   <item><description>Call <see cref="DiscoverTablesAsync(DatabaseGraph, string[], System.Collections.Generic.HashSet{string})"/> to add <see cref="TableNode"/> instances for the target schemas.</description></item>
    ///   <item><description>Call <see cref="BuildForeignKeyRelationshipsAsync(DatabaseGraph, string[], System.Collections.Generic.HashSet{string})"/> to add edges that represent foreign key relationships between discovered tables.</description></item>
    /// </list>
    /// Implementations are expected to be provider-specific (e.g., SQL Server, PostgreSQL) and should:
    /// <list type="bullet">
    ///   <item><description>Be idempotent (safe to call multiple times without duplicating nodes/edges).</description></item>
    ///   <item><description>Respect the <paramref name="schemas"/> filter and <paramref name="ignoredTables"/> exclusion set.</description></item>
    ///   <item><description>Only add edges where both the dependent and principal tables exist in the provided <see cref="DatabaseGraph"/>.</description></item>
    ///   <item><description>Maintain the auxiliary <see cref="DatabaseGraph.NodesByName"/> index when adding nodes.</description></item>
    /// </list>
    /// </remarks>
    public interface IDatabaseDependencyDiscoverer
    {
        /// <summary>
        /// Discovers foreign key relationships among tables in the specified <paramref name="schemas"/> and
        /// adds corresponding directed edges to the <paramref name="graph"/>.
        /// </summary>
        /// <param name="graph">
        /// The target <see cref="DatabaseGraph"/> that already contains the relevant <see cref="TableNode"/> nodes.
        /// Edges with <see cref="ITableDependencyEdge"/> metadata will be added to this graph.
        /// </param>
        /// <param name="schemas">
        /// One or more schema names (for example, "dbo") to include when resolving relationships. An empty array
        /// may be interpreted by implementations as "all accessible schemas".
        /// </param>
        /// <param name="ignoredTables">
        /// A set of fully-qualified table names in the form "schema.table" to exclude from relationship building.
        /// Use a case-insensitive comparer (for example, <see cref="StringComparer.OrdinalIgnoreCase"/>).
        /// </param>
        /// <returns>
        /// A task that completes when all applicable foreign key relationships have been analyzed and edges
        /// have been added to <paramref name="graph"/>.
        /// </returns>
        /// <remarks>
        /// - Edge direction: from the dependent (child/FK) table to the principal (parent/PK) table.<br/>
        /// - Edge metadata: implementations should populate <see cref="ITableDependencyEdge.ColumnBindings"/> to map
        ///   foreign key columns to their referenced primary key columns.<br/>
        /// - Idempotency: implementations should avoid creating duplicate edges when invoked repeatedly.
        /// </remarks>
        /// <example>
        /// <code language="csharp">
        /// // graph contains nodes for tables discovered earlier
        /// await discoverer.BuildForeignKeyRelationshipsAsync(graph, new[] { "sales", "dbo" }, ignored: new(StringComparer.OrdinalIgnoreCase));
        /// </code>
        /// </example>
        Task BuildForeignKeyRelationshipsAsync(DatabaseGraph graph, string[] schemas, HashSet<string> ignoredTables);

        /// <summary>
        /// Discovers tables available in the specified <paramref name="schemas"/> and adds a corresponding
        /// <see cref="TableNode"/> for each non-ignored table to the <paramref name="graph"/>.
        /// </summary>
        /// <param name="graph">
        /// The <see cref="DatabaseGraph"/> to populate with discovered tables. Implementations should call
        /// <see cref="DirectedGraph{TNode, TEdgeData}.AddNode(TNode)"/> and maintain <see cref="DatabaseGraph.NodesByName"/>
        /// using the fully-qualified key "schema.table".
        /// </param>
        /// <param name="schemas">
        /// One or more schema names (for example, "dbo") to include. An empty array may be interpreted by
        /// implementations as "all accessible schemas".
        /// </param>
        /// <param name="ignoredTables">
        /// A set of fully-qualified table names in the form "schema.table" to exclude from discovery.
        /// Use a case-insensitive comparer (for example, <see cref="StringComparer.OrdinalIgnoreCase"/>).
        /// </param>
        /// <returns>
        /// A task that completes when discovery has finished and all applicable tables have been added to
        /// <paramref name="graph"/>.
        /// </returns>
        /// <remarks>
        /// - Implementations should not remove existing nodes from <paramref name="graph"/>.<br/>
        /// - Adding a table that already exists should be a no-op (ensure no duplicates).<br/>
        /// - This method does not create edges; call
        ///   <see cref="BuildForeignKeyRelationshipsAsync(DatabaseGraph, string[], HashSet{string})"/> to materialize dependencies.
        /// </remarks>
        /// <example>
        /// <code language="csharp">
        /// var ignored = new HashSet&lt;string&gt;(StringComparer.OrdinalIgnoreCase) { "sys.diagrams" };
        /// await discoverer.DiscoverTablesAsync(graph, new[] { "dbo", "sales" }, ignored);
        /// </code>
        /// </example>
        Task DiscoverTablesAsync(DatabaseGraph graph, string[] schemas, HashSet<string> ignoredTables);
    }
}
