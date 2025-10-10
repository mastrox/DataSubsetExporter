using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
 
namespace DataSubset.DbDependencyGraph.Core.DependencyGraph
{
    /// <summary>
    /// Represents a directed graph of database tables and their dependencies.
    /// </summary>
    /// <remarks>
    /// This graph specializes <see cref="DirectedGraph{TNode, TEdgeData}"/> for database modeling, where:
    /// <list type="bullet">
    ///   <item><description><typeparamref name="TableNode"/> is the node type representing a table (schema and name).</description></item>
    ///   <item><description><see cref="ITableDependencyEdgeData"/> describes edge metadata such as column bindings between tables.</description></item>
    /// </list>
    /// An auxiliary index <see cref="NodesByName"/> is provided for fast lookup of tables by their fully-qualified name
    /// in the form <c>schema.table</c> using a case-insensitive comparison. The contents of this dictionary are not
    /// automatically synchronized with the underlying graph; callers are responsible for populating and maintaining it
    /// when adding or removing nodes unless managed elsewhere in the codebase.
    /// </remarks>
    /// <seealso cref="TableNode"/>
    /// <seealso cref="ITableDependencyEdgeData"/>
    /// <seealso cref="DirectedGraph{TNode, TEdgeData}"/>
    public class DatabaseGraph : DirectedGraph<TableNode, ITableDependencyEdgeData>
    {
        /// <summary>
        /// Gets a case-insensitive lookup of table nodes keyed by fully-qualified name in the format <c>schema.table</c>.
        /// </summary>
        /// <remarks>
        /// Keys are compared using <see cref="StringComparer.OrdinalIgnoreCase"/> to accommodate typical database
        /// name comparisons. This collection is intended to provide O(1) retrieval of a <see cref="TableNode"/>
        /// when the schema and table name are known.
        /// <para>
        /// Note: This dictionary is not automatically updated by <see cref="DirectedGraph{TNode, TEdgeData}"/> operations.
        /// Ensure you add/remove entries in tandem with graph mutations, or provide a coordinated mechanism elsewhere.
        /// </para>
        /// </remarks>
        public Dictionary<string, TableNode> NodesByName { get; private set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="DatabaseGraph"/> class.
        /// </summary>
        /// <param name="nodeComparer">
        /// Optional equality comparer used by the underlying graph to compare <see cref="TableNode"/> instances.
        /// If <see langword="null"/>, the default comparer is used.
        /// </param>
        /// <remarks>
        /// Also initializes <see cref="NodesByName"/> using <see cref="StringComparer.OrdinalIgnoreCase"/> to enable
        /// case-insensitive lookups by fully-qualified table name.
        /// </remarks>
        public DatabaseGraph(IEqualityComparer<TableNode>? nodeComparer = null) : base(nodeComparer)
        {
            NodesByName = new Dictionary<string, TableNode>(StringComparer.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Finds a table node by schema and table name using the <see cref="NodesByName"/> index.
        /// </summary>
        /// <param name="schema">The schema name (for example, <c>dbo</c>).</param>
        /// <param name="tableName">The table name (for example, <c>Users</c>).</param>
        /// <returns>
        /// The matching <see cref="TableNode"/> if found; otherwise, <see langword="null"/>.
        /// </returns>
        /// <remarks>
        /// The lookup is performed against the fully-qualified key <c>schema.table</c> using a case-insensitive comparison.
        /// </remarks>
        /// <example>
        /// <code language="csharp">
        /// var graph = new DatabaseGraph();
        /// // ... populate graph and NodesByName ...
        /// var users = graph.FindTable("dbo", "Users");
        /// if (users is not null)
        /// {
        ///     // Use the table node
        /// }
        /// </code>
        /// </example>
        public TableNode? FindTable(string schema, string tableName)
        {
            var fullName = $"{schema}.{tableName}";
            return NodesByName.TryGetValue(fullName, out var node) ? node : null;
        }
    }
}
