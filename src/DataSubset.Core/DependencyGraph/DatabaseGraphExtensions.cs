// Node representing a table in the dependency graph
namespace DataSubset.Core.DependencyGraph
{
    /// <summary>
    /// Provides extension methods for working with <see cref="DatabaseGraph"/> objects.
    /// </summary>
    /// <remarks>This class contains helper methods to simplify common operations on <see
    /// cref="DatabaseGraph"/> instances, such as managing nodes and relationships within the graph. All methods are
    /// static and operate directly on the provided <see cref="DatabaseGraph"/> instance.</remarks>
    public static class DatabaseGraphExtensions
    {

        /// <summary>
        /// Get or create node
        /// </summary>
        /// <param name="graph"></param>
        /// <param name="schema"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public static TableNode GetOrCreateNode(this DatabaseGraph graph, string schema, string tableName)
        {
            var fullName = $"{schema}.{tableName}";
            if (!graph.NodesByName.ContainsKey(fullName))
            {
                var node = new TableNode(schema, tableName);
                graph.NodesByName[fullName] = node;
                graph.AddNode(node);
            }
            return graph.NodesByName[fullName];
        }
    }
}