namespace DataSubsetCore.DependencyGraph
{
    /// <summary>
    /// Represents a directed edge between two nodes in a dependency graph.
    /// The edge can optionally carry additional metadata of type <typeparamref name="TEdgeData" />.
    /// </summary>
    /// <typeparam name="TNode">The type used to represent nodes in the graph.</typeparam>
    /// <typeparam name="TEdgeData">The type of metadata associated with the edge.</typeparam>
    public class GraphEdge<TNode, TEdgeData>
    {
        /// <summary>
        /// Gets the source (tail) node of the directed edge.
        /// </summary>
        public TNode? Source { get; }

        /// <summary>
        /// Gets the target (head) node of the directed edge.
        /// </summary>
        public TNode Target { get; }

        /// <summary>
        /// Gets the metadata associated with this edge, if any.
        /// </summary>
        public TEdgeData? Data { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="GraphEdge{TNode, TEdgeData}"/> class.
        /// </summary>
        /// <param name="source">The source (tail) node of the edge.</param>
        /// <param name="target">The target (head) node of the edge.</param>
        /// <param name="data">Optional metadata associated with the edge.</param>
        public GraphEdge(TNode? source, TNode target, TEdgeData? data)
        {
            Source = source;
            Target = target;
            Data = data;
        }

        /// <summary>
        /// Returns a concise string that represents the edge direction in the form "Source -> Target".
        /// </summary>
        /// <returns>A string representation of the edge direction.</returns>
        public override string ToString()
        {
            return $"{Source} -> {Target}";
        }
    }

    // Statistics about the graph
    public class GraphStatistics
    {
        public int NodeCount { get; set; }
        public int EdgeCount { get; set; }
        public int RootNodeCount { get; set; }
        public int LeafNodeCount { get; set; }
        public bool HasCycles { get; set; }

        public override string ToString()
        {
            return $"Nodes: {NodeCount}, Edges: {EdgeCount}, Roots: {RootNodeCount}, Leaves: {LeafNodeCount}, Cycles: {HasCycles}";
        }
    }
}