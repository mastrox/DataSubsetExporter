using System.Diagnostics.CodeAnalysis;

namespace DataSubset.Core.DependencyGraph
{

    /// <summary>
    /// Represents a generic directed graph where nodes are of type <typeparamref name="TNode"/>
    /// and edges can carry metadata of type <typeparamref name="TEdgeData"/>.
    /// Internally uses adjacency and reverse-adjacency lists for efficient traversal.
    /// </summary>
    /// <typeparam name="TNode">The node identifier/type.</typeparam>
    /// <typeparam name="TEdgeData">The edge payload/metadata type.</typeparam>
    /// <remarks>
    /// - Thread-safety: This type is not thread-safe. Synchronize externally if accessed concurrently.
    /// - Equality: Node identity is determined by the supplied <see cref="IEqualityComparer{T}"/> (or default).
    /// - Edges: Edge equality is determined by <see cref="GraphEdge{TNode, TEdgeData}"/> implementation.
    /// </remarks>
    public class DirectedGraph<TNode, TEdgeData>
    where TNode : notnull
    {
        private readonly Dictionary<TNode, HashSet<GraphEdge<TNode, TEdgeData>>> _adjacencyList;
        private readonly Dictionary<TNode, HashSet<GraphEdge<TNode, TEdgeData>>> _reverseAdjacencyList;
        private readonly IEqualityComparer<TNode> _nodeComparer;

        /// <summary>
        /// Initializes a new instance of the <see cref="DirectedGraph{TNode, TEdgeData}"/> class.
        /// </summary>
        /// <param name="nodeComparer">
        /// Optional node equality comparer. If <c>null</c>, <see cref="EqualityComparer{T}.Default"/> is used.
        /// </param>
        public DirectedGraph(IEqualityComparer<TNode>? nodeComparer = null)
        {
            _nodeComparer = nodeComparer ?? EqualityComparer<TNode>.Default;
            _adjacencyList = new Dictionary<TNode, HashSet<GraphEdge<TNode, TEdgeData>>>(_nodeComparer);
            _reverseAdjacencyList = new Dictionary<TNode, HashSet<GraphEdge<TNode, TEdgeData>>>(_nodeComparer);
        }

        /// <summary>
        /// Ensures the specified node exists in the graph. If it does not exist, it is added.
        /// </summary>
        /// <param name="node">The node to add or ensure exists.</param>
        /// <remarks>Time complexity: amortized O(1).</remarks>
        public void AddNode(TNode node)
        {
            if (!_adjacencyList.ContainsKey(node))
            {
                _adjacencyList[node] = new HashSet<GraphEdge<TNode, TEdgeData>>();
                _reverseAdjacencyList[node] = new HashSet<GraphEdge<TNode, TEdgeData>>();
            }
        }

        /// <summary>
        /// Adds a directed edge from <paramref name="source"/> to <paramref name="target"/>.
        /// </summary>
        /// <param name="source">The source node.</param>
        /// <param name="target">The target node.</param>
        /// <param name="edgeData">Optional edge metadata; if omitted, the default value of <typeparamref name="TEdgeData"/> is used.</param>
        /// <remarks>
        /// - If either node does not exist, it will be created.
        /// - Time complexity: O(1) average for insertion.
        /// </remarks>
        public void AddEdge(TNode source, TNode target, TEdgeData? edgeData = default)
        {
            AddNode(source);
            AddNode(target);

            var edge = new GraphEdge<TNode, TEdgeData>(source, target, edgeData);
            _adjacencyList[source].Add(edge);
            _reverseAdjacencyList[target].Add(edge);
        }

        /// <summary>
        /// Removes a directed edge from <paramref name="source"/> to <paramref name="target"/>, if it exists.
        /// </summary>
        /// <param name="source">The source node.</param>
        /// <param name="target">The target node.</param>
        /// <returns><c>true</c> if an edge was found and removed; otherwise, <c>false</c>.</returns>
        /// <remarks>Time complexity: O(out-degree(source)).</remarks>
        public bool RemoveEdge(TNode source, TNode target)
        {
            if (!_adjacencyList.ContainsKey(source) || !_reverseAdjacencyList.ContainsKey(target))
                return false;

            var edgeToRemove = _adjacencyList[source].FirstOrDefault(e => _nodeComparer.Equals(e.Target, target));
            if (edgeToRemove != null)
            {
                _adjacencyList[source].Remove(edgeToRemove);
                _reverseAdjacencyList[target].Remove(edgeToRemove);
                return true;
            }
            return false;
        }

        /// <summary>
        /// Removes the specified <paramref name="node"/> and all incoming and outgoing edges incident to it.
        /// </summary>
        /// <param name="node">The node to remove.</param>
        /// <returns><c>true</c> if the node existed and was removed; otherwise, <c>false</c>.</returns>
        /// <remarks>Time complexity: O(in-degree(node) + out-degree(node)).</remarks>
        public bool RemoveNode(TNode node)
        {
            if (!_adjacencyList.ContainsKey(node))
                return false;

            // Remove all outgoing edges
            var outgoingEdges = _adjacencyList[node].ToList();
            foreach (var edge in outgoingEdges)
            {
                if (edge.Target != null && _reverseAdjacencyList.ContainsKey(edge.Target))
                {
                    _reverseAdjacencyList[edge.Target].Remove(edge);
                }
            }

            // Remove all incoming edges
            var incomingEdges = _reverseAdjacencyList[node].ToList();
            foreach (var edge in incomingEdges)
            {
                if (edge.Source != null && _adjacencyList.ContainsKey(edge.Source))
                {
                    _adjacencyList[edge.Source].Remove(edge);
                }
            }

            _adjacencyList.Remove(node);
            _reverseAdjacencyList.Remove(node);
            return true;
        }

        /// <summary>
        /// Gets all nodes currently present in the graph.
        /// </summary>
        /// <returns>An enumeration of nodes in the graph.</returns>
        /// <remarks>
        /// The returned enumeration reflects live graph contents and is not a defensive copy.
        /// </remarks>
        public IEnumerable<TNode> GetNodes()
        {
            return _adjacencyList.Keys;
        }

        /// <summary>
        /// Gets all outgoing edges for the specified <paramref name="node"/>.
        /// </summary>
        /// <param name="node">The node whose outgoing edges are requested.</param>
        /// <returns>
        /// An enumeration of <see cref="GraphEdge{TNode, TEdgeData}"/> where each edge's source is <paramref name="node"/>.
        /// If the node is unknown, an empty sequence is returned.
        /// </returns>
        public IEnumerable<GraphEdge<TNode, TEdgeData>> GetOutgoingEdges(TNode node)
        {
            return _adjacencyList.TryGetValue(node, out var edges) ? edges : Enumerable.Empty<GraphEdge<TNode, TEdgeData>>();
        }

        /// <summary>
        /// Gets all incoming edges for the specified <paramref name="node"/>.
        /// </summary>
        /// <param name="node">The node whose incoming edges are requested.</param>
        /// <returns>
        /// An enumeration of <see cref="GraphEdge{TNode, TEdgeData}"/> where each edge's target is <paramref name="node"/>.
        /// If the node is unknown, an empty sequence is returned.
        /// </returns>
        public IEnumerable<GraphEdge<TNode, TEdgeData>> GetIncomingEdges(TNode node)
        {
            return _reverseAdjacencyList.TryGetValue(node, out var edges) ? edges : Enumerable.Empty<GraphEdge<TNode, TEdgeData>>();
        }

        /// <summary>
        /// Gets the direct successors (out-neighbors) of the specified <paramref name="node"/>.
        /// </summary>
        /// <param name="node">The node whose successors are requested.</param>
        /// <returns>An enumeration of nodes that are targets of outgoing edges from <paramref name="node"/>.</returns>
        public IEnumerable<TNode> GetSuccessors(TNode node)
        {
            return GetOutgoingEdges(node).Select(e => e.Target);
        }

        /// <summary>
        /// Gets the direct predecessors (in-neighbors) of the specified <paramref name="node"/>.
        /// </summary>
        /// <param name="node">The node whose predecessors are requested.</param>
        /// <returns>An enumeration of nodes that have edges pointing to <paramref name="node"/>.</returns>
        public IEnumerable<TNode> GetPredecessors(TNode node)
        {
            return GetIncomingEdges(node).Where(a=>a.Source != null).Select(e => e.Source)!;
        }

        /// <summary>
        /// Determines whether there exists a directed edge from <paramref name="source"/> to <paramref name="target"/>.
        /// </summary>
        /// <param name="source">The source node.</param>
        /// <param name="target">The target node.</param>
        /// <returns><c>true</c> if such an edge exists; otherwise, <c>false</c>.</returns>
        public bool HasEdge(TNode source, TNode target)
        {
            return GetOutgoingEdges(source).Any(e => _nodeComparer.Equals(e.Target, target));
        }

        /// <summary>
        /// Determines whether the specified <paramref name="node"/> exists in the graph.
        /// </summary>
        /// <param name="node">The node to check.</param>
        /// <returns><c>true</c> if the node exists; otherwise, <c>false</c>.</returns>
        public bool HasNode(TNode node)
        {
            return _adjacencyList.ContainsKey(node);
        }

        /// <summary>
        /// Gets nodes with no incoming edges (i.e., nodes with in-degree 0).
        /// </summary>
        /// <returns>An enumeration of root nodes.</returns>
        public IEnumerable<TNode> GetRootNodes()
        {
            return _adjacencyList.Keys.Where(node => !_reverseAdjacencyList[node].Any());
        }

        /// <summary>
        /// Gets nodes with no outgoing edges (i.e., nodes with out-degree 0).
        /// </summary>
        /// <returns>An enumeration of leaf nodes.</returns>
        public IEnumerable<TNode> GetLeafNodes()
        {
            return _adjacencyList.Keys.Where(node => !_adjacencyList[node].Any());
        }

        /// <summary>
        /// Produces a topological ordering of the nodes using Kahn's algorithm.
        /// </summary>
        /// <returns>A list of nodes in topological order.</returns>
        /// <exception cref="InvalidOperationException">
        /// Thrown if the graph contains a cycle, making a topological sort impossible.
        /// </exception>
        /// <remarks>
        /// - Applicable only to Directed Acyclic Graphs (DAGs).
        /// - Time complexity: O(V + E), where V is the number of nodes and E is the number of edges.
        /// </remarks>
        public List<TNode> TopologicalSort()
        {
            var result = new List<TNode>();
            var inDegree = new Dictionary<TNode, int>(_nodeComparer);
            var queue = new Queue<TNode>();

            // Initialize in-degree count
            foreach (var node in GetNodes())
            {
                inDegree[node] = GetIncomingEdges(node).Count();
            }

            // Find all nodes with no incoming edges
            foreach (var node in inDegree.Where(kvp => kvp.Value == 0).Select(kvp => kvp.Key))
            {
                queue.Enqueue(node);
            }

            while (queue.Count > 0)
            {
                var current = queue.Dequeue();
                result.Add(current);

                // For each successor of current node
                foreach (var successor in GetSuccessors(current))
                {
                    inDegree[successor]--;
                    if (inDegree[successor] == 0)
                    {
                        queue.Enqueue(successor);
                    }
                }
            }

            // Check for cycles
            if (result.Count != GetNodes().Count())
            {
                throw new InvalidOperationException("Cycle detected in graph - topological sort not possible");
            }

            return result;
        }

        /// <summary>
        /// Detects whether the graph contains any cycles using depth-first search (DFS).
        /// </summary>
        /// <param name="considerSelfCycles">
        /// If <c>true</c>, self-loops (edges from a node to itself) are considered cycles; otherwise they are ignored.
        /// </param>
        /// <returns><c>true</c> if a cycle is detected; otherwise, <c>false</c>.</returns>
        /// <remarks>Time complexity: O(V + E).</remarks>
        public bool HasCycles(bool considerSelfCycles)
        {
            var visited = new HashSet<TNode>(_nodeComparer);
            var recursionStack = new HashSet<TNode>(_nodeComparer);

            foreach (var node in GetNodes())
            {
                if (!visited.Contains(node))
                {
                    if (HasCycleDFS(node, visited, recursionStack, considerSelfCycles))
                        return true;
                }
            }
            return false;
        }

        /// <summary>
        /// DFS utility to detect back-edges indicating cycles.
        /// </summary>
        /// <param name="node">The current node.</param>
        /// <param name="visited">Set of visited nodes.</param>
        /// <param name="recursionStack">Nodes on the current DFS recursion stack.</param>
        /// <param name="considerSelfCycles">Whether to consider self-loops as cycles.</param>
        /// <returns><c>true</c> if a cycle is found starting from <paramref name="node"/>; otherwise, <c>false</c>.</returns>
        private bool HasCycleDFS(TNode node, HashSet<TNode> visited, HashSet<TNode> recursionStack, bool considerSelfCycles)
        {
            visited.Add(node);
            recursionStack.Add(node);

            foreach (var successor in GetSuccessors(node))
            {
                if (!considerSelfCycles && successor!.Equals(node))
                    continue;

                if (!visited.Contains(successor))
                {
                    if (HasCycleDFS(successor, visited, recursionStack, considerSelfCycles))
                        return true;
                }
                else if (recursionStack.Contains(successor))
                {
                    return true;
                }
            }

            recursionStack.Remove(node);
            return false;
        }

        /// <summary>
        /// Computes the strongly connected components (SCCs) of the graph using Tarjan's algorithm.
        /// </summary>
        /// <returns>
        /// A list of components, where each component is a list of nodes forming a maximal set with mutual reachability.
        /// </returns>
        /// <remarks>Time complexity: O(V + E).</remarks>
        public List<List<TNode>> GetStronglyConnectedComponents()
        {
            var index = 0;
            var stack = new Stack<TNode>();
            var indices = new Dictionary<TNode, int>(_nodeComparer);
            var lowLinks = new Dictionary<TNode, int>(_nodeComparer);
            var onStack = new HashSet<TNode>(_nodeComparer);
            var components = new List<List<TNode>>();

            foreach (var node in GetNodes())
            {
                if (!indices.ContainsKey(node))
                {
                    StrongConnect(node, ref index, stack, indices, lowLinks, onStack, components);
                }
            }

            return components;
        }

        /// <summary>
        /// Tarjan's algorithm recursive worker that identifies SCC roots and extracts components.
        /// </summary>
        /// <param name="node">The node being processed.</param>
        /// <param name="index">Monotonically increasing index for DFS discovery times.</param>
        /// <param name="stack">The working stack of nodes.</param>
        /// <param name="indices">Map of discovery indices per node.</param>
        /// <param name="lowLinks">Map of low-link values per node.</param>
        /// <param name="onStack">Set of nodes currently on the stack.</param>
        /// <param name="components">Accumulator for discovered components.</param>
        private void StrongConnect(TNode node, ref int index, Stack<TNode> stack,
            Dictionary<TNode, int> indices, Dictionary<TNode, int> lowLinks,
            HashSet<TNode> onStack, List<List<TNode>> components)
        {
            indices[node] = index;
            lowLinks[node] = index;
            index++;
            stack.Push(node);
            onStack.Add(node);

            foreach (var successor in GetSuccessors(node))
            {
                if (!indices.ContainsKey(successor))
                {
                    StrongConnect(successor, ref index, stack, indices, lowLinks, onStack, components);
                    lowLinks[node] = Math.Min(lowLinks[node], lowLinks[successor]);
                }
                else if (onStack.Contains(successor))
                {
                    lowLinks[node] = Math.Min(lowLinks[node], indices[successor]);
                }
            }

            if (lowLinks[node] == indices[node])
            {
                var component = new List<TNode>();
                TNode w;
                do
                {
                    w = stack.Pop();
                    onStack.Remove(w);
                    component.Add(w);
                } while (!_nodeComparer.Equals(w, node));

                components.Add(component);
            }
        }

        /// <summary>
        /// Computes basic statistics about the graph.
        /// </summary>
        /// <returns>
        /// A <see cref="GraphStatistics"/> instance containing counts of nodes, edges, root nodes,
        /// leaf nodes, and a cycle presence flag.
        /// </returns>
        public GraphStatistics GetStatistics()
        {
            var nodeCount = GetNodes().Count();
            var edgeCount = _adjacencyList.Values.Sum(edges => edges.Count);
            var rootCount = GetRootNodes().Count();
            var leafCount = GetLeafNodes().Count();
            var hasCycles = HasCycles(false);

            return new GraphStatistics
            {
                NodeCount = nodeCount,
                EdgeCount = edgeCount,
                RootNodeCount = rootCount,
                LeafNodeCount = leafCount,
                HasCycles = hasCycles
            };
        }
    }
}