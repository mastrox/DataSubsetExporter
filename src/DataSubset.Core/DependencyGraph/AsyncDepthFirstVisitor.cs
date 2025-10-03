using DataSubset.Core.Configurations;
using DataSubset.Core.DependencyGraph;
using DependencyTreeApp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataSubset.Core.DependencyGraph
{
    //TODO
    ///// <summary>
    ///// Base class for depth-first traversal over a <see cref="DatabaseGraph"/> of <see cref="TableNode"/>s.
    ///// Provides both pre-order (node processed before its children) and post-order (children before node) traversal flavors.
    ///// </summary>
    ///// <remarks>
    ///// Derive from this class and implement:
    ///// <list type="bullet">
    /////   <item><description><see cref="GetSkipCurrentNode(TableNode, ITableDependencyEdge?, int)"/> to decide whether a node and its subtree are skipped.</description></item>
    /////   <item><description><see cref="GetNodeContext(TableNode, ITableDependencyEdge?, int)"/> to create a per-node context that is passed to child visits.</description></item>
    /////   <item><description><see cref="ProcessCurrentNode(TableNode, ITableDependencyEdge?, int, TNodeContext?)"/> to perform work for each visited node.</description></item>
    ///// </list>
    ///// <para>
    ///// The traversal interprets outgoing edges from a table as that table's dependencies (children).
    ///// The initial "root" invocation uses a synthetic edge where the source and edge data are <see langword="null"/> and depth is 0.
    ///// </para>
    ///// <para>Thread-safety: instances are not thread-safe.</para>
    ///// </remarks>
    ///// <typeparam name="TNodeContext">An implementation-defined context type computed per node and propagated to its children.</typeparam>
    ///// <param name="dbGraph">The database dependency graph to traverse. Must be populated prior to traversal.</param>
    //public abstract class AsyncDepthFirstVisitor<TResult,TNodeContext>(DatabaseGraph dbGraph)
    //{
    //    /// <summary>
    //    /// Traverses the graph in post-order depth-first fashion starting from the specified root tables.
    //    /// </summary>
    //    /// <param name="rootTables">Root tables identified by <c>(schema, tableName)</c> tuples.</param>
    //    /// <exception cref="InvalidOperationException">
    //    /// Thrown when any specified root table does not exist in <paramref name="dbGraph"/>.
    //    /// </exception>
    //    /// <remarks>
    //    /// Post-order means children are visited before the current node is processed. Skipping logic is applied via
    //    /// <see cref="GetSkipCurrentNode(TableNode, ITableDependencyEdge?, int)"/>; when it returns <see langword="true"/>,
    //    /// the current node is neither processed nor descended into.
    //    /// After child traversal completes, <see cref="ProcessCurrentNode(TableNode, ITableDependencyEdge?, int, TNodeContext?)"/> is invoked.
    //    /// </remarks>
    //    public async IAsyncEnumerable<TResult> VisitTablePostOrder((string schema, string tableName)[] rootTables)
    //    {
    //        foreach (var table in rootTables)
    //        {
    //            var currentTable = dbGraph.FindTable(table.schema, table.tableName);
    //            if (currentTable == null)
    //            {
    //                throw new InvalidOperationException($"Table {table.schema}.{table.tableName} not found in dependency graph");
    //            }

    //            var rootEdge = new GraphEdge<TableNode, ITableDependencyEdge>(source: null, target: currentTable, data: null);
    //            foreach(var a in await VisitTablePostOrder(rootEdge, depth: 0, default))
    //                yield return a;
    //        }
    //    }

    //    /// <summary>
    //    /// Traverses the graph in pre-order depth-first fashion starting from the specified root tables.
    //    /// </summary>
    //    /// <param name="rootTables">Root tables identified by <c>(schema, tableName)</c> tuples.</param>
    //    /// <exception cref="InvalidOperationException">
    //    /// Thrown when any specified root table does not exist in <paramref name="dbGraph"/>.
    //    /// </exception>
    //    /// <remarks>
    //    /// Pre-order means the current node is processed before its children are visited. Skipping logic is applied via
    //    /// <see cref="GetSkipCurrentNode(TableNode, ITableDependencyEdge?, int)"/>; when it returns <see langword="true"/>,
    //    /// the current node is neither processed nor descended into.
    //    /// <see cref="ProcessCurrentNode(TableNode, ITableDependencyEdge?, int, TNodeContext?)"/> is invoked first; then the traversal descends into children.
    //    /// </remarks>
    //    public void VisitTablePreOrder((string schema, string tableName)[] rootTables)
    //    {
    //        foreach (var table in rootTables)
    //        {
    //            var currentTable = dbGraph.FindTable(table.schema, table.tableName);
    //            if (currentTable == null)
    //            {
    //                throw new InvalidOperationException($"Table {table.schema}.{table.tableName} not found in dependency graph");
    //            }

    //            var rootEdge = new GraphEdge<TableNode, ITableDependencyEdge>(source: null, target: currentTable, data: null);
    //            VisitTablPreOrder(rootEdge, depth: 0, default);
    //        }
    //    }

    //    /// <summary>
    //    /// Recursive worker that performs a post-order DFS from the supplied edge target.
    //    /// </summary>
    //    /// <param name="dependencyEdge">The incoming edge to the <paramref name="dependencyEdge"/>.Target node.</param>
    //    /// <param name="depth">Current traversal depth. Root depth is 0.</param>
    //    /// <param name="parentNodeContext">The context computed for the parent node, if any; passed to <see cref="ProcessCurrentNode(TableNode, ITableDependencyEdge?, int, TNodeContext?)"/>.</param>
    //    /// <remarks>
    //    /// Invokes <see cref="GetSkipCurrentNode(TableNode, ITableDependencyEdge?, int)"/> before descending; if skipped, the subtree is not visited.
    //    /// Calls <see cref="GetNodeContext(TableNode, ITableDependencyEdge?, int)"/> to compute the current node's context that will be supplied to children.
    //    /// After visiting all children, calls <see cref="ProcessCurrentNode(TableNode, ITableDependencyEdge?, int, TNodeContext?)"/>.
    //    /// </remarks>
    //    private IAsyncEnumerable<TResult> VisitTablePostOrder(GraphEdge<TableNode, ITableDependencyEdge> dependencyEdge, int depth, TNodeContext? parentNodeContext)
    //    {
    //        var currentTable = dependencyEdge.Target;
    //        var dependencies = dbGraph.GetOutgoingEdges(currentTable);

    //        TNodeContext currentNodeContext = GetNodeContext(currentTable, dependencyEdge.Data, depth);

    //        if (GetSkipCurrentNode(currentTable, dependencyEdge.Data, depth))
    //        {
    //            yield break;
    //        }

    //        if (dependencies != null)
    //        {
    //            foreach (var dependency in dependencies)
    //            {
    //                var dependencyDepth = depth + 1;
    //                VisitTablePostOrder(dependency, dependencyDepth, currentNodeContext);
    //            }
    //        }

    //        ProcessCurrentNode(currentTable, dependencyEdge.Data, depth, parentNodeContext);
    //    }

    //    /// <summary>
    //    /// Computes and returns the context associated with the current node at the given depth.
    //    /// </summary>
    //    /// <param name="currentTable">The table node for which the context is being created.</param>
    //    /// <param name="data">The incoming edge metadata (if any) that led to <paramref name="currentTable"/>.</param>
    //    /// <param name="depth">The current traversal depth; root is 0.</param>
    //    /// <returns>
    //    /// A context instance to be propagated to child node visits. May be <see langword="null"/> if no context is required.
    //    /// </returns>
    //    /// <remarks>
    //    /// This context is computed once per node visit and is passed to child invocations as <c>parentNodeContext</c>
    //    /// and to <see cref="ProcessCurrentNode(TableNode, ITableDependencyEdge?, int, TNodeContext?)"/> for the corresponding node,
    //    /// depending on traversal order.
    //    /// </remarks>
    //    protected abstract TNodeContext GetNodeContext(TableNode currentTable, ITableDependencyEdge? data, int depth);

    //    /// <summary>
    //    /// Recursive worker that performs a pre-order DFS from the supplied edge target.
    //    /// </summary>
    //    /// <param name="dependencyEdge">The incoming edge to the <paramref name="dependencyEdge"/>.Target node.</param>
    //    /// <param name="depth">Current traversal depth. Root depth is 0.</param>
    //    /// <param name="parentNodeContext">The context computed for the parent node, if any; passed to <see cref="ProcessCurrentNode(TableNode, ITableDependencyEdge?, int, TNodeContext?)"/>.</param>
    //    /// <remarks>
    //    /// Invokes <see cref="GetSkipCurrentNode(TableNode, ITableDependencyEdge?, int)"/> before processing; if skipped, the subtree is not visited.
    //    /// Processes the current node first via <see cref="ProcessCurrentNode(TableNode, ITableDependencyEdge?, int, TNodeContext?)"/>, then descends into children.
    //    /// Calls <see cref="GetNodeContext(TableNode, ITableDependencyEdge?, int)"/> to compute the context to supply to child visits.
    //    /// </remarks>
    //    private void VisitTablPreOrder(GraphEdge<TableNode, ITableDependencyEdge> dependencyEdge, int depth, TNodeContext? parentNodeContext)
    //    {
    //        var currentTable = dependencyEdge.Target;
    //        var dependencies = dbGraph.GetOutgoingEdges(currentTable);

    //        if (GetSkipCurrentNode(currentTable, dependencyEdge.Data, depth))
    //        {
    //            return;
    //        }

    //        ProcessCurrentNode(currentTable, dependencyEdge.Data, depth, parentNodeContext);

    //        TNodeContext currentNodeContext = GetNodeContext(currentTable, dependencyEdge.Data, depth);
    //        if (dependencies != null)
    //        {
    //            foreach (var dependency in dependencies)
    //            {
    //                var dependencyDepth = depth + 1;
    //                VisitTablPreOrder(dependency, dependencyDepth, currentNodeContext);
    //            }
    //        }
    //    }

    //    /// <summary>
    //    /// Determines whether the current node should be skipped (neither processed nor descended into) at the specified depth.
    //    /// </summary>
    //    /// <param name="currentTable">The table node currently being visited.</param>
    //    /// <param name="data">The incoming edge metadata (if any) that led to <paramref name="currentTable"/>.</param>
    //    /// <param name="depth">The current traversal depth; root is 0.</param>
    //    /// <returns>
    //    /// <see langword="true"/> to skip processing and traversal of <paramref name="currentTable"/> and its children; otherwise, <see langword="false"/>.
    //    /// </returns>
    //    protected abstract bool GetSkipCurrentNode(TableNode currentTable, ITableDependencyEdge? data, int depth);

    //    /// <summary>
    //    /// Processes the current node at the specified depth.
    //    /// </summary>
    //    /// <param name="currentTable">The table node currently being visited.</param>
    //    /// <param name="data">The incoming edge metadata (if any) that led to <paramref name="currentTable"/>.</param>
    //    /// <param name="depth">The current traversal depth; root is 0.</param>
    //    /// <param name="currentNodeContext">
    //    /// The context associated with the current node, as produced by <see cref="GetNodeContext(TableNode, ITableDependencyEdge?, int)"/>.
    //    /// For pre-order traversal this is the parent's context; for post-order traversal this is also the parent's context, while the current node's
    //    /// own context is forwarded to its children.
    //    /// </param>
    //    /// <remarks>
    //    /// Implementations may use <paramref name="depth"/> to control formatting, batching, or other behavior sensitive to hierarchy,
    //    /// and <paramref name="currentNodeContext"/> to carry per-node state between steps.
    //    /// </remarks>
    //    protected abstract void ProcessCurrentNode(TableNode currentTable, ITableDependencyEdge? data, int depth, TNodeContext? currentNodeContext);
    //}
}
