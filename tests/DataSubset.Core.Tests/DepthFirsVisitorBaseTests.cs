using System;
using System.Collections.Generic;
using System.Linq;
using DataSubsetCore.DependencyGraph;
using DataSubset.Core.DependencyGraph;
using Xunit;

namespace DataSubset.Core.Tests
{
    public class DepthFirsVisitorBaseTests
    {
        private static (DatabaseGraph graph, TableNode A, TableNode B, TableNode C, TableNode D, TableNode E) BuildGraph()
        {
            var g = new DatabaseGraph();

            var A = g.GetOrCreateNode("s", "A");
            var B = g.GetOrCreateNode("s", "B");
            var C = g.GetOrCreateNode("s", "C");
            var D = g.GetOrCreateNode("s", "D");
            var E = g.GetOrCreateNode("s", "E");

            // Outgoing edges represent dependencies (children)
            // A -> B, A -> C, B -> D, C -> E
            g.AddEdge(A, B);
            g.AddEdge(A, C);
            g.AddEdge(B, D);
            g.AddEdge(C, E);

            return (g, A, B, C, D, E);
        }

        [Fact]
        public void PreOrder_ProcessesParentBeforeChildren()
        {
            var (g, A, B, C, D, E) = BuildGraph();
            var recVisitor = new RecordingVisitor(g);

            recVisitor.VisitTablePreOrder(new[] { ("s", "A") });

            // Pre-order constraints:
            // A must be first
            // A before B and C
            // B before D, C before E
            var order = recVisitor.Processed;
            var pos = order.Select((n, i) => (n, i)).ToDictionary(x => x.n, x => x.i, StringComparer.OrdinalIgnoreCase);

            Assert.Equal("s.A", order.First());
            Assert.True(pos["s.A"] < pos["s.B"]);
            Assert.True(pos["s.A"] < pos["s.C"]);
            Assert.True(pos["s.B"] < pos["s.D"]);
            Assert.True(pos["s.C"] < pos["s.E"]);
        }

        [Fact]
        public void PostOrder_ProcessesChildrenBeforeNode()
        {
            var (g, A, B, C, D, E) = BuildGraph();
            var recVisitor = new RecordingVisitor(g);

            recVisitor.VisitTablePostOrder(new[] { ("s", "A") });

            // Post-order constraints:
            // D before B, E before C, and B&C before A
            var order = recVisitor.Processed;
            var pos = order.Select((n, i) => (n, i)).ToDictionary(x => x.n, x => x.i, StringComparer.OrdinalIgnoreCase);

            Assert.True(pos["s.D"] < pos["s.B"]);
            Assert.True(pos["s.E"] < pos["s.C"]);
            Assert.True(pos["s.B"] < pos["s.A"]);
            Assert.True(pos["s.C"] < pos["s.A"]);
        }

        [Fact]
        public void SkipNode_SkipsSubtree_InPreOrder()
        {
            var (g, A, B, C, D, E) = BuildGraph();
            // Skip C -> also skips E subtree
            var v = new RecordingVisitor(g, skipFullNames: new[] { "s.C" });

            v.VisitTablePreOrder(new[] { ("s", "A") });

            Assert.DoesNotContain("s.C", v.Processed);
            Assert.DoesNotContain("s.E", v.Processed);

            // Other branch still visited
            Assert.Contains("s.B", v.Processed);
            Assert.Contains("s.D", v.Processed);
        }

        [Fact]
        public void SkipNode_SkipsSubtree_InPostOrder()
        {
            var (g, A, B, C, D, E) = BuildGraph();
            var v = new RecordingVisitor(g, skipFullNames: new[] { "s.C" });

            v.VisitTablePostOrder(new[] { ("s", "A") });

            Assert.DoesNotContain("s.C", v.Processed);
            Assert.DoesNotContain("s.E", v.Processed);

            Assert.Contains("s.B", v.Processed);
            Assert.Contains("s.D", v.Processed);
        }

        [Fact]
        public void ContextPropagation_PreOrder_PassesParentsContext()
        {
            var (g, A, B, C, D, E) = BuildGraph();
            var v = new RecordingVisitor(g);

            v.VisitTablePreOrder(new[] { ("s", "A") });

            // Root receives null parent context
            Assert.True(v.ReceivedContext.TryGetValue("s.A", out var aCtx));
            Assert.Null(aCtx);

            // B receives context computed for A at depth 0
            Assert.True(v.ReceivedContext.TryGetValue("s.B", out var bCtx));
            Assert.Equal(v.ExpectedContext("s.A", 0), bCtx);

            // D receives context computed for B at depth 1
            Assert.True(v.ReceivedContext.TryGetValue("s.D", out var dCtx));
            Assert.Equal(v.ExpectedContext("s.B", 1), dCtx);

            // C -> E path
            Assert.True(v.ReceivedContext.TryGetValue("s.C", out var cCtx));
            Assert.Equal(v.ExpectedContext("s.A", 0), cCtx);

            Assert.True(v.ReceivedContext.TryGetValue("s.E", out var eCtx));
            Assert.Equal(v.ExpectedContext("s.C", 1), eCtx);
        }

        [Fact]
        public void Visit_Throws_WhenRootTableNotFound()
        {
            var g = new DatabaseGraph();
            var v = new RecordingVisitor(g);

            Assert.Throws<InvalidOperationException>(() => v.VisitTablePreOrder(new[] { ("s", "Missing") }));
            Assert.Throws<InvalidOperationException>(() => v.VisitTablePostOrder(new[] { ("s", "Missing") }));
        }

        private sealed class RecordingVisitor : DepthFirsVisitorBase<string>
        {
            private readonly HashSet<string> _skip;
            public List<string> Processed { get; } = new();
            public Dictionary<string, string?> ReceivedContext { get; } = new(StringComparer.OrdinalIgnoreCase);

            public RecordingVisitor(DatabaseGraph graph, IEnumerable<string>? skipFullNames = null) : base(graph)
            {
                _skip = new HashSet<string>(skipFullNames ?? Enumerable.Empty<string>(), StringComparer.OrdinalIgnoreCase);
            }

            public string ExpectedContext(string fullName, int depth) => $"CTX:{fullName}:{depth}";

            protected override string GetNodeContext(TableNode currentTable, ITableDependencyEdge? data, int depth)
            {
                return ExpectedContext(currentTable.FullName, depth);
            }

            protected override bool GetSkipCurrentNode(TableNode currentTable, ITableDependencyEdge? data, int depth)
            {
                return _skip.Contains(currentTable.FullName);
            }

            protected override void ProcessCurrentNode(TableNode currentTable, ITableDependencyEdge? data, int depth, string? currentNodeContext)
            {
                Processed.Add(currentTable.FullName);
                ReceivedContext[currentTable.FullName] = currentNodeContext;
            }
        }
    }
}