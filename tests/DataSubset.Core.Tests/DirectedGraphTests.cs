using System;
using System.Linq;
using DataSubset.Core.DependencyGraph;
using Xunit;

namespace DataSubset.Core.Tests
{
    public class DirectedGraphTests
    {
        [Fact]
        public void AddEdge_CreatesNodesAndEdges()
        {
            var g = new DirectedGraph<string, string>();
            g.AddEdge("A", "B", "edgeAB");

            Assert.True(g.HasNode("A"));
            Assert.True(g.HasNode("B"));
            Assert.True(g.HasEdge("A", "B"));
            Assert.Single(g.GetOutgoingEdges("A"));
            Assert.Single(g.GetIncomingEdges("B"));
        }

        [Fact]
        public void RemoveEdge_RemovesOnlyThatEdge()
        {
            var g = new DirectedGraph<string, string>();
            g.AddEdge("A", "B");
            g.AddEdge("A", "C");

            var removed = g.RemoveEdge("A", "B");

            Assert.True(removed);
            Assert.False(g.HasEdge("A", "B"));
            Assert.True(g.HasEdge("A", "C"));
        }

        [Fact]
        public void RemoveNode_RemovesIncidentEdges()
        {
            var g = new DirectedGraph<string, string>();
            g.AddEdge("A", "B");
            g.AddEdge("B", "C");

            var removed = g.RemoveNode("B");

            Assert.True(removed);
            Assert.Empty(g.GetSuccessors("A"));
            Assert.Empty(g.GetPredecessors("C"));
            Assert.False(g.HasNode("B"));
        }

        [Fact]
        public void RootAndLeafNodes_AreComputedCorrectly()
        {
            var g = new DirectedGraph<string, string>();
            g.AddEdge("A", "B");
            g.AddEdge("B", "C");
            g.AddNode("D"); // isolated node

            var roots = g.GetRootNodes().ToList();
            var leaves = g.GetLeafNodes().ToList();

            Assert.Contains("A", roots);
            Assert.Contains("D", roots);
            Assert.Contains("C", leaves);
            Assert.Contains("D", leaves);
            Assert.DoesNotContain("B", roots);
            Assert.DoesNotContain("B", leaves);
        }

        [Fact]
        public void TopologicalSort_ReturnsValidOrderForDAG()
        {
            var g = new DirectedGraph<string, string>();
            g.AddEdge("A", "B");
            g.AddEdge("A", "C");
            g.AddEdge("B", "D");
            g.AddEdge("C", "D");

            var order = g.TopologicalSort();

            var pos = order.Select((n, i) => (n, i)).ToDictionary(x => x.n, x => x.i);
            Assert.True(pos["A"] < pos["B"]);
            Assert.True(pos["A"] < pos["C"]);
            Assert.True(pos["B"] < pos["D"]);
            Assert.True(pos["C"] < pos["D"]);
        }

        [Fact]
        public void TopologicalSort_ThrowsOnCycle()
        {
            var g = new DirectedGraph<string, string>();
            g.AddEdge("A", "B");
            g.AddEdge("B", "C");
            g.AddEdge("C", "A");

            Assert.Throws<InvalidOperationException>(() => g.TopologicalSort());
        }

        [Fact]
        public void HasCycles_SelfLoopHonorsFlag()
        {
            var g = new DirectedGraph<string, string>();
            g.AddEdge("A", "A");

            Assert.False(g.HasCycles(considerSelfCycles: false));
            Assert.True(g.HasCycles(considerSelfCycles: true));
        }

        [Fact]
        public void StronglyConnectedComponents_AreDetected()
        {
            var g = new DirectedGraph<string, string>();
            // Component 1: A <-> B
            g.AddEdge("A", "B");
            g.AddEdge("B", "A");
            // Component 2: C
            g.AddNode("C");
            // Component 3: D <-> E
            g.AddEdge("D", "E");
            g.AddEdge("E", "D");

            var sccs = g.GetStronglyConnectedComponents();

            Assert.Equal(3, sccs.Count);
            Assert.Contains(sccs, c => c.Count == 2 && c.Contains("A") && c.Contains("B"));
            Assert.Contains(sccs, c => c.Count == 1 && c.Contains("C"));
            Assert.Contains(sccs, c => c.Count == 2 && c.Contains("D") && c.Contains("E"));
        }

        [Fact]
        public void GetStatistics_ReturnsExpectedValues()
        {
            var g = new DirectedGraph<string, string>();
            g.AddEdge("A", "B");
            g.AddEdge("B", "C");
            g.AddNode("D"); // isolated

            var stats = g.GetStatistics();

            Assert.Equal(4, stats.NodeCount);
            Assert.Equal(2, stats.EdgeCount);
            Assert.Equal(2, stats.RootNodeCount); // A, D
            Assert.Equal(2, stats.LeafNodeCount); // C, D
            Assert.False(stats.HasCycles);
        }
    }
}