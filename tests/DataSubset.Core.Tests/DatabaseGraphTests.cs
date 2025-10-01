using System.Linq;
using DataSubsetCore.DependencyGraph;
using Xunit;

namespace DataSubset.Core.Tests
{
    public class DatabaseGraphTests
    {
        [Fact]
        public void GetOrCreateNode_AddsNode_ToGraphAndIndex()
        {
            var g = new DatabaseGraph();

            var node = g.GetOrCreateNode("dbo", "Users");

            Assert.NotNull(node);
            Assert.True(g.HasNode(node));
            Assert.Same(node, g.FindTable("dbo", "Users"));
        }

        [Fact]
        public void GetOrCreateNode_IsIdempotent_ReturnsSameInstance_AndNoDuplicateNodes()
        {
            var g = new DatabaseGraph();

            var n1 = g.GetOrCreateNode("dbo", "Users");
            var n2 = g.GetOrCreateNode("dbo", "Users");

            Assert.Same(n1, n2);
            Assert.Single(g.GetNodes());
        }

        [Fact]
        public void FindTable_IsCaseInsensitive()
        {
            var g = new DatabaseGraph();

            var node = g.GetOrCreateNode("dbo", "Users");

            var found = g.FindTable("DBO", "users");
            Assert.Same(node, found);
        }

        [Fact]
        public void FindTable_ReturnsNull_WhenNodeNotInIndex()
        {
            var g = new DatabaseGraph();

            // Add to graph only, not to NodesByName index
            var tn = new TableNode("dbo", "Products");
            g.AddNode(tn);

            var found = g.FindTable("dbo", "Products");
            Assert.Null(found);
            Assert.True(g.HasNode(tn)); // still present in the underlying graph
        }

        [Fact]
        public void RemoveNode_DoesNotAutoUpdateIndex_AsDocumented()
        {
            var g = new DatabaseGraph();

            var node = g.GetOrCreateNode("dbo", "Users");

            var removed = g.RemoveNode(node);
            Assert.True(removed);
            Assert.False(g.HasNode(node));

            // Index still returns the node because it is not auto-synchronized
            var fromIndex = g.FindTable("dbo", "Users");
            Assert.Same(node, fromIndex);
        }
    }
}