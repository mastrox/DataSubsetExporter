using DataSubset.DbDependencyGraph.Core.DependencyGraph;
using Xunit;

namespace DataSubset.DbDependencyGraph.Core.Tests
{
    public class GraphEdgeTests
    {
        [Fact]
        public void ToString_PrintsSourceArrowTarget()
        {
            var e = new GraphEdge<string, string>("S", "T", "meta");
            Assert.Equal("S -> T", e.ToString());
        }
    }
}