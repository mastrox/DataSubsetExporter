using DataSubset.Core.DependencyGraph;
using Xunit;

namespace DataSubset.Core.Tests
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