using System.Linq;
using DataSubsetCore.Configurations;
using DataSubsetCore.DependencyGraph;
using Xunit;

namespace DataSubset.Core.Tests
{
    public class FkTableDependencyEdgeTests
    {
        [Fact]
        public void Properties_AndToString_AreSetCorrectly()
        {
            var bindings = new[]
            {
                new ColumnBinding { SourceColumn = "ChildId", TargetColumn = "ParentId" },
                new ColumnBinding { SourceColumn = "ChildType", TargetColumn = "ParentType" }
            };

            var edge = new FkTableDependencyEdge(
                sourceSchema: "public",
                sourceTable: "OrderLine",
                columnBindings: bindings,
                constraintName: "FK_OrderLine_Order");

            Assert.Equal("FK_OrderLine_Order", edge.ConstraintName);
            Assert.Equal("public", edge.SourceSchema);
            Assert.Equal("OrderLine", edge.SourceTable);
            Assert.Equal(2, edge.ColumnBindings.Count());

            var s = edge.ToString();
            Assert.Contains("ChildId -> ParentId", s);
            Assert.Contains("ChildType -> ParentType", s);
            Assert.Contains("public.OrderLine", s);
        }
    }
}