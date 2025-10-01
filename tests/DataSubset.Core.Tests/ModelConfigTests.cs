using System.Linq;
using DataSubsetCore.Configurations;
using Xunit;

namespace DataSubset.Core.Tests
{
    public class ModelConfigTests
    {
        [Fact]
        public void Validate_ReportsMissingTableName()
        {
            var mc = new ModelConfig(); // Schema defaults to "public", TableName is empty
            var errors = mc.Validate();
            Assert.Contains(errors, e => e.Contains("Table name is required"));
        }

        [Fact]
        public void AddImplicitRelation_AvoidsDuplicates_AndContainsRelationWorks()
        {
            var mc = new ModelConfig("s", "t");

            var rel = new ImplicitRelation
            {
                TargetSchema = "s2",
                TargetTable = "t2",
                ColumnBindings = new[]
                {
                    new ColumnBinding { SourceColumn = "A", TargetColumn = "B" }
                },
                WhereClause = "A IS NOT NULL"
            };

            mc.AddImplicitRelation(rel);
            mc.AddImplicitRelation(rel); // duplicate

            Assert.True(mc.ContainsRelation(rel));
            Assert.Equal(1, mc.ImplicitRelationCount);
        }

        [Fact]
        public void GetRelationsBySourceColumn_FiltersBySource()
        {
            var mc = new ModelConfig("s", "t");
            mc.AddImplicitRelation(new ImplicitRelation
            {
                TargetSchema = "s2",
                TargetTable = "t2",
                ColumnBindings = new[]
                {
                    new ColumnBinding { SourceColumn = "A", TargetColumn = "B" }
                }
            });
            mc.AddImplicitRelation(new ImplicitRelation
            {
                TargetSchema = "s3",
                TargetTable = "t3",
                ColumnBindings = new[]
                {
                    new ColumnBinding { SourceColumn = "X", TargetColumn = "Y" }
                }
            });

            var byA = mc.GetRelationsBySourceColumn("A");
            Assert.Single(byA);
            Assert.Equal("s2.t2", byA[0].TargetFullName);
        }

        [Fact]
        public void Equals_AndHashCode_AreCaseInsensitiveOnFullName()
        {
            var a = new ModelConfig("dbo", "Users");
            var b = new ModelConfig("DBO", "users");

            Assert.True(a.Equals(b));
            Assert.Equal(a.GetHashCode(), b.GetHashCode());
            Assert.Equal("dbo.Users", a.FullName);
        }

        [Fact]
        public void ImplicitRelation_Validate_ReportsMissingData()
        {
            var ir = new ImplicitRelation
            {
                TargetSchema = "",
                TargetTable = "",
                ColumnBindings = Enumerable.Empty<ColumnBinding>()
            };

            var errors = ir.Validate();
            Assert.Contains(errors, e => e.Contains("ColumnBindings"));
            Assert.Contains(errors, e => e.Contains("Target schema is required"));
            Assert.Contains(errors, e => e.Contains("Target table is required"));
        }
    }
}