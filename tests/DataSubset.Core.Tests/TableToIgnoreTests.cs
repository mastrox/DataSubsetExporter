using DataSubsetCore.Configurations;
using Xunit;

namespace DataSubset.Core.Tests
{
    public class TableToIgnoreTests
    {
        [Fact]
        public void Matches_IsCaseInsensitive()
        {
            var t = new TableToIgnore("dbo", "Users");
            Assert.True(t.Matches("DBO", "users"));
            Assert.False(t.Matches("dbo", "Orders"));
        }

        [Fact]
        public void Validate_ReturnsErrorsForEmptyFields()
        {
            var t = new TableToIgnore();
            var errors = t.Validate();
            Assert.Contains(errors, e => e.Contains("Schema name is required"));
            Assert.Contains(errors, e => e.Contains("Table name is required"));
        }

        [Fact]
        public void Equality_AndHashCode_AreCaseInsensitive()
        {
            var a = new TableToIgnore("dbo", "Users");
            var b = new TableToIgnore("DBO", "users");

            Assert.True(a.Equals(b));
            Assert.Equal(a.GetHashCode(), b.GetHashCode());
            Assert.Equal("dbo.Users", a.ToString());
        }
    }
}