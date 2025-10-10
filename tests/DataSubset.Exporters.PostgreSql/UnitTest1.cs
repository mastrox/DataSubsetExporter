using DataSubset.Exporters.Common;

namespace DataSubset.Exporters.PostgreSql
{
    public class UnitTest1
    {
        [Fact]
        public void Test1()
        {

            InserStatementExporter exp= new InserStatementExporter(null!);
        }
    }
}