using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataSubset.Exporters.Common
{
    public record SelectionCondition((string column, object? value)[]? parentValue, string? whereCondition)
    {
    }
}
