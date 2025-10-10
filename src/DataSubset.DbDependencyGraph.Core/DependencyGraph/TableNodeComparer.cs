// Node representing a table in the dependency graph
namespace DataSubset.DbDependencyGraph.Core.DependencyGraph
{
    /// <summary>
    /// Provides a case-insensitive equality comparer for <see cref="TableNode"/> instances based on their fully qualified name.
    /// </summary>
    /// <remarks>
    /// - Equality is determined by comparing <see cref="TableNode.FullName"/> values using <see cref="StringComparer.OrdinalIgnoreCase"/>.
    /// - Two null nodes are considered equal; a single null node is not equal to a non-null node.
    /// - Hash codes are produced with the same ordinal, case-insensitive semantics to remain consistent with <see cref="Equals(TableNode, TableNode)"/>.
    /// </remarks>
    /// <seealso cref="IEqualityComparer{T}"/>
    public class TableNodeComparer : IEqualityComparer<TableNode>
    {
        /// <summary>
        /// Determines whether two <see cref="TableNode"/> instances are equal.
        /// </summary>
        /// <param name="x">The first node to compare.</param>
        /// <param name="y">The second node to compare.</param>
        /// <returns>
        /// true if both are null, or if their <see cref="TableNode.FullName"/> values are equal using an ordinal, case-insensitive comparison; otherwise, false.
        /// </returns>
        public bool Equals(TableNode? x, TableNode? y)
        {
            if (x == null && y == null) return true;
            if (x == null || y == null) return false;
            return StringComparer.OrdinalIgnoreCase.Equals(x.FullName, y.FullName);
        }

        /// <summary>
        /// Returns a hash code for the specified <see cref="TableNode"/>.
        /// </summary>
        /// <param name="obj">The node for which a hash code is to be returned.</param>
        /// <returns>
        /// A case-insensitive hash code for <paramref name="obj"/>'s <see cref="TableNode.FullName"/>, or 0 if <paramref name="obj"/> or its <see cref="TableNode.FullName"/> is null.
        /// </returns>
        /// <remarks>
        /// The hash code is computed using <see cref="StringComparer.OrdinalIgnoreCase"/> to stay consistent with <see cref="Equals(TableNode, TableNode)"/>.
        /// </remarks>
        public int GetHashCode(TableNode obj)
        {
            return obj?.FullName?.GetHashCode(StringComparison.OrdinalIgnoreCase) ?? 0;
        }
    }
}