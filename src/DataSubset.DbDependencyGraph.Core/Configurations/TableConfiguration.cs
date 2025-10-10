using System.ComponentModel.DataAnnotations;
using System.Text.Json.Serialization;

namespace DataSubset.DbDependencyGraph.Core.Configurations
{
    /// <summary>
    /// Represents model configuration for a specific table, including implicit relations
    /// that are not defined as foreign keys in the database schema.
    /// </summary>
    public class TableConfiguration
    {
        /// <summary>
        /// The database schema name where the table is located.
        /// </summary>
        public string Schema { get; set; } = "public";

        /// <summary>
        /// The name of the table this configuration applies to.
        /// </summary>
        public string TableName { get; set; } = string.Empty;

        /// <summary>
        /// List of implicit relations that define custom relationships
        /// not represented by foreign keys in the database.
        /// </summary>
        public List<ImplicitRelation>? ImplicitRelations { get; set; } = new();

        /// <summary>
        /// Gets the full qualified name of the table (schema.tablename).
        /// </summary>
        [JsonIgnore]
        public string FullName => $"{Schema}.{TableName}";

        /// <summary>
        /// Indicates whether this configuration has any implicit relations defined.
        /// </summary>
        public bool HasImplicitRelations => ImplicitRelations?.Count > 0;

        /// <summary>
        /// Gets the count of implicit relations defined for this table.
        /// </summary>
        public int ImplicitRelationCount => ImplicitRelations?.Count ?? 0;

        public TableConfiguration()
        {
        }

        public TableConfiguration(string schema, string tableName)
        {
            Schema = schema ?? throw new ArgumentNullException(nameof(schema));
            TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
        }

        /// <summary>
        /// Adds an implicit relation to this model configuration.
        /// </summary>
        /// <param name="relation">The implicit relation to add.</param>
        public void AddImplicitRelation(ImplicitRelation relation)
        {
            if (relation == null)
                throw new ArgumentNullException(nameof(relation));

            ImplicitRelations ??= new List<ImplicitRelation>();

            // Check if relation already exists
            if (!ContainsRelation(relation))
            {
                ImplicitRelations.Add(relation);
            }
        }

        /// <summary>
        /// Removes an implicit relation from this model configuration.
        /// </summary>
        /// <param name="relation">The implicit relation to remove.</param>
        /// <returns>True if the relation was found and removed; otherwise, false.</returns>
        public bool RemoveImplicitRelation(ImplicitRelation relation)
        {
            if (relation == null || ImplicitRelations == null)
                return false;

            return ImplicitRelations.Remove(relation);
        }

        /// <summary>
        /// Checks if the specified implicit relation already exists in this configuration.
        /// </summary>
        /// <param name="relation">The implicit relation to check.</param>
        /// <returns>True if the relation exists; otherwise, false.</returns>
        public bool ContainsRelation(ImplicitRelation relation)
        {
            if (relation == null || ImplicitRelations == null)
                return false;

            return ImplicitRelations.Exists(r =>
                string.Equals(r.TargetSchema, relation.TargetSchema, StringComparison.OrdinalIgnoreCase) &&
                string.Equals(r.TargetTable, relation.TargetTable, StringComparison.OrdinalIgnoreCase) &&
                string.Equals(r.WhereClause, relation.WhereClause, StringComparison.OrdinalIgnoreCase) &&
                AreColumnBindingsEqual(r.ColumnBindings, relation.ColumnBindings));
        }

        /// <summary>
        /// Compares two collections of column bindings for equality.
        /// </summary>
        /// <param name="bindings1">The first collection of column bindings.</param>
        /// <param name="bindings2">The second collection of column bindings.</param>
        /// <returns>True if both collections contain the same column bindings; otherwise, false.</returns>
        private bool AreColumnBindingsEqual(IEnumerable<ColumnBinding> bindings1, IEnumerable<ColumnBinding> bindings2)
        {
            if (bindings1 == null && bindings2 == null)
                return true;
            
            if (bindings1 == null || bindings2 == null)
                return false;

            var list1 = bindings1.ToList();
            var list2 = bindings2.ToList();

            if (list1.Count != list2.Count)
                return false;

            // Check if all bindings in list1 exist in list2
            return list1.All(b1 => list2.Any(b2 =>
                string.Equals(b1.SourceColumn, b2.SourceColumn, StringComparison.OrdinalIgnoreCase) &&
                string.Equals(b1.TargetColumn, b2.TargetColumn, StringComparison.OrdinalIgnoreCase)));
        }

        /// <summary>
        /// Gets all implicit relations that use a specific source column.
        /// </summary>
        /// <param name="sourceColumn">The source column name.</param>
        /// <returns>A list of implicit relations using the specified source column.</returns>
        public List<ImplicitRelation> GetRelationsBySourceColumn(string sourceColumn)
        {
            if (ImplicitRelations == null)
                return new List<ImplicitRelation>();

            return ImplicitRelations.FindAll(ir =>
                ir.ColumnBindings.Any(cb => string.Equals(cb.SourceColumn, sourceColumn, StringComparison.OrdinalIgnoreCase)));
        }

        /// <summary>
        /// Validates this model configuration.
        /// </summary>
        /// <returns>A list of validation error messages. Empty if configuration is valid.</returns>
        public List<string> Validate()
        {
            var errors = new List<string>();

            if (string.IsNullOrWhiteSpace(Schema))
                errors.Add("Schema name is required.");

            if (string.IsNullOrWhiteSpace(TableName))
                errors.Add("Table name is required.");

            if (ImplicitRelations != null)
            {
                for (int i = 0; i < ImplicitRelations.Count; i++)
                {
                    var relation = ImplicitRelations[i];
                    var relationErrors = relation.Validate();
                    foreach (var error in relationErrors)
                    {
                        errors.Add($"Implicit relation {i + 1}: {error}");
                    }
                }
            }

            return errors;
        }

        
        public override bool Equals(object? obj)
        {
            if (obj is not TableConfiguration other)
                return false;

            return string.Equals(FullName, other.FullName, StringComparison.OrdinalIgnoreCase);
        }

        public override int GetHashCode()
        {
            return StringComparer.OrdinalIgnoreCase.GetHashCode(FullName);
        }

        public override string ToString()
        {
            var relationInfo = HasImplicitRelations ? $" ({ImplicitRelationCount} relations)" : " (no relations)";
            return $"{FullName}{relationInfo}";
        }
    }

    /// <summary>
    /// Represents an implicit relation between tables that is not defined
    /// as a foreign key constraint in the database schema.
    /// </summary>
    public class ImplicitRelation
    {
                /// <summary>
        /// The schema name of the target table.
        /// </summary>
        [Required]
        [JsonPropertyName("targetSchema")]
        public string TargetSchema { get; set; } = string.Empty;

        /// <summary>
        /// The name of the target table.
        /// </summary>
        [Required]
        [JsonPropertyName("targetTable")]
        public string TargetTable { get; set; } = string.Empty;

        public IEnumerable<ColumnBinding> ColumnBindings { get; set; } = Array.Empty<ColumnBinding>();

        /// <summary>
        /// Optional WHERE clause to apply additional filtering when following this relation.
        /// </summary>
        [JsonPropertyName("whereClause")]
        public string? WhereClause { get; set; }

        /// <summary>
        /// Gets the full qualified name of the target table (schema.tablename).
        /// </summary>
        [JsonIgnore]
        public string TargetFullName => $"{TargetSchema}.{TargetTable}";

        /// <summary>
        /// Indicates whether this relation has a WHERE clause defined.
        /// </summary>
        [JsonIgnore]
        public bool HasWhereClause => !string.IsNullOrWhiteSpace(WhereClause);

        public ImplicitRelation()
        {
        }


        /// <summary>
        /// Validates this implicit relation.
        /// </summary>
        /// <returns>A list of validation error messages. Empty if relation is valid.</returns>
        public List<string> Validate()
        {
            var errors = new List<string>();

            if (!ColumnBindings.Any())
                errors.Add("At least oune ColumnBindings must be configured");

            if (string.IsNullOrWhiteSpace(TargetSchema))
                errors.Add("Target schema is required.");

            if (string.IsNullOrWhiteSpace(TargetTable))
                errors.Add("Target table is required.");

      
            return errors;
        }



       
    }

    public class ColumnBinding
    {
        [Required]
        public required string SourceColumn { get; set; }

        [Required]
        public required string TargetColumn { get; set; }
    }
}