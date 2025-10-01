using System.ComponentModel.DataAnnotations;
using System.Text.Json.Serialization;

namespace DataSubsetCore.Configurations
{
    /// <summary>
    /// Represents the root configuration object for export operations,
    /// containing tables to export, model configurations, and table ignore settings.
    /// </summary>
    public class ExportConfig
    {
        /// <summary>
        /// List of tables to be exported with their specific configurations.
        /// </summary>
        [JsonPropertyName("tablesToExport")]
        public List<TableExportConfig> TablesToExport { get; set; } = new();

        /// <summary>
        /// List of model configurations that define implicit relations and table behaviors.
        /// </summary>
        [JsonPropertyName("modelConfig")]
        public List<ModelConfig> ModelConfig { get; set; } = new();

        /// <summary>
        /// Configuration for tables that should be ignored during processing.
        /// </summary>
        [JsonPropertyName("tableIgnoreConfig")]
        public List<TableToIgnore>? TableToIgnore { get; set; }

        /// <summary>
        /// Gets the total number of tables configured for export.
        /// </summary>
        [JsonIgnore]
        public int TotalTablesToExport => TablesToExport?.Count ?? 0;

        /// <summary>
        /// Gets the total number of model configurations defined.
        /// </summary>
        [JsonIgnore]
        public int TotalModelConfigurations => ModelConfig?.Count ?? 0;

        /// <summary>
        /// Indicates whether any tables are configured to be ignored.
        /// </summary>
        [JsonIgnore]
        public bool HasIgnoreConfig => (TableToIgnore?.Count ?? 0) > 0;

        public ExportConfig()
        {
        }

        /// <summary>
        /// Gets export configuration for a specific table.
        /// </summary>
        /// <param name="schema">The schema name</param>
        /// <param name="tableName">The table name</param>
        /// <returns>The table export configuration if found; otherwise, null</returns>
        public TableExportConfig? GetTableExportConfig(string schema, string tableName)
        {
            return TablesToExport?.FirstOrDefault(t =>
                t.Schema.Equals(schema, StringComparison.OrdinalIgnoreCase) &&
                t.TableName.Equals(tableName, StringComparison.OrdinalIgnoreCase));
        }

        /// <summary>
        /// Gets model configuration for a specific table.
        /// </summary>
        /// <param name="schema">The schema name</param>
        /// <param name="tableName">The table name</param>
        /// <returns>The model configuration if found; otherwise, null</returns>
        public ModelConfig? GetModelConfig(string schema, string tableName)
        {
            return ModelConfig?.FirstOrDefault(m =>
                m.Schema.Equals(schema, StringComparison.OrdinalIgnoreCase) &&
                m.TableName.Equals(tableName, StringComparison.OrdinalIgnoreCase));
        }



        /// <summary>
        /// Validates the export configuration.
        /// </summary>
        /// <returns>A list of validation error messages</returns>
        public List<string> Validate()
        {
            var errors = new List<string>();

            // Validate tables to export
            if (TablesToExport != null)
            {
                foreach (var tableConfig in TablesToExport)
                {
                    var tableErrors = tableConfig.Validate();
                    foreach (var error in tableErrors)
                    {
                        errors.Add($"Table Export Config: {error}");
                    }
                }
            }

            // Validate model configurations
            if (ModelConfig != null)
            {
                foreach (var modelConfig in ModelConfig)
                {
                    var modelErrors = modelConfig.Validate();
                    foreach (var error in modelErrors)
                    {
                        errors.Add($"Model Config ({modelConfig.FullName}): {error}");
                    }
                }
            }



            return errors;
        }
    }

    /// <summary>
    /// Represents configuration for a specific table to be exported.
    /// </summary>
    public class TableExportConfig
    {
        /// <summary>
        /// The database schema name.
        /// </summary>
        [Required]
        [JsonPropertyName("schema")]
        public string Schema { get; set; } = string.Empty;

        /// <summary>
        /// The table name.
        /// </summary>
        [Required]
        [JsonPropertyName("tableName")]
        public string TableName { get; set; } = string.Empty;

        /// <summary>
        /// Optional WHERE clause to filter which rows to export.
        /// </summary>
        [JsonPropertyName("whereClause")]
        public string? WhereClause { get; set; }

        /// <summary>
        /// Optional PrimaryKey clause to filter which rows to export.
        /// </summary>
        [JsonPropertyName("primaryKeyValue")]
        public PrimaryKeyValue[]? PrimaryKeyValue { get; set; }

        /// <summary>
        /// Gets the full qualified name of the table (schema.tablename).
        /// </summary>
        [JsonIgnore]
        public string FullName => $"{Schema}.{TableName}";

        /// <summary>
        /// Indicates whether this export configuration has a WHERE clause.
        /// </summary>
        [JsonIgnore]
        public bool HasWhereClause => !string.IsNullOrWhiteSpace(WhereClause);

        public TableExportConfig()
        {
        }

        public TableExportConfig(string schema, string tableName, string? whereClause = null)
        {
            Schema = schema ?? throw new ArgumentNullException(nameof(schema));
            TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
            WhereClause = whereClause;
        }

        /// <summary>
        /// Validates this table export configuration.
        /// </summary>
        /// <returns>A list of validation error messages</returns>
        public List<string> Validate()
        {
            var errors = new List<string>();

            if (string.IsNullOrWhiteSpace(Schema))
            {
                errors.Add("Schema name is required");
            }

            if (string.IsNullOrWhiteSpace(TableName))
            {
                errors.Add("Table name is required");
            }

            return errors;
        }

        public override bool Equals(object? obj)
        {
            if (obj is not TableExportConfig other) return false;
            return Schema.Equals(other.Schema, StringComparison.OrdinalIgnoreCase) &&
                   TableName.Equals(other.TableName, StringComparison.OrdinalIgnoreCase);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(
                Schema.ToLowerInvariant(),
                TableName.ToLowerInvariant());
        }

        public override string ToString()
        {
            var whereInfo = HasWhereClause ? $" WHERE {WhereClause}" : "";
            return $"{FullName}{whereInfo}";
        }
    }
}