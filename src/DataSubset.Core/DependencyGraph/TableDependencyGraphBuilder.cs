// Node representing a table in the dependency graph
using DataSubset.Core.Configurations;
using DataSubset.Core.DependencyGraph;
using Microsoft.Extensions.Logging;
using System.Data;

namespace DependencyTreeApp
{


    /// <summary>
    /// Builds and manages a table dependency graph.
    /// </summary>
    /// <remarks>
    /// This builder:
    /// - Discovers tables and primary keys via <see cref="IDatabaseDependencyDiscoverer"/>.
    /// - Adds foreign key relationships as directed edges (source depends on target).
    /// - Applies optional implicit relations from <see cref="TableConfiguration"/> definitions.
    /// The underlying graph is a <see cref="DatabaseGraph"/> whose edges carry <see cref="ITableDependencyEdge"/> metadata.
    /// </remarks>
    public class TableDependencyGraphBuilder
    {
        /// <summary>
        /// The directed graph of tables and dependency edges.
        /// </summary>
        private readonly DatabaseGraph graph;

        /// <summary>
        /// Service responsible for discovering tables and database-defined relationships.
        /// </summary>
        private readonly IDatabaseDependencyDiscoverer dbDependencyDiscoverer;
        private readonly ILogger? logger;

        /// <summary>
        /// Initializes a new instance of the <see cref="TableDependencyGraphBuilder"/> class.
        /// </summary>
        /// <param name="dbDependencyDiscoverer">The discoverer used to populate tables and foreign key edges.</param>
        /// <param name="logger">Optional logger for diagnostic messages.</param>
        public TableDependencyGraphBuilder(IDatabaseDependencyDiscoverer dbDependencyDiscoverer, ILogger? logger = null)
        {
            this.dbDependencyDiscoverer = dbDependencyDiscoverer;
            this.logger = logger;
            graph = new DatabaseGraph(new TableNodeComparer());
        }

        /// <summary>
        /// Gets the underlying database graph instance.
        /// </summary>
        public DatabaseGraph Graph => graph;

        /// <summary>
        /// Builds the dependency graph for the specified schemas.
        /// </summary>
        /// <param name="schemas">Schemas to include (e.g., "dbo").</param>
        /// <returns>The populated <see cref="DatabaseGraph"/>.</returns>
        /// <remarks>
        /// Steps performed:
        /// 1) Discover tables and their primary keys.
        /// 2) Build foreign key relationships.
        /// 3) Apply implicit relations from model configuration.
        /// </remarks>
        public async Task<DatabaseGraph> BuildDependencyGraphAsync(string[] schemas)
        {
            return await BuildDependencyGraphAsync(schemas, null, null);
        }

        /// <summary>
        /// Builds the dependency graph for the specified schemas, applying ignore rules and implicit relations.
        /// </summary>
        /// <param name="schemas">Schemas to include (e.g., "dbo").</param>
        /// <param name="tablesConfigurations">Optional table configurations used to add implicit relation edges between existing tables.</param>
        /// <param name="tablesToIgnore">Optional list of tables to exclude from discovery, FK building, and implicit relation processing.</param>
        /// <returns>The populated <see cref="DatabaseGraph"/>.</returns>
        /// <remarks>
        /// Steps performed:
        /// 1) Discover tables and their primary keys (respecting <paramref name="tablesToIgnore"/>).
        /// 2) Build foreign key relationships (respecting <paramref name="tablesToIgnore"/>).
        /// 3) Apply implicit relations from model configuration (only between nodes already present; missing targets are skipped).
        /// </remarks>
        public async Task<DatabaseGraph> BuildDependencyGraphAsync(string[] schemas, List<TableConfiguration>? tablesConfigurations, List<TableToIgnore>? tablesToIgnore)
        {
            var ignoredTables = new HashSet<string>(tablesToIgnore?.Select(t => t.FullName) ?? new List<string>(),
                StringComparer.OrdinalIgnoreCase);

            logger?.LogDebug("Building dependency graph for schemas: {Schemas}", string.Join(", ", schemas));

            // Step 1: Discover all tables and their primary keys
            await dbDependencyDiscoverer.DiscoverTablesAsync(graph, schemas, ignoredTables);

            // Step 2: Build foreign key relationships
            await dbDependencyDiscoverer.BuildForeignKeyRelationshipsAsync(graph, schemas, ignoredTables);

            // Step 3: Apply model configurations (if any)
            BuildImplicitRelationsRelationshipsAsync(tablesConfigurations, tablesToIgnore);

            logger?.LogDebug("Dependency graph built with {TableCount} tables", graph.GetNodes().Count());

            return graph;
        }

        /// <summary>
        /// Gets tables ordered such that each table appears after its dependencies (topological order).
        /// </summary>
        /// <returns>
        /// A list of <see cref="TableNode"/> in dependency order. If cycles exist, a warning is printed and
        /// nodes are returned in unspecified order.
        /// </returns>
        public List<TableNode> GetTablesInDependencyOrder()
        {
            try
            {
                return graph.TopologicalSort();
            }
            catch (InvalidOperationException ex)
            {
                Console.WriteLine($"Warning: {ex.Message}");

                // Find and report cycles
                var scc = graph.GetStronglyConnectedComponents();
                var cycles = scc.Where(component => component.Count > 1).ToList();

                if (cycles.Any())
                {
                    Console.WriteLine("Detected cycles:");
                    foreach (var cycle in cycles)
                    {
                        Console.WriteLine($"  Cycle: {string.Join(" -> ", cycle.Select(n => n.FullName))}");
                    }
                }

                // Return nodes in an arbitrary order
                return graph.GetNodes().ToList();
            }
        }

        /// <summary>
        /// Gets tables with no dependencies (in-degree 0) — root nodes.
        /// </summary>
        public List<TableNode> GetRootTables()
        {
            return graph.GetRootNodes().ToList();
        }

        /// <summary>
        /// Gets tables that no other tables depend on (out-degree 0) — leaf nodes.
        /// </summary>
        public List<TableNode> GetLeafTables()
        {
            return graph.GetLeafNodes().ToList();
        }

        /// <summary>
        /// Finds a table node by schema and table name.
        /// </summary>
        /// <param name="schema">The schema name.</param>
        /// <param name="tableName">The table name.</param>
        /// <returns>The matching <see cref="TableNode"/> if found; otherwise, null.</returns>
        public TableNode? FindTable(string schema, string tableName)
        {
            var fullName = $"{schema}.{tableName}";
            return graph.NodesByName.TryGetValue(fullName, out var node) ? node : null;
        }

        /// <summary>
        /// Gets all table nodes currently present in the graph.
        /// </summary>
        public IEnumerable<TableNode> GetAllTables()
        {
            return graph.GetNodes();
        }

        /// <summary>
        /// Gets direct dependencies for the specified table (successors).
        /// </summary>
        /// <param name="table">The table whose dependencies are requested.</param>
        /// <returns>Tables that this <paramref name="table"/> depends on.</returns>
        public IEnumerable<TableNode> GetDependencies(TableNode table)
        {
            return graph.GetSuccessors(table);
        }

        /// <summary>
        /// Gets direct dependents for the specified table (predecessors).
        /// </summary>
        /// <param name="table">The table whose dependents are requested.</param>
        /// <returns>Tables that depend on the specified <paramref name="table"/>.</returns>
        public IEnumerable<TableNode> GetDependents(TableNode table)
        {
            return graph.GetPredecessors(table);
        }

        /// <summary>
        /// Adds implicit relation edges defined in <see cref="TableConfiguration"/> entries for tables already present in the graph.
        /// </summary>
        /// <param name="tableImpicitRelations">The set of table configurations.</param>
        /// <param name="tablesToIgnore">Optional list of tables to ignore when considering implicit relations.</param>
        private void BuildImplicitRelationsRelationshipsAsync(IEnumerable<TableConfiguration>? tableImpicitRelations, List<TableToIgnore>? tablesToIgnore)
        {
            var ignoredTables = new HashSet<string>(
                tablesToIgnore?.Select(t => t.FullName) ?? new List<string>(),
                StringComparer.OrdinalIgnoreCase);

            var modelConfigsByFullName = tableImpicitRelations?.ToDictionary(a => a.FullName) ?? new Dictionary<string, TableConfiguration>();

            // traverse all graph nodes
            graph.GetNodes().ToList().ForEach(node =>
            {
                var fullName = node.FullName;
                if (modelConfigsByFullName.ContainsKey(fullName))
                {
                    var modelConfig = modelConfigsByFullName[fullName];
                    if (modelConfig.ImplicitRelations != null && modelConfig.ImplicitRelations.Any())
                    {
                        foreach (var implicitRelation in modelConfig.ImplicitRelations)
                        {
                            var targetFullName = implicitRelation.TargetFullName;
                            if (graph.NodesByName.ContainsKey(targetFullName))
                            {
                                var targetNode = graph.NodesByName[targetFullName];
                                // Add edge to graph (node depends on targetNode)
                                var edgeData = new ImplicitRelTableDependencyEdge(
                                    targetNode.Schema,
                                    targetNode.Name,
                                    implicitRelation.ColumnBindings!,
                                    implicitRelation.WhereClause);

                                graph.AddEdge(node, targetNode, edgeData);
                                if (logger?.IsEnabled(LogLevel.Debug) == true)
                                {
                                    var columnBindings = implicitRelation.ColumnBindings?.ToList() ?? new List<ColumnBinding>();
                                    if (columnBindings.Any())
                                    {
                                        foreach (var binding in columnBindings)
                                        {
                                            logger.LogDebug("  IMPLICIT RELATION: {0}.{1} -> {2}.{3}", node.FullName,binding.SourceColumn, targetNode.FullName, binding.TargetColumn );
                                        }
                                    }
                                    else
                                    {
                                        logger.LogDebug("  IMPLICIT RELATION: {0} -> {1}", node.FullName, targetNode.FullName);
                                    }
                                }
                            }
                            //else
                            //{
                            //    if (verbose)
                            //    {
                            //        Console.WriteLine($"  Warning: Implicit relation target table {targetFullName} not found for {fullName}");
                            //    }
                            //}
                        }
                    }
                }
            });
        }

        /// <summary>
        /// Writes a concise summary of the dependency graph to the console, including counts and cycle info.
        /// </summary>
        public void PrintGraphSummary()
        {
            Console.WriteLine("\n=== DEPENDENCY GRAPH SUMMARY ===");

            var stats = graph.GetStatistics();
            Console.WriteLine($"Graph Statistics: {stats}");

            var rootTables = GetRootTables();
            Console.WriteLine($"Root tables (no dependencies): {rootTables.Count}");
            foreach (var table in rootTables.Take(5))
            {
                Console.WriteLine($"  - {table}");
            }
            if (rootTables.Count > 5)
            {
                Console.WriteLine($"  ... and {rootTables.Count - 5} more");
            }

            var leafTables = GetLeafTables();
            Console.WriteLine($"Leaf tables (no dependents): {leafTables.Count}");
            foreach (var table in leafTables.Take(5))
            {
                Console.WriteLine($"  - {table}");
            }
            if (leafTables.Count > 5)
            {
                Console.WriteLine($"  ... and {leafTables.Count - 5} more");
            }

            var tablesWithoutPK = graph.GetNodes().Where(n => !n.PrimaryKeyColumns.Any()).ToList();
            if (tablesWithoutPK.Any())
            {
                Console.WriteLine($"Tables without primary key: {tablesWithoutPK.Count}");
                foreach (var table in tablesWithoutPK.Take(5))
                {
                    Console.WriteLine($"  - {table.FullName}");
                }
                if (tablesWithoutPK.Count > 5)
                {
                    Console.WriteLine($"  ... and {tablesWithoutPK.Count - 5} more");
                }
            }

            // Check for cycles
            if (stats.HasCycles)
            {
                Console.WriteLine("\nCYCLES DETECTED:");
                var scc = graph.GetStronglyConnectedComponents();
                var cycles = scc.Where(component => component.Count > 1).ToList();

                foreach (var cycle in cycles)
                {
                    Console.WriteLine($"  Cycle: {string.Join(" -> ", cycle.Select(n => n.FullName))}");
                }
            }

            Console.WriteLine("=== END SUMMARY ===\n");
        }

        /// <summary>
        /// Prints a dependency tree for a specific table without edge metadata.
        /// </summary>
        /// <param name="schema">The schema of the table.</param>
        /// <param name="tableName">The table name.</param>
        /// <remarks>
        /// Traverses dependencies recursively while preventing infinite loops with a visited set.
        /// </remarks>
        public void PrintTableDependencies(string schema, string tableName)
        {
            var table = FindTable(schema, tableName);
            if (table == null)
            {
                Console.WriteLine($"Table {schema}.{tableName} not found in dependency graph.");
                return;
            }

            Console.WriteLine($"\n=== DEPENDENCY TREE FOR {table.FullName} ===");
            Console.WriteLine($"Primary Key: {(table.PrimaryKeyColumns.Any() ? string.Join(", ", table.PrimaryKeyColumns) : "None")}");

            // Recursively print dependencies with indentation to visualize the dependency tree.
            var visited = new HashSet<TableNode>(new TableNodeComparer());
            PrintDependenciesRecursive(table, 0, visited);

            Console.WriteLine("=== END DEPENDENCY TREE ===\n");
        }

        /// <summary>
        /// Recursively prints dependencies for a node with indentation.
        /// </summary>
        /// <param name="node">The current node.</param>
        /// <param name="indent">Indentation level (spaces).</param>
        /// <param name="visited">Set of visited nodes to avoid cycles.</param>
        private void PrintDependenciesRecursive(TableNode node, int indent, HashSet<TableNode> visited)
        {
            // Prevent circular recursion
            if (!visited.Add(node))
            {
                Console.WriteLine($"{new string(' ', indent)}- {node.FullName} (already visited)");
                return;
            }

            // Retrieve direct dependencies
            var dependencies = GetDependencies(node).ToList();
            foreach (var dep in dependencies)
            {
                Console.WriteLine($"{new string(' ', indent)}- {dep.FullName}" +
                                  (dep.PrimaryKeyColumns.Any() ? $" [PK: {string.Join(", ", dep.PrimaryKeyColumns)}]" : " [No PK]"));
                // Recursive call for each dependency
                PrintDependenciesRecursive(dep, indent + 2, visited);
            }
        }

        /// <summary>
        /// Prints a dependency tree for a specific table including edge details (FK/implicit).
        /// </summary>
        /// <param name="schema">The schema of the table.</param>
        /// <param name="tableName">The table name.</param>
        public void PrintTableDependencyWithEdgeInfo(string schema, string tableName)
        {
            var table = FindTable(schema, tableName);
            if (table == null)
            {
                Console.WriteLine($"Table {schema}.{tableName} not found in dependency graph.");
                return;
            }

            Console.WriteLine($"\n=== DEPENDENCY TREE WITH EDGE INFO FOR {table.FullName} ===");
            Console.WriteLine($"Primary Key: {(table.PrimaryKeyColumns.Any() ? string.Join(", ", table.PrimaryKeyColumns) : "None")}");

            // Recursively print dependencies with edge information
            var visited = new HashSet<TableNode>(new TableNodeComparer());
            PrintDependenciesWithEdgeInfoRecursive(table, 0, visited);

            Console.WriteLine("=== END DEPENDENCY TREE ===\n");
        }

        /// <summary>
        /// Recursively prints dependencies and their edge metadata for a node.
        /// </summary>
        /// <param name="node">The current node.</param>
        /// <param name="indent">Indentation level (spaces).</param>
        /// <param name="visited">Set of visited nodes to avoid cycles.</param>
        private void PrintDependenciesWithEdgeInfoRecursive(TableNode node, int indent, HashSet<TableNode> visited)
        {
            // Prevent circular recursion
            if (!visited.Add(node))
            {
                Console.WriteLine($"{new string(' ', indent)}- {node.FullName} (already visited)");
                return;
            }

            // Get all outgoing edges
            var outgoingEdges = graph.GetOutgoingEdges(node).ToList();

            foreach (var edge in outgoingEdges)
            {
                var depNode = edge.Target;
                var edgeData = edge.Data;

                // Print the dependency node
                Console.WriteLine($"{new string(' ', indent)}- {depNode.FullName}" +
                                  (depNode.PrimaryKeyColumns.Any() ?
                                   $" [PK: {string.Join(", ", depNode.PrimaryKeyColumns)}]" :
                                   " [No PK]"));

                // Print edge details with extra indent
                if (edgeData is FkTableDependencyEdge fkEdge)
                {
                    var columnBindings = fkEdge.ColumnBindings.ToList();
                    if (columnBindings.Any())
                    {
                        foreach (var binding in columnBindings)
                        {
                            Console.WriteLine($"{new string(' ', indent + 4)}FK: {node.FullName}.{binding.SourceColumn} -> " +
                                             $"{depNode.FullName}.{binding.TargetColumn}" +
                                             $" (Constraint: {fkEdge.ConstraintName ?? "unnamed"})");
                        }
                    }
                    else
                    {
                        Console.WriteLine($"{new string(' ', indent + 4)}FK: {node.FullName} -> {depNode.FullName}" +
                                         $" (Constraint: {fkEdge.ConstraintName ?? "unnamed"})");
                    }
                }
                else if (edgeData is ImplicitRelTableDependencyEdge implicitEdge)
                {
                    var columnBindings = implicitEdge.ColumnBindings.ToList();
                    if (columnBindings.Any())
                    {
                        foreach (var binding in columnBindings)
                        {
                            Console.WriteLine($"{new string(' ', indent + 4)}IMPLICIT: {node.FullName}.{binding.SourceColumn} -> " +
                                             $"{depNode.FullName}.{binding.TargetColumn}" +
                                             (string.IsNullOrEmpty(implicitEdge.WhereClause) ? "" : $" WHERE {implicitEdge.WhereClause}"));
                        }
                    }
                    else
                    {
                        Console.WriteLine($"{new string(' ', indent + 4)}IMPLICIT: {node.FullName} -> {depNode.FullName}" +
                                         (string.IsNullOrEmpty(implicitEdge.WhereClause) ? "" : $" WHERE {implicitEdge.WhereClause}"));
                    }
                }

                // Recursive call for this dependency
                PrintDependenciesWithEdgeInfoRecursive(depNode, indent + 2, visited);
            }
        }

    }
}