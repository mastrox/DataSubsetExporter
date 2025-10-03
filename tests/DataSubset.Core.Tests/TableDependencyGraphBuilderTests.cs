using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DataSubset.Core.Configurations;
using DataSubset.Core.DependencyGraph;
using DependencyTreeApp;
using Xunit;

namespace DataSubset.Core.Tests
{
    public class TableDependencyGraphBuilderTests
    {
        [Fact]
        public async Task BuildDependencyGraph_AppliesImplicitRelations()
        {
            // Arrange: A, B exist; implicit relation A -> B
            var modelA = new TableConfiguration("s", "A");
            modelA.ImplicitRelations = new List<ImplicitRelation>
            {
                new()
                {
                    TargetSchema = "s",
                    TargetTable  = "B",
                    ColumnBindings = new[] { new ColumnBinding { SourceColumn = "AId", TargetColumn = "BId" } },
                    WhereClause = "BId > 0"
                }
            };
            var modelConfigs = new List<TableConfiguration> { modelA };
            var ignored = new List<TableToIgnore>();
            var fake = new FakeDiscoverer(
                tables: new[] { ("s","A"), ("s","B") },
                fks: Array.Empty<FkDef>()
            );

            var builder = new TableDependencyGraphBuilder(fake);

            // Act
            var graph = await builder.BuildDependencyGraphAsync(new[] { "s" }, modelConfigs, ignored);

            // Assert: implicit edge A -> B present with metadata
            var a = builder.FindTable("s", "A")!;
            var b = builder.FindTable("s", "B")!;
            var edges = graph.GetOutgoingEdges(a).Where(e => e.Target.Equals(b)).ToList();
            Assert.Single(edges);
            var edgeData = Assert.IsType<ImplicitRelTableDependencyEdge>(edges[0].Data);
            Assert.Equal("s", edgeData.SourceSchema);
            Assert.Equal("B", edgeData.SourceTable);
            var binding = Assert.Single(edgeData.ColumnBindings);
            Assert.Equal("AId", binding.SourceColumn);
            Assert.Equal("BId", binding.TargetColumn);
            Assert.Equal("BId > 0", edgeData.WhereClause);

            // Sanity: builder.Graph is the same instance returned
            Assert.Same(builder.Graph, graph);
        }

        [Fact]
        public async Task BuildDependencyGraph_AddsForeignKeys_WithEdgeData()
        {
            // Arrange: A -> B FK with bindings and name
            var modelConfigs = new List<TableConfiguration>();
            var ignored = new List<TableToIgnore>();
            var fk = new FkDef(("s","A"), ("s","B"),
                constraint: "FK_A_B",
                bindings: new[]
                {
                    new ColumnBinding { SourceColumn = "BId", TargetColumn = "Id" }
                });
            var fake = new FakeDiscoverer(
                tables: new[] { ("s","A"), ("s","B") },
                fks: new[] { fk }
            );
            var builder = new TableDependencyGraphBuilder(fake);

            // Act
            var graph = await builder.BuildDependencyGraphAsync(new[] { "s" },modelConfigs, ignored);

            // Assert
            var a = builder.FindTable("s","A")!;
            var b = builder.FindTable("s","B")!;
            var e = graph.GetOutgoingEdges(a).Single(x => x.Target.Equals(b));
            var data = Assert.IsType<FkTableDependencyEdge>(e.Data);
            Assert.Equal("FK_A_B", data.ConstraintName);
            var cb = Assert.Single(data.ColumnBindings);
            Assert.Equal("BId", cb.SourceColumn);
            Assert.Equal("Id", cb.TargetColumn);
        }

        [Fact]
        public async Task GetTablesInDependencyOrder_TopologicalForDAG()
        {
            // A -> B
            var fake = new FakeDiscoverer(
                tables: new[] { ("s","A"), ("s","B") },
                fks: new[] { new FkDef(("s","A"), ("s","B")) }
            );
            var builder = new TableDependencyGraphBuilder(fake);
            await builder.BuildDependencyGraphAsync(new[] { "s" },new(), new());

            var order = builder.GetTablesInDependencyOrder();
            var pos = order.Select((n,i)=>(n,i)).ToDictionary(t => t.n.FullName, t => t.i, StringComparer.OrdinalIgnoreCase);

            Assert.True(pos["s.A"] < pos["s.B"]);
        }

        [Fact]
        public async Task GetTablesInDependencyOrder_Cycle_ReturnsAllNodes()
        {
            // A <-> B cycle
            var fake = new FakeDiscoverer(
                tables: new[] { ("s","A"), ("s","B") },
                fks: new[]
                {
                    new FkDef(("s","A"), ("s","B")),
                    new FkDef(("s","B"), ("s","A"))
                }
            );
            var builder = new TableDependencyGraphBuilder(fake);
            await builder.BuildDependencyGraphAsync(new[] { "s" },new(),new());

            var order = builder.GetTablesInDependencyOrder();
            // Order unspecified, but both must be present
            Assert.Contains(order, n => n.FullName == "s.A");
            Assert.Contains(order, n => n.FullName == "s.B");
        }

        [Fact]
        public async Task RootAndLeafTables_ComputedCorrectly()
        {
            // A -> B -> C and D isolated
            var fake = new FakeDiscoverer(
                tables: new[] { ("s","A"), ("s","B"), ("s","C"), ("s","D") },
                fks: new[]
                {
                    new FkDef(("s","A"), ("s","B")),
                    new FkDef(("s","B"), ("s","C"))
                }
            );
            var builder = new TableDependencyGraphBuilder(fake);
            await builder.BuildDependencyGraphAsync(new[] { "s" },new(),new());

            var roots = builder.GetRootTables().Select(n => n.FullName).ToList();
            var leaves = builder.GetLeafTables().Select(n => n.FullName).ToList();

            Assert.Contains("s.A", roots);
            Assert.Contains("s.D", roots);
            Assert.Contains("s.C", leaves);
            Assert.Contains("s.D", leaves);
            Assert.DoesNotContain("s.B", roots);
            Assert.DoesNotContain("s.B", leaves);
        }

        [Fact]
        public async Task IgnoredTables_AreSkipped_AndPassedToDiscoverer()
        {
            var ignored = new List<TableToIgnore> { new("s","Ignored") };
            var fake = new FakeDiscoverer(
                tables: new[] { ("s","Visible"), ("s","Ignored") },
                fks: Array.Empty<FkDef>()
            );
            var builder = new TableDependencyGraphBuilder(fake);

            await builder.BuildDependencyGraphAsync(new[] { "s" }, new(), ignored);

            Assert.NotNull(builder.FindTable("s","Visible"));
            Assert.Null(builder.FindTable("s","Ignored"));

            // Verify discoverer received the ignored set
            Assert.Contains("s.Ignored", fake.LastIgnoredTables, StringComparer.OrdinalIgnoreCase);
        }

        [Fact]
        public async Task Build_PassesSchemas_ToDiscoverer()
        {
            var fake = new FakeDiscoverer(
                tables: new[] { ("s","A") },
                fks: Array.Empty<FkDef>()
            );
            var builder = new TableDependencyGraphBuilder(fake);

            await builder.BuildDependencyGraphAsync(new[] { "s", "x" },new(), new());

            Assert.Equal(new[] { "s", "x" }, fake.SchemasForDiscover?.ToArray());
            Assert.Equal(new[] { "s", "x" }, fake.SchemasForFks?.ToArray());
        }

        // -------- Test helpers --------

        private sealed record FkDef((string schema, string table) Child,
                                    (string schema, string table) Parent,
                                    string? constraint = null,
                                    IEnumerable<ColumnBinding>? bindings = null);

        private sealed class FakeDiscoverer : IDatabaseDependencyDiscoverer
        {
            private readonly List<(string schema, string table)> _tables;
            private readonly List<FkDef> _fks;

            public IEnumerable<string>? SchemasForDiscover { get; private set; }
            public IEnumerable<string>? SchemasForFks { get; private set; }
            public HashSet<string> LastIgnoredTables { get; private set; } = new(StringComparer.OrdinalIgnoreCase);

            public FakeDiscoverer(IEnumerable<(string schema, string table)> tables, IEnumerable<FkDef> fks)
            {
                _tables = tables.ToList();
                _fks = fks.ToList();
            }

            public Task DiscoverTablesAsync(DatabaseGraph graph, string[] schemas, HashSet<string>? ignoredTables)
            {
                SchemasForDiscover = schemas.ToArray();
                if(ignoredTables != null)
                    LastIgnoredTables = new HashSet<string>(ignoredTables, StringComparer.OrdinalIgnoreCase);

                foreach (var (schema, table) in _tables)
                {
                    var full = $"{schema}.{table}";
                    if (ignoredTables?.Contains(full) == true) 
                        continue;
                    graph.GetOrCreateNode(schema, table);
                }
                return Task.CompletedTask;
            }

            public Task BuildForeignKeyRelationshipsAsync(DatabaseGraph graph, string[] schemas, HashSet<string> ignoredTables)
            {
                SchemasForFks = schemas.ToArray();
                if (ignoredTables != null)
                    LastIgnoredTables = new HashSet<string>(ignoredTables, StringComparer.OrdinalIgnoreCase);

                foreach (var fk in _fks)
                {
                    var childFull = $"{fk.Child.schema}.{fk.Child.table}";
                    var parentFull = $"{fk.Parent.schema}.{fk.Parent.table}";
                    if (ignoredTables?.Contains(childFull) == true || ignoredTables?.Contains(parentFull)== true) 
                        continue;

                    // ensure nodes exist (defensive: discovery should have added them)
                    var child = graph.FindTable(fk.Child.schema, fk.Child.table) ?? graph.GetOrCreateNode(fk.Child.schema, fk.Child.table);
                    var parent = graph.FindTable(fk.Parent.schema, fk.Parent.table) ?? graph.GetOrCreateNode(fk.Parent.schema, fk.Parent.table);

                    var edgeData = new FkTableDependencyEdge(
                        sourceSchema: fk.Child.schema,
                        sourceTable: fk.Child.table,
                        columnBindings: fk.bindings ?? Array.Empty<ColumnBinding>(),
                        constraintName: fk.constraint ?? string.Empty);

                    graph.AddEdge(child, parent, edgeData);
                }
                return Task.CompletedTask;
            }
        }
    }
}