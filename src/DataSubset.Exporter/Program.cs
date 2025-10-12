// Check for help flag first
using DataSubset.DbDependencyGraph.Core.DependencyGraph;
using DataSubset.Exporter;
using DataSubset.Exporters.Common;
using DataSubset.Exporters.PostgreSql;
using DataSubset.PostgreSql;
using DependencyTreeApp;

if (args.Length == 0 || args.Contains("-h") || args.Contains("--help"))
{
    PrintUsage();
    return 0;
}

// Parse command-line arguments
var configFile = GetArgument(args, "-c", "--config-file");
var outputFile = GetArgument(args, "-o", "--output-file");
var formatStr = GetArgument(args, "-f", "--format");
var dbTypeStr = GetArgument(args, "-d", "--db-type");

// Validate required arguments
if (string.IsNullOrEmpty(configFile))
{
    Console.WriteLine("Error: Config file is required.");
    PrintUsage();
    return 1;
}

if (string.IsNullOrEmpty(formatStr))
{
    Console.WriteLine("Error: Format is required.");
    PrintUsage();
    return 1;
}

if (formatStr != "insert" && formatStr != "binary")
{
    Console.WriteLine($"Error: Invalid format '{formatStr}'. Must be 'insert' or 'binary'.");
    PrintUsage();
    return 1;
}

if (string.IsNullOrEmpty(dbTypeStr))
{
    Console.WriteLine("Error: Database type is required.");
    PrintUsage();
    return 1;
}

IDatabaseDependencyDiscoverer databaseDependencyDiscoverer;
DbExporterEngineBase dbExporterEngineBase;
switch (dbTypeStr.Trim())
{
    case "postgres":
        databaseDependencyDiscoverer = new PostgreSqlDependencyDiscoverer("TODO");
        dbExporterEngineBase = new PostgreSqlExporterEngine("TODO");
        break;
    case "sqlserver":
    case "mysql":
        Console.WriteLine($"Error: {dbTypeStr} not implemented yet");
        return 1;
    default:
        Console.WriteLine($"Error: Invalid database type '{dbTypeStr}'. Must be 'postgres', 'sqlserver', or 'mysql'.");
        PrintUsage();
        return 1;
}

InsertStatementExporter? insertStatementExporter = null;
switch (formatStr.Trim())
{
    case "insert":
        insertStatementExporter = new InsertStatementExporter(dbExporterEngineBase);
        break;
    case "binary":
        Console.WriteLine($"Error: {formatStr} not implemented yet");
        return 1;
        break;
    default:
        Console.WriteLine($"Error: Invalid format '{formatStr}'. Must be 'insert' or 'binary'.");
        PrintUsage();
        return 1;
}
var tableDependencyGraphBuilder = new TableDependencyGraphBuilder(databaseDependencyDiscoverer);
var graph = await tableDependencyGraphBuilder.BuildDependencyGraphAsync(new[] { "export" });

if (outputFile != null)
{
    using var writer = new StreamWriter(outputFile);
    if (insertStatementExporter != null)
    {
        await foreach (var item in insertStatementExporter.GetItemsToExportInInsertOrder(new[] { new DataSubset.DbDependencyGraph.Core.Configurations.TableExportConfig("export", "table1") }, graph))
        {
            await writer.WriteLineAsync(item);
        }
    }
}
else
{
    if (insertStatementExporter != null)
    {
        await foreach (var item in insertStatementExporter.GetItemsToExportInInsertOrder(new[] { new DataSubset.DbDependencyGraph.Core.Configurations.TableExportConfig("export", "table1") }, graph))
        {
            Console.WriteLine(item);
        }
    }
}

return 0;

static string? GetArgument(string[] args, string shortName, string longName)
{
    for (int i = 0; i < args.Length; i++)
    {
        if (args[i] == shortName || args[i] == longName)
        {
            if (i + 1 < args.Length)
            {
                return args[i + 1];
            }
        }
    }
    return null;
}

static void PrintUsage()
{
    Console.WriteLine("DataSubset.Exporter - Export database subsets");
    Console.WriteLine();
    Console.WriteLine("Usage: DataSubset.Exporter [options]");
    Console.WriteLine();
    Console.WriteLine("Options:");
    Console.WriteLine("  -c, --config-file <filepath>   Required. Path to the configuration file.");
    Console.WriteLine("  -o, --output-file <filepath>   Optional. Path to the output file.");
    Console.WriteLine("  -f, --format <format>          Required. Export format: 'insert' or 'binary'.");
    Console.WriteLine("  -d, --db-type <type>           Required. Database type: 'postgres', 'sqlserver', or 'mysql'.");
    Console.WriteLine("  -h, --help                     Display this help message.");
    Console.WriteLine();
    Console.WriteLine("Examples:");
    Console.WriteLine("  DataSubset.Exporter -c config.json -f insert -d postgres");
    Console.WriteLine("  DataSubset.Exporter --config-file config.json --output-file output.sql --format insert --db-type sqlserver");
}
