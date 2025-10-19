# DataSubset.Exporter

A .NET 8 utility for exporting consistent subset.

## Prerequisites

- .NET SDK 8.x
- Supported OS: Windows, Linux, macOS

## Build

- Visual Studio:
  - Open the solution, set DataSubset.Exporter as the startup project, Build Solution.
- CLI:
  - From repo root: `dotnet build -c Release`

## Run

- Visual Studio:
  - Use Debug/Run. Pass arguments via Project Properties > Debug > Application arguments.
- CLI:
  - From repo root:
    - `dotnet run --project src/DataSubset.Exporter -- --help`
    - Example (generic):
      - # Usage Examples 
        Supported arguments :
        - -c, --config-file <filepath>   Required
        - -o, --output-file <filepath>   Optional
        - -f, --format <insert|binary>   Required (only 'insert' implemented)
        - -d, --db-type <postgres|sqlserver|mysql> Required (only 'postgres' implemented)
        - -h, --help
        
        Note: 'binary' format and db types 'sqlserver'/'mysql' are not implemented and will return an error.
        
        ### Windows PowerShell
        - Minimal (stdout):
          dotnet run --project src/DataSubset.Exporter -- -c .\config.json -f insert -d postgres
        
        - With output file:
          dotnet run --project src\DataSubset.Exporter -- -c .\config.json -o .\output.sql -f insert -d postgres
        
        - Using long switches:
          dotnet run --project src/DataSubset.Exporter -- --config-file .\config.json --output-file .\output.sql --format insert --db-type postgres
        
        ### Windows CMD
        - Minimal (stdout):
          dotnet run --project src\DataSubset.Exporter -- -c config.json -f insert -d postgres
        
        - With output file:
          dotnet run --project src\DataSubset.Exporter -- -c config.json -o output.sql -f insert -d postgres
        
        ### Linux/macOS Bash
        - Minimal (stdout):
          dotnet run --project src/DataSubset.Exporter -- -c ./config.json -f insert -d postgres
        
        - With output file:
          dotnet run --project src/DataSubset.Exporter -- -c ./config.json -o ./output.sql -f insert -d postgres
        
        ## Visual Studio (Application arguments)
        - Minimal (stdout):
          -c config.json -f insert -d postgres
        
        - With output file:
          -c config.json -o output.sql -f insert -d postgres
        
        ## Help
            - Show usage/help:
          dotnet run --project src/DataSubset.Exporter -- --help

Note: Use `--help` to list all available options and usage examples provided by the app.

## Configuration

- Command-line arguments are the primary way to configure runs.
- Environment variables can override or supply defaults (e.g., credentials).
- If supported by the app, an appsettings.json in the project or working directory can provide defaults.

## Testing

- If the solution contains tests:
  - `dotnet test`

## Publish

- Framework-dependent:
  - `dotnet publish src/DataSubset.Exporter -c Release -o ./.artifacts/publish`
- Self-contained (example for win-x64):
  - `dotnet publish src/DataSubset.Exporter -c Release -r win-x64 --self-contained true -p:PublishSingleFile=true -o ./.artifacts/publish/win-x64`

Adjust runtime identifier (RID) as needed (e.g., linux-x64, osx-arm64).

## Troubleshooting

- Ensure `dotnet --info` reports .NET 8 SDK.
- If TLS/SSL issues occur when connecting to data sources, verify certificates and connection strings.
- For large exports, consider running Release configuration and increasing process/file limits on Linux/macOS.

## License

See LICENSE file (if present) for terms.
