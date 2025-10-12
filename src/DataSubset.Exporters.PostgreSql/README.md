Pseudocode plan (detailed)
- Describe purpose: common library for exporting DataSubset to PostgreSQL
- List prerequisites: .NET 8, Npgsql
- Provide installation instructions via NuGet (PackageReference)
- Describe configuration options (connection string, schema, table mappings, batch size, transaction behavior)
- Show DI registration example for ASP.NET Core / generic host
- Show simple usage example with a hypothetical IExporter or PostgreSqlExporter API
- Describe extension points (custom mappers, custom type handlers)
- Provide testing notes and sample unit test strategy
- Provide contributing & license info

# DataSubset.Exporters.PostgreSql

Common library for exporting DataSubset results to PostgreSQL databases.

This package provides helpers and a lightweight exporter implementation to persist "data subsets" into PostgreSQL using Npgsql. It focuses on correctness, batching, and extensibility (custom mapping and type handling).

## Features
- Export DataSubset to PostgreSQL tables
- Designed for .NET 8 and Npgsql

## Requirements
- .NET 8
- Npgsql (the package uses standard ADO.NET patterns compatible with Npgsql)

## Installation
Add the package to your project via PackageReference or dotnet CLI. Replace with the actual package id when available.

PackageReference:
