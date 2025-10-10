# DataSubset.PostgreSql

PostgreSQL-specific dependency discovery utilities for DataSubset. Implements Npgsql-backed schema inspection and graph traversal to extract relationship-aware, minimal subsets of data for testing, migration, and export scenarios.

- Target framework: .NET 8
- Package ID: DataSubset.PostgreSql
- Depends on: DataSubset.Core
- Uses: Npgsql

## Install

- dotnet CLI
  - `dotnet add package DataSubset.PostgreSql`
- NuGet Package Manager
  - `Install-Package DataSubset.PostgreSql`

## Features

- Schema inspection via Npgsql (tables, primary/foreign keys, constraints)
- Relationship graph construction from PostgreSQL catalogs
- Configurable traversal to compute minimal dependent row sets
- Works with DataSubset.Core abstractions

## Getting started
do
1. Reference both DataSubset.PostgreSql and DataSubset.Core.
2. Provide a PostgreSQL connection string.
3. Use the PostgreSQL discovery service to read schema and build a relationship graph.
4. Apply traversal rules to resolve the subset of related rows for your seed data.
5. Export or process the resulting subset as needed.

Ensure your application or tooling has network access and permissions to read the PostgreSQL catalogs.

## Links

- NuGet: https://www.nuget.org/packages/DataSubset.PostgreSql
- Core library: https://www.nuget.org/packages/DataSubset.Core
- Npgsql: https://www.nuget.org/packages/Npgsql

## License

See the LICENSE file in this repository.