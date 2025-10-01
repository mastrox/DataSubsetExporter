# DataSubset.Core

Core library providing graph utilities, dependency resolution, and data-subsetting primitives used by DataSubset services and tools. Includes traversal algorithms, configuration abstractions, and helpers for building and analyzing data subset graphs.

- Target framework: .NET 8
- Package ID: DataSubset.Core
- Related provider: DataSubset.PostgreSql

## Install

- dotnet CLI
  - `dotnet add package DataSubset.Core`
- NuGet Package Manager
  - `Install-Package DataSubset.Core`

## Features

- Model-agnostic graph representation of relational data
- Relationship-aware traversal and dependency resolution
- Configurable rules for inclusion/exclusion
- Configurable implicit relationships
- Deterministic, minimal data subset extraction
- Extensible abstractions for database-specific providers

## Getting started

1. Define or load your data model and relationships (tables/entities, foreign keys).
2. Build a dependency graph using the provided abstractions.
3. Configure traversal rules (breadth/depth limits, filters).
4. Execute traversal to resolve the minimal set of related rows/entities.
5. Export or hand off the subset to your persistence layer or provider.

For PostgreSQL-backed schema inspection and discovery, see DataSubset.PostgreSql.

## Links

- NuGet: https://www.nuget.org/packages/DataSubset.Core
- PostgreSQL provider: https://www.nuget.org/packages/DataSubset.PostgreSql

## License

See the LICENSE file in this repository.