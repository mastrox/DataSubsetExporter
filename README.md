# DataSubsetExporter

A tool to export a coherent subset of data from a relational database by traversing relationships and preserving referential integrity. 
Useful for creating reproducible test datasets, sharing focused slices of production data, or migrating related records between environments.

## Key concepts
- Traversal Foreign Key: follow foreign-key relationships to include related rows, .
- Traversal Impicit Relation: relations bwtween table configurable by user
- Filters: include only rows that match provided predicates (by ID, SQL WHERE, or custom rules).
- Export formats: INSERTs statements
- Consistency: It maintain referential integrity.

## Features
- Export by primary key and/or by WHERE clause.
- Include/exclude tables and columns.
- Output to file or stdout (TODO stdout).
- Handles cycles and avoids duplicate rows.
- Simple CLI suitable for automation and CI pipelines TODO.

## Getting started

Prerequisites
- .NET 8 SDK or runtime
- A supported ADO.NET provider (e.g., Microsoft SQL Server, PostgreSQL). Ensure the connection string uses a supported provider.

## Behavior and guarantees
- Referential integrity: rows referenced by included rows will be included when possible to preserve FK relationships (subject to include/exclude rules).
- Deduplication: each table row is exported once even if reachable by multiple paths.
- Cycles: detected and handled via visited-set; traversal stops when revisiting rows.
- Performance: exporting large graphs may be slow—use filters or sampling.

## Examples of output
- SQL: INSERT statements ordered to respect FK constraints.

## Configuration


## Troubleshooting
- Permission errors: verify the user has SELECT on required tables and schema metadata access.
- Missing foreign keys: the tool relies on FK metadata; for databases without FK constraints, provide manual relation mappings.
- Large exports: add filters. Use dry-run to estimate size.

## Contributing
- Fork the repository, create a feature branch, add tests, and submit a pull request.
- Include unit tests for traversal logic, deduplication, and output generation.
- Keep changes compatible with .NET 8.

## License
MIT Licens applies

## Contact
Open issues or pull requests on the project repository for bugs or feature requests.
