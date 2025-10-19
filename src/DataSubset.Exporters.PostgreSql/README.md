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
