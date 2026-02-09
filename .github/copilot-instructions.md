# Copilot Instructions — BeastieBot3

## Project Overview

.NET 9 CLI tool that aggregates biological taxonomy data from IUCN Red List, Catalogue of Life, Wikidata, and Wikipedia into SQLite databases. Built with `Spectre.Console.Cli` for command hierarchy and `Microsoft.Data.Sqlite` for all persistence.

## Build & Verify

```bash
dotnet build                                                        # build
dotnet run --project BeastieBot3/BeastieBot3.csproj -- [command]     # run a command
dotnet run --project BeastieBot3/BeastieBot3.csproj -- show-paths    # verify INI config
```

No test suite exists — verify changes by building and running the relevant CLI command.

## Architecture

### Command Tree

Commands are registered in `Program.cs` under branches: `col`, `iucn`, `iucn api`, `wikidata`, `wikipedia`, `common-names`. Each branch groups related subcommands.

### Adding a New Command

1. Create a `sealed class` inheriting `Command<TSettings>` (sync) or `AsyncCommand<TSettings>` (async — use for HTTP/long-running work). The async signature includes `CancellationToken`.
2. Define a nested `Settings` class inheriting `CommonSettings` (provides `--settings-dir` and `--ini-file`).
3. Register in `Program.cs` via `config.AddBranch()`/`config.AddCommand<T>()` with `.WithDescription()` and `.WithExample()`.

```csharp
internal sealed class MyCommand : AsyncCommand<MyCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("-d|--database <PATH>")]
        [Description("Override database path.")]
        public string? DatabasePath { get; init; }
    }

    public override async Task<int> ExecuteAsync(CommandContext context, Settings settings, CancellationToken ct) {
        var paths = new PathsService(settings.IniFile);
        var dbPath = paths.ResolveIucnDatabasePath(settings.DatabasePath);
        using var store = SomeStore.Open(dbPath);
        // ... work ...
        return 0;
    }
}
```

### Configuration Flow

`paths.ini` (INI sections `[Datasets]`, `[Datastore]`, `[Reports]`) → `IniPathReader` → `PathsService` (typed facade with `Get*Path()` / `Resolve*()` methods) → commands. CLI `--settings-dir` / `--ini-file` options override defaults. API keys load from `.env` via `EnvFileLoader`.

### SQLite Store Pattern

All stores (`CommonNameStore`, `IucnApiCacheStore`, `WikidataCacheStore`, `WikipediaCacheStore`) follow one pattern:

- **Private constructor** + **static `Open(path)`** factory that creates the directory, opens a connection with `ReadWriteCreate` mode, sets `PRAGMA journal_mode=WAL` and `PRAGMA foreign_keys=ON`, and calls `EnsureSchema()`.
- **`EnsureSchema()`** uses `CREATE TABLE IF NOT EXISTS` / `CREATE INDEX IF NOT EXISTS`.
- **`IDisposable`** — always wrap in `using` at call sites.
- Raw SQL with **parameterized queries** (`@param`) via `SqliteCommand`. No ORM.
- HTTP-calling stores embed `ApiImportMetadataStore` for request tracking/retry.

### Report Output

Use `ReportPathResolver` to resolve output directories/files. Priority: explicit CLI `--output` → `paths.GetReportOutputDirectory()` → `data-analysis` fallback. Reports are typically Markdown, sometimes with companion CSV.

## Code Conventions

- **C# / .NET 9** with nullable reference types enabled.
- **File-scoped namespaces** (`namespace BeastieBot3.Iucn;`).
- **Brace style**: opening brace on same line.
- **Naming**: PascalCase types/methods, camelCase locals/params, `_camelCase` private fields.
- **`var`** when type is obvious from the right-hand side.
- **`Spectre.Console`** for all user-facing output (progress bars, tables, markup).

## Key Directories

| Directory | Purpose |
|---|---|
| `BeastieBot3/Iucn/` | IUCN data import, API caching, crosscheck reports |
| `BeastieBot3/Wikidata/` | Wikidata SPARQL seeding, entity caching, coverage reports |
| `BeastieBot3/Wikipedia/` | Wikipedia page caching, taxon matching |
| `BeastieBot3/WikipediaLists/` | Wikipedia list generation using YAML defs + Mustache templates |
| `BeastieBot3/CommonNames/` | Multi-source common name aggregation and disambiguation |
| `BeastieBot3/Col/` | Catalogue of Life import and profiling |
| `BeastieBot3/Taxonomy/` | Scientific name normalization, authority parsing, taxon ladders |
| `BeastieBot3/Configuration/` | INI reading, path resolution, env loading |
| `BeastieBot3/Infrastructure/` | `ApiImportMetadataStore`, `ReportPathResolver` |
| `BeastieBot3/rules/` | YAML/text rule files and Mustache templates for list generation |
| `BeastieBot3/BeastieLegacy/` | Legacy code from the previous system — read for output format reference, but don't reuse directly. Left mostly as-is intentionally. |
