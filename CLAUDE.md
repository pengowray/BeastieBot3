# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Verify

```bash
dotnet build                                                         # build
dotnet run --project BeastieBot3/BeastieBot3.csproj -- [command]    # run a command
dotnet run --project BeastieBot3/BeastieBot3.csproj -- show-paths   # verify INI config is loading correctly
```

No test suite exists — verify changes by building and running the relevant CLI command.

## Project Overview

.NET 9 CLI tool that aggregates biological taxonomy data from IUCN Red List, Catalogue of Life (CoL), Wikidata, and Wikipedia into local SQLite databases. Used for normalizing vernacular (common) names, resolving taxonomic synonyms, and detecting naming conflicts across sources.

## Architecture

### Command Tree

All commands are registered in `BeastieBot3/Program.cs` under top-level branches: `col`, `iucn`, `iucn api`, `wikidata`, `wikipedia`, `common-names`. Each branch groups related subcommands and is wired up via `config.AddBranch()` / `config.AddCommand<T>()`.

### Configuration Flow

`paths.ini` (sections: `[Datasets]`, `[Datastore]`, `[Reports]`) → `IniPathReader` → `PathsService` (typed facade with `Get*Path()` / `Resolve*()` methods) → commands. CLI flags `--settings-dir` / `--ini-file` override the default INI location. API keys (IUCN token, Wikidata user-agent) load from `.env` via `EnvFileLoader`.

### SQLite Store Pattern

All stores (`CommonNameStore`, `IucnApiCacheStore`, `WikidataCacheStore`, `WikipediaCacheStore`) share one pattern:

- **Private constructor** + **static `Open(path)`** factory — creates the directory, opens with `ReadWriteCreate` mode, sets `PRAGMA journal_mode=WAL` and `PRAGMA foreign_keys=ON`, calls `EnsureSchema()`.
- **`EnsureSchema()`** uses `CREATE TABLE IF NOT EXISTS` / `CREATE INDEX IF NOT EXISTS`.
- **`IDisposable`** — always wrap in `using` at call sites.
- Raw SQL with **parameterized queries** (`@param`) via `SqliteCommand`. No ORM.
- HTTP-calling stores embed `ApiImportMetadataStore` for request tracking and retry queue management.

### Adding a New Command

1. Create a `sealed class` inheriting `Command<TSettings>` (sync) or `AsyncCommand<TSettings>` (async — use for HTTP/long-running work).
2. Define a nested `Settings : CommonSettings` (`CommonSettings` provides `--settings-dir` and `--ini-file`).
3. Register in `Program.cs` with `.WithDescription()` and `.WithExample()`.

```csharp
internal sealed class MyCommand : AsyncCommand<MyCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("-d|--database <PATH>")]
        [Description("Override database path.")]
        public string? DatabasePath { get; init; }
    }

    public override async Task<int> ExecuteAsync(CommandContext context, Settings settings, CancellationToken ct) {
        var paths = new PathsService(settings.IniFile);
        var dbPath = paths.ResolveSomeDatabasePath(settings.DatabasePath);
        using var store = SomeStore.Open(dbPath);
        // work...
        return 0;
    }
}
```

### Report Output

Use `ReportPathResolver` to resolve output paths. Priority: explicit CLI `--output` → `paths.GetReportOutputDirectory()` → `data-analysis` fallback. Reports are typically Markdown, sometimes with a companion CSV.

## Key Directories

| Directory | Purpose |
|---|---|
| `BeastieBot3/Iucn/` | IUCN CSV import, API caching, crosscheck and consistency reports |
| `BeastieBot3/Wikidata/` | SPARQL seeding, Wikidata entity caching, coverage reports |
| `BeastieBot3/Wikipedia/` | Wikipedia page fetching and taxon matching |
| `BeastieBot3/WikipediaLists/` | Wikipedia list generation using YAML definitions + Mustache templates |
| `BeastieBot3/CommonNames/` | Multi-source common name aggregation, conflict detection, disambiguation reports |
| `BeastieBot3/Col/` | Catalogue of Life ColDP import and profiling |
| `BeastieBot3/Taxonomy/` | Scientific name normalisation, authority parsing, `TaxonLadder` hierarchy |
| `BeastieBot3/Configuration/` | INI reading, path resolution, `.env` loading |
| `BeastieBot3/Infrastructure/` | `ApiImportMetadataStore`, `ReportPathResolver` |
| `BeastieBot3/rules/` | YAML rule files and Mustache templates for list generation |
| `BeastieBot3/BeastieLegacy/` | Legacy code — read for output format reference only; do not reuse directly |

## Code Conventions

- **File-scoped namespaces**: `namespace BeastieBot3.Iucn;`
- **Brace style**: opening brace on same line.
- **Naming**: PascalCase types/methods, camelCase locals/params, `_camelCase` private fields.
- **`var`** when type is obvious from the right-hand side.
- **`Spectre.Console`** for all user-facing output (progress bars, tables, markup). Never use `Console.WriteLine` directly.
- Nullable reference types are enabled — use `?` and handle null checks throughout.
