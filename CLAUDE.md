# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Verify

```bash
dotnet build                                                         # build
dotnet run --project BeastieBot3/BeastieBot3.csproj -- [command]    # run a command
dotnet run --project BeastieBot3/BeastieBot3.csproj -- show-paths   # verify INI config is loading correctly
```

No unit-test suite exists — verify CLI changes by building and running the relevant command.

For the local web UI (`serve`), read-only Playwright smoke tests live in `e2e/` (`cd e2e && npm install && npm test`). They launch `serve` on a throwaway port and only issue read-only GETs — a network guard aborts any `POST /api/jobs` so they never run a command, download, or mutate anything.

## Project Overview

.NET 9 CLI tool that aggregates biological taxonomy data from IUCN Red List, Catalogue of Life (CoL), Wikidata, and Wikipedia into local SQLite databases. Used for normalizing vernacular (common) names, resolving taxonomic synonyms, and detecting naming conflicts across sources.

## Architecture

### Command Tree

Commands **self-register via attributes** — `Program.cs` no longer hand-wires the tree (it configures only the error handler and the lone `ServeCommand`). Each command class carries a `[CommandInfo("branch sub", CommandKind.X, "description", ...)]` attribute; `CommandRegistry.ConfigureAll` (`Web/Commands/CommandRegistry.cs`) scans the assembly for these at startup and builds the entire Spectre.Console.Cli branch tree. Branches are declared once as assembly attributes in `CommandClassification.cs` (`[assembly: CommandBranch("iucn api", "...")]`). Top-level branches: `col`, `iucn`, `iucn api`, `wikidata`, `wikipedia`, `common-names`.

`CommandClassification.cs` is the **single source of truth** for the command tree. The same attributes drive the web UI catalogue (`/api/commands`): `CommandKind` (`ReadOnly` | `Mutates` | `Destructive`) gates re-confirmation, and the orthogonal `RerunEffect` (`ReadOnly` | `IdempotentAdd` | `Discovers` | `Rebuilds` | `ClearsCache` | `FreshDataset`) tells the user what a re-run will do.

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
3. Annotate the class with `[CommandInfo("branch sub", CommandKind.X, "description", Rerun = RerunEffect.Y, Examples = new[]{ ... })]`. That attribute is the registration — `CommandRegistry` derives the path, description, and examples from it. If the command introduces a new branch, add a `[assembly: CommandBranch("branch", "...")]` line to `CommandClassification.cs`. Do **not** edit `Program.cs`.

```csharp
[CommandInfo("iucn my-thing", CommandKind.Mutates, "Do the thing",
    Rerun = RerunEffect.IdempotentAdd, Examples = new[] { "iucn my-thing --limit 100" })]
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
| --- | --- |
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

## IUCN SQLite Rules

### Schema Essentials

- Key columns: `taxonId` (INTEGER NOT NULL), `assessmentId` (INTEGER NOT NULL). All other CSV columns are TEXT.
- Main view: `view_assessments_html_taxonomy_html` joins `assessments_html` with `taxonomy_html` on `taxonId`. Prefer `_html` tables for production queries.
- Boolean flags (`possiblyExtinct`, `possiblyExtinctInTheWild`) are TEXT `"true"`/`"false"` — use `IFNULL(flag,'false') = 'true'`.
- The importer recreates views every run. Missing views? Rerun `iucn import --force`.

### Performance

- **No extra CTEs** — we import one IUCN release at a time, so no `MAX(redlist_version)` needed.
- **Keep queries sargable** — never wrap columns in `LOWER()`, `UPPER()`, or `LIKE '%foo%'`. Normalize values before binding to parameters.
- Stick to exact matches on indexed columns (`kingdomName`, `className`, `redlistCategory`).
- Need a new filter? Add a matching index in the importer, don't bolt `ORDER BY` on a random column.

### Assessment Types

- **Species**: `infraType` and `infraName` are both NULL/empty.
- **Subspecies**: `infraType` contains `"ssp."` or `"subsp."` (used interchangeably).
- **Varieties**: `infraType` contains `"var."`.
- **Subpopulations/Regional**: `subpopulationName` is NOT NULL/empty. Exclude from most Wikipedia lists with `WHERE (subpopulationName IS NULL OR TRIM(subpopulationName) = '')`.

## Wikipedia List Generation

### YAML Modular Structure

Four-file config in `rules/`:

- `taxa-groups.yml` — taxonomic groups with kingdom/class/order filters (shared by lists and charts).
- `list-presets.yml` — section presets (ex, cr, threatened, etc.) with template expansion.
- `wikipedia-lists.yml` — combines taxa groups + presets via `taxa_group:` and `preset:` references.
- `chart-groups.yml` — chart group definitions referencing taxa groups, with completeness flags and template names.

Shorthand: `{ id: birds-cr, taxa_group: birds, preset: cr }`. Loader in `WikipediaListDefinitionLoader.cs` merges and expands `{taxa_name}`, `{taxa_slug}` templates.

### Status Code Mapping

| Database value | Wikipedia code |
| --- | --- |
| Critically Endangered + `possiblyExtinct='true'` | `CR(PE)` |
| Critically Endangered + `possiblyExtinctInTheWild='true'` | `CR(PEW)` |
| Critically Endangered | `CR` |
| EX, EW | Omit `year=` parameter |
| Legacy `LR/CD`, `LR/NT` | → `NT` |
| Legacy `LR/LC` | → `LC` |

### Listing Styles (`display.listing_style` in YAML)

- **Style A** (scientific name focus) — plants, invertebrates. Sort by scientific name.
- **Style B** (common name focus) — default. Always includes scientific name in parens.
- **Style C** (common name only) — mammals, birds, bats, sharks. Fallback to scientific name when no common name.

### Infrarank Display

- **Animals**: hide `"ssp."` rank label (`Genus species subspecies`).
- **Plants**: always use `"subsp."` (not `"ssp."`), keep visible.
- **Varieties**: always show `"var."`.
- Infraspecific display modes: `SeparateSections` (default, bold headers) or `GroupedUnderSpecies` (sub-bullets, abbreviated genus).

### Mustache Templates

Templates in `rules/wikipedia/templates/` use custom delimiters `<? ?>`. Inverted sections (`<?^ var ?>`) don't work with custom delimiters — use a separate boolean guard variable instead. Use `null` for falsy values (empty strings may be truthy in Stubble).

## Wikipedia Chart Generation

The `wikipedia generate-charts` command produces IUCN Red List bar chart files for the MediaWiki Extension:Chart format. For each chart group defined in `chart-groups.yml`, it generates:

- **`.tab`** — Wikimedia Commons tabular data JSON (Frictionless Data format, CC0-1.0) for the `Data:` namespace.
- **`.Bar.chart`** — Single shared Extension:Chart bar chart definition JSON for the `Data:` namespace. Per-group wikitext uses `|data=` to override the data source.
- **`.wikitext`** — Wikitext snippet using `{{image frame}}` + `{{#chart:...|data=...}}` to embed on Wikipedia, replacing templates like `{{IUCN mammal chart}}`.

### Status Category Ordering

Bars are ordered: EX, EW, CR(PE), CR(PEW), CR, EN, VU, NT, LC, DD. All bars are mutually exclusive — CR excludes PE/PEW species, and NT absorbs any LR/cd species.

### Scope Filtering

Only global species-level assessments are counted. The query excludes:

- Subspecies and varieties (`infraType` IS NULL or empty)
- Subpopulations/regional assessments (`subpopulationName` IS NULL or empty)
- Non-global scopes (`scopes LIKE '%Global%'`) — **note**: this filter needs auditing; the `scopes` column is not yet indexed.

### Chart Group Configuration (`chart-groups.yml`)

Each group references a `taxa_group` from `taxa-groups.yml` and adds:

- `comprehensive` — whether IUCN considers the group fully assessed (affects caption text).
- `template_name` — Wikipedia template this chart replaces (e.g. `IUCN mammal chart`).
- `chart_name` — used in filenames (e.g. `IUCN Red List mammals.tab`).

A `taxa_group` of `~` (null) means no taxonomic filter — counts all species in the database.

### Extension:Chart Constraints

- **No custom colors** — Extension:Chart uses a fixed 10-color accessibility palette. IUCN status colours are not available.
- **No stacked bars** — only `bar` type (grouped), not stacked.
- **Chart sizing** controlled via `{{image frame|max-width=N}}` in wikitext, not in the chart definition.
- **Localization** via `LocalizableString` objects (language-code-keyed JSON). Charts render in the wiki's content language.

## Workflow

- Always test with `--limit` before running full batches: `dotnet run --project BeastieBot3/BeastieBot3.csproj -- wikipedia generate-lists --list <id> --limit 100`
- Keep diagnostic queries in dedicated report commands — never fold them into the main list generation SQL.
- If a query exceeds ~1 minute, you almost certainly regressed the query plan.

## Detailed Documentation

| Doc | Content |
| --- | --- |
| `docs/LLM-guidelines.md` | Comprehensive schema details, query patterns, and "never again" lessons |
| `docs/wikipedia-list-formatting.md` | Wikipedia list output format specifications |
| `docs/common-names.md` | Common names aggregation workflow |
| `docs/iucn-api-discover-by-family.md` | IUCN API discovery strategy |
| `docs/wikipedia-chart-generation.md` | Chart generation workflow, Extension:Chart format, output files |

## Code Conventions

- **File-scoped namespaces**: `namespace BeastieBot3.Iucn;`
- **Brace style**: opening brace on same line.
- **Naming**: PascalCase types/methods, camelCase locals/params, `_camelCase` private fields.
- **`var`** when type is obvious from the right-hand side.
- **`Spectre.Console`** for all user-facing output (progress bars, tables, markup). Never use `Console.WriteLine` directly.
- Nullable reference types are enabled — use `?` and handle null checks throughout.
