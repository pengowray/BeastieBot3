# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Verify

```bash
dotnet build                                                         # build
dotnet test                                                          # run the xUnit suite (BeastieBot3.Tests)
dotnet run --project BeastieBot3/BeastieBot3.csproj -- [command]    # run a command
dotnet run --project BeastieBot3/BeastieBot3.csproj -- show-paths   # verify INI config is loading correctly
```

The `BeastieBot3.Tests` xUnit project pins the pure count/status logic (TaxonFilterSql count scope, IUCN status mapping, prose/classification helpers) and the in-memory store seam. It's young — coverage is the pure logic + one store, not full CLI flows — so still verify behavioural CLI changes by building and running the relevant command. Internal types are visible to the test project via `<InternalsVisibleTo>`; a store can be exercised over `:memory:` through `SqliteStore.EnableForeignKeys` + a store's `OpenFromConnection`.

For the local web UI (`serve`), read-only Playwright smoke tests live in `e2e/` (`cd e2e && npm install && npm test`). They launch `serve` on a throwaway port and only issue read-only GETs — a network guard aborts any `POST /api/jobs` so they never run a command, download, or mutate anything.

## Project Overview

.NET 9 CLI tool that aggregates biological taxonomy data from IUCN Red List, Catalogue of Life (CoL), Wikidata, and Wikipedia into local SQLite databases. Used for normalizing vernacular (common) names, resolving taxonomic synonyms, and detecting naming conflicts across sources.

## Architecture

### Command Tree

Commands **self-register via attributes** — `Program.cs` no longer hand-wires the tree (it configures only the error handler and the lone `ServeCommand`). Each command class carries a `[CommandInfo("branch sub", CommandKind.X, "description", ...)]` attribute; `CommandRegistry.ConfigureAll` (`Web/Commands/CommandRegistry.cs`) scans the assembly for these at startup and builds the entire Spectre.Console.Cli branch tree. Branches are declared once as assembly attributes in `CommandClassification.cs` (`[assembly: CommandBranch("iucn api", "...")]`). Top-level branches: `col`, `iucn`, `iucn api`, `wikidata`, `wikipedia`, `common-names`, `sprat`, `redlist`.

`CommandClassification.cs` is the **single source of truth** for the command tree. The same attributes drive the web UI catalogue (`/api/commands`): `CommandKind` (`ReadOnly` | `Mutates` | `Destructive`), and the orthogonal `RerunEffect` (`ReadOnly` | `IdempotentAdd` | `Discovers` | `Rebuilds` | `PlansDownloads` | `ClearsCache` | `Imports`), which is the pill and hover hint on the Run command page and beside Workflows command buttons (labels and hints in `app.js` `EFFECTS`; each hint must be true for every command with that effect, and `RerunNote` carries the command-specific part; a sentence several commands share is a const in `RerunNotes`, such as `RerunNotes.DuringIucnRefresh`, which every `iucn api` download command adds because each one falls back to the open refresh's cutoff). Every `Mutates`/`Destructive` command sets `Rerun` explicitly; a `ReadOnly` command may leave it unset, meaning `ReadOnly`. `ReportOnlyWith` names options that make a run only report (`--status` on `iucn api cache-all`, `wikipedia update`, `wikidata iucn-assessment-items`) and `ChangesOnlyWith` options without which a run only reports (`wikipedia prune-queue --apply`); a Workflows button for such a run is styled read-only and has no effect pill. `CommandClassificationTests` pins all of this, that every named option is one the command has, and that every effect has an `EFFECTS` entry.

**Confirmation rule** (comment above `CommandKind`): the web UI asks before a run only when that run, with the options chosen, deletes downloaded data without downloading it again (`wikidata reset-cache`), or deletes a dataset imported from files you downloaded by hand (the IUCN release zips, the ColDP zips, the SPRAT report CSV) to import it again (`iucn import` / `col import` / `sprat import` with `--force`). `wikipedia titles-dump --force` also deletes and re-imports, but the command downloads the dump itself and `wikipedia update` runs it every time (replacing the imported dump whenever a newer one is published), so it is `Mutates` and never asks. It never asks for runs that only report, add, download again over existing copies (download `--force`: each old copy stays until its new copy arrives), rebuild from data already stored locally (`iucn api project-view`, `common-names aggregate --replace`, `wikidata rebuild-indexes --force`), or delete only entries no command can use (`wikipedia prune-queue --apply`, which `wikipedia update` runs every time). A command whose runs can meet the rule is `Destructive`; `ConfirmWhen` names the options that make a run meet it (empty = every run asks), `ConfirmText` is the dialog's headline (default `Reason`). An option counts under any name the command declares for it (`-f` for `-f|--force`, via `CommandReflector.AllNamesOf`). The server decides, in `Web/Commands/CommandPreflight.cs`, served by `/api/commands/preflight`; the web UI asks it before every run (Run command form and Workflows buttons) and only follows it, and the Run button reads "Run (confirm)" exactly when the run will ask. A command can instead register a file preflight there that reads the configured files (`iucn import`: `IucnImportPreflight`, which asks only when this run would replace imported rows). `PromptOption` (`wikidata reset-cache --force`) is the option that skips the command's own terminal prompt: a web job cannot answer one (Spectre throws in a non-interactive console), so after the user confirms in the web dialog the web UI adds that option to the run, and the command refuses with a message when it has no terminal and no `--force`.

### Configuration Flow

`paths.ini` (sections: `[General]`, `[Datasets]`, `[Datastore]`; `reports_dir` lives under `[Datastore]`) → `IniPathReader` → `PathsService` (typed facade with `Get*Path()` / `Resolve*()` methods) → commands. CLI flags `--settings-dir` / `--ini-file` override the default INI location. API keys (IUCN token, Wikidata user-agent) load from `.env` via `EnvFileLoader`.

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
3. Annotate the class with `[CommandInfo("branch sub", CommandKind.X, "description", Rerun = RerunEffect.Y, Examples = new[]{ ... })]`. `Rerun` is required unless the kind is `ReadOnly`; a `Destructive` command also needs `Reason` and usually `ConfirmWhen` (see the confirmation rule above). That attribute is the registration — `CommandRegistry` derives the path, description, and examples from it. If the command introduces a new branch, add a `[assembly: CommandBranch("branch", "...")]` line to `CommandClassification.cs`. Do **not** edit `Program.cs`.

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
| `BeastieBot3/CommonNames/` | Multi-source common name aggregation, ambiguous-name and disambiguation reports (`CommonNameStore.QueryAmbiguousNames` is the one ambiguity rule, shared by list generation and `report --report ambiguous`) |
| `BeastieBot3/Col/` | Catalogue of Life ColDP import and profiling |
| `BeastieBot3/Taxonomy/` | Scientific name normalisation, authority parsing, `TaxonLadder` hierarchy |
| `BeastieBot3/Configuration/` | INI reading, path resolution, `.env` loading |
| `BeastieBot3/Infrastructure/` | `ApiImportMetadataStore`, `ReportPathResolver` |
| `BeastieBot3/WikidataEdits/` | `wikidata iucn-status-plan` dry run: IUCN status (P141) edits for Wikidata taxon items, link confidence tiers, rank variants, wbeditentity payload builders, plan store |
| `BeastieBot3/Audit/` | `redlist audit-site` generator: unified `AuditFinding` model, per-report producers, shared `HtmlListRenderer`/CSV writer, release-pinned commentary |
| `BeastieBot3/rules/` | YAML rule files and Mustache templates for list generation (`rules/audit/commentary.yml` for the audit site) |
| `BeastieBot3/BeastieLegacy/` | Legacy code — read for output format reference only; do not reuse directly |

## IUCN SQLite Rules

### Schema Essentials

- Key columns: `taxonId` (INTEGER NOT NULL), `assessmentId` (INTEGER NOT NULL). All other CSV columns are TEXT.
- Main view: `view_assessments_html_taxonomy_html` joins `assessments_html` with `taxonomy_html` on `taxonId`. Prefer `_html` tables for production queries.
- Boolean flags (`possiblyExtinct`, `possiblyExtinctInTheWild`) are TEXT `"true"`/`"false"` — use `IFNULL(flag,'false') = 'true'`.
- The importer recreates views every run. Missing views? Rerun `iucn import --force`.

### Performance

- **No extra CTEs** — one IUCN release per DB file, so no `MAX(redlist_version)` needed. This is **enforced** (downstream `COUNT(*)` / `DISTINCT redlist_version` readers don't filter by `import_id`, so a mixed DB silently double-counts). When the configured `Datastore:IUCN_sqlite_from_cvs` already holds a different release, `IucnImportCommand` imports into `IUCN_<release>.sqlite` beside it instead and prints the `paths.ini` line to change — it never edits `paths.ini`, so every other reader stays on the old release until the user does. `IucnImporter` still refuses a cross-release zip for whatever DB it is handed. `--force` re-imports zips already imported; wiping a DB that holds a *different* release additionally needs `--replace-release`. Same-release multi-zip imports accumulate. The release version is the zip path's `YYYY-N` **relative to the CSV dir**, falling back to the CSV-directory name; the command stops up front if a zip's path names a different release than the CSV folder. `ExtractRedlistVersionFromPath` strips uuids first (IUCN's `redlist_species_data_<uuid>.zip` hex groups contain digit pairs like `1373-414`) and only accepts a plausible `19xx/20xx-N` release, so ISO dates and year ranges don't read as releases; pinned by `IucnReleaseVersionTests`.
- **Keep queries sargable** — never wrap columns in `LOWER()`, `UPPER()`, or `LIKE '%foo%'`. Normalize values before binding to parameters (`TaxonFilterSql` is the reference: `value.Trim().ToUpperInvariant()` on the parameter, plain `=` on the column). Swept 2026-09-02: the remaining `TRIM(col) = ''` sites are all on unindexed columns (`infraType`, `subpopulationName`, `scopes`), where no plan is defeated and the scan happens anyway. The one live cost is the canonical `scopes LIKE '%Global%'`, which adds ~0.55s to a full-table count (0.75s vs 0.20s on 2026-1); fixing it properly means an indexed `is_global` column written at import.
- Stick to exact matches on indexed columns (`kingdomName`, `className`, `redlistCategory`).
- Need a new filter? Add a matching index in the importer, don't bolt `ORDER BY` on a random column.

### Re-importing the API cache for a new release

The API cache (`IUCN_api_cache_sqlite`) carries **no release version** — a payload downloaded during 2025-2 is indistinguishable from one downloaded today — so it is exempt from the one-release-per-DB rule and a re-import means "fetch everything again that is older than a date". `iucn api refresh-start` records that date as a **session** (`refresh_sessions` table in the cache); every download command falls back to the active session's cutoff when given no threshold flag of its own, so resuming is a plain `iucn api cache-all --full` and the date is never retyped. `cache-all` prints a phase-by-phase plan (with per-phase outstanding counts) before downloading and the standing totals after; `--status` prints just that and exits. In the web UI the whole API route is the "Build or update the API dataset" step (probe `IucnApiUpdateAll`); the individual phases live in the flow's collapsed "Step by step" panel. The cutoff is fixed, not a rolling window: `--max-age-hours` recomputes from "now" on each run, so a multi-day refresh re-fetches its own work — use `--refresh-before` or a session. `--force` re-downloads everything but ignores `downloaded_at`, so an interrupted force run restarts from the first item; prefer a session for anything long.

A session also drives the family-paging sweep and a final `--retry-tombstones` pass (404/410 tombstones are permanent and invisible to every normal run, but that verdict is only true of the release it was recorded against). It closes itself once nothing predates the cutoff and every requested phase has run; the remaining counts (`CountTaxaDownloadedBefore` / `CountAssessmentsDownloadedBefore`, which also give the session's starting totals) leave out cached rows with a 404/410 tombstone, because those keep their old `downloaded_at` and only the tombstone pass asks for them again. They are computed as "old rows" minus "old rows with a tombstone": a `NOT EXISTS` on `failed_requests` needs the row id, which is not in the `downloaded_at` index, and made the polled count read every old row with its JSON (0.04s to 43s cold at the start of a re-import). Every row the count includes must be one some step requests, or the session never closes: with a cutoff (a session, `--refresh-before` or `--max-age-hours`) `cache-taxa` also queues every taxa row older than it (a species dropped from the CSV that no cached species lists is reached by nothing else), and decides by the row whose `root_sis_id` is the id before falling back to `taxa_lookup`, which also maps a subspecies id to its species' row. The tombstone pass decides by when the 404 was recorded (`failed_requests.last_attempt_at` against the cutoff), not by a download date, for the same reason. `iucn api refresh-status` reports progress between runs; `refresh-abandon` exits without losing downloads. Decision logic is pure in `IucnRefreshMath` and pinned by `IucnRefreshSessionTests`.

`downloaded_at` is stored as a UTC `"O"` string. Read it with `StoredUtc.Parse` (`Infrastructure/StoredUtc.cs`), never plain `DateTime.TryParse`, which converts the trailing `Z` to local time and shifts every refresh boundary by the machine's offset.

### Workflow step state (the web UI's lights)

A `FlowStep` in `Web/Flows/FlowCatalogue.cs` may carry `Probe = FlowStepProbes.X`. Without one a step can only report when its command last ran — which answers a different question, and says nothing at all for a step done by hand. Probes read the real files (`IucnReleaseStateReader`, `IucnApiCacheStateReader`, `ColUpdateStateReader` + `ColArtifacts`) and return `("ok" | "todo" | "backlog", detail)`; `todo` renders amber with the detail line under the step title, `backlog` blue with "more to do" (a queue worked down over sessions, not something overdue — a permanently-190k-title Wikipedia queue is not a warning). Status precedence in `FlowEvaluator`: blocked > running > probe > job history. Decisions are pure functions over a state record — add cases to `FlowStepProbeTests` / `FlowApiProbeTests` / `FlowColProbeTests` / `FlowWikiProbeTests`.

Probes are polled every ~10s across every flow, so they must stay offline, cheap, and open databases **read-only with no schema work**. Anything that can be slow belongs behind a cache the polled path only reads: `ColUpdateStateReader.Read(paths, readArchives: false)` takes what is already cached and warms the rest on a background task, because the first read of a ColDP archive takes ~20 seconds and blocked the dashboard. `WikiCoverageStateReader.Read` returns the last background snapshot (60s TTL) and never blocks, because its counts join IUCN taxa against both caches over an `ATTACH` and take about a second. A probe that can't answer yet must say so rather than assert the negative.

CoL staleness (`ColRebuild`) is measured against the CoL import time, **not** against paths.ini's mtime — paths.ini changes for unrelated reasons, and using it marked every CoL-derived output stale forever.

### Wikidata / Wikipedia cache priority

`wikipedia update` is the one-button form: it runs the whole ladder below in order (in-process via `Program.BuildApp()`, the same way the web job runner launches commands), measures `WikiCoverageStateReader` before each rung to skip rungs with nothing to do, caps downloads at `--limit` per rung (no cap by default, like the other commands), and prints a result line under each rung plus a before/after table for the run (`WikiUpdateProgress`, a pure diff of two `WikiCoverageState` snapshots, pinned by `WikiUpdateProgressTests`). `--status` prints the plan without running anything; `--include-rest` adds failed retries and the low-priority queue; `--until-done` repeats the ladder in rounds until nothing is left or a round moves no progress metric (queue sizes and failure counts deliberately don't count, or failing retries would loop forever). Rounds 2+ skip once-per-run rungs and only re-queue titles / re-run the full match when Wikidata gained items or links. The settle rung uses `match-taxa --pending-only`.

Every gap count must be one a step can actually close, or the ladder looks stuck and its gates fire forever. `WikiCoverageStateReader`'s eligible set excludes subpopulations **and varieties** (the matcher's `ShouldSkip` and backfill's `IsEligible` skip both; 980 varieties once sat in "never checked" permanently), match-status counts are joined to the current release (old-release `pending` rows are never re-evaluated), and `TaxaNeverSearched` is counted directly, not as a subtraction. In the web UI it is the "Update the caches" step of `wiki-reports`; the individual ladder steps live in the collapsed "Step by step" panel (`FlowSection.StepByStep`).

The `wiki-reports` flow orders this work by priority rather than by pipeline stage, because the queues are far larger than any one session: bulk Wikidata sweep (`seed-taxa`, one cursor-paged SPARQL query per property) → download queued items → per-taxon search for the rest (`backfill-iucn`, slow) → queue Wikipedia titles → match → fetch what taxa are waiting on → fetch the rest; retrying failures and re-downloading old copies sit in Maintenance. The levers that make that order enforceable:

- `wikipedia fetch-pages --awaited-only` — only pages a taxon with no article is waiting on: **every candidate in its attempt log**, not just the one its match row names (`WikipediaCacheStore.AwaitedPagePredicate`, shared with the reader). Naming only the first undownloaded candidate settled a taxon with N redlink candidates over N runs; `--newest-first` takes the titles queued most recently, i.e. a new release's taxa, since the default order is oldest-first.
- `--refresh-only` (on `fetch-pages` and `wikidata cache-entities`) — re-download cached copies **without** dragging the never-fetched queue along, which is what plain `--refresh-days` / `--max-age-hours` does.
- `wikipedia prune-queue --apply` runs inside `wikipedia update` before the match step. Older matcher runs queued IUCN synonyms verbatim, authority and note included (`Eumeces schneideri (Daudin, 1802) [orth. error]`), and no article can have such a title. `BareScientificName.CarriesAuthorityOrNote` decides, and it is deliberately narrower than "Strip changed it" because the queue also holds common-name titles from Wikidata sitelinks ("Gila spotted whiptail"), which Strip cuts too. Two related fetch rules: the action API answers `invalid` (not `missing`) for a title MediaWiki cannot have, and `WikipediaApiClient` records that as missing rather than letting it reach the REST endpoint and fail with a 403 forever; and a failed download is retried once per run (`WikiFetchScope.FailedBefore`), because a failure stamps `last_seen_at` and nothing else, so without the cutoff `--failed-only --limit 2000` cycled the same few hundred titles.
- `wikidata seed-taxa` sweeps **one property per pass** (P627 IUCN taxon id, then P141 conservation status), each with its own cursor. It was one query UNIONing both and joining `instance of: taxon`, grouped and sorted; the query service stopped being able to run that inside its 60s limit and no batch size helped, because the numeric-id computation and sort happen before the `LIMIT`. Split, each pass answers in seconds. The `instance of: taxon` join also excluded 852 items that carry an IUCN taxon id but are typed as something else, so dropping it recovered data as well as time. A cache written before the split falls back to the old combined cursor rather than restarting from zero.
- `wikidata backfill-iucn` links a taxon whose name already matches exactly one cached item (method `CachedName`, no network) and keeps the link when a search finds an item already cached; both used to `continue` without writing anything, so those taxa stayed "never searched" and were re-searched every run. It records taxa it searched for and did not find (`wikidata_backfill_misses`) and skips them next run, so a run after a new release works on taxa never searched. `--retry-missing` / `--retry-missing-after <DAYS>` re-search them; that belongs in the refresh tier, and after a CoL update (new synonyms) it is worth doing.

Steps also expose their command's full option form (the same one the Run command page generates), pre-filled from the args the step declares.

### Assessment Types

- **Species**: `infraType` and `infraName` are both NULL/empty.
- **Subspecies**: `infraType` contains `"ssp."` or `"subsp."` (used interchangeably).
- **Varieties**: `infraType` contains `"var."`.
- **Subpopulations/Regional**: `subpopulationName` is NOT NULL/empty. Exclude from most Wikipedia lists with `WHERE (subpopulationName IS NULL OR TRIM(subpopulationName) = '')`.

## Wikipedia List Generation

### YAML Modular Structure

Four-file config in `rules/`:

- `taxa-groups.yml` — taxonomic groups with kingdom/class/order filters (shared by lists and charts). A group with `children:` makes parent pages that link its sub-groups' lists (the sub-group's list for the same preset; failing that, its all-status list, but only on a page that already links at least one sub-group list for its own preset); `generate-lists` warns about each sub-group a parent page cannot link. See "Parent Lists" in `docs/wikipedia-list-formatting.md`.
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
| `docs/wikidata-iucn-status.md` | Wikidata IUCN status dry run: decisions (references, item per assessment, rank variants, coordination), tiers, safety properties, what's not built |
| `docs/redlist-audit-site.md` | `redlist audit-site` generator: producers, unified model, commentary mechanism, output structure |

## Code Conventions

- **File-scoped namespaces**: `namespace BeastieBot3.Iucn;`
- **Brace style**: opening brace on same line.
- **Naming**: PascalCase types/methods, camelCase locals/params, `_camelCase` private fields.
- **`var`** when type is obvious from the right-hand side.
- **`Spectre.Console`** for all user-facing output (progress bars, tables, markup). Never use `Console.WriteLine` directly.
- Nullable reference types are enabled — use `?` and handle null checks throughout.
