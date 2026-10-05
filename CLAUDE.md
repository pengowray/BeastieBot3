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

The public species site has its own test project, `BeastieBot3.Site.Tests` (net10.0, WebApplicationFactory over a fixture database built from `SiteDbSchema.Ddl`); `dotnet test` runs it and `BeastieBot3.Tests`. Building the solution needs the .NET 10 SDK (installed in `~/.dotnet` beside .NET 9).

For the local web UI (`serve`), read-only Playwright smoke tests live in `e2e/` (`cd e2e && npm install && npm test`). They launch `serve` on a throwaway port and only issue read-only GETs — a network guard aborts any `POST /api/jobs` so they never run a command, download, or mutate anything.

## Project Overview

.NET 10 CLI tool that aggregates biological taxonomy data from IUCN Red List, Catalogue of Life (CoL), Wikidata, and Wikipedia into local SQLite databases. Used for normalizing vernacular (common) names, resolving taxonomic synonyms, and detecting naming conflicts across sources.

## Architecture

### Command Tree

Commands **self-register via attributes** — `Program.cs` no longer hand-wires the tree (it configures only the error handler and the lone `ServeCommand`). Each command class carries a `[CommandInfo("branch sub", CommandKind.X, "description", ...)]` attribute; `CommandRegistry.ConfigureAll` (`Web/Commands/CommandRegistry.cs`) scans the assembly for these at startup and builds the entire Spectre.Console.Cli branch tree. Branches are declared once as assembly attributes in `CommandClassification.cs` (`[assembly: CommandBranch("iucn api", "...")]`). Top-level branches: `col`, `iucn`, `iucn api`, `wikidata`, `wikipedia`, `common-names`, `sprat`, `redlist`, `site`.

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
- Commands that only read a store open it with `OpenReadOnly` (`CommonNameStore`, `WikipediaCacheStore`): no folder creation, no WAL pragma, no schema work, and any write fails. `WikipediaCacheStore.OpenReadOnly` returns null when the file is missing.
- Microsoft.Data.Sqlite 10.0.12 bundles SQLite 3.53.3, built with `SQLITE_DQS=0`. Write string values in single quotes or as parameters: a double-quoted word must be a column, a select alias or a table, or the statement (including `CREATE INDEX`) fails with "no such column" (`SqliteQuotingTests`). When a column exists only in some datasets, check it with `PRAGMA table_info` first (`SpratTableColumns` in `BeastieBot3/Sprat`: `Has` tests a column and `Select` gives `NULL` for a missing one; `SpratListQueryService` and `site build-db` use it).

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
| `BeastieBot3/Wikipedia/` | Wikipedia page fetching and taxon matching (`TaxonPageMatcher` matches one taxon to a page for `match-taxa`; `WikiPageKingdom` decides whether a page is about a taxon in another kingdom) |
| `BeastieBot3/WikipediaLists/` | Wikipedia list generation using YAML definitions + Mustache templates |
| `BeastieBot3/CommonNames/` | Multi-source common name aggregation, ambiguous-name and disambiguation reports. `CommonNameChooser` is the one place a taxon's English name is chosen (a `rules-list.txt` override, else the store's best name with junk and ambiguous names skipped and the capitalization rules applied, then a check that the name is usable as a common name), used by the lists, `site build-db` and `report --report trace`; it takes the store's `taxa.id`. `CommonNameStore.QueryAmbiguousNames` builds `AmbiguousNames`, the one ambiguity rule, shared by the chooser and `report --report ambiguous`: a name two or more taxa have is kept by the taxon with the best source priority for it (`KeeperPriority`), a species beats its own subspecies/varieties/subpopulations on a tie, and every other taxon skips it. When the only taxa with the name are a species and its own subspecies/varieties/subpopulations, the chooser's source order (`GetSourcePriority`, which puts a taxobox name before IUCN's main name) decides instead; on a tie for first place the species keeps the name, or no taxon does when the species is not among the tied. Store taxa with the same scientific name and kingdom (an old IUCN id beside its current id) count as one taxon, so a name kept by one of them is kept by all of them (`AmbiguousNames.Keeps(taxonId, name)`). `CommonNameQuality` classifies a name as good, repairable or junk. `ScientificNameCheck` decides which Wikipedia titles, taxobox names and Wikidata labels `aggregate` stores (the taxon's own names first, then a shape test and a word test against the store's genera, epithets and the words of IUCN/CoL English names). `WikipediaPageMatch` decides which of the taxa matched to one Wikipedia page take its names: the taxon named in the page's taxobox, else the taxa matched by their own name. A species or subspecies matched to a page about its genus or a higher taxon takes none of the page's names, unless the taxobox marks the genus monotypic or the store has exactly one species in it (`IsGenusPageOfSpecies`, which reads the taxobox parameters, not the rank stored in the cache). A title or taxobox name equal to the page's own taxobox taxon is a scientific name (`IsTaxoboxSubject`). `CommonNameStore.GetWikipediaArticleTitle` returns `GetWikipediaNamePage` (the page a taxon's `wikipedia_title` or `wikipedia_taxobox` name came from), else `GetMatchedWikipediaPage` (the page of its `exact` Wikipedia cross-reference). The lists (`StoreBackedCommonNameProvider`) link the taxon's own scientific name instead of that matched page when English Wikipedia has a page or redirect with that title that is not a disambiguation page (`EnwikiTitleCheck` finds the title in `enwiki_dump_titles` or as a cached `wiki_pages` row; a cached disambiguation page, or a cached redirect to one, does not count), so the line for Leucoraja wallacei links `[[Leucoraja wallacei]]` (a redirect to the genus article), not `[[Leucoraja]]`. They skip any page that `WikiPageKingdom` finds is about a taxon in another kingdom. When the taxon's own scientific name is a cached disambiguation page and no other step gives a page, they link that name followed by a bracketed word for the taxon's kingdom ("Ficus variegata (plant)", "Orestias elegans (fish)"). They use a title only as a link target. Details in `docs/common-names.md` |
| `BeastieBot3/Col/` | Catalogue of Life ColDP import and profiling; `col build-placement` (CoL groups for list headings) |
| `BeastieBot3/Taxonomy/` | Scientific name normalisation, authority parsing, `TaxonLadder` hierarchy, `TaxonomyTreeBuilder` (list headings), `TaxonPlacement*` (CoL nodes between IUCN ranks) |
| `BeastieBot3/Configuration/` | INI reading, path resolution, `.env` loading |
| `BeastieBot3/Infrastructure/` | `ApiImportMetadataStore`, `ReportPathResolver` |
| `BeastieBot3/WikidataEdits/` | `wikidata iucn-status-plan` dry run: IUCN status (P141) edits for Wikidata taxon items, link confidence tiers, rank variants, wbeditentity payload builders, plan store. The P141 value table is `WikidataStatusValues` in `BeastieBot3.Shared` |
| `BeastieBot3/Audit/` | `redlist audit-site` generator: unified `AuditFinding` model, per-report producers, shared `HtmlListRenderer`/CSV writer, release-pinned commentary |
| `BeastieBot3/rules/` | YAML rule files and Mustache templates for list generation (`rules/audit/commentary.yml` for the audit site) |
| `BeastieBot3.Shared/` | Code shared by the CLI and the public site: `IucnCitationParts`, the `{{cite iucn}}` / `{{IUCN status}}` / taxobox status / name-italics renderers, `SpeciesListLine` (the bullet line of a generated list; `SpeciesLineFormatter` resolves the names and builds its `SpeciesListEntry` with `ToEntry`), `WikidataCitation` (`{{cite Q}}` and QuickStatements v1 commands and links, with the assessment item model `WikidataItemModel`), `WikidataStatusEdit` (QuickStatements commands for a taxon item's IUCN status, P141), `WikidataStatusValues` (the P141 value for each IUCN category), and the site database schema (`SiteDbSchema`) |
| `BeastieBot3/SiteBuild/` | `site build-db` builds the public site's database; `site check-citations` reports on citation parsing; `IucnCitationPartsParser` (title annotations, putting a citation's parts together), `IucnDoiSelector` (DOI selection), `SiteNameSet` (the site's name table), `SiteWikidataItems` (chooses each assessment's Wikidata item and lists the properties it has), `P141JsonReferences` (reads the P141 references that the Wikidata cache's index does not record) |
| `BeastieBot3/Iucn/Citations/` | IUCN citation code shared by the Wikidata dry run and the site build: `CreditNameSplitter` (splits a `credits[].full` string into names, told how many to expect by the `value[]` count), `IucnAuthorNameParser` (one name as a person, an organisation or a name kept as published), `AssessorNamePool` (repairs names with a letter lost to an encoding error), `AssessorGivenNames` (a person's full given names from `value[]`, only when exactly one entry fits the surname and initials), `IucnCitationText` (`StripAccessedOn`, `ExtractDoi`) |
| `BeastieBot3/Iucn/Doi/` | `iucn resolve-dois`: for assessments that have no DOI from IUCN's citation, GBIF or Wikidata, finds DOIs in Crossref's list of IUCN DOIs and by doi.org handle lookups, and saves them in the DOI cache (`Datastore:IUCN_doi_cache_sqlite`). See `docs/public-site.md` |
| `BeastieBot3/Iucn/Gbif/` | `iucn gbif-download` and the reader for GBIF's CC BY 4.0 copy of the IUCN checklist (the main DOI source) |
| `BeastieBot3.Site/` | The public species site (net10.0 Razor Pages). Opens the site database read-only. References only `BeastieBot3.Shared`, never the `BeastieBot3` project, whose local web UI (`BeastieBot3/Web`) runs commands |
| `deploy/oracle/` | Scripts and README for running the site on an Oracle Cloud Always Free VM behind Caddy |
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
- **A `LIMIT` can change the plan.** Adding `LIMIT` to `col report-subgenus-homonyms` made SQLite drop the automatic index on the materialized genus list and compare every genus with every subgenus: over 15 minutes instead of about 5 seconds. Apply a report's `--limit` in C# while reading rows, never as an SQL `LIMIT`, and compare `EXPLAIN QUERY PLAN` with and without the `LIMIT`.
- **A correlated `NOT EXISTS` against `view_assessments_html_taxonomy_html` reads `assessments_html` again for every outer row.** Build the inner set once with `WITH x AS MATERIALIZED (SELECT DISTINCT ...)`, which SQLite searches through an automatic index: `iucn report-orphan-infraranks` went from 77 s to 1.2 s (`IucnOrphanInfraranksReportCommand.OrphanQuery`, shared with the audit site).

### Re-importing the API cache for a new release

The API cache (`IUCN_api_cache_sqlite`) carries **no release version** — a payload downloaded during 2025-2 is indistinguishable from one downloaded today — so it is exempt from the one-release-per-DB rule and a re-import means "fetch everything again that is older than a date". `iucn api refresh-start` records that date as a **session** (`refresh_sessions` table in the cache); every download command falls back to the active session's cutoff when given no threshold flag of its own, so resuming is a plain `iucn api cache-all --full` and the date is never retyped. `cache-all` prints a phase-by-phase plan (with per-phase outstanding counts) before downloading and the standing totals after; `--status` prints just that and exits. In the web UI the whole API route is the "Build or update the API dataset" step (probe `IucnApiUpdateAll`); the individual phases live in the flow's collapsed "Step by step" panel. The cutoff is fixed, not a rolling window: `--max-age-hours` recomputes from "now" on each run, so a multi-day refresh re-fetches its own work — use `--refresh-before` or a session. `--force` re-downloads everything but ignores `downloaded_at`, so an interrupted force run restarts from the first item; prefer a session for anything long.

A session also drives the family-paging sweep and a final `--retry-tombstones` pass (404/410 tombstones are permanent and invisible to every normal run, but that verdict is only true of the release it was recorded against). It closes itself once nothing predates the cutoff and every requested phase has run; the remaining counts (`CountTaxaDownloadedBefore` / `CountAssessmentsDownloadedBefore`, which also give the session's starting totals) leave out cached rows with a 404/410 tombstone, because those keep their old `downloaded_at` and only the tombstone pass asks for them again. They are computed as "old rows" minus "old rows with a tombstone": a `NOT EXISTS` on `failed_requests` needs the row id, which is not in the `downloaded_at` index, and made the polled count read every old row with its JSON (0.04s to 43s cold at the start of a re-import). Every row the count includes must be one some step requests, or the session never closes: with a cutoff (a session, `--refresh-before` or `--max-age-hours`) `cache-taxa` also queues every taxa row older than it (a species dropped from the CSV that no cached species lists is reached by nothing else), and decides by the row whose `root_sis_id` is the id before falling back to `taxa_lookup`, which also maps a subspecies id to its species' row. The tombstone pass decides by when the 404 was recorded (`failed_requests.last_attempt_at` against the cutoff), not by a download date, for the same reason. `iucn api refresh-status` reports progress between runs; `refresh-abandon` exits without losing downloads. Decision logic is pure in `IucnRefreshMath` and pinned by `IucnRefreshSessionTests`.

To re-check tombstones after a session has closed, run `cache-taxa --retry-tombstones --refresh-before <the session's cutoff>` (`refresh_sessions.cutoff_utc`, which `refresh-status` prints). It then asks only about the 404s recorded before the cutoff, and a stopped run carries on where it left off. Without a cutoff it asks about every tombstone, and a stopped run starts again from the first one.

`cache-assessments` works on one queue per run:

- the default queue: the assessment ids listed in cached taxon records. With a cutoff, it also takes cached payloads older than the cutoff that no taxon record lists (`GetAssessmentsNoTaxonRecordLists`), because the session's remaining count includes them;
- `--csv-missing`: the CSV export's assessment ids with no cached payload. These are mostly subpopulations (`taxa_lookup` maps a subpopulation id to its species' record, whose `assessments[]` leaves the subpopulation out) and taxa whose own taxon record the API answers with 404. With `--force` it asks again about tombstoned ids, but does not download CSV assessments that are already cached;
- `--stale-latest`: cached payloads whose `latest` flag differs from their taxon record's and that were downloaded before that record. When the payload is newer, the taxon record is the stale one, so the command prints a `cache-taxa --refresh-before` line instead of queueing the payload. This queue reads every payload (7 to 8 seconds when the cache file is already in memory).

`--refresh-before` and `--max-age-hours` are refused with `--csv-missing` and `--stale-latest`. `cache-all` runs `cache-assessments --csv-missing` after `cache-assessments`, except when there is no CSV database or when `--skip-assessments` or `--assessment-failed-only` is given. It runs `cache-assessments --stale-latest` when it is given `--stale-latest` or `--full`. Its plan lists both phases. `--retry-tombstones` also asks about tombstoned assessment ids that no taxon record lists.

`iucn api project-view` reads whether an assessment is current from its taxon record (`taxa_assessment_backlog.latest`), because a payload keeps the `latest` flag it had when it was downloaded. It uses the payload's own flag only for payloads that no taxon record lists (the ones `--csv-missing` downloads). `project-view` empties the projection before writing, so a build that stops part way leaves an empty projection. `IucnProjectionState.Exists` therefore means the file holds a finished build: when the newest build record has no end time (`UnfinishedBuildStartedAt`), the projection reads as empty on the workflow lights and on `/api/dataset-compare`.

`cache-taxa` and `cache-assessments` decide which queued items are due before the progress bar starts and print the split on one line ("Taxa in the queue: N. To download: N. Already downloaded after <cutoff>: N. ..."), so the bar and its time estimate count downloads only; `--limit` trims the queue before that decision. After a 429, `IucnApiClient` waits for Retry-After (IUCN sends none, so it waits `IUCN_API_RATELIMIT_SECONDS`, default 60 seconds) and then keeps a minimum time between request starts (`IucnApiPace` in `Iucn/IucnApiClient.cs`): 0 until the first 429, then 1 s, 1.5 times longer after each further 429 up to 5 s, 5% shorter after every 100 answers that are not 429, and back to 0 once it is under 1 s. The constants are in code, not in `IucnApiConfiguration`. Each command has its own client, so each `cache-all` phase starts with no minimum time. The minimum time and the pause after a 429 are measured on the monotonic clock (`TimeProvider.GetTimestamp`), so setting the system clock back does not delay requests; a Retry-After date is compared with the system clock. Tests pass a fake `TimeProvider` to `IucnApiClient`'s internal constructor.

`downloaded_at` is stored as a UTC `"O"` string. Read it with `StoredUtc.Parse` (`Infrastructure/StoredUtc.cs`), never plain `DateTime.TryParse`, which converts the trailing `Z` to local time and shifts every refresh boundary by the machine's offset.

### Workflow step state (the web UI's lights)

A `FlowStep` in `Web/Flows/FlowCatalogue.cs` may carry `Probe = FlowStepProbes.X`. Without one a step can only report when its command last ran — which answers a different question, and says nothing at all for a step done by hand. Probes read the real files (`IucnReleaseStateReader`, `IucnApiCacheStateReader`, `ColUpdateStateReader` + `ColArtifacts`, `PublicSiteStateReader` in `Web/Flows/PublicSiteState.cs`) and return `("ok" | "todo" | "backlog", detail)`; `todo` renders amber with the detail line under the step title, `backlog` blue with "more to do" (a queue worked down over sessions, not something overdue — a permanently-190k-title Wikipedia queue is not a warning). The detail line is plain text (`textContent`), so probe strings must not use backticks; step notes are rendered with code spans. Status precedence in `FlowEvaluator`: blocked > running > probe > job history. Decisions are pure functions over a state record — add cases to `FlowStepProbeTests` / `FlowApiProbeTests` / `FlowColProbeTests` / `FlowWikiProbeTests` / `PublicSiteProbeTests` (and `PublicSiteStateReaderTests`, `SiteDoiCountTests`, `DatasetCompareTests` for the readers).

The time a SQLite file last changed is the newer of the main file's time and the `-wal` file's time, counting the `-wal` file only when it is not empty, and never the `-shm` file, which every reader touches (`PublicSiteStateReader.SqliteChangedAt`; `DatasetStatsService`'s cache key includes the `-wal` file too). The IUCN API cache's change time is `MAX(downloaded_at)`, not the file's time.

A step's `InputSourceIds` are required inputs: a missing one blocks the step. An input that the command uses only when it exists (the GBIF checklist and the DOI cache for `site build-db` and `iucn resolve-dois`) belongs in the `OutputSourceIds` of the step that makes it, never in `InputSourceIds`. `FlowCatalogueTests` checks every workflow button against `CommandRegistry` and the command's options, so a renamed command or option fails the tests instead of showing "Unknown command", and checks that every source id is in `DataSourceCatalogue`. `StatusService` opens every database read-only without pooling: `site build-db` renames a new file over `site.sqlite`, and a pooled handle kept reading the old file (on Windows it would block the rename).

Probes are polled every ~10s across every flow, so they must stay offline, cheap, and open databases **read-only with no schema work**. Anything that can be slow belongs behind a cache the polled path only reads: `ColUpdateStateReader.Read(paths, readArchives: false)` takes what is already cached and warms the rest on a background task, because the first read of a ColDP archive takes ~20 seconds and blocked the dashboard. `WikiCoverageStateReader.Read` returns the last background snapshot (60s TTL) and never blocks, because its counts join IUCN taxa against both caches over an `ATTACH` and take about a second. `SiteDoiCountReader` (`Web/Flows/PublicSiteDoiCount.cs`) makes the count for the light of the public site's "Find missing DOIs" step. It runs `IucnDoiScopeReader` (about 9 to 17 s) on a background task, and runs it again at once when the IUCN Red List database or the GBIF checklist changes, and at most every 30 minutes when only the API cache or the Wikidata cache changed. The part of the count that reads the DOI cache is one primary-key lookup per assessment on a read-only connection, repeated whenever the DOI cache changes. Only `PublicSiteStateReader.Read(PathsService)` starts it; `Read(PublicSitePaths)` starts no background work, for tests. A probe that can't answer yet must say so rather than assert the negative.

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

`wikipedia match-taxa` rejects a page about a taxon in another kingdom (the plant *Ficus variegata* was matched to "Ficus variegata (gastropod)" through a Wikidata item that `backfill-iucn` had linked to it by name). `WikiPageKingdom.Conflict` looks for group words in four places: the taxobox `kingdom`, the bracketed word after the taxobox `genus` or `taxon` ("Ficus (gastropod)"), the bracketed word at the end of the page's title and of the title that redirected to it, and the "<group> described in <year>" categories. A table gives the kingdoms each group word allows ("gastropod": ANIMALIA; "plant": PLANTAE or CHROMISTA). A page is about another kingdom only when at least one place names a group in the table and no place names a group in the taxon's own kingdom. A title whose bracketed word names another kingdom is rejected without being queued for download. The matcher also tries each IUCN scientific name of the taxon followed by a bracketed word for its kingdom ("Ficus variegata (plant)", match method `kingdom-qualified`), after the IUCN names and before the synonyms, when the cache has downloaded that title or `enwiki_dump_titles` lists it. Every run, `--pending-only` included, checks the page of each existing match against the taxon's kingdom and matches a taxon again when its page is about another kingdom; the summary row "Already matched to a page about another kingdom, checked again" counts these taxa. `TaxonPageMatcher` contains the matching for one taxon, so tests run it over an in-memory cache.

`wiki_taxobox_data` is written when a page is downloaded, so after a change to `Taxonomy/TaxoboxParser`, run `wikipedia reparse-taxoboxes --dry-run`, then `wikipedia reparse-taxoboxes` (it parses the cached wikitext again and saves only the rows that changed, 500 pages per transaction, downloading nothing), then `common-names aggregate --source wikipedia --replace` to read the common names again from the new fields.

### Assessment Types

- **Species**: `infraType` and `infraName` are both NULL/empty.
- **Subspecies**: `infraType` contains `"ssp."` or `"subsp."` (used interchangeably).
- **Varieties**: `infraType` contains `"var."`.
- **Subpopulations/Regional**: `subpopulationName` is NOT NULL/empty. Exclude from most Wikipedia lists with `WHERE (subpopulationName IS NULL OR TRIM(subpopulationName) = '')`.

## Wikipedia List Generation

### YAML Modular Structure

Four-file config in `rules/`:

- `taxa-groups.yml` — taxonomic groups with kingdom/class/order filters (shared by lists and charts). A group's `sub_groups:` block (rank, presets, one line per value) splits its pages by one rank: `TaxaSubGroups.Expand` turns each entry into a group with the parent's filters plus that rank, a place in the parent's `children`, and list entries placed after the parent's in `wikipedia-lists.yml`; the loader and the Taxa grouping page both read groups through it. Used for the order splits that keep pages under Wikipedia's 2 MB and ~3,600-template limits. A group with `children:` makes parent pages that link its sub-groups' lists (the sub-group's list for the same preset; failing that, its all-status list, but only on a page that already links at least one sub-group list for its own preset); `generate-lists` warns about each sub-group a parent page cannot link. See "Parent Lists" in `docs/wikipedia-list-formatting.md`.
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

### Catalogue of Life groups in headings

IUCN classifies only by kingdom, phylum, class, order, family and genus, so a list could not separate snakes from lizards inside Squamata or toothed from baleen whales. Headings can now include the CoL nodes between two IUCN ranks (suborder Serpentes, order Cetacea with suborders Odontoceti/Mysticeti, infraclasses Batoidea/Selachii, subfamilies and tribes). IUCN stays the backbone: CoL nodes are only inserted between IUCN ranks, never used to move a taxon to another IUCN order or family.

- **Placement** (`col build-placement`, `Col/TaxonPlacementBuild.cs`, `Col/TaxonPlacementStore.cs`, pure rules in `Taxonomy/TaxonPlacementBuilder.cs`): every IUCN global species is matched to its accepted CoL usage (`Col/ColLineageMatcher.cs`: synonyms followed through `parentID`, "provisionally accepted" accepted, kingdom checked on the accepted row; 99.3% of 2026-1) and its `parentID` chain is read. For each IUCN class/order, order/family and family/genus pair, a CoL node is kept when at least 80% of the IUCN taxon's matched species have it (vote) and the IUCN parent holds at least 90% of that node's matched species, counted over the whole lineage (containment: CoL order Cetacea passes under IUCN Artiodactyla; CoL Perciformes fails under IUCN Scorpaeniformes). The vote runs once over the whole release, never per list, so a family sits under the same CoL node on every page. Unmatched species inherit their family's or genus's path. A node whose CoL rank is not strictly between the two IUCN ranks (Cetacea inside order Artiodactyla) has `ShowRank = false` and its heading shows the name without a rank.
- **Store**: `<COL_sqlite>.placement.sqlite`, keyed by the IUCN database (path + size + mtime, WAL included) and stamped with the CoL file, `AlgorithmVersion` (vote/containment rules; bump = re-vote in seconds) and `MatcherVersion` (matching rules; bump = cold rematch, about 2 minutes). Species matches and CoL nodes are cached per CoL file, so a new IUCN release re-votes in seconds. Anchor columns are stored upper-cased and indexed so a future page filter can `ATTACH` the file and join on the IUCN columns. One placement file holds a placement for each IUCN database it was built from (the CSV database, and the API projection with `--dataset api`), so building the placement for one IUCN database leaves the placement for the other current. `wikipedia generate-lists` loads it, building it first when missing or stale; `--no-col-enrichment` turns it off. The old `.enrich-cache.sqlite` sidecar and `ColTaxonomyEnricher` are gone.
- **Headings** (`TaxonomyTreeBuilder` intermediate layers, gates in `intermediate_groups:` in `wikipedia-lists.yml`): before grouping by a configured level, the CoL nodes between it and the IUCN rank above become one heading layer when every gate passes: at least 30 species, at least 6 values of the level below, the layer leaves at most 80% as many headings, at most 15 CoL groups, no CoL group above 90% of the species, at least 5 species outside the largest group, and the heading budget allows it. A CoL group covering a single family is demoted to that family. A group that holds nearly everything is looked through (subclass Neoselachii, then Batoidea/Selachii). At most one CoL layer between two configured levels (`max_layers`). A layer is only tried when every entry has the same IUCN parent, so mixed-class pages get none. Below family, auto-split reads subfamily/tribe/subtribe from the family-to-genus path, and rejects a split where one group holds over 85% (`max_dominance`), trying the next rank.
- **Heading budget**: H2 is the status section and nothing goes deeper than H6. Every configured level reserves one heading level; a CoL layer, a virtual group heading or an auto-split is added only when the levels after it still fit.
- **Virtual groups** (`taxon-rules.yml`) are matched by CoL clade first (`clades: [Serpentes]`), then by family list, then the default group, and run inside the tree builder, so lumping, CoL layers and auto-split apply inside Snakes/Lizards/Cetaceans. A taxon with one non-empty virtual group gets no virtual heading.
- `structure-metrics.json` records every auto-split and layer decision (`decisions`, `layer_decisions`), which is how to see why a heading did or did not appear.
- **IUCN "NOT ASSIGNED"** (`rules/iucn-not-assigned.yml`, `Iucn/IucnNotAssignedRules.cs`): IUCN stores the text "NOT ASSIGNED" for some orders (8 ray-finned fish families, a few fungi) and families (corals, fungi). A rule gives a family an order, or a genus a family. The rules are applied by a TEMP VIEW with the IUCN view's name on the list generator's, the charts', the grouping page's and the placement build's read-only connections, so headings, page membership, counts and the CoL vote agree; the audit site and reports open their own connections and see IUCN's values. Those connections must be opened with `IucnNotAssignedRules.ConnectionString` (pooling off): a pooled connection keeps its temp view and would hand it to the next caller (`ApplyTo` refuses a pooled connection). The rules' fingerprint is part of the placement's IUCN stamp, so editing the file rebuilds the placement (state `NotAssignedRulesChanged`). A "NOT ASSIGNED" value with no rule gets no heading (`ShouldSkipGroup`, taken out before lumping, still counted as one small group for `min_groups_for_other`); its items are grouped by the next level beside the other headings, with no "Other" lumping there. On a parent page, `BuildChildBreakdown(splitNotAssigned: true)` and the orphan sections both list such a value by the next rank (table row and section "Pomacentridae"). Two rules for one taxon are rejected. generate-lists without `--rules`, `col build-placement` and the workflow light read the file from `RulesPaths.Resolve(paths).SourceRulesDir` (`IucnNotAssignedRules.LoadForPaths`), so they agree on whether the placement is current. `col build-placement --report` lists every "NOT ASSIGNED" taxon with its rule and CoL's placement, with a rule line to copy.
- CoL has nothing between order and family for birds, ray-finned fish orders or flowering plants, and no subfamilies for Fabaceae, Orchidaceae, Poaceae or Cyperaceae; those lists keep IUCN's ranks.

### Posting drafts to Wikipedia

`wikipedia post-drafts` posts every generated list (IUCN and `australia/`) to `User:Beastie Bot/Draft 2026/<article title>` (`--base`) plus an index at the base title. Which files are posted comes from the list definitions (`wikipedia-lists.yml`, `SpratListGroups`), never a folder listing. `DraftPageBuilder` (pure, pinned by `DraftPageBuilderTests`) moves `[[Category:]]` links into `{{Draft categories}}` (user pages must not be in article categories) and adds `__NOINDEX__` and a banner dated from the file's mtime, so reposting an unchanged file gives identical text and the sha1 comparison against the live revision skips it. Lists over 2048 KiB are not posted and are named on the index. Without `--apply` it only reads Wikipedia. `--apply` logs in with a bot password from `WIKIPEDIA_BOT_USERNAME` / `WIKIPEDIA_BOT_PASSWORD` (shell env or `.env`); saves use `assert=user`, `maxlag=5`, `watchlist=nochange` and a 10 s delay.

## Public Species Site

`BeastieBot3.Site` is an unofficial public site (Beastie Bot Species Status) that shows each IUCN Red List taxon's latest global assessment, history, regional assessments, names and links, and generates `{{cite iucn}}` (with the assessment's authors and a DOI), `{{IUCN status}}` and taxobox status wikitext. Full notes: `docs/public-site.md`; deployment: `deploy/oracle/README.md`.

- `site build-db` writes `Datastore:site_sqlite` from the CSV export (taxa and latest assessments), the API cache (history and citations), GBIF's checklist (DOIs; `Datasets:GBIF_IUCN_dir`), the DOI cache (DOIs found by `iucn resolve-dois`), the common names store (English names), the Wikidata and Wikipedia caches (Wikidata items of taxa and of assessments, the taxon items' P141 statements, article titles), the CoL placement file (Catalogue of Life ids) and SPRAT (EPBC listings in `epbc_listing`, for whole taxa and for populations; the build warns and skips SPRAT when the `sprat_species` table is missing, and warns and reads NULL when a listed-name column is missing). Taxa that are in the API cache but not in the CSV export are rows with `in_release = 0`, with history only and a `current_taxon_id` pointing to the taxon in the release with the same name; `taxon_link` (`SiteTaxonLinks`) holds that link and a second kind, to the one taxon in the same kingdom whose IUCN synonyms list the old id's name, and the site shows the global assessments of linked ids in one "Combined assessment history" table. `assessment.has_taxonomic_notes` records only whether IUCN's taxonomic notes have text, never the notes. It replaces the file only when the build finishes; a running site switches to the new file within about 30 seconds. Increase `SiteDbSchema.Version` whenever a table or column is added, removed, renamed or changes what it holds; the site answers 503 for any other version, so deploy the new site and the rebuilt database together.
- After a new Red List release, run `iucn resolve-dois --refresh-crossref`, then `iucn resolve-dois --scope latest-regional`, before `site build-db`. The web UI's "Update the public species site" workflow (`public-site`) has the whole update in order, with lights.
- The IUCN Red List Terms of Use limit what the site may hold and offer: the site database must not contain narrative text or coded threats, habitats or countries, and the site must not offer downloads or an API that returns assessment fields. Leave such fields out of the database itself; hiding them on the page is not enough.
- A DOI is only taken from a source that states it: IUCN's citation text, GBIF, Wikidata, Crossref's list of IUCN DOIs, or a doi.org lookup that confirms a candidate exists (`iucn resolve-dois`). It is used only when the taxon id and assessment id inside it match the assessment's (`IucnDoiSelector`). A DOI built from a year is never used unless doi.org says it exists.
- `common_name_en` is chosen by `CommonNameChooser`, the lists' own rule, so a change to the ambiguity rule changes the English names on the site too. The `name` table leaves out common names that `CommonNameQuality` finds to be junk and stores repairable ones repaired (`SiteNameSet`); the build summary counts both.
- Full given names: `IucnCitationPartsParser` sets `CitationAuthor.GivenNames` ("Catherine" for "Sayer, C.") with `AssessorGivenNames`, from the assessor credit's `value[]` list, only when exactly one entry fits the surname and every initial. In 2026-1, 89.4% of the person entries in the author lists of latest assessments have given names. `CiteIucnOptions.FullGivenNames` makes `CiteIucnRenderer` use them; on the site it is the option "Full given names instead of initials" (`fullnames=1`).
- Schema 4 adds `assessment.wikidata_item_qid` / `wikidata_item_properties` and the meta key `wikidata_item_model`. `SiteWikidataItems` takes the items from the Wikidata cache table that `wikidata iucn-assessment-items` fills (kept when P31 is scholarly article, data set or evaluation and not taxon; an errata version shares the item of the assessment that its DOI names). The item model comes from `rules/wikidata/iucn-status.yml` (`WikidataIucnEditConfig.LoadFromRules` / `ToItemModel`), the same file and model as the Wikidata dry run; `WikidataItemModel` in `BeastieBot3.Shared` has the defaults for a missing file or key. `WikidataCitation` writes `{{cite Q}}` and QuickStatements commands that create an item, add the statements it lacks, or replace its title and English label. `Pages/WikidataCite.cs` shows them and catches an exception from each call, so a failing call leaves out only its own box. The site never edits Wikidata.
- Schema 8 adds `taxon.wikidata_p141`, `wikidata_item_downloaded`, `wikidata_p627_deprecated`, `wikidata_other_items` and `assessment.wikidata_item_titles`, `wikidata_item_label_en`, `wikidata_item_assessment_id` (schema versions 6 and 7 were used only on a branch, never on main). With the item's titles, `WikidataCitation.FixCommands` replaces a "Name: author list" title with "Name" (it removes the old title by its exact text and language) and sets the English label from the item model. The name in titles and labels comes from `TitleNameFor`: the item's own title, else the title Crossref registered for the DOI (`iucn resolve-dois` stores it in `crossref_works.title`; `site build-db` copies its name into `IucnCitationParts.RegisteredName`), else IUCN's citation name.
- The "IUCN conservation status on Wikidata" part (`WikidataStatusEdit`) compares the taxon item's P141 with the latest global assessment, counting only statements with a reference that cites IUCN, and gives Replace and Keep QuickStatements commands; Replace removes only IUCN statements at the item's best rank. It gives no commands for a name-matched item. When two or more items state the taxon's IUCN taxon ID (at any rank), or the taxon's item states it only at deprecated rank (two kinds of link in the dry run's tiers C and D), it gives no commands either, and the assessment item commands leave out main subject (P921).
- A Wikidata item that `backfill-iucn` matched to a taxon by name is left out of `taxon.wikidata_qid` when its English description names a group in another kingdom ("species of insect" for a plant; `SiteBuildRules.DescribesAnotherKingdom`, 13 items in 2026-1).
- `wwwroot/site.js` updates the wikitext as the citation options change: it GETs the page with the form's query and replaces the `data-live-region` elements, never the form. After a 429 answer, the script writes a message in the status line and shows the Update wikitext button again; after any other failure, the browser goes to the page with the new options. `BeastieBot3.Site.Tests/browser/live-update.cjs` is the Playwright check (steps in `docs/public-site.md`).
- `/update` (`Pages/Update.cshtml`, logic in `BeastieBot3.Site/Update/`, strings in `Display/UpdateText.cs`) takes pasted wikitext and returns it with `{{IUCN status}}` templates, status cells of wikitables and taxobox status lines brought up to the latest global assessment, byte for byte elsewhere, with a report. It is the only page that answers POST (`UseGetAndHeadOnly` allows it there and raises the body limit to 2 MB plus 64 KB); details in `docs/public-site.md`.
- New `site build-db` tests start from `BeastieBot3.Tests/SiteBuild/SiteBuildSourceFixture` (import it with `using static`).
- Site UI strings are in `BeastieBot3.Site/Display/SiteText.cs`, the About page text in `Pages/About.cshtml`, and category labels in `Display/IucnCategories.cs`. Load the `ui-text` and `no-riddlespeak` skills before changing them, and ask the user to sign off new or changed strings.

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
| `docs/wikidata-iucn-status.md` | Wikidata IUCN status dry run: decisions (references, item per assessment, rank variants, coordination), the assessment item model shared with the public site, tiers, safety properties, what's not built |
| `docs/redlist-audit-site.md` | `redlist audit-site` generator: producers, unified model, commentary mechanism, output structure |
| `docs/public-site.md` | Public species site: parts, release update steps and the `public-site` workflow, site database contract, citation parsing, full given names and DOI rules, `iucn resolve-dois`, Wikidata items of assessments (`{{cite Q}}`, QuickStatements commands), citation options and live updates, site settings and theme, known gaps |

## Code Conventions

- **File-scoped namespaces**: `namespace BeastieBot3.Iucn;`
- **Brace style**: opening brace on same line.
- **Naming**: PascalCase types/methods, camelCase locals/params, `_camelCase` private fields.
- **`var`** when type is obvious from the right-hand side.
- **`Spectre.Console`** for all user-facing output (progress bars, tables, markup). Never use `Console.WriteLine` directly.
- **Colours in the local web UI** (`Web/wwwroot/style.css`) are tokens only: light on `:root`, dark twice (under `prefers-color-scheme: dark` and under `[data-theme="dark"]`), and the two dark blocks must match. Add no hex colours outside the token blocks and set no colours inline from JS; use a class. The public site's `site.css` and the audit site's `audit.css` also have two dark token blocks that must match.
- Nullable reference types are enabled — use `?` and handle null checks throughout.
