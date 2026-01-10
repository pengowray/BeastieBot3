# BeastieBot3 AI Contributor Notes

These notes exist so the next AI (or human) that drops into this repo avoids the expensive mistakes we just tripped over.

## Paths + CLI quick facts
- Command routing lives in [BeastieBot3/Program.cs](BeastieBot3/Program.cs); verify flag names and activation logic there before adding new options.
- Everything that needs filesystem locations (datasets, caches, reports) should go through [BeastieBot3/PathsService.cs](BeastieBot3/PathsService.cs) plus `--settings-dir` / `--ini-file` overrides. Do not hardcode `D:\` paths outside the INI plumbing.
- Reports pick their destination using [BeastieBot3/ReportPathResolver.cs](BeastieBot3/ReportPathResolver.cs), which falls back to `<repo>/data-analysis`. Reuse that helper so every analyzer drops files under the same umbrella.
- [run-log.txt](run-log.txt) shows the latest Wikipedia list batch. The current log has “0 taxa, dataset unknown” for every list, which means the INI did not point at a populated SQLite file. Fix the INI and re-run a single list with a tight `--limit` before queueing the whole suite.

## IUCN SQLite performance rules
- The database exposes assessments per taxon via the `view_assessments_html_taxonomy_html` view. This is a simple join on `taxonId` between assessments and taxonomy tables.
- **Do NOT wrap queries in extra CTEs** like `WITH latest AS (SELECT taxonId, MAX(redlist_version) ...)`. We import a single IUCN release at a time, so all rows share the same import. The `import_metadata` table tracks which zip file was imported.
- Column comparisons must stay sargable. Never wrap columns in `LOWER`, `UPPER`, `LIKE '%foo%'`, or other functions unless the column is truly case-insensitive. Normalize values **before** binding them to parameters instead.
- Stick to exact matches on indexed columns (e.g., `kingdomName`, `className`, `redlistCategory`). Filters that match against constants run quickly; function calls on the column force SQLite to scan everything.
- If you need boolean flags like `possiblyExtinct`, compare using `IFNULL(flag,'false') = 'true'` (or pre-normalize the value) but do not add `LOWER()` on either side.

## IUCN datastore layout cheat-sheet
- **Key column renames (2025-01 schema update)**:
  - `internalTaxonId` → `taxonId` (INTEGER NOT NULL)
  - `assessmentId` is now INTEGER NOT NULL
  - `redlist_version` removed from data tables (kept only in `import_metadata` for provenance)
- `view_assessments_html_taxonomy_html` joins `assessments_html` with `taxonomy_html` on `taxonId`. It contains denormalized columns (`kingdomName`, `className`, `scientificName_taxonomy`, etc.). The naming convention uses uppercase for high-level ranks (e.g., `AMPHIBIA`) and mixed case for genus/species. Normalize inputs to match that reality instead of transforming columns in SQL.
- The view avoids column name conflicts by aliasing taxonomy columns that also exist in assessments (e.g., `scientificName_taxonomy` instead of duplicate `scientificName`).
- Boolean-ish flags arrive as text ("true"/"false"). Treat them as such—comparisons should remain string equality checks after applying `IFNULL(...,'false')` to handle null rows.
- Legacy LR/* categories and combined views (threatened/endangered) are assembled in code, not SQL. Keep SQL focused on primitive filters (kingdom, class, status) and let the generator group/label.
- [BeastieBot3/IucnImporter.cs](BeastieBot3/IucnImporter.cs) writes four tables (`assessments`, `assessments_html`, `taxonomy`, `taxonomy_html`) plus `import_metadata`. The `taxonId` and `assessmentId` columns are INTEGER NOT NULL; all other CSV columns are TEXT.
- Indexes exist on `taxonId`, `assessmentId`, `scientificName`, and the kingdom→species ladder. If a query needs a new filter, add a matching index inside the importer instead of bolting an `ORDER BY` on a random column.
- The importer recreates `view_assessments_taxonomy` and `view_assessments_html_taxonomy_html` every run. If a dev database is missing them, rerun `iucn import --force`—do not try to patch the view manually.
- Each zip import is logged with filename and `redlist_version` in `import_metadata`. If you need to replay a single archive, delete the corresponding row (or pass `--force`) so the importer will reinsert it cleanly.

## Non-HTML tables (_html suffix)
- The `assessments` and `taxonomy` tables (without `_html`) are the plain-text versions of the data. The `assessments_html` and `taxonomy_html` tables contain HTML-formatted fields.
- **Prefer the `_html` versions** for all production queries. The non-HTML tables are kept primarily for analysis and comparison.
- In future, we may add an import option to skip plain-text tables entirely to reduce database size.

## Verification vs production queries
- It is absolutely fine to add targeted verification commands (e.g., `iucn report-taxonomy-consistency`, `iucn report-name-changes`) or to spin up quick scripts/notebooks to check assumptions such as casing, null rates, or category coverage.
- Those checks should stay separate from the hot-path generator queries. Run expensive scans in a dedicated report/command or behind a CLI flag so normal list generation stays fast.
- When you discover a mismatch (say casing drift in `familyName`), document the expected shape here and optionally wire a lightweight analyzer under `docs/` or `reports/` so the next pass can be re-run without embedding the check into every list execution.

## YAML list definitions
- The YAML config should cover every taxonomic group + status combination without copy/paste explosions. Reuse defaults aggressively and only override templates or grouping when a list genuinely needs a bespoke layout.
- Current required coverage: amphibians, arthropods, birds, fishes, insects, invertebrates, reptiles, plants, chromista, fungi (yes, some overlap); statuses EX, EW, CR, EN, combined EN+CR ("endangered"), combined CR+EN+VU ("threatened"), NT (with legacy LR/nt), LC (with LR/lc), DD, LR/cd. Plan lists so editors get meaningful slices without us generating near-duplicate files.

## General workflow hygiene
- Run `dotnet build` after structural changes, then target a single list with `dotnet run -- wikipedia generate-lists --list <id> --limit 100` before kicking off the full batch.
- Avoid speculative refactors of legacy SQL or filesystem layout unless you have benchmarks or actual requirements. The data pipeline is already heavy; accidental O(n²) work makes the CLI appear "hung" even when it is just crawling.
- Log timings when you touch anything that hits SQLite. `sqlite3 .timer on` is fine, but kill the command if it exceeds a minute—you almost certainly regressed the query plan.

## IUCN API cache schema
- [BeastieBot3/IucnApiCacheStore.cs](BeastieBot3/IucnApiCacheStore.cs) owns the cache.
- Tables to know:
  - `taxa` holds the raw `/api/v4/taxa/sis/{id}` JSON keyed by `root_sis_id`; `taxa_lookup` maps every nested SIS ID back to that root so we only download once per tree.
  - `taxa_assessment_backlog` queues assessment IDs discovered inside each taxa payload. `GetAssessmentBacklogOrdered()` prioritizes latest and most recent years, so do not reshuffle unless you want to starve fresh data.
  - `assessments` stores `/api/v4/assessment/{id}` payloads with the same `import_metadata` bookkeeping as the taxa table.
  - `failed_requests` tracks endpoint/id pairs, retry windows, and error payloads. Use `--failed-only` to drain that queue before saturating the API again.
- All write paths run inside SQLite transactions. Reuse the helper instead of sprinkling ad-hoc SQL so we keep WAL + foreign keys enabled consistently.

## Catalogue of Life SQLite mirrors
- [BeastieBot3/ColImporter.cs](BeastieBot3/ColImporter.cs) ingests ColDP archives into a dedicated database per snapshot. Column names get sanitized to lower snake case and stored as TEXT so we can ingest whichever optional TSV the release ships that month.
- Each table receives an `import_id`, plus opportunistic indexes on populated taxonomy columns. If you add a query that filters on something slow, enhance `EnsureIndexes` rather than creating runtime `CREATE INDEX` statements.
- The importer emits multiple FTS5 tables (`nameusage_scientific_name_fts`, `_taxon_context_fts`, `_authorship_fts`, `_notes_fts`, `vernacularname_fts`, `distribution_area_fts`) so text searches never need `LIKE '%foo%'`. Rebuilds happen automatically after each import; do not attempt to update the FTS tables directly.
- Dataset metadata (`dataset_metadata`, `source_*` YAML blobs, reference JSONL) also land in SQLite so reports can cite provenance without cracking the zip again.

## Wikidata cache schema
- [BeastieBot3/WikidataCacheStore.cs](BeastieBot3/WikidataCacheStore.cs) maintains everything under one `wikidata_cache_sqlite` file.
- Key tables:
  - `wikidata_entities` keeps discovery/download timestamps, HTTP error state, labels, descriptions, and cached JSON.
  - `wikidata_p627_values`, `wikidata_p141_statements`, and `wikidata_p141_references` are flattened claim tables so downstream reports never parse JSON on the hot path.
  - `wikidata_taxon_name_index` plus `wikidata_scientific_names` power fast `normalized_name` lookups; rebuild them via `wikidata rebuild-indexes` when adding new normalization logic.
  - `wikidata_pending_iucn_matches` records provisional IUCN ↔ Wikidata matches discovered by the matcher so we can re-emit coverage reports without re-scanning everything.
- Long-running runs should respect `GetPendingEntities` ordering (new first, then refresh windows). If you need a one-off spot check, cap `--limit` instead of rewriting the queue logic.

## Wikipedia cache schema
- [BeastieBot3/WikipediaCacheStore.cs](BeastieBot3/WikipediaCacheStore.cs) plus [docs/wikipedia-cache-plan.md](docs/wikipedia-cache-plan.md) describe the full pipeline.
- Tables to keep in mind:
  - `wiki_pages` tracks fetch state, HTML/wikitext blobs, redirect flags, SHA hashes, and error counts.
  - `wiki_page_categories`, `wiki_redirect_edges`, and `wiki_taxobox_data` let us audit classification decisions without re-downloading anything.
  - `taxon_wiki_matches` and `taxon_wiki_match_attempts` capture every synonym/title we tried along with the verdict; use them for regression testing whenever the matcher changes.
  - `wiki_missing_titles` keeps rejected titles (404s, deleted pages, etc.) so we do not hammer the same bad endpoints.
- `wikipedia cache-status`, `wikipedia enqueue-wikidata`, `wikipedia fetch-pages`, and `wikipedia match-taxa` all rely on the same schema. Add new columns only via the store so WAL + FK constraints stay enabled.

## Existing report commands worth rerunning
- `iucn report-taxonomy-consistency` ([BeastieBot3/IucnTaxonomyConsistencyCommand.cs](BeastieBot3/IucnTaxonomyConsistencyCommand.cs)) rebuilds scientific names from taxonomy pieces and flags genus/species/infra mismatches using [BeastieBot3/IucnScientificNameVerifier.cs](BeastieBot3/IucnScientificNameVerifier.cs).
- `iucn report-taxonomy-cleanup` ([BeastieBot3/IucnTaxonomyCleanupCommand.cs](BeastieBot3/IucnTaxonomyCleanupCommand.cs)) surfaces whitespace anomalies, stray infrarank markers, and authority noise via [BeastieBot3/IucnDataCleanupAnalyzer.cs](BeastieBot3/IucnDataCleanupAnalyzer.cs).
- `iucn report-html-consistency`, `iucn report-synonym-formatting`, and `iucn report-name-changes` focus on HTML vs text mismatches, bad synonym spacing, and SIS IDs with renamed taxa. Keep them as separate analyzers—no bolting these checks onto the main Wikipedia list SQL.
- `iucn report-col-crosscheck` depends on the COL mirror; run it after any new ColDP import to catch synonym drift.
- For Wikidata/Wikipedia alignment, lean on `wikidata report-coverage`, `wikidata report-coverage-details`, `wikidata report-iucn-freshness`, and `wikipedia cache-status` before trusting a batch edit.
- Sample targeted runs (always pin a limit before scaling up):

```bash
dotnet run --project BeastieBot3 -- iucn report-taxonomy-consistency --limit 5000 --max-samples 10
dotnet run --project BeastieBot3 -- iucn report-taxonomy-cleanup --limit 5000 --max-samples 10
dotnet run --project BeastieBot3 -- wikidata report-coverage --include-subpopulations
```

## Quick sqlite sanity checks
- Confirm the importer views exist:

```bash
sqlite3 "D:/datasets/beastiebot/IUCN_2025-2.sqlite" "SELECT name FROM sqlite_master WHERE type='view' AND name LIKE 'view_assessments%';"
```

- Spot-check the taxonomy view without touching production SQL:

```bash
sqlite3 "D:/datasets/beastiebot/IUCN_2025-2.sqlite" \
"SELECT taxonId, scientificName, scientificName_taxonomy, kingdomName, className FROM view_assessments_html_taxonomy_html LIMIT 20;"
```

- Inspect the API cache for stale taxa before downloading again:

```bash
sqlite3 "D:/datasets/beastiebot/iucn_api_cache.sqlite" \
"SELECT root_sis_id, downloaded_at FROM taxa ORDER BY downloaded_at ASC LIMIT 10;"
```

- Check Wikidata coverage when debugging matcher regressions:

```bash
sqlite3 "D:/datasets/beastiebot/wikidata_cache.sqlite" \
"SELECT COUNT(*) FROM wikidata_taxon_name_index WHERE normalized_name LIKE 'ursus%';"
```

- Remember the rule above: these inspection helpers are great, but never fold them into the giant Wikipedia list query. Keep every diagnostic either in its own CLI command or as a notebook/SQL snippet you run manually.

Keep this file updated whenever we learn a new "never again" lesson.
