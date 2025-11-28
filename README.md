# BeastieBot3

Command-line utilities that normalize and cross-check biodiversity datasets such as the IUCN Red List and Catalogue of Life. This repo now includes an API crawler that mirrors the latest IUCN JSON payloads into a local SQLite cache so we can work offline and re-use responses across commands.

## Prerequisites

- .NET 9 SDK
- SQLite3 (for inspecting the generated cache files)
- An IUCN Red List API token stored in a local `.env` (see `.env.example`)

## Quick Start

```bash
# Restore packages and build
cd BeastieBot3
dotnet build BeastieBot3.sln

# Cache species-level taxa JSON into the API database
dotnet run --project BeastieBot3 \
  -- iucn api cache-taxa \
  --limit 200 \
  --max-age-hours 24
```

The command reads SIS IDs from the CSV-derived `IUCN_sqlite_from_cvs` database, calls `/api/v4/taxa/sis/<id>` for each species, and stores the raw JSON in `iucn_api_cache.sqlite` alongside import metadata, lookup rows for nested SIS IDs, and a retry queue for temporary failures.

Use `--failed-only` to exclusively retry entries from the queue, `--force` to bypass freshness checks, and `--cache` / `--source-db` to override database locations when necessary.

## CLI Command Reference

### Global

- `show-paths` &mdash; Print every key/value pair resolved from your INI so you can confirm paths before running longer jobs. Example: `dotnet run --project BeastieBot3 -- show-paths --settings-dir c:/configs`.

### Catalogue of Life (`col`)

- `col check` &mdash; Detects the mounted COL dataset inside the container and reports the discovered paths.
- `col import` &mdash; Loads ColDP zip archives into per-release SQLite files; pass `--force` to re-import an existing snapshot.
- `col report-subgenus-homonyms` &mdash; Flags subgenus names that collide with genus names inside the imported COL database.
- `col report-nameusage-fields` &mdash; Profiles whitespace, ASCII coverage, or other anomalies across the `nameusage` table; combine with `--columns scientificName,authorship` or `--limit 100000` to scope the scan.

### IUCN datasets (`iucn`)

- `iucn import` &mdash; Imports the CSV releases into SQLite, refreshing lookup tables that power the rest of the tooling.
- `iucn report-html-consistency` &mdash; Compares HTML vs. plain-text assessment fields to detect normalization differences (use `--limit` to spot-check).
- `iucn report-taxonomy-consistency` &mdash; Rebuilds scientific names from taxonomic pieces and verifies field alignment.
- `iucn report-taxonomy-cleanup` &mdash; Highlights taxonomy columns that need whitespace normalization or marker cleanup.
- `iucn report-col-crosscheck` &mdash; Cross-references IUCN species with Catalogue of Life entries for synonymy and authority alignment.
- `iucn report-name-changes` &mdash; Surfaces SIS taxon ids whose `taxon_scientific_name` changed across assessments; emit Markdown via `--output` when needed.
- `iucn report-synonym-formatting` &mdash; Scans cached IUCN synonyms for double spaces, stray whitespace, or embedded markup and saves both Markdown plus CSV summaries.

#### IUCN API cache (`iucn api`)

- `iucn api cache-taxa` &mdash; Downloads `/api/v4/taxa/sis/{sis_id}` payloads into `iucn_api_cache.sqlite`; pair with `--limit` or `--failed-only` to control retries.
- `iucn api cache-assessments` &mdash; Pulls `/api/v4/assessment/{assessment_id}` payloads for already-cached taxa, using the same retry queue semantics.
- `iucn api cache-all` &mdash; Convenience wrapper that runs the taxa and assessments cache steps back-to-back (accepts `--taxa-limit` / `--assessment-limit`).

### Wikidata cache (`wikidata`)

- `wikidata seed-taxa` &mdash; Queries the public SPARQL endpoint for taxa carrying IUCN identifiers and enqueues their Q-ids locally.
- `wikidata cache-entities` &mdash; Downloads JSON for queued taxa, persists it to SQLite, and builds lookup indexes.
- `wikidata cache-all` &mdash; Runs the seed and cache passes sequentially so you can keep the mirror current in one command.
- `wikidata backfill-iucn` &mdash; Looks for IUCN taxa that still lack cached Wikidata entities and tries taxon-name/synonym searches to fill the gaps; use `--limit` plus `--queue-all-synonyms` for targeted runs.
- `wikidata report-coverage` &mdash; Summarizes how many IUCN taxa currently match cached Wikidata entities (optionally include subpopulations).
- `wikidata reset-cache` &mdash; Deletes downloaded Wikidata JSON payloads while keeping the seed queue intact; add `--force` to skip prompts.
- `wikidata rebuild-indexes` &mdash; Rebuilds lookup tables (currently the normalized taxon-name index) without redownloading entities; append `--force` to drop and recreate from scratch.

## Wikidata Cache

The `wikidata` CLI branch mirrors Wikidata taxa that expose IUCN identifiers (properties `P141` / `P627`). The workflow has two steps:

1. `wikidata seed-taxa` queries the public SPARQL endpoint for Q-ids that reference IUCN conservation data and adds them to `wikidata_cache_sqlite`.
2. `wikidata cache-entities` (or `wikidata cache-all`) downloads the JSON for each queued entity, stores the payload, and builds lookup indexes for `P627`, `P141` references, `P225` names, and `P105`/`P171` taxonomy so downstream commands can match on taxon id or scientific name.

```bash
# Discover 2k Wikidata taxa with IUCN IDs and download their JSON payloads
dotnet run --project BeastieBot3 -- wikidata cache-all \
  --seed-limit 2000 \
  --download-limit 500

# Produce a coverage report showing how many IUCN taxa were matched
dotnet run --project BeastieBot3 -- wikidata report-coverage
```

Environment variables such as `WIKIDATA_USER_AGENT`, `WIKIDATA_REQUEST_DELAY_MS`, and `WIKIDATA_SPARQL_BATCH_SIZE` (see `.env.example`) allow you to tune the request cadence when mirroring Wikidata at scale.

- `wikidata seed-taxa` keeps paging until the SPARQL endpoint stops returning new rows and automatically reduces its batch size when Wikidata responds with 504/timeout errors, so the default run should eventually catch up without manual throttling.
- `wikidata cache-entities` now drains the entire pending queue by default (processing it in smaller batches under the hood). Supply `--limit` to cap the number processed in a session or `--batch-size` to change the chunk size pulled from SQLite per round.
- `wikidata cache-all` chains the seed and cache steps, letting you keep the mirror caught up in a single run.
- `wikidata backfill-iucn` focuses on taxa missing cached entities and tries additional scientific-name and synonym lookups before queuing downloads.
- `wikidata report-coverage` shows how many taxa currently map via P627 or scientific-name matching, making it clear when another download pass is necessary.
- `wikidata reset-cache` clears stored Wikidata JSON blobs but leaves the queue alone so you can redownload without reseeding.
- `wikidata rebuild-indexes` rebuilds the normalized taxon-name index inside `wikidata_cache_sqlite` without deleting your downloaded payloads. Run it after pulling new code that introduces lookup tables, or add `--force` to drop and recreate the index from scratch if you suspect corruption.
