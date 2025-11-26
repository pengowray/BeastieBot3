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
- `wikidata rebuild-indexes` rebuilds the normalized taxon-name index inside `wikidata_cache_sqlite` without deleting your downloaded payloads. Run it after pulling new code that introduces lookup tables, or add `--force` to drop and recreate the index from scratch if you suspect corruption.
