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
