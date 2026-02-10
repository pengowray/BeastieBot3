# IUCN API: Discover Missing Taxa by Family

The `iucn api discover-by-family` command discovers taxa that are missing from the local API cache by scanning IUCN family endpoints. This is essential for finding species that have been **removed from the Red List** or **reclassified** under different taxonomy, so they no longer appear in the CSV export used by `cache-taxa`.

## Why This Exists

The standard `cache-taxa` command builds its work queue from SIS IDs in the CSV-imported IUCN database. If a species was:

- **Removed** from the Red List (delisted)
- **Merged** into another taxon (synonym)
- **Reclassified** under a different name
- Only present in **historical** assessments

...then its SIS ID won't appear in the CSV export, and `cache-taxa` will never fetch it. The family endpoint covers all taxa — including historical ones — providing a way to backfill missing records.

## How It Works

1. **Fetches the family list** from `/api/v4/taxa/family/` (or uses `--family` filter)
2. **Paginates** through `/api/v4/taxa/family/{name}` (100 records per page) to collect SIS IDs
3. **Compares** discovered SIS IDs against the local cache (`taxa_lookup` table)
4. **Downloads** missing taxa via `/api/v4/taxa/sis/{id}` — the same endpoint and storage path as `cache-taxa`

Downloaded taxa are stored identically to `cache-taxa` output: the `taxa`, `taxa_lookup`, and `taxa_assessment_backlog` tables are all populated, so a subsequent `cache-assessments` run will pick up any new assessment IDs.

## Usage

### Dry Run (Recommended First Step)

See what's missing without downloading anything:

```bash
dotnet run --project BeastieBot3/BeastieBot3.csproj -- iucn api discover-by-family --dry-run
```

This scans all families and prints a summary table showing how many taxa are missing per family.

### Download All Missing Taxa

```bash
dotnet run --project BeastieBot3/BeastieBot3.csproj -- iucn api discover-by-family
```

### Target Specific Families

Useful for testing or resuming after a failure:

```bash
dotnet run --project BeastieBot3/BeastieBot3.csproj -- iucn api discover-by-family --family Felidae
dotnet run --project BeastieBot3/BeastieBot3.csproj -- iucn api discover-by-family --family "Felidae,Canidae,Ursidae"
```

### Force Re-download All

Download all discovered taxa regardless of cache status:

```bash
dotnet run --project BeastieBot3/BeastieBot3.csproj -- iucn api discover-by-family --force
```

### Refresh Stale Entries

Re-download entries older than 30 days:

```bash
dotnet run --project BeastieBot3/BeastieBot3.csproj -- iucn api discover-by-family --max-age-hours 720
```

### Limit Downloads

Cap the number of SIS IDs processed (useful for testing):

```bash
dotnet run --project BeastieBot3/BeastieBot3.csproj -- iucn api discover-by-family --limit 50
```

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `--cache <PATH>` | from INI | Override path to the API cache SQLite database |
| `--family <NAME>` | all | Comma-separated list of families to scan |
| `--limit <N>` | unlimited | Maximum SIS IDs to process |
| `--force` | off | Download all taxa, not just missing/stale ones |
| `--dry-run` | off | Report missing IDs without downloading |
| `--sleep-ms <MS>` | 250 | Delay between API requests (ms) |
| `--max-age-hours <HOURS>` | — | Re-download entries older than this |
| `-s\|--settings-dir <DIR>` | app dir | Directory containing paths.ini |
| `--ini-file <FILE>` | paths.ini | INI filename to read |

## Recommended Workflow

1. **Dry run** to see the scope of missing data:
   ```bash
   beastiebot3 iucn api discover-by-family --dry-run
   ```

2. **Download** missing taxa:
   ```bash
   beastiebot3 iucn api discover-by-family
   ```

3. **Fetch assessments** for newly discovered taxa:
   ```bash
   beastiebot3 iucn api cache-assessments
   ```

4. **Report** taxa with no current assessment:
   ```bash
   beastiebot3 iucn api report-no-current
   ```
   This generates a Markdown report (grouped by taxonomy) and a companion CSV listing every taxon that has no latest assessment — i.e. species that were removed, delisted, or reclassified.

## Performance Notes

- The family list endpoint returns ~800–1000 families
- Each family requires 1+ paginated requests (100 taxa per page; large families like Orchidaceae may need many pages)
- Discovery phase (scanning families) is the slower part; actual downloads reuse the fast `cache-taxa` path
- With default `--sleep-ms 250`, expect the full scan to take 30–60+ minutes depending on total family/page count
- Use `--family` to target specific families when iterating or debugging

## Prerequisites

- `IUCN_API_TOKEN` environment variable must be set (or `.env` file present)
- The API cache database path must be configured in `paths.ini` under `Datastore:IUCN_api_cache_sqlite`
