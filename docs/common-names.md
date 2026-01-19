# Common Names Commands

The `common-names` command group provides tools for aggregating, disambiguating, and reporting on common names for IUCN Red List species from multiple data sources.

## Overview

Common names for species are notoriously ambiguous. The same name can refer to different species ("snapper" can refer to dozens of fish species), and the same species may have many common names across different sources. This system:

1. **Aggregates** common names from multiple authoritative sources
2. **Detects conflicts** where the same name is used for different species
3. **Applies capitalization rules** based on a curated rules file
4. **Generates reports** for Wikipedia editors to help with disambiguation

## Performance Notes

These commands process large amounts of data and can take significant time to run. All times measured on a Windows desktop with SSD storage:

| Command | Fresh Run | Re-run | Notes |
|---------|-----------|--------|-------|
| `init` | ~5-6 min | ~5-6 min | Same time (upserts 183k taxa) |
| `aggregate --source iucn` | ~7 min | ~7 min | Processes 178k assessments |
| `aggregate --source wikidata` | ~20 min | ~20 min | Matches 180k entities |
| `aggregate --source wikipedia` | ~22 min | ~22 min | Parses 30k page taxoboxes |
| `aggregate --source col` | ~110 min | ~110 min | Includes COL synonym import |
| `aggregate` (all sources) | ~160 min | ~160 min | Total of above |
| `detect-conflicts` | ~45 min | ~45 min | Analyzes 385k common names |
| `init --aggregate` + `detect-conflicts` | ~210 min | - | Full fresh setup (~3.5 hours) |

**Re-running commands:**
- All commands use **UPSERT** operations - safe to re-run at any time
- Re-running takes approximately the same time as a fresh run
- No data is lost when re-running; existing records are updated in place
- Use `common-names sources` to check which sources have been aggregated

## Data Sources

| Source | Import Type | Description |
|--------|-------------|-------------|
| IUCN Red List | `iucn` | Common names from IUCN API assessments (~175k names) |
| Catalogue of Life | `col` | English vernacular names from COL database (~119k names) |
| Wikidata | `wikidata` | P1843 taxon common name claims (~70k names) |
| Wikidata Labels | `wikidata_label` | Item labels filtered for common name patterns (~17k names) |
| Wikipedia | `wikipedia_title` / `wikipedia_taxobox` | Article titles and taxobox names (~33k names) |

## Commands

### `common-names init`

Initializes the common name database with taxa from IUCN and capitalization rules.

```bash
# Basic initialization
beastiebot3 common-names init

# Initialize and immediately aggregate all sources
beastiebot3 common-names init --aggregate

# Limit taxa for testing
beastiebot3 common-names init --limit 1000
```

**Behavior with existing data:**
- **Taxa**: Uses UPSERT - existing taxa with the same `(primary_source, primary_source_id)` are updated, new taxa are inserted
- **Caps rules**: Uses UPSERT - existing rules for the same word are updated with new correct form
- **Safe to re-run**: Refreshes data without losing existing records or causing duplicates
- Re-running takes approximately the same time as a fresh run (~5-6 minutes)

**Options:**
- `--aggregate` - After initialization, run aggregation from all available sources
- `--skip-taxa` - Only import caps rules (skip taxa import)
- `--skip-caps` - Only import taxa (skip caps rules import)
- `--limit <N>` - Import only N taxa (useful for testing)

### `common-names aggregate`

Imports common names from external data sources into the common names store.

```bash
# Aggregate from all available sources
beastiebot3 common-names aggregate

# Aggregate from a specific source
beastiebot3 common-names aggregate --source iucn
beastiebot3 common-names aggregate --source wikidata
beastiebot3 common-names aggregate --source wikipedia
beastiebot3 common-names aggregate --source col

# Limit records for testing
beastiebot3 common-names aggregate --source iucn --limit 1000
```

**Behavior with existing data:**
- Uses UPSERT on `(taxon_id, normalized_name, source, language)`
- If the same common name from the same source already exists for a taxon, it updates:
  - `raw_name` - updated to new value
  - `display_name` - updated only if new value is non-null (preserves existing)
  - `is_preferred` - keeps the maximum (true wins over false)
- **Safe to re-run**: Refreshes data without losing existing records or causing duplicates
- Records are tracked in `import_runs` table (view with `common-names sources`)
- Re-running a source takes approximately the same time as a fresh run

**Source matching:**
- Each source attempts to match its taxa against the common names database
- Matching is done by scientific name (canonical name or synonym)
- Names that can't be matched to a known taxon are skipped

### `common-names sources`

Shows the status of all data sources - which are available and which have been aggregated.

```bash
beastiebot3 common-names sources
```

Displays a table showing:
- **Available** - Whether the source database file exists
- **Aggregated** - Whether an import run has been completed
- **Records** - Number of records added in the last run
- **Last Run** - Timestamp of the last aggregation

### `common-names detect-conflicts`

Analyzes the common names database to find ambiguous names (same name used for different valid taxa).

```bash
# Detect conflicts
beastiebot3 common-names detect-conflicts

# Clear existing conflicts before detection
beastiebot3 common-names detect-conflicts --clear-existing
```

**Behavior with existing data:**
- By default, adds new conflicts while preserving existing ones
- Use `--clear-existing` to start fresh
- **Safe to re-run**: Use `--clear-existing` for a complete refresh
- Detection takes ~45 minutes on a full dataset (~385k common names)

### `common-names report`

Generates markdown reports about common name conflicts and capitalization issues.

```bash
# Generate default reports to console
beastiebot3 common-names report

# Generate a specific report
beastiebot3 common-names report --report ambiguous
beastiebot3 common-names report --report caps

# Output to file
beastiebot3 common-names report --report ambiguous -o reports/ambiguous.md

# Limit output
beastiebot3 common-names report --report all --limit 100
```

**Available reports:**
- `summary` - Overview statistics
- `ambiguous` - Names that refer to multiple valid species
- `ambiguous-iucn` - Ambiguous names where at least one taxon is IUCN-listed
- `caps` - Missing capitalization rules
- `wiki-disambig` - Names that may need Wikipedia disambiguation
- `iucn-preferred` - Conflicts between IUCN preferred names
- `all` - Generate all reports

## Typical Workflow

> **Time expectation:** A complete fresh setup takes approximately 3-4 hours. Once set up, re-running individual sources or conflict detection takes the same time as shown in the Performance Notes table above.

### First-time setup

```bash
# Initialize with all data
beastiebot3 common-names init --aggregate

# Detect conflicts
beastiebot3 common-names detect-conflicts

# Generate reports
beastiebot3 common-names report --report all
```

### Updating data

```bash
# Re-aggregate a specific source after updating its cache
beastiebot3 common-names aggregate --source iucn

# Or refresh everything
beastiebot3 common-names aggregate

# Re-run conflict detection
beastiebot3 common-names detect-conflicts --clear-existing
```

### Checking status

```bash
# See which sources are available and when they were last aggregated
beastiebot3 common-names sources
```

## Database Schema

The common names store (`common_names.sqlite`) contains:

- **taxa** - Unified taxa from IUCN (the "backbone")
- **scientific_name_synonyms** - Alternative scientific names for matching
- **common_names** - All common names from all sources
- **common_name_conflicts** - Detected ambiguities
- **caps_rules** - Capitalization rules from caps.txt
- **import_runs** - Tracking of aggregation runs for each source

## Filtering Logic

When aggregating common names, certain entries are filtered out:

### IUCN Source
- **Species codes**: Entries matching "Species code: XX" pattern (placeholder names)
- **Scientific names**: Entries that match the taxon's actual scientific name parts (genus, species, infraspecific epithet)

### All Sources
- Names are normalized for comparison (lowercase, punctuation stripped)
- Language is limited to English ('en') by default
