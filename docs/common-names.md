# Common Names Commands

The `common-names` command group provides tools for aggregating, disambiguating, and reporting on common names for IUCN Red List species from multiple data sources.

## Overview

Common names for species are notoriously ambiguous. The same name can refer to different species ("snapper" can refer to dozens of fish species), and the same species may have many common names across different sources. This system:

1. **Aggregates** common names from multiple authoritative sources
2. **Finds ambiguous names**, common names shared by two or more taxa, which
   `wikipedia generate-lists` skips (see [Ambiguous common names](#ambiguous-common-names))
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
| `init --aggregate` | ~170 min | - | Full fresh setup (~3 hours) |

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
- Nothing is deleted, so a source whose upstream data dropped or renamed a name leaves the old
  row behind: the store ends up holding the union of every release ever aggregated

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

# Replace one source instead of adding to it (e.g. after a new CoL release)
beastiebot3 common-names aggregate --source col --replace
```

**Behavior with existing data:**
- Uses UPSERT on `(taxon_id, normalized_name, source, language)`
- If the same common name from the same source already exists for a taxon, it updates:
  - `raw_name` - updated to new value
  - `is_preferred` - keeps the maximum (true wins over false)
  - (Display capitalization is applied at read time via the caps rules, not stored.)
- **Safe to re-run**: Refreshes data without losing existing records or causing duplicates
- Records are tracked in `import_runs` table (view with `common-names sources`)
- Re-running a source takes approximately the same time as a fresh run

**`--replace`:**
- Deletes what the named source contributed last time, then imports it again, so the store
  matches that source's current contents. Other sources are untouched.
- Removes that source's rows from `common_names`, `scientific_name_synonyms` and
  `taxon_cross_references`, plus any taxon it created (`--create-missing`) that no source
  refers to any more. Never removes taxa seeded by `init`.
- Matters most for CoL: its taxon ids are reissued between releases, so a stale
  `("col", oldId)` cross-reference (checked first when matching) can attach a new name to the
  wrong taxon.
- IUCN synonyms are only purged when `--include-synonyms` is also given, since that is the only
  flag that re-imports them.
- Rejected with `--limit`, which would delete a source and re-import only part of it.
- Recorded per source, and shown in the `Replaced` column of `common-names sources`. A source
  reading `never` there still holds everything it has ever contributed.

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

### Ambiguous common names

`wikipedia generate-lists` takes each taxon's English common name from this store. It skips an
ambiguous common name: one shared by two or more valid, non-fossil taxa in the store, in any
kingdom, including taxa that share a scientific synonym.

For a species entry, `generate-lists` uses the first of these that the taxon has:

1. A common name set for the taxon in `rules/rules-list.txt`. It is used even if it is
   ambiguous.
2. A common name in the store that is not ambiguous. Names are tried in source order: Wikipedia
   article title, Wikipedia taxobox, Wikidata label, IUCN preferred name, other IUCN names, other
   Wikidata names, Catalogue of Life.

When the taxon has neither, the entry shows only its scientific name.

A section heading shows the scientific name. The sentence under it gives the taxon's common name,
taken from the first of: `rules/taxon-rules.yml`, `rules/rules-list.txt`, a common name in the
store that is not ambiguous, and the title of the Wikipedia article that the scientific name
redirects to. The rules files and the redirect title are used even if the name is ambiguous.

The ambiguous names are worked out from `common_names` each time `generate-lists` runs, so
nothing has to be rebuilt after aggregating. `generate-lists` prints how many names it skips,
under the path of the store, and the `init`, `aggregate` and `report --report summary`
summaries show the same count. To see the names, run `common-names report --report ambiguous`,
which writes `common-name-ambiguous-<timestamp>.md` to the reports folder. It reads the same
query as `generate-lists` (`CommonNameStore.QueryAmbiguousNames`), so it lists the same names.
On the September 2026 store it listed 10,165. With `--kingdom`, the report counts only the taxa
in that kingdom, so it leaves out names shared by taxa in different kingdoms, which
`generate-lists` still skips. In the web UI the report is the optional "List ambiguous common
names" step of the "Wikipedia reports pipeline" workflow.

With `--use-legacy-names`, `generate-lists` reads names from the Wikidata and IUCN API caches
instead of this store and skips no names as ambiguous.

`sprat generate-lists` reads the same store and skips the same ambiguous names. When the store
has no common name for a species, or only ambiguous ones, the list uses the SPRAT common name. A
subspecies always gets its SPRAT common name.

`common-names detect-conflicts` was removed in September 2026. It stored pairs of taxa in the
same kingdom that share a common name in `common_name_conflicts`, and nothing read those rows.

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
- `summary` - Overview statistics, including the number of ambiguous English names
- `ambiguous` - Names shared by two or more valid taxa, in any kingdom: the names
  `wikipedia generate-lists` skips. With `--kingdom`, only names shared within that kingdom
- `ambiguous-iucn` - Ambiguous names where at least one taxon is IUCN-listed
- `caps` - Missing capitalization rules
- `wiki-disambig` - Names that may need Wikipedia disambiguation
- `iucn-preferred` - Conflicts between IUCN preferred names
- `all` - Generate all reports

## Typical Workflow

> **Time expectation:** A complete fresh setup takes approximately 3-4 hours. Once set up, re-running individual sources takes the same time as shown in the Performance Notes table above.

### First-time setup

```bash
# Initialize with all data
beastiebot3 common-names init --aggregate

# Generate reports
beastiebot3 common-names report --report all
```

### Updating data

```bash
# Re-aggregate a specific source after updating its cache
beastiebot3 common-names aggregate --source iucn

# Or refresh everything
beastiebot3 common-names aggregate

# Mirror a source exactly after an upstream release, dropping what it no longer lists
beastiebot3 common-names aggregate --source col --replace

# List the common names that wikipedia generate-lists now skips as ambiguous
beastiebot3 common-names report --report ambiguous
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
- **common_name_conflicts** - Left over from the removed `detect-conflicts` command. Nothing
  writes or reads it; `aggregate --replace` empties it
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
