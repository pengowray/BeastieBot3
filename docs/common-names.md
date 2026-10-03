# Common Names Commands

The `common-names` command group provides tools for aggregating, disambiguating, and reporting on common names for IUCN Red List species from multiple data sources.

## Overview

Common names for species are notoriously ambiguous. The same name can refer to different species ("snapper" can refer to dozens of fish species), and the same species may have many common names across different sources. This system:

1. **Aggregates** common names from multiple authoritative sources
2. **Finds ambiguous names**, common names shared by two or more taxa, and works out which
   taxon, if any, each one is used for (see [Ambiguous common names](#ambiguous-common-names))
3. **Applies capitalization rules** based on a curated rules file
4. **Generates reports** for Wikipedia editors to help with disambiguation

## Performance Notes

These commands process large amounts of data and can take significant time to run. Times were measured on a Windows desktop with SSD storage, except the Wikidata and Wikipedia rows, which were measured on a Linux desktop in October 2026:

| Command | Fresh Run | Re-run | Notes |
|---------|-----------|--------|-------|
| `init` | ~5-6 min | ~5-6 min | Same time (upserts 183k taxa) |
| `aggregate --source iucn` | ~7 min | ~7 min | Processes 178k assessments |
| `aggregate --source wikidata` | ~6-7 min | ~6-7 min | October 2026 |
| `aggregate --source wikipedia` | ~2 min | ~2 min | October 2026: 98,843 matched pages |
| `aggregate --source col` | ~110 min | ~110 min | Includes COL synonym import |
| `aggregate` (all sources) | ~160 min | ~160 min | Sum of the earlier Windows times for all four sources |
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
- A Wikidata item's names go to the store taxon found first by these, in order
  (`ResolveWikidataTaxon`): the item's IUCN taxon ids (P627) as they are in the Wikidata cache
  now; the taxon an earlier run recorded for the item (a Wikidata cross-reference); the item's
  scientific names (P225), as the canonical name and then as a synonym. The current P627 comes
  first because a run without `--replace` keeps old cross-references, which name the taxon the
  item had before its P627 changed.
- A Wikipedia page that `wikipedia match-taxa` matched to more than one taxon gives its names
  only to some of them (`WikipediaPageMatch`): the taxa whose accepted name is the scientific
  name in the page's taxobox; failing that, the taxa with that name as a synonym; failing that,
  the taxa matched by their own name rather than through a synonym or a Catalogue of Life name;
  failing that, all of them. A taxon that takes no names from the page gets a
  `taxon_cross_references` row with `match_type` `other_taxon_page`.
- The store's Wikipedia article for a taxon (`CommonNameStore.GetWikipediaArticleTitle`) is the
  page its `wikipedia_title` or `wikipedia_taxobox` name came from. A taxon with neither (for
  example one whose article title is a scientific name) gets the page of its `exact` Wikipedia
  cross-reference, never an `other_taxon_page` one. The lists use this title only as a link
  target, never as the text of a line.
- Stored rows change only when a source is aggregated again: after a change to the filtering
  rules below, run `aggregate --source wikipedia --replace` and `aggregate --source wikidata
  --replace`.

### `common-names sources`

Shows the status of all data sources - which are available and which have been aggregated.

```bash
beastiebot3 common-names sources
```

Displays a table showing:
- **Available** - Whether the source database file exists
- **Aggregated** - Whether an import run has been completed
- **Records** - Number of names from the source that the store holds now
- **Last Run** - Timestamp of the last aggregation

`common-names sources` and `common-names report` open the store read-only
(`CommonNameStore.OpenReadOnly`).

### Ambiguous common names

An ambiguous common name is an English common name that two or more taxa in the Common names
store have. Only valid, non-fossil taxa are counted, from any kingdom, including taxa that share a
scientific synonym, and names are compared ignoring case, spaces and punctuation. A junk name (see
[Common name quality](#common-name-quality)) does not count as a name the taxon has, and a
repairable name counts under its repaired form.
`wikipedia generate-lists`, `sprat generate-lists`, `site build-db` and
`common-names report --report ambiguous` apply the same rule to these names
(`CommonNameStore.QueryAmbiguousNames`, `AmbiguousNames`), and work out the names from the store
each time they run, so there is nothing to rebuild after aggregating.

Each ambiguous name is used for the taxon that has it from the highest-priority source, and
skipped for all the other taxa that have it. The sources, highest priority first:

1. Wikipedia article title
2. Wikipedia taxobox
3. Wikidata label
4. IUCN main name
5. Other IUCN names
6. Other Wikidata names
7. Catalogue of Life

If two or more taxa have the name from sources of equal priority, the name is skipped for all of
them, except that a species takes priority over its own subspecies, varieties and subpopulations.
For example, Panthera leo has "Lion" as its Wikipedia article title and Panthera leo ssp. leo has
it only as one of its other IUCN names, so Panthera leo is listed as "Lion". Panthera tigris has
"Tiger" as its Wikipedia article title, so it is listed as "Tiger", although a grouper also has
"Tiger" from the Catalogue of Life.

`CommonNameChooser` is the one place a taxon's English name is chosen. `wikipedia generate-lists`,
`sprat generate-lists`, `site build-db` and `common-names report --report trace` all use it. For a
species entry it takes the first of these that the taxon has:

1. A common name set for the taxon in `rules/rules-list.txt`. It is used even if it is
   ambiguous.
2. The taxon's first common name in source order that is not junk and not skipped for it,
   repaired where `CommonNameQuality` can repair it, with the capitalization rules applied.

When the name from step 1 or 2 is not usable as a common name (the scientific name again, a
working name such as "sp. nov.", or a name with an authority and year), the taxon gets no common
name. When the taxon has no common name, the entry shows only its scientific name. The trace report
shows the chooser's pick from the store (without `rules-list.txt`).

A section heading shows the scientific name. The sentence under it gives the taxon's common name,
taken from the first of: `rules/taxon-rules.yml`, `rules/rules-list.txt`, the taxon's first
common name in the store that is not skipped for it, and the title of the Wikipedia article that
the scientific name redirects to. The rules files and the redirect title are used even if the
name is ambiguous.

`generate-lists` prints how many ambiguous names the store has, under the path of the store, and
the `init`, `aggregate` and `report --report summary` summaries show the same count. To see the
names, run `common-names report --report ambiguous`, which writes
`common-name-ambiguous-<timestamp>.md` to the reports folder. The report has one table for each
ambiguous name, and its "Uses This Name" column is Yes for the taxon the name is used for. On the
store of 3 October 2026 it listed 10,303 names: 6,970 used for one taxon each and 3,333 used for
no taxon (equal-priority ties). With `--kingdom`, the report counts only the taxa in that kingdom,
so it leaves out names shared by taxa in different kingdoms. In the web UI the report is the optional
"List ambiguous common names" step of the "Wikipedia reports pipeline" workflow.

With `--use-legacy-names`, `generate-lists` reads names from the Wikidata and IUCN API caches
instead of this store and skips no names as ambiguous.

`sprat generate-lists` reads the same store and applies the same rule. When the store has no
common name for a species, or only names that are skipped for it, the list uses the SPRAT common
name. A subspecies always gets its SPRAT common name.

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
- `ambiguous` - Names shared by two or more valid taxa, in any kingdom, and which taxon
  `wikipedia generate-lists` uses each name for. With `--kingdom`, only names shared within that kingdom
- `ambiguous-iucn` - Ambiguous names where at least one taxon is IUCN-listed
- `caps` - Missing capitalization rules
- `wiki-disambig` - Names that may need Wikipedia disambiguation
- `iucn-preferred` - Conflicts between IUCN preferred names
- `trace` - For sample taxa in each major group, every English name the store has, why each one
  was rejected, and the name the chooser picks
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

# List the ambiguous common names and which taxon each one is used for
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

### Wikipedia titles, taxobox names and Wikidata labels

`ScientificNameCheck.IsScientificName` decides whether a Wikipedia article title (without its
disambiguation), a taxobox name or a Wikidata English label is a scientific name. `aggregate`
stores only the ones that are not. The rules, in order:

1. The name is one of the taxon's own names, one of them followed by an authority or a note
   ("Myristica fatua Sw."), or the first word or words of one (the genus of a monotypic genus, the
   species of a subspecies), ignoring case, rank markers and a subgenus: a scientific name. A genus
   taken from a synonym does not count when it is an English word ("Orca" for Orcinus orca).
2. Otherwise only a name shaped like a scientific name can be one: two to four words, the first a
   capitalised word of plain letters and the rest lower case. A single word is a scientific name
   when it is a genus in the store and not an English word ("Strumigenys", but not "Platypus").
   A name with any other shape is a common name. Double quotes around a genus
   ("\"Hyla\" nicefori") are ignored.
3. The first word is one of the taxon's genera ("Gobio gobio" for Gobio latus): a scientific name.
4. A word is an English word ("Pygmy hippopotamus", "Alligator gar"): a common name. A first word
   that is a genus in the store does not count as English, and neither does a later word that is
   one of the taxon's epithets.
5. A word is a genus or epithet in the store, or one of the taxon's epithets (another combination
   of the same species, such as "Rubroshorea ovata" for Shorea ovata): a scientific name. An
   epithet that is also the taxon's genus (the "gorilla" of Gorilla gorilla) does not count.
6. Otherwise a common name. When the taxon's names are not known, the shape alone decides, and a
   name with the shape of a scientific name is taken to be one.

A word is English when at least 3 different English common names from IUCN and the Catalogue of
Life use it. A word that is also an epithet somewhere in the store needs at least 10 ("gazelle" in
"Dorcas gazelle" counts as English, "montana" in "Aiouea montana" does not).

### Wikipedia taxobox

`TaxoboxCommonName` reads the taxobox's name field. The field is split into lines at `<br>`, and
the first line that is a usable English name is taken. Lines in italics (a scientific name), in a
non-Latin script, naming a family ("Salamandridae") or repeating the page title are skipped; a
line that starts with a lower-case letter continues the line before. `CommonNameQuality` then
repairs or rejects what is left, and `ScientificNameCheck` drops a name that is the taxon's
scientific name. Junk stored by earlier runs stays until `aggregate --source wikipedia --replace`.

### Common name quality

`CommonNameQuality.Assess` gives each name one of three verdicts. The lists, `site build-db` and
the ambiguity rule never use a junk name, and use a repairable name in its repaired form.

- **Junk**: wiki markup that cannot be removed, author citations with a year ("Calvert, 1902"),
  OCR errors from scanned books in the Catalogue of Life (a backslash inside a name, or, in a name
  labelled English, a digit standing for a letter or capitals inside words), names cut off at a
  bracket ("Pholidoscelis polops (Cope"), a gloss with no name ("meaning large bear cat"), and
  IUCN's placeholder "Species code: X".
- **Repairable**: a good name with extra text, such as a citation template after the name ("Sunda
  slow loris{sfn|Groves|2005|p=122}"), the next infobox parameter, a footnote marker, an author and
  year in brackets, or a translation in brackets ("Da Xiong Mao (meaning large bear cat)"). The
  extra text is removed.
- **Good**: everything else, including names that look odd but are real ("Cassin's 17-year
  Cicada", "European pilchard (=sardine)").
