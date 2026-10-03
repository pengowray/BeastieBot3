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

These commands process large amounts of data and can take significant time to run. Times were measured on a Windows desktop with SSD storage, except the IUCN, Wikidata and Wikipedia rows, which were measured on a Linux desktop in October 2026:

| Command | Fresh Run | Re-run | Notes |
|---------|-----------|--------|-------|
| `init` | ~5-6 min | ~5-6 min | Same time (upserts 183k taxa) |
| `aggregate --source iucn` | ~48 s | ~48 s | Processes 178k assessments. Measured on 3 October 2026 |
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
| Wikidata Labels | `wikidata_label` | Item labels filtered for common name patterns (20,064 names in October 2026) |
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
- A species or subspecies matched to a page about its genus or a higher taxon takes neither the
  page's title nor its taxobox name: "Casque-headed tree frogs" on the page "Trachycephalus" is not
  a name of *Trachycephalus vermiculatus* (`WikipediaPageMatch.IsGenusPageOfSpecies`). A page is
  about a genus or a higher taxon when its taxobox has none of the parameters that name a species
  (`binomial`, `trinomial`, `species`, `subspecies`, `binomial_text`, `species_text`,
  `trinomial_text`), and its `taxon` parameter (or, without one, its `genus` parameter) is a
  single word. The check ignores the rank stored in the Wikipedia cache, which is "genus" for some
  species pages, such as the Raiatea starling's. The species does take the page's names when the
  taxobox marks the genus monotypic or the store has exactly one species in the page's genus
  (`CommonNameStore.CountSpeciesInGenus`). Either way the species keeps its `exact`
  cross-reference to the page, so the lists can still link it. `aggregate --source wikipedia`
  prints the number of species and subspecies it skipped this way (511 in October 2026).
- The store's Wikipedia article for a taxon (`CommonNameStore.GetWikipediaArticleTitle`) is the
  page its `wikipedia_title` or `wikipedia_taxobox` name came from (`GetWikipediaNamePage`). A
  taxon with neither (for example one whose article title is a scientific name) gets the page of
  its `exact` Wikipedia cross-reference (`GetMatchedWikipediaPage`), never an `other_taxon_page`
  one. The lists use a title only as a link target, never as the text of a line.
- The lists (`StoreBackedCommonNameProvider`) choose the link for a taxon in this order:
  1. The page its `wikipedia_title` or `wikipedia_taxobox` name came from.
  2. When the taxon has an `exact` cross-reference, the taxon's own scientific name, if English
     Wikipedia has a page or a redirect with that title and the title is not a disambiguation
     page. `EnwikiTitleCheck` looks for the title in `enwiki_dump_titles` and for a `wiki_pages`
     row with status `cached`. A cached page with `is_disambiguation = 1`, or a cached redirect
     to one, does not count, even when `enwiki_dump_titles` lists it. A title the cache has not
     downloaded counts when `enwiki_dump_titles` lists it, so it can still be a disambiguation
     page. For a subspecies or variety it tries the name without a rank marker first, then the
     scientific name that the line shows.
  3. The page of the `exact` cross-reference.
  4. When the own name is a cached disambiguation page (or a cached redirect to one), the own name
     followed by the bracketed word for the taxon's kingdom, if English Wikipedia has that title:
     "Ficus variegata (plant)", "Orestias elegans (fish)". This step does not need an `exact`
     cross-reference. When there are several such titles, a plant takes "(plant)" before "(tree)"
     and "(palm)" (`WikiPageKingdom.QualifiedTitlesFor`).

  No step links a page about a taxon in another kingdom (`WikiPageKingdom`, the same check that
  `wikipedia match-taxa` makes). A page counts as about another kingdom when at least one of the
  following gives a kingdom and none of them gives the taxon's IUCN kingdom: the taxobox
  `kingdom` parameter, the bracketed word after the taxobox `genus` or `taxon` parameter ("Ficus
  (gastropod)"), the bracketed word at the end of the title, and the `<group> described in
  <year>` categories ("Gastropods described in 1798"). A bracketed word or category group that is
  not in the tables in `WikiPageKingdom`, such as "(disambiguation)" or "Taxa described in", is
  ignored. When no step gives a page, the line links the scientific name itself.

  So the line for *Leucoraja wallacei* links `[[Leucoraja wallacei]]`, a redirect to the genus
  article, not `[[Leucoraja]]`. *Moolgarda buchanani* has no page or redirect with its own name,
  so its line links the page of its cross-reference, "Crenimugil buchanani". The fig *Ficus
  variegata* and the palm *Gaussia princeps* link "Ficus variegata (plant)" and "Gaussia princeps
  (plant)". The pages "Ficus variegata" and "Gaussia princeps" are cached disambiguation pages, so
  step 2 skips those two titles. Since 3 October 2026, `wikipedia match-taxa` matches the two
  plants to the "(plant)" pages, and `common-names aggregate` has stored those pages as their
  `exact` cross-references, so step 3 gives the "(plant)" pages. Until then, the cross-references
  were "Ficus variegata (gastropod)" and "Gaussia princeps (crustacean)"; step 3 skips such a page,
  because it is about another kingdom, and step 4 gives the "(plant)" page.
  Section headings and links to a parent species use the same order
  (`StoreBackedCommonNameProvider.GetWikipediaArticleTitleByScientificName`). Without a
  Wikipedia cache the lists skip steps 2 and 4 and the kingdom check. `sprat generate-lists`
  creates the provider from a Wikipedia cache it has already opened, and that provider checks only
  `wiki_pages` in steps 2 and 4, not `enwiki_dump_titles`.
- Stored rows change only when a source is aggregated again: after a change to the filtering
  rules below, run `aggregate --source wikipedia --replace` and `aggregate --source wikidata
  --replace`, then `site build-db`.

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
scientific synonym, and names are compared ignoring case, spaces and punctuation. Store taxa with
the same scientific name and kingdom count as one taxon. The store can hold a species under both
an old IUCN id and its current id (*Arthroleptella bicolor* is 58057 and 121376651), and both are
given the same Wikipedia title. A name used for one of the two taxa is used for the other too. A
junk name (see [Common name quality](#common-name-quality)) does not count as a name the taxon
has, and a repairable name counts under its repaired form.
`wikipedia generate-lists`, `sprat generate-lists`, `site build-db` and
`common-names report --report ambiguous` apply the same rule to these names
(`CommonNameStore.QueryAmbiguousNames`, `AmbiguousNames`), and work out the names from the store
each time they run, so there is nothing to rebuild after aggregating.

Each ambiguous name is used for at most one taxon, and skipped for all the other taxa that have
it (a name set for a taxon in `rules/rules-list.txt` is used even so; see below). `AmbiguousNames`
decides which taxon may use it. Step 0 applies to a name that only a species and its own
subspecies, varieties and subpopulations have. Steps 1 to 4 apply to every other ambiguous name.

0. When the taxa that have the name are one species and its own subspecies, varieties and
   subpopulations, each taxon's best source for the name is compared in the chooser's order
   (`CommonNameStore.GetSourcePriority`; see the chooser below): Wikipedia article title,
   Wikipedia taxobox, Wikidata label, IUCN main name, other IUCN names, other Wikidata names,
   Catalogue of Life. The taxon whose best source comes first may use the name. If two or more
   taxa have their best source at the same place in that order, the species may use the name, and
   if the species is not one of them, no taxon may use it.
1. The taxon that has the name from the highest-priority source may use it, unless step 2 applies.
   The sources, highest priority first (`AmbiguousNames.KeeperPriority`):
   1. Wikipedia article title
   2. IUCN main name
   3. Wikipedia taxobox
   4. Wikidata label
   5. Other IUCN names
   6. Other Wikidata names
   7. Catalogue of Life
2. One Wikipedia article can be about two or more IUCN taxa, for example when IUCN has split a
   species and Wikipedia still has one article for it. Step 2 applies when the highest-priority
   source is the title of such an article. The candidates are then the taxa that have the name as
   the article's title, and the other taxa that have the name from any source and are matched to
   the same article (a `wikipedia` cross-reference, usually `other_taxon_page`). These rules are
   tried in order on all the candidates, and the first rule that picks one taxon decides:
   - a species takes priority over its own subspecies, varieties and subpopulations;
   - the one candidate that has the name as its IUCN main name may use it (if two or more
     candidates have it as their IUCN main name, the taxobox and then the Wikidata label choose
     between them, as in step 3);
   - otherwise the taxon that has the name as the article's title may use it, as in step 1 (if two
     or more taxa have the name as the article's title, step 4 applies to them).
3. When two or more taxa have the name as their IUCN main name, and no taxon has it as a
   Wikipedia article title, the one taxon among them that also has the name from its Wikipedia
   taxobox may use it. If that does not pick one taxon, the one taxon among them that also has the
   name as its Wikidata label may use it (`AmbiguousNames.IucnMainTieBreak`).
4. If two or more taxa still have the name at equal priority, the name is skipped for all of them,
   except that a species takes priority over its own subspecies, varieties and subpopulations.

Steps 2 and 4 compare a species with its own subspecies only when an unrelated taxon also has the
name. Step 0 was added in October 2026. Before it, when a species had a name from its taxobox and
its nominate subspecies had the same name as its IUCN main name, step 1 gave the name to the
subspecies, because step 1 ranks an IUCN main name above a taxobox name. About 30 species lost a
name to their own subspecies this way, and 13 of them had no English name left (the
*Chrysoritis* opals).

A taxon that may use a name is not always listed under it: the chooser (below) tries the taxon's
own names in its own order and takes the first one that is not skipped for the taxon.

Examples from the store of 3 October 2026:

- *Panthera leo* has "Lion" as its Wikipedia article title, and *Panthera leo* ssp. *leo* has it
  only as one of its other IUCN names, so *Panthera leo* is listed as "Lion".
- *Lithobates sylvaticus* has "Wood frog" as its Wikipedia article title, so it is listed as
  "Wood frog", although "Wood frog" is also the IUCN main name of *Papurana daemeli*.
- "Torchwood" is the IUCN main name of *Amyris ignea* and the taxobox name of *Balanites
  maughamii*, so *Amyris ignea* is listed as "Torchwood" and *Balanites maughamii* as "Manduro",
  one of its Wikidata common names (P1843). Until October 2026, *Balanites maughamii* was listed
  as "Torchwood" and *Amyris ignea* had no English name.
- "White oak" is the IUCN main name of both *Quercus alba* and *Grevillea baileyana*, and only
  *Quercus alba* also has it from its taxobox, so *Quercus alba* is listed as "White oak".
- "Silver wattle" is the IUCN main name of both *Acacia dealbata* and *Acacia neriifolia*, and
  neither has it from a taxobox or as a Wikidata label, so no taxon is listed as "Silver wattle".
  *Acacia rivalis*, which has it from its taxobox, is listed as "Creek wattle", its IUCN main name.
- The article "Scarlet-bellied mountain tanager" has *Anisognathus igniventris* in its taxobox, and
  *Anisognathus lunulatus* is matched to the same article. "Scarlet-bellied mountain-tanager" is the
  IUCN main name of *A. lunulatus*, so *A. lunulatus* is listed by that name, and *A. igniventris*
  is listed by its IUCN main name, "Fire-bellied mountain-tanager".
- The article "Golden tanager" has *Tangara arthus* in its taxobox, and "Golden tanager" is the
  IUCN main name of *Tangara aurulenta*. English Wikipedia has no page or redirect with the title
  "Tangara aurulenta", so *T. aurulenta* is not matched to the article, step 2 does not apply, and
  *T. arthus* is listed as "Golden tanager".
- "Water opal" is a taxobox name of *Chrysoritis palmus* and the IUCN main name of its subspecies
  *Chrysoritis palmus* ssp. *palmus*, so step 0 applies. In the chooser's order a taxobox name
  comes before an IUCN main name, so *Chrysoritis palmus* is listed as "Water opal", and the name
  is skipped for the subspecies. The species has no other English name. Before step 0 was added,
  the subspecies used the name and the species was listed by its scientific name.

Until October 2026, the order of the sources in step 1 was the order in which the chooser tries a
taxon's own names (below), with the Wikipedia taxobox and the Wikidata label before the IUCN main
name, and there were no steps 0, 2 and 3. On the store of 3 October 2026, the change affected the
English name of 313 taxa, counting the chooser's pick from the store without `rules-list.txt`: 84
taxa gained an English name, 44 lost theirs, and 185 got a different one. These counts were
measured before step 0 was added.

`CommonNameChooser` is the one place a taxon's English name is chosen. `wikipedia generate-lists`,
`sprat generate-lists`, `site build-db` and `common-names report --report trace` all use it. For a
species entry it takes the first of these that the taxon has:

1. A common name set for the taxon in `rules/rules-list.txt`. It is used even if it is
   ambiguous.
2. The taxon's first common name in this source order (`CommonNameStore.GetSourcePriority`) that
   is not junk and not skipped for it: Wikipedia article title, Wikipedia taxobox, Wikidata label,
   IUCN main name, other IUCN names, other Wikidata names, Catalogue of Life. This is a different
   order from the one that decides which taxon uses an ambiguous name. The name is repaired where
   `CommonNameQuality` can repair it. A name from a Wikipedia article title keeps
   the title's capitals, with the first letter upper case ("Large Palau flying fox", "Banded
   martin"). A name from another source that is one of the taxon's article titles apart from its
   capitals is shown with the title's capitals. Any other name, including a taxobox name, gets the
   capitalization rules ("White Ash" becomes "White ash").

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
ambiguous name, and its "Uses This Name" column is Yes for the taxon the name is used for (on
two rows when an old and a current IUCN id have the same scientific name). On the store of
3 October 2026, before step 0 was added, it listed 10,371 names: 7,078 used for one taxon each
and 3,293 used for no taxon (equal-priority ties). With `--kingdom`, the report counts only the taxa in that kingdom,
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
stores only the ones that are not.

For a Wikipedia title or taxobox name, `aggregate` first compares the name with the scientific
name in the page's own taxobox: the `taxon`, `binomial` or `trinomial` parameter, or `genus` with
`species` and `subspecies` (`WikipediaPageMatch.IsTaxoboxSubject`). If they are equal, ignoring
case, rank markers, a subgenus and the hybrid sign, the name is a scientific name and the rules
below are not applied. For example, the page titled "Tliltocatl epicureanus" is matched to
*Brachypelma epicureanum*, and its taxobox has `taxon = Tliltocatl epicureanus`, so the title is a
scientific name, although the store has no genus *Tliltocatl* and no epithet *epicureanus*. The
rules, in order:

1. The name is one of the taxon's own names, one of them followed by an authority or a note
   ("Myristica fatua Sw."), or the first word or words of one (the genus of a monotypic genus, the
   species of a subspecies), ignoring case, rank markers and a subgenus: a scientific name. A genus
   taken from a synonym does not count when it is an English word ("Orca" for Orcinus orca).
2. Otherwise only a name shaped like a scientific name can be one: two to four words, the first a
   capitalised word of plain letters and the rest lower case. A single word is a scientific name
   when it is a genus in the store and not an English word ("Strumigenys", but not "Platypus").
   A name with any other shape is a common name. Before any rule is applied, these are removed
   from the name: double quotes around a genus ("\"Hyla\" nicefori"), the hybrid sign ×
   ("Yucca × schottii"), an "x" between the genus and the epithet ("Yucca x schottii"), and a
   period before a hyphen inside an epithet ("Cyanea st.-johnii"). An "x" in any other position
   stays in the name, so "Eurasian Teal x Green-winged Teal" is a common name.
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
