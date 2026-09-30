# Wikipedia List Formatting Guide

This document describes the formatting rules for Wikipedia IUCN species lists, comparing legacy (BeastieLegacy circa 2016) and new implementations.

## Species Listing Styles

### Overview

Three listing styles are available, configured via `display.listing_style` in YAML (use PascalCase for values):

| Style | Name | Use Cases | Example |
|-------|------|-----------|---------|
| A | ScientificNameFocus | Plants, invertebrates (large counts, rare common names) | `''[[Pinus radiata]]'', Monterey pine` |
| B | CommonNameFocus | Default for most animals | `[[Western gorilla]] (''Gorilla gorilla'')` |
| C | CommonNameOnly | Mammals, birds, bats, sharks & rays | `[[Western gorilla]]` |

### Style A: Scientific Name Focus

**Best for**: Plants, invertebrates, groups with large species counts or where common names are rare/inconsistent.

```wikitext
* ''[[Abies fanjingshanensis]]''
* ''[[Abies fraseri]]'', Fraser fir
* ''[[Abies guatemalensis]]'', Guatemalan fir
* ''[[Wikilink|Scientific name]]'', Common name  (when article uses common name as title)
```

**Rules**:
- Scientific name first, always italicized
- Common name follows after comma (if available)
- Link points to Wikipedia article if known, otherwise to scientific name (red link expected)
- Sort by scientific name

### Style B: Common Name Focus (Default)

**Best for**: Most animal groups where both common and scientific names are used.

```wikitext
* [[Western gorilla]] (''Gorilla gorilla'')
* [[Wikilink|Common name]] (''Scientific name'')
* [[Scientific name|Common name]] (''Scientific name'')  (when page is at scientific name)
* ''[[Scientific name]]''  (fallback when no common name)
```

**Rules**:
- Common name first with link, scientific name in parentheses after
- Always include scientific name (even when common name is same as article title)
- Link to Wikipedia article, or use scientific name as link target if no article
- Sort by scientific name

### Style C: Common Name Only

**Best for**: Well-known groups like mammals, birds, bats, sharks where all species have unambiguous common names.

```wikitext
* [[Gorilla]]
* [[Wikilink|Common name]]
* ''[[Scientific name]]''  (fallback when no common name)
```

**Rules**:
- Only common name shown (with link)
- Fall back to italicized scientific name if no common name
- Sort by scientific name

## Formatting Infraspecific Taxa

### Subspecies

**Animals** (hide "ssp." rank marker):
```wikitext
* ''[[Gorilla gorilla gorilla]]''
```

**Plants** (always show "subsp." not "ssp."):
```wikitext
* [[Picea engelmannii subsp. mexicana|''Picea engelmannii'' subsp. ''mexicana'']], Mexican spruce
```

### Varieties

Always show "var." for all kingdoms:
```wikitext
* [[Abies pinsapo var. marocana|''Abies pinsapo'' var. ''marocana'']], Moroccan fir
```

### Link Format for Infraspecific

The pipe format `[[link|display]]` is needed to properly italicize only the scientific parts:
- `[[Pinus mugo subsp. rotundata|''Pinus mugo'' subsp. ''rotundata'']]`
- NOT: `''[[Pinus mugo subsp. rotundata]]''` (this italicizes "subsp." incorrectly)

However, for animal subspecies where we hide the rank marker:
- `''[[Gorilla gorilla gorilla]]''` is correct (whole thing is italicized)

## Taxonomic Section Headings

### Legacy Behavior

The legacy code used:
- Order and Family level groupings
- `force-split` rule to force subdivision of certain taxa (e.g., Chordata, Squamata)
- `below` rule to insert intermediate taxa
- Merged small groups into "Other" when ≥5 groups with ≤4 species each

### New Behavior

Similar to legacy but with enhancements:
- COL enrichment provides additional ranks (superfamily, subfamily, tribe, etc.)
- `min_items` parameter in YAML to control merging threshold
- `other_label` parameter to customize the "Other" bucket name
- Virtual groups for paraphyletic groupings (e.g., Cetaceans vs Even-toed ungulates)

### Heading Format

Headings should include the rank label:
```wikitext
==Order: [[Chiroptera]]==
{{main|Bat}}

===Suborder [[Yangochiroptera]]===
====Family [[Emballonuridae]]====
```

### Merging Small Groups

When 3+ families each have ≤4 species, combine into "Other [parent]":
```wikitext
==== Other Squaliformes ====
'''Species'''
* [[Centrophorus westraliensis|Western Gulper shark]] {{IUCN status|...}} (Family: [[Centrophoridae]])
* [[Deania profundorum|Arrowhead dogfish]] {{IUCN status|...}} (Family: Centrophoridae)
```

**Rules**:
- "Other Squaliformes" is NOT a link
- First mention of each family is linked, subsequent are not
- Items sorted taxonomically (by family, then species)

### Don't Merge Top Level

Never create a single "Other mammals" combining unrelated orders. Keep top-level order headings even with few species.

## Taxa Sections (Species/Subspecies/Varieties)

Under each taxonomic heading, group taxa by type:

```wikitext
===Class: [[Pinopsida]]===
{{main|Conifer}}
'''Species'''
{{div col|colwidth=30em}}
*''[[Abies fanjingshanensis]]''
*''[[Abies fraseri]]'', Fraser fir
...
{{div col end}}
'''Subspecies'''
{{div col|colwidth=30em}}
*[[Abies nordmanniana subsp. equi-trojani|''Abies nordmanniana'' subsp. ''equi-trojani'']], Kazdagi fir
...
{{div col end}}
'''Varieties'''
{{div col|colwidth=30em}}
*[[Abies guatemalensis var. guatemalensis|''Abies guatemalensis'' var. ''guatemalensis'']]
...
{{div col end}}
'''Stocks and populations'''
...
```

### Section Visibility Rules

- If only one section type exists and it's "Species", hide the heading
- "Stocks and populations" heading used for regional assessments (subpopulations)
- For EX/PE/EW lists, separate sections for each status + taxon type:
  - "Extinct species", "Possibly extinct species", "Extinct in the wild species"
  - "Extinct subspecies", etc.

## Subspecies Grouping Under Species

For comprehensive lists (all statuses), subspecies can appear as sub-bullets:

```wikitext
* ''[[Genus species]]''
** ''G. s.'' subsp. ''subspecies1''
** ''G. s.'' subsp. ''subspecies2''
```

Enable via `display.group_subspecies: true` in YAML.

## Parent Lists (sub-groups)

A taxa group with `children:` in `taxa-groups.yml` (its sub-groups, as the Taxa grouping page calls
them) makes its lists parent pages. A parent page has:

- a summary table with one row per class (the rank the sub-groups are defined at), the EX to DD
  counts, Total, and CR total;
- one section per linked sub-group: its heading, `{{main|...}}` to the sub-group's list, and the
  number of species in the page's categories;
- one section per remaining class, listing that class's species on the parent page itself.

For each of its presets, a parent page links each sub-group to:

1. the sub-group's list for the same preset (`plants-lc` links `magnoliopsida-lc`);
2. otherwise the sub-group's all-status list (`plants-lc` links `conifers-all-status`), but only
   when the page already links at least one sub-group list for its own preset. An all-status list
   alone does not make a page a parent page: `plants-nt` is an ordinary list because no sub-group of
   plants has an `nt` list;
3. otherwise nothing. The sub-group's species are then listed on the parent page under their class
   heading.

`wikipedia generate-lists` prints these lines under each list's "Generating" line:

- `Sub-group lists linked: ...` for a parent page, with the list ids it links;
- a warning for each sub-group a parent page cannot link, naming the missing list id and the
  wikipedia-lists.yml entry to change;
- a note for a list whose group has sub-groups but that links none of them, so it has no summary
  table.

After "Save sub-groups", the Taxa grouping page checks the group's lists against the draft rules
and shows the warnings as a list under the status line. It gives one warning per sub-group, naming
every preset whose list the sub-group lacks (`No corals lists for presets cr, en, vu and ex, ...`),
where `generate-lists` prints one per list. The loader records one `ChildLinkNote` per list and
sub-group (`WikipediaListConfig.ChildLinkNotes`); `ChildLinkReport` turns them into these messages
(`ForList` for `generate-lists`, `WarningsForGroup` for the Taxa grouping page), and
`SubGroupLinkTests` pins both.

`see_also:` on a group adds a "Related lists" section to each of its lists, with a link to the
named group's list for the same preset (or its all-status list). It applies to ordinary lists as
well as parent pages.

Parent groups in the shipped rules: `fish` (ray-finned fishes, sharks and rays), `invertebrates`
(insects, gastropods, bivalves, crustaceans, corals, arachnids), and `plants` (dicots, monocots,
conifers, cycads; parent pages for `threatened` and `lc` only). `SubGroupShippedRulesTests` checks the
shipped rules: every parent page links all its sub-groups, and no list gets a sub-group warning.

A sub-group must be defined by a single `value:` at the rank the parent's sub-groups share (class
for all three parents). The summary table and the sub-group sections match each sub-group by that
one value, so a sub-group with a `values: [...]` filter, or one defined only at a higher rank than
the other sub-groups, gets no table row and no section.

Mosses are not a sub-group of `plants`. The `bryopsida` group (named "Mosses") covers class
Bryopsida only, so a "Mosses" section on the plants pages would count only 96 of the 112
threatened mosses in 2026-1. Widening the group to the other moss classes (Sphagnopsida,
Andreaeopsida, Takakiopsida, Polytrichopsida) would give it a `values: [...]` filter, and so no
table row and no section. The plants pages list Bryopsida species in an ordinary class section.

## Legacy Rules File (rules-list.txt)

The legacy `rules-list.txt` supports:

| Syntax | Purpose | Example |
|--------|---------|---------|
| `X = Y` | Common name | `Mammalia = mammal` |
| `X = Y ! Z` | Common name + plural | `Mollusca = mollusc ! molluscs` |
| `X plural Y` | Plural only | `Testudines plural turtles and tortoises` |
| `X adj Y` | Adjective form | `Mammalia adj mammalian` |
| `X wikilink Y` | Disambiguate link | `Anura wikilink Anura (frog)` |
| `X force-split true` | Always subdivide | `Chordata force-split true` |
| `X below Y : Z` | Insert taxon below | `(not currently used)` |
| `X includes Y` | Description | `Afrosoricida includes tenrecs and golden moles` |
| `X comprises Y` | Gray text | `Salamandridae comprises true salamanders and newts` |
| `X typo-of Y` | IUCN name correction | `Speocirolana thermydromis typo-of Speocirolana thermydronis` |

## New YAML Rules (taxon-rules.yml)

Extends legacy with:
- `global_exclusions` - regex patterns to exclude taxa
- `virtual_groups` - paraphyletic groupings (e.g., Squamata → Snakes/Lizards/Worm lizards)
- `main_article` - {{main|...}} link for taxa
- Per-list overrides

## IUCN Status Template

All entries include the status template:
```wikitext
{{IUCN status|CR|12345/67890|1|year=2024}}
```

- First parameter: status code (CR, EN, VU, NT, LC, DD, EX, EW, CR(PE), CR(PEW))
- Second parameter: taxonId/assessmentId
- Third parameter: `1` to make link visible
- `year=` parameter: assessment year (omitted for EX/EW)

## Key Differences: Legacy vs New

| Feature | Legacy (2016) | New |
|---------|---------------|-----|
| Config format | rules-list.txt | YAML (wikipedia-lists.yml, taxon-rules.yml) |
| Intermediate ranks | Manual via `below` rule | COL enrichment automatic |
| Virtual groups | Hardcoded | YAML configurable |
| Merge threshold | Fixed (5 groups, 4 items) | Configurable via `min_items` |
| Listing styles | Hardcoded per kingdom | YAML configurable per list |
| Regional assessments | Included | Configurable via `exclude_regional_assessments` (default: false) |
| IUCN status template | Not used | Always included |
| Infraspecific sections | Separate sections by default | Configurable via `separate_infraspecific_sections` (default: false) |

## Implementation Status

### Completed Features

- **Three listing styles** (A: ScientificNameFocus, B: CommonNameFocus, C: CommonNameOnly) - `WikipediaListGenerator.BuildNameFragment()`
- **Infraspecific formatting** with proper italics - `WikipediaListGenerator.BuildInfraspecificLink()`
  - Animals: hides "ssp." rank marker
  - Plants: shows "subsp." (normalized from "ssp.")
  - Varieties: always shows "var."
- **Regional assessment filtering** - `DisplayPreferences.ExcludeRegionalAssessments`
- **Infraspecific section separation** - `DisplayPreferences.SeparateInfraspecificSections`
  - Adds "Species", "Subspecies", "Varieties", "Stocks and populations" headings
- **Default listing styles per taxa group** - configured in `taxa-groups.yml`:
  - Plants, fungi, invertebrates → `ScientificNameFocus`
  - Mammals, birds, sharks/rays → `CommonNameOnly`
  - Others → `CommonNameFocus` (default)
- **Taxonomy rank labels** in headings - `GroupingLevelDefinition.ShowRankLabel`
  - When enabled, shows "Family: [[Familyidae]]" instead of just "[[Familyidae]]"
- **"Other" bucket family annotation** - `DisplayPreferences.IncludeFamilyInOtherBucket`
  - Adds "(Family: [[Familyidae]])" to items in "Other X" sections
  - First occurrence of each family is linked, subsequent are not
- **Small group merging** - `GroupingLevelDefinition.MinItems` threshold
  - Groups with fewer than N items are merged into "Other" bucket
  - Custom bucket name via `GroupingLevelDefinition.OtherLabel`

### Configuration Examples

#### Enable rank labels and family annotation in grouping:
```yaml
grouping:
  - level: order
    show_rank_label: true
  - level: family
    show_rank_label: true
    min_items: 5           # Merge families with <5 species
    other_label: "Other"   # Custom label for merged bucket
display:
  include_family_in_other_bucket: true
```

### Catalogue of Life name feedback

The Catalogue of Life is used to fix two name problems in list output (`BeastieBot3/Col/ColNameResolver.cs`,
the shared per-taxon resolver; IUCN stays the name of record throughout):

- **Article links (offline, `wikipedia match-taxa`):** four kinds of CoL name are added as candidate
  article titles (`IucnSynonymService`), each validated against the enwiki cache before it is
  recorded, so a wrong guess just fails to match rather than producing a wrong link. This fixes
  redlinks where the article lives at the accepted or current name. Re-run `match-taxa` (part of the
  col-update flow) for it to take effect.

  | Match method | When |
  | --- | --- |
  | `col-accepted` | the IUCN name is a CoL synonym of an accepted name |
  | `col-corrected` | the IUCN name is a formatting-equivalent slip (mojibake, diacritic, spacing) |
  | `col-variant` | CoL writes the same name a different legitimate way (see below) |
  | `col-accepted-via-synonym` | the IUCN name is unknown to CoL, but another name IUCN records for the taxon is a CoL synonym of an accepted name |

  `col-variant` uses `LatinNameVariant`, which asks whether two names are **one name written two
  ways**: gender agreement after a genus transfer (`Schistura striatus` / `striata`), a patronym
  formed with one -i or two (`lesueuri` / `lesueurii`), transliterated Greek (`rithymna` /
  `rhithymna`). It requires the genus to match exactly and the epithets to be equal once those
  endings and spellings are folded. This is deliberately **not** an edit-distance rule: over the 799
  close matches in 2026-1 it accepts 611 and rejects 188, and the rejects are the point.
  `Cordia santacruzensis` / `Cora santacruzensis`, `Sorex monticola` / `Shorea monticola`, and
  `Elater turcicus` / `Elater suecicus` are all one or two edits apart and are different taxa; linking
  any of them would put a wrong link on a published list.

  `col-accepted-via-synonym` is the second hop, and is what reaches CoL when the two catalogues
  disagree about the genus outright (`Idiopoma javanica` / `Filopaludina javanica`). It is gated on
  `ColNameResolution.NameIsUnknownToCol`, not on "the first hop returned no accepted name": almost
  every taxon is accepted in CoL under its IUCN name and also returns no accepted name, and running
  the hop for those would add several CoL queries per taxon across the whole Red List for nothing.
- **Displayed name (generation):** a garbled scientific name is replaced with the CoL spelling only
  when the difference is formatting-equivalent (mojibake, a diacritic, encoding, or spacing), via
  `IucnSpeciesRecord.ScientificNameOverride`, for full species only. A genuine spelling difference is
  left as IUCN records it. Disabled by `--no-col-enrichment`.

### Pending Features

- Integration of COL-enriched hierarchy with rank labels
