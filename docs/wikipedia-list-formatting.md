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

### Pending Features

- Integration of COL-enriched hierarchy with rank labels
