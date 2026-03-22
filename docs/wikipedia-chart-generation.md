# Wikipedia Chart Generation

## Overview

The `wikipedia generate-charts` command produces IUCN Red List bar chart data files for Wikipedia, using the MediaWiki [Extension:Chart](https://www.mediawiki.org/wiki/Extension:Chart) format. It replaces the deprecated `Module:Chart` pie charts (e.g. `{{IUCN mammal chart}}`).

## Usage

```bash
# Generate all chart groups
dotnet run --project BeastieBot3/BeastieBot3.csproj -- wikipedia generate-charts

# Generate specific groups
dotnet run --project BeastieBot3/BeastieBot3.csproj -- wikipedia generate-charts --group mammals --group birds

# Specify output directory
dotnet run --project BeastieBot3/BeastieBot3.csproj -- wikipedia generate-charts --output-dir charts/

# With explicit config paths (when not running from project root)
dotnet run --project BeastieBot3/BeastieBot3.csproj -- wikipedia generate-charts \
    --chart-config BeastieBot3/rules/chart-groups.yml \
    --taxa-config BeastieBot3/rules/taxa-groups.yml
```

## Output Files

Per group, two files are generated with a common base name like `IUCN Red List mammals 2025-2`. One shared chart definition and a summary file are also produced.

### `.tab` — Tabular Data (JSON, per group)

Uploaded to Wikimedia Commons as `Data:IUCN Red List mammals 2025-2.tab`. Uses the [Frictionless Data](https://specs.frictionlessdata.io/tabular-data-resource/) standard for tabular data resources.

Structure:
- `license`: CC0-1.0
- `description`: Localizable string (`{"en": "..."}`)
- `sources`: Free-text notes about methodology, scope, and any LR/cd merging
- `schema.fields`: Two columns — `category` (string) and `count` (number)
- `data`: Array of `[category_code, count]` pairs in display order

### `.Bar.chart` — Chart Definition (JSON, shared)

A single shared file uploaded to Wikimedia Commons as `Data:IUCN Red List species.Bar.chart`. Defines chart type, axis labels, and formatting. Per-group wikitext overrides the data source at embed time using `|data=`.

Structure:
- `type`: `"bar"`
- `title`: Generic localizable chart title
- `xAxis` / `yAxis`: Axis titles; `yAxis.format` is `false` (no abbreviation)

### `.wikitext` — Wikipedia Embedding Snippet (per group)

Wikitext to replace templates like `{{IUCN mammal chart}}`. Uses `{{image frame}}` with `{{#chart:IUCN Red List species.Bar.chart|data=IUCN Red List mammals 2025-2.tab}}` to embed with the shared chart definition and per-group data.

### `summary.txt` — Run Summary

Plain text file with a table of all group counts, notes about LR/cd merging, and a list of generated files. Written once at the end of the run.

## Status Category Ordering

Bars appear in this order (matching Wikipedia convention):

| Position | Code | Database filter |
|----------|------|-----------------|
| 1 | EX | `redlistCategory = 'Extinct'` |
| 2 | EW | `redlistCategory = 'Extinct in the Wild'` |
| 3 | CR(PE) | `redlistCategory = 'Critically Endangered'` AND `possiblyExtinct = 'true'` |
| 4 | CR(PEW) | `redlistCategory = 'Critically Endangered'` AND `possiblyExtinctInTheWild = 'true'` |
| 5 | CR | `redlistCategory = 'Critically Endangered'` AND both PE flags false |
| 6 | EN | `redlistCategory = 'Endangered'` |
| 7 | VU | `redlistCategory = 'Vulnerable'` |
| 8 | NT | `redlistCategory = 'Near Threatened'` + `'Lower Risk/conservation dependent'` |
| 9 | LC | `redlistCategory = 'Least Concern'` |
| 10 | DD | `redlistCategory = 'Data Deficient'` |

All bars are **mutually exclusive** — they sum to the total assessed count:

- **CR** excludes CR(PE) and CR(PEW) species. The `.tab` sources field and `.wikitext` caption document this.
- **NT** absorbs LR/cd (Lower Risk/conservation dependent). When LR/cd species exist, the count is noted in the sources text.

## Scope Filtering

Only global species-level assessments are counted:

```sql
WHERE (v.infraType IS NULL OR v.infraType = '')           -- species only
  AND (v.subpopulationName IS NULL OR TRIM(v.subpopulationName) = '')  -- no subpopulations
  AND (v.scopes IS NULL OR v.scopes = '' OR v.scopes LIKE '%Global%')  -- global scope
```

**Known limitation**: The `scopes LIKE '%Global%'` filter is not backed by an index and has not been fully audited for edge cases. A future improvement should index the `scopes` column and verify its values across all assessments.

## Configuration

### `chart-groups.yml`

Defines which taxonomic groups get charts. Each entry has:

```yaml
mammals:
  taxa_group: mammals      # Reference to taxa-groups.yml (null = all species)
  chart_name: "mammals"    # Used in filenames and chart titles
  comprehensive: true      # Whether IUCN considers this group fully assessed
  template_name: "IUCN mammal chart"  # Wikipedia template to replace (optional)
  caption: "Custom caption"           # Override default caption (optional)
```

### `taxa-groups.yml`

Shared with the list generation pipeline. Defines taxonomic filters (kingdom, class, order) for each group. Chart generation reuses these filters for counting.

## Available Chart Groups

| Group | Taxa Group | Comprehensive | Template |
|-------|-----------|--------------|----------|
| all | (none — all species) | No | — |
| animals | Animalia | No | — |
| mammals | Mammalia | Yes | IUCN mammal chart |
| birds | Aves | Yes | IUCN bird chart |
| reptiles | Reptilia | Yes | IUCN reptile chart |
| amphibians | Amphibia | Yes | IUCN amphibian chart |
| sharks-rays | Chondrichthyes | Yes | IUCN shark chart |
| ray-finned-fishes | Actinopterygii | No | — |
| bats | Chiroptera | Yes | — |
| rodents | Rodentia | Yes | — |
| primates | Primates | Yes | — |
| insects | Insecta | No | — |
| corals | Anthozoa | Yes | — |
| crustaceans | Malacostraca | No | — |
| gastropods | Gastropoda | No | — |
| plants | Plantae | No | — |
| conifers | Pinopsida | Yes | — |
| cycads | Cycadopsida | Yes | — |
| fungi | Fungi | No | — |

## Extension:Chart Format Notes

Extension:Chart is the replacement for the deprecated `Extension:Graph` on Wikimedia wikis.

- **Chart types**: `line`, `area`, `bar`, `pie`. We use `bar`.
- **Colors**: Fixed 10-color accessibility palette — no custom colors available. IUCN status colours (red for CR, orange for EN, etc.) cannot be used.
- **Sizing**: Charts fill their container width. Use `{{image frame|max-width=N}}` to constrain.
- **Localization**: Text fields use `LocalizableString` objects (`{"en": "...", "fr": "..."}`). Charts render in the wiki's content language.
- **Embedding**: `{{#chart:Name.chart}}` parser function. Can override data source with `|data=Other.tab`.
- **Data namespace**: Both `.tab` and `.chart` files live in `Data:` namespace on Wikimedia Commons.

## Architecture

| File | Purpose |
|------|---------|
| `WikipediaLists/ChartGeneratorCommand.cs` | CLI command, YAML loading, path resolution |
| `WikipediaLists/IucnChartDataBuilder.cs` | SQL queries for aggregate counts per status per group |
| `WikipediaLists/ChartOutputWriter.cs` | Generates per-group `.tab`/`.wikitext`, shared `.chart`, and `summary.txt` |
| `rules/chart-groups.yml` | Chart group definitions |
| `rules/taxa-groups.yml` | Taxonomic group filters (shared with list generation) |

## Alignment with IUCN Published Statistics

The counts should align with IUCN's [summary statistics](https://www.iucnredlist.org/resources/summary-statistics). Key differences to watch for:

- Our CR count excludes CR(PE) and CR(PEW) (they are separate bars), while IUCN's published CR total includes them.
- Our NT count includes LR/cd species (merged), matching IUCN's approach.
- The `scopes` filter may exclude or include edge cases differently from IUCN's own totals — audit periodically.
- IUCN may count some taxa differently at species vs. subspecies rank boundaries.
