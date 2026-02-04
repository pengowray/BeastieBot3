# BeastieBot3 Project Walkthrough

## Overview

BeastieBot3 is a .NET 9.0 CLI application designed to aggregate and process biological taxonomy data from multiple sources. It downloads, caches, cross-references, and analyzes species data from IUCN Red List, Catalogue of Life (COL), Wikidata, and Wikipedia to generate reports and Wikipedia species lists.

## Architecture

The project follows a command-based architecture using Spectre.Console.Cli. Commands are organized into domain-specific branches (IUCN, COL, Wikidata, Wikipedia, Common Names), with shared services and repositories for data access.

```
BeastieBot3/
├── Program.cs                    # Entry point, CLI command registration
├── [Domain]Commands              # CLI commands organized by data source
├── [Domain]Stores/Repositories   # SQLite data access layers
├── [Domain]ApiClients            # HTTP clients for external APIs
├── [Domain]Parsers/Analyzers     # Data parsing and analysis utilities
├── Helpers/Normalizers           # String normalization utilities
└── WikipediaLists/               # Wikipedia list generation subsystem
```

## Data Sources & Workflows

### 1. IUCN Red List

The IUCN module handles species conservation assessment data.

#### Import Pipeline
1. **CSV Import** (`iucn import`)
   - [IucnImportCommand.cs](../BeastieBot3/IucnImportCommand.cs) - CLI entry point
   - [IucnImporter.cs](../BeastieBot3/IucnImporter.cs) - Parses IUCN CSV exports into SQLite

2. **API Caching** (`iucn api cache-full`)
   - [IucnApiClient.cs](../BeastieBot3/IucnApiClient.cs) - HTTP client with rate limiting
   - [IucnApiCacheStore.cs](../BeastieBot3/IucnApiCacheStore.cs) - SQLite cache for API responses
   - [IucnApiCacheTaxaCommand.cs](../BeastieBot3/IucnApiCacheTaxaCommand.cs) - Downloads taxa JSON
   - [IucnApiCacheAssessmentsCommand.cs](../BeastieBot3/IucnApiCacheAssessmentsCommand.cs) - Downloads assessments

#### Analysis & Reports
- [IucnTaxonomyConsistencyCommand.cs](../BeastieBot3/IucnTaxonomyConsistencyCommand.cs) - Validates taxonomy consistency
- [IucnScientificNameVerifier.cs](../BeastieBot3/IucnScientificNameVerifier.cs) - Checks name formatting
- [IucnDataCleanupAnalyzer.cs](../BeastieBot3/IucnDataCleanupAnalyzer.cs) - Identifies data quality issues
- [IucnTaxonNameChangeReportCommand.cs](../BeastieBot3/IucnTaxonNameChangeReportCommand.cs) - Tracks name changes
- [IucnSynonymFormattingReportCommand.cs](../BeastieBot3/IucnSynonymFormattingReportCommand.cs) - Synonym analysis
- [IucnHtmlConsistencyCommand.cs](../BeastieBot3/IucnHtmlConsistencyCommand.cs) - HTML/text comparison

#### Key Files
| File | Purpose |
|------|---------|
| [IucnTaxonomyRepository.cs](../BeastieBot3/IucnTaxonomyRepository.cs) | Query IUCN SQLite data |
| [IucnTaxaJsonParser.cs](../BeastieBot3/IucnTaxaJsonParser.cs) | Parse API JSON responses |
| [IucnSisIdProvider.cs](../BeastieBot3/IucnSisIdProvider.cs) | Iterate IUCN species IDs |
| [IucnSynonymService.cs](../BeastieBot3/IucnSynonymService.cs) | Retrieve species synonyms |

---

### 2. Catalogue of Life (COL)

COL provides comprehensive taxonomic reference data.

#### Import Pipeline
1. **ColDP Import** (`col import`)
   - [ColImportCommand.cs](../BeastieBot3/ColImportCommand.cs) - CLI entry point
   - [ColImporter.cs](../BeastieBot3/ColImporter.cs) - Parses ColDP ZIP archives (TSV + YAML)

2. **Verification** (`col check`)
   - [CheckColCommand.cs](../BeastieBot3/CheckColCommand.cs) - Verifies dataset is mounted

#### Analysis
- [ColNameUsageFieldProfileCommand.cs](../BeastieBot3/ColNameUsageFieldProfileCommand.cs) - Data quality profiling
- [ColSubgenusHomonymReportCommand.cs](../BeastieBot3/ColSubgenusHomonymReportCommand.cs) - Detects name collisions
- [IucnColCrosscheckCommand.cs](../BeastieBot3/IucnColCrosscheckCommand.cs) - Cross-reference IUCN vs COL

#### Key Files
| File | Purpose |
|------|---------|
| [ColTaxonRepository.cs](../BeastieBot3/ColTaxonRepository.cs) | Query COL taxa |
| [ColNameUsageRepository.cs](../BeastieBot3/ColNameUsageRepository.cs) | Query name usage records |

---

### 3. Wikidata

Wikidata links taxonomy data with Wikipedia and provides structured knowledge graph data.

#### Caching Pipeline
1. **Seed** (`wikidata seed`) - Queries Wikidata SPARQL for all taxon entities
   - [WikidataSeedCommand.cs](../BeastieBot3/WikidataSeedCommand.cs)
   
2. **Cache** (`wikidata cache-items`) - Downloads full entity JSON
   - [WikidataCacheItemsCommand.cs](../BeastieBot3/WikidataCacheItemsCommand.cs)
   - [WikidataEntityDownloader.cs](../BeastieBot3/WikidataEntityDownloader.cs)

3. **Combined** (`wikidata cache-full`) - Runs seed + cache
   - [WikidataCacheFullCommand.cs](../BeastieBot3/WikidataCacheFullCommand.cs)

#### Analysis & Reports
- [WikidataCoverageReportCommand.cs](../BeastieBot3/WikidataCoverageReportCommand.cs) - IUCN coverage stats
- [WikidataCoverageDetailsCommand.cs](../BeastieBot3/WikidataCoverageDetailsCommand.cs) - Detailed coverage analysis
- [WikidataIucnFreshnessReportCommand.cs](../BeastieBot3/WikidataIucnFreshnessReportCommand.cs) - Stale P141 values
- [WikidataWikipediaMismatchReportCommand.cs](../BeastieBot3/WikidataWikipediaMismatchReportCommand.cs) - Sitelink issues

#### Key Files
| File | Purpose |
|------|---------|
| [WikidataCacheStore.cs](../BeastieBot3/WikidataCacheStore.cs) | SQLite cache for entities |
| [WikidataApiClient.cs](../BeastieBot3/WikidataApiClient.cs) | HTTP + SPARQL client |
| [WikidataEntityParser.cs](../BeastieBot3/WikidataEntityParser.cs) | Parse entity JSON |
| [WikidataIucnMatchLookup.cs](../BeastieBot3/WikidataIucnMatchLookup.cs) | Match IUCN taxa to Wikidata |
| [WikidataIucnFreshnessAnalyzer.cs](../BeastieBot3/WikidataIucnFreshnessAnalyzer.cs) | Compare conservation status |

---

### 4. Wikipedia

Wikipedia caching enables matching IUCN taxa to encyclopedia articles.

#### Caching Pipeline
1. **Enqueue** (`wikipedia enqueue`) - Queue titles from Wikidata sitelinks
   - [WikipediaEnqueueCommand.cs](../BeastieBot3/WikipediaEnqueueCommand.cs)
   - [WikipediaEnqueueTaxaCommand.cs](../BeastieBot3/WikipediaEnqueueTaxaCommand.cs) - Queue higher taxa

2. **Fetch** (`wikipedia fetch`) - Download page content
   - [WikipediaFetchCommand.cs](../BeastieBot3/WikipediaFetchCommand.cs)
   - [WikipediaPageFetcher.cs](../BeastieBot3/WikipediaPageFetcher.cs)

3. **Match** (`wikipedia match`) - Link IUCN taxa to Wikipedia
   - [WikipediaMatchTaxaCommand.cs](../BeastieBot3/WikipediaMatchTaxaCommand.cs)

#### Key Files
| File | Purpose |
|------|---------|
| [WikipediaCacheStore.cs](../BeastieBot3/WikipediaCacheStore.cs) | SQLite cache for pages |
| [WikipediaApiClient.cs](../BeastieBot3/WikipediaApiClient.cs) | Action + REST API client |
| [TaxoboxParser.cs](../BeastieBot3/TaxoboxParser.cs) | Parse taxobox templates |
| [WikipediaTitleHelper.cs](../BeastieBot3/WikipediaTitleHelper.cs) | Title normalization |

---

### 5. Common Names

Aggregates vernacular names from all sources for disambiguation analysis.

#### Pipeline
1. **Initialize** (`common-names init`)
   - [CommonNameInitCommand.cs](../BeastieBot3/CommonNameInitCommand.cs)

2. **Aggregate** (`common-names aggregate --source <source>`)
   - [CommonNameAggregateCommand.cs](../BeastieBot3/CommonNameAggregateCommand.cs)

3. **Report** (`common-names report --report <type>`)
   - [CommonNameReportCommand.cs](../BeastieBot3/CommonNameReportCommand.cs)

#### Key Files
| File | Purpose |
|------|---------|
| [CommonNameStore.cs](../BeastieBot3/CommonNameStore.cs) | Unified names database |
| [CommonNameNormalizer.cs](../BeastieBot3/CommonNameNormalizer.cs) | Name normalization |
| [CommonNameProvider.cs](../BeastieBot3/CommonNameProvider.cs) | Best name selection |
| [CapsFileParser.cs](../BeastieBot3/CapsFileParser.cs) | Capitalization rules |
| [CommonNameDetectConflictsCommand.cs](../BeastieBot3/CommonNameDetectConflictsCommand.cs) | Find ambiguous names |

---

### 6. Wikipedia List Generation

Generates Wikipedia species list articles from IUCN data.

#### Commands
- `wikipedia-list generate` - [WikipediaListCommand.cs](../BeastieBot3/WikipediaLists/WikipediaListCommand.cs)

#### Key Files (WikipediaLists/)
| File | Purpose |
|------|---------|
| [WikipediaListGenerator.cs](../BeastieBot3/WikipediaLists/WikipediaListGenerator.cs) | Core list generation |
| [WikipediaListDefinition.cs](../BeastieBot3/WikipediaLists/WikipediaListDefinition.cs) | List config model |
| [WikipediaListDefinitionLoader.cs](../BeastieBot3/WikipediaLists/WikipediaListDefinitionLoader.cs) | YAML loader |
| [WikipediaTemplateRenderer.cs](../BeastieBot3/WikipediaLists/WikipediaTemplateRenderer.cs) | Mustache templates |
| [IucnListQueryService.cs](../BeastieBot3/WikipediaLists/IucnListQueryService.cs) | Query species for lists |
| [IucnRedlistStatus.cs](../BeastieBot3/WikipediaLists/IucnRedlistStatus.cs) | Conservation status codes |
| [TaxonRulesService.cs](../BeastieBot3/WikipediaLists/TaxonRulesService.cs) | Exclusion/override rules |
| [ColTaxonomyEnricher.cs](../BeastieBot3/WikipediaLists/ColTaxonomyEnricher.cs) | Add COL taxonomy ranks |
| [StoreBackedCommonNameProvider.cs](../BeastieBot3/WikipediaLists/StoreBackedCommonNameProvider.cs) | Common names for lists |

---

## Shared Infrastructure

### Configuration
| File | Purpose |
|------|---------|
| [Program.cs](../BeastieBot3/Program.cs) | CLI registration, command tree |
| [PathsService.cs](../BeastieBot3/PathsService.cs) | INI path configuration |
| [IniPathReader.cs](../BeastieBot3/IniPathReader.cs) | INI file parsing |
| [EnvFileLoader.cs](../BeastieBot3/EnvFileLoader.cs) | .env file loading |
| [ShowPathsCommand.cs](../BeastieBot3/ShowPathsCommand.cs) | Display configured paths |
| [ReportPathResolver.cs](../BeastieBot3/ReportPathResolver.cs) | Resolve output paths |

### API Configurations
| File | Purpose |
|------|---------|
| [IucnApiConfiguration.cs](../BeastieBot3/IucnApiConfiguration.cs) | IUCN API settings |
| [WikidataConfiguration.cs](../BeastieBot3/WikidataConfiguration.cs) | Wikidata API settings |
| [WikipediaConfiguration.cs](../BeastieBot3/WikipediaConfiguration.cs) | Wikipedia API settings |

### Scientific Name Handling
| File | Purpose |
|------|---------|
| [ScientificNameNormalizer.cs](../BeastieBot3/ScientificNameNormalizer.cs) | Name normalization |
| [ScientificNameHelper.cs](../BeastieBot3/ScientificNameHelper.cs) | Name construction |
| [AuthorityNormalizer.cs](../BeastieBot3/AuthorityNormalizer.cs) | Authority string handling |

### Taxonomy Utilities
| File | Purpose |
|------|---------|
| [TaxonLadder.cs](../BeastieBot3/TaxonLadder.cs) | Hierarchical classification |
| [TaxonLadderFactory.cs](../BeastieBot3/TaxonLadderFactory.cs) | Create ladders from sources |
| [TaxonLadderAlignment.cs](../BeastieBot3/TaxonLadderAlignment.cs) | Compare classifications |
| [TaxonomyTreeBuilder.cs](../BeastieBot3/TaxonomyTreeBuilder.cs) | Build taxonomy trees |
| [TaxonSources.cs](../BeastieBot3/TaxonSources.cs) | Source identifier constants |

### Import Tracking
| File | Purpose |
|------|---------|
| [ApiImportMetadataStore.cs](../BeastieBot3/ApiImportMetadataStore.cs) | Track import operations |

### HTML Utilities
| File | Purpose |
|------|---------|
| [IucnHtmlUtilities.cs](../BeastieBot3/IucnHtmlUtilities.cs) | HTML to text conversion |

---

## Database Schema Overview

The application uses multiple SQLite databases:

1. **IUCN Database** (from CSV import) - Taxa, assessments, synonyms
2. **IUCN API Cache** - Raw JSON responses from IUCN API
3. **Wikidata Cache** - Entity JSON, sitelinks, P141 indexes
4. **Wikipedia Cache** - Page content, wikitext, taxobox data
5. **Common Names Store** - Unified vernacular names from all sources

---

## CLI Command Tree

```
beastiebot3
├── col                           # Catalogue of Life
│   ├── check                     # Verify COL mount
│   ├── import                    # Import ColDP archive
│   └── profile                   # Profile name usage fields
├── iucn                          # IUCN Red List
│   ├── import                    # Import CSV exports
│   ├── api                       # API operations
│   │   ├── cache-taxa            # Cache taxa JSON
│   │   ├── cache-assessments     # Cache assessment JSON
│   │   └── cache-full            # Run full caching
│   └── reports                   # Various analysis reports
├── wikidata                      # Wikidata
│   ├── seed                      # Seed taxon Q-IDs
│   ├── cache-items               # Download entities
│   ├── cache-full                # Seed + cache
│   ├── rebuild-indexes           # Rebuild lookup indexes
│   └── reports                   # Coverage, freshness reports
├── wikipedia                     # Wikipedia
│   ├── enqueue                   # Queue pages for download
│   ├── enqueue-taxa              # Queue higher taxa pages
│   ├── fetch                     # Download queued pages
│   ├── match                     # Match IUCN to Wikipedia
│   └── status                    # Cache statistics
├── common-names                  # Common name aggregation
│   ├── init                      # Initialize database
│   ├── aggregate                 # Import from sources
│   ├── sources                   # Show source status
│   └── report                    # Generate reports
├── wikipedia-list                # Wikipedia list generation
│   └── generate                  # Generate species lists
└── show-paths                    # Display configured paths
```

---

## Getting Started

1. Configure paths in `paths.ini` for your environment
2. Import IUCN data: `beastiebot3 iucn import`
3. Import COL data: `beastiebot3 col import`
4. Build Wikidata cache: `beastiebot3 wikidata cache-full`
5. Build Wikipedia cache: `beastiebot3 wikipedia enqueue && beastiebot3 wikipedia fetch`
6. Aggregate common names: `beastiebot3 common-names init && beastiebot3 common-names aggregate --source all`
7. Generate reports or Wikipedia lists as needed

---

## Legacy Code

The `BeastieLegacy/` directory contains older code that is not actively run but serves as reference for business logic and historical approaches. Do not modify unless migrating functionality to the main codebase.
