# Red List audit site (`redlist audit-site`)

Generates an **unofficial, unaffiliated** static website that gathers data observations about an
IUCN Red List release, intended to be shared with the IUCN Red List team to help with data review.
The bundle is self-contained HTML plus CSV downloads, with relative links so it works from a local
folder, a static host, or an email attachment.

```bash
# Full build (scans the whole release; the CoL crosscheck is the long pole)
dotnet run --project BeastieBot3/BeastieBot3.csproj -- redlist audit-site

# Fast test build, capped rows per report
dotnet run --project BeastieBot3/BeastieBot3.csproj -- redlist audit-site --limit 5000
```

The default output directory is `<Datastore:reports_dir>/redlist-audit-2026` (the configured reports
directory in `paths.ini`, e.g. `D:\datasets\beastiebot\reports\redlist-audit-2026`), falling back to
`./reports/redlist-audit-2026` only when no reports directory is configured. Pass `--output` to
override. The release label comes from `import_metadata.redlist_version` in the IUCN CSV database
(falling back to the dataset folder name).

## Tone

The site is neutral and non-judgmental throughout. It describes "observations" and
"opportunities", never "errors" or "problems", and it states plainly that any observation may be
incomplete or mistaken. When editing copy, avoid em-dashes and "not X but Y" phrasing.

## Architecture (`BeastieBot3/Audit/`)

- **`Model/AuditFinding`** — one shared row shape every listing maps onto (ids, names, Linnaean
  ladder, status, field/current/suggested, issueType, detail, notes, plus `Extra` for
  report-specific columns). `Key` (`"{taxonId}:{issueType}"`) pins one-time commentary to a row.
- **`Model/AuditReport`** — a report: neutral `Summary`, optional `SummaryTables`, a column list,
  and findings pre-sorted by importance. The full-list page always shows every row on one page
  (filter box + click-to-sort), never split into per-group pages.
- **`Model/AuditColumn` + `AuditColumns`** — column definitions and a factory of reusable columns
  (scientific name, status badge, taxonomy, ids, Red List link, field/current/suggested). Defined
  once, rendered identically in HTML and CSV.
- **`AuditMapping`** — rank/full-species derivation, status-code normalisation, threat-order sort key.
- **`IucnStatusVisuals`** — status badge colour (ported from the legacy palette; a reading aid, not
  the official IUCN colours).
- **`Producers/`** — most reports are one `IAuditReportProducer` (returns `null` when its data source
  is unavailable so the command skips it). A producer that needs one pass over the data to emit
  several related pages implements `IAuditReportSetProducer` instead (returns an empty list when
  unavailable); `SingleReportProducer` adapts the ordinary producers so the command iterates one
  uniform list. Each opens what it needs through `AuditContext` and maps results to `AuditFinding`.
  The Catalogue of Life crosscheck (`Producers/ColCrosscheck/`) is the set producer.
- **`Rendering/`** — `HtmlListRenderer` (the one sortable/filterable table renderer), `AuditCsvWriter`
  (same columns to CSV), `AuditPageLayout` (page chrome + disclaimer), `AuditSiteRenderer`
  (orchestrates index, per-report detail pages, full-list pages, methodology, assets), `HtmlText`
  (escaping, a whitespace visualiser, a tiny Markdown subset), `AuditAssets` (embedded CSS + JS).
- **`RedlistAuditSiteCommand`** — the `redlist audit-site` command; runs the producers and writes the bundle.

The reusable seams already in the codebase that producers call directly: `IucnTaxonomyRepository`,
`IucnDataCleanupAnalyzer`, `IucnScientificNameVerifier`, `IucnHtmlUtilities`,
`IucnTaxaTaxonomyExtractor`, `IucnRedlistStatus`, `TaxonFilterSql`, `ColTaxonRepository`,
`ScientificNameDifference`, `AuthorityNormalizer`, and the shared `Infrastructure/IucnUrls.Species(...)`
and `Infrastructure/ColUrls.Taxon(...)` link helpers.

## Reports

IUCN-owned (the body): failed assessments (empty-scope HTTP 500), taxonomy field cleanup, synonym
whitespace irregularities, synonym markup/unusual characters, nomenclatural notes written inside
synonym names, orphan subspecies/varieties, taxa
with no current assessment, HTML vs plain-text narrative fields, scientific name vs components, and
the seven Catalogue of Life crosscheck pages (see below). The three synonym reports share one scan
(`SynonymFormattingScan`, memoised per connection): one lists whitespace problems (each kind counted
separately, including spaces inside parentheses or before a comma), the second lists markup, stray
HTML entities, curly quotes, and encoding artefacts with per-kind percentages and a with/without-HTML
consistency table, and the third (`SynonymNameNotesProducer`) lists synonyms whose name field also
carries a bracketed nomenclatural note such as `[orth. error]`. Plain non-ASCII letters are never
flagged on their own. The notes report's second summary table is the one that carries it: notes are
grouped by wording with punctuation, spacing and case set aside, which is what shows `orth. error`
being written five ways. A bracketed publication year (`[1803]`) is counted but not listed, being
standard nomenclature. The English common name
report (`CommonNameIssuesProducer`) refreshes a hand-compiled 2016 review of Red List common-name
oddities against the current release: species codes in the name field, all-capitals names, stray
whitespace, an acute accent or backtick used as an apostrophe, a leading "The", a "(FB)" FishBase
marker, ampersand/slash separators, non-English-script (Greek/Cyrillic) characters, and the curated
"likely plural" endings; checks that now find nothing (comma inside parentheses, literal question
marks) are still listed at zero. Low-value 2016 checks are dropped on purpose (abbreviation dots,
spelling, and the broad "possible plural" sweep).
Methodology: text hygiene by field. The scientific-name-change report appears only when the
field-based check finds a name that changed across assessment versions (it produces nothing in
current data and is omitted, via the producer returning null when empty).

### Catalogue of Life crosscheck (`Producers/ColCrosscheck/`)

`ColCrosscheckEngine` matches the release against the Catalogue of Life in one scan and sorts the
findings into seven separate report pages (most actionable first, noisiest last):

| Report id | Page | What it lists |
| --- | --- | --- |
| `col-close-match` | Names with a close CoL match | no exact match, but a near CoL name (likely spelling/encoding) |
| `col-synonym` | Species/subspecies CoL treats as a synonym | an assessed taxon whose name CoL records as a synonym of another accepted name (with the accepted name's authority/year, and whether IUCN already records that name as a synonym) |
| `col-synonym-higher` | Higher-rank names CoL treats as a synonym | a genus/family/order/class name CoL records only as a synonym (with the accepted name's authority/year) |
| `col-classification` | Higher-rank placement differences that look like spelling variants | a higher taxon whose parent differs like a typo (fuzzy/encoding), same phylum only |
| `col-reorg` | Higher-rank names placed differently in CoL | a higher taxon under a genuinely different parent (not a typo), same phylum only |
| `col-authority` | Minor naming authority differences | an exact name match whose author name differs like a typo (spelling/diacritic/encoding); differences only in spacing, punctuation, or the year are dropped |
| `col-not-found` | Names not found in CoL | no exact match and no near candidate |

Each report carries every one of its rows on the full-list page and the CSV (there is no
HTML-subset / CSV-superset split), shows the IUCN status badge like the other reports, and links each
row to its Catalogue of Life entry (`ColUrls.Taxon`, `catalogueoflife.org/data/taxon/{id}`). Columns
are ordered IUCN side first (name, rank, status, assessment year), then the CoL side (matched value,
authority, `CoL year`, link, cross-checks), then taxonomy context. The `CoL year` is the name's
`namePublishedInYear` when present, otherwise the year parsed from its authority string.

**Synonym reports.** Both synonym reports show the CoL accepted name's authority (which carries the
year, a hint at when the name was established). `col-synonym` also cross-references the CoL accepted
name against IUCN's own synonyms: `IucnSynonymIndex` scans the IUCN API cache once (the only place
IUCN carries synonyms, `taxon.synonyms[]`, reconstructing the bare name from the structured fields)
and the report flags whether IUCN already records that name as a synonym "of same taxon" (the
catalogues are reversed on which name is accepted) or "of other taxon". The column is blank when the
API cache is unavailable; under `--limit` the index is partial, so run without a limit for a complete
answer.

**ColDP shape.** This ColDP `nameusage` table has no `acceptedNameUsageID` column; a synonym's
`parentID` points at its accepted taxon, and every accepted name carries its higher-rank ancestors
(kingdom..family) inline. So synonymy is resolved through `parentID` (this fixed a silent zero-count
bug where the missing column always read NULL), and the higher-rank placement comparison reads the
inline ancestor columns instead of walking the tree.

**Two passes.** The first pass matches each assessed taxon (exact name, then genus/species/infra
components): no match becomes `col-not-found` or, via a fuzzy pass over the same genus and epithet
(`ScientificNameDifference` + `FindByGenericName`/`FindBySpecificEpithet`), `col-close-match`; a
synonym match becomes `col-synonym`; an accepted match has its authority compared for `col-authority`.
The second pass compares the distinct higher-rank names IUCN uses (genus, family, order, class):
names CoL records only as synonyms (zero accepted usages anywhere, so homonyms are not misreported)
become `col-synonym-higher`, and a differing parent placement becomes `col-classification` (typo) or
`col-reorg` (genuine), gated to the same phylum. IUCN's upper-case higher-rank names are folded to
CoL's Linnaean capitalisation for the exact, index-backed lookup.

**Difference buckets.** `ColDifference` decides what the placement and authority reports keep from a
`ScientificNameDifference`: a spelling/encoding/diacritic/punctuation difference is a typo, an
unrelated value is a genuine difference, and identical/whitespace/letter-case is dropped (IUCN's
upper-case house style is not a data slip). For authorities the comparison is on the author-name
letters only (accented letters kept): digits (years), whitespace, and all punctuation are stripped
first, so a difference that is purely one of those is dropped and only a real author-name difference
(a spelling, diacritic, or encoding slip) is kept. All comparison and display decode HTML entities first (the `_html` view
stores `&` as `&amp;`).

**Big lists.** The full-list pages can run to many thousands of rows; `content-visibility: auto` on
the sortable table rows lets the browser skip layout and paint for off-screen rows so the page stays
responsive.

## Output structure

```
reports/redlist-audit-2026/
  index.html                 overview, disclaimer, report tables (IUCN-owned, then methodology)
  methodology.html           how it was made, scope, caveats
  <report>.html              description + commentary + summary tables + short preview + links
  <report>-list.html         full sortable/filterable list (or a per-group index when very large)
  <report>-g-<class>.html     per-group pages when a report is split by class
  csv/<report>.csv           every row, CC0
  assets/audit.css, audit.js shared, embedded; no external dependencies
```

Each report page embeds a short preview and links out to the full list and the CSV.

## Year-specific vs generic commentary

- **One-time, release-pinned prose** lives in `rules/audit/commentary.yml`, keyed by `report` and
  `release` (or `release: any` to carry forward). `AuditCommentary` shows only entries matching the
  release being built, so commentary written about 2025-2 does not appear for 2026-1 unless it is
  marked `any`. Entries can be `scope: report` (page-level) or `scope: finding` (matched to a row by
  `key`).
- **Release-agnostic notes** are generated in code from each row's own fields (for example the
  empty-scope explanation on a failed assessment), so they carry forward to future releases
  unchanged.

When a new release is imported, re-run the command. The data-driven reports, counts, and code notes
update automatically; revisit `commentary.yml` to retire or re-pin the human prose.
