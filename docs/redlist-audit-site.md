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
- **`Producers/`** — one `IAuditReportProducer` per report. Each opens what it needs through
  `AuditContext`, reuses the existing analyzers where they are already callable, and maps results to
  `AuditFinding`. Returns `null` when its data source is unavailable so the command skips it.
- **`Rendering/`** — `HtmlListRenderer` (the one sortable/filterable table renderer), `AuditCsvWriter`
  (same columns to CSV), `AuditPageLayout` (page chrome + disclaimer), `AuditSiteRenderer`
  (orchestrates index, per-report detail pages, full-list pages, methodology, assets), `HtmlText`
  (escaping, a whitespace visualiser, a tiny Markdown subset), `AuditAssets` (embedded CSS + JS).
- **`RedlistAuditSiteCommand`** — the `redlist audit-site` command; runs the producers and writes the bundle.

The reusable seams already in the codebase that producers call directly: `IucnTaxonomyRepository`,
`IucnDataCleanupAnalyzer`, `IucnScientificNameVerifier`, `IucnHtmlUtilities`,
`IucnTaxaTaxonomyExtractor`, `IucnRedlistStatus`, `TaxonFilterSql`, `ColTaxonRepository`,
`AuthorityNormalizer`, `TaxonLadder*`, and the shared `Infrastructure/IucnUrls.Species(...)` helper.

## Reports

IUCN-owned (the body): failed assessments (empty-scope HTTP 500), taxonomy field cleanup, synonym
whitespace irregularities, synonym markup/unusual characters, orphan subspecies/varieties, taxa
with no current assessment, HTML vs plain-text narrative fields, scientific name vs components, and
differences from the Catalogue of Life. The two synonym reports share one scan
(`SynonymFormattingScan`, memoised per connection): one lists whitespace problems (each kind counted
separately, including spaces inside parentheses or before a comma), the other lists markup, stray
HTML entities, curly quotes, and encoding artefacts with per-kind percentages and a with/without-HTML
consistency table. Plain non-ASCII letters are never flagged on their own.
Methodology: text hygiene by field. The scientific-name-change report appears only when the
field-based check finds a name that changed across assessment versions (it produces nothing in
current data and is omitted, via the producer returning null when empty).

The Catalogue of Life crosscheck is much higher volume than the others, so its HTML pages list only
the higher-signal rows (a name with no exact CoL match, a name CoL treats as a synonym, and
placement differences above genus); authority and genus/species-level differences are summarised by
class. The CSV download still carries every row (`AuditReport.CsvFindings` holds the complete set
while `Findings` holds the HTML subset). Authority comparison and display decode HTML entities first
(the `_html` view stores `&` as `&amp;`), so they do not report spurious differences.

The primary CoL match is exact, but when it fails a fuzzy pass (`ScientificNameDifference` +
`ColTaxonRepository.FindByGenericName`/`FindBySpecificEpithet`) looks for near matches among names in
the same genus or sharing the epithet. A formatting-equivalent candidate is reported as the likely
same name with the reason it differs (spacing, punctuation, Unicode encoding, diacritics, case, or a
combination); otherwise the closest spelling variants are offered as possible alternatives with their
edit distance. The best candidate fills the "CoL value" column; the detail line carries the
explanation.

Very large full lists split into a recursive taxonomic tree (class, then order, then family) so no
single page exceeds the row threshold; the CSV still holds the whole report.

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
