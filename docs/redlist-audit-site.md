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
**The index opens with "Start here"**: up to five reports with `TriageRank > 0` and a non-zero count,
lowest rank first, each with its live count, its change since the previous release, its Action chip,
and the producer's one-line `TriageReason` (which must add to the title, not restate it).
Release-specific colour for that block goes in `commentary.yml` under `report: index`.

**Then three sections**, declared once in `AuditSiteRenderer.IndexSections` and selected by each
report's `SectionId`: `records` (absent, unreachable, or not current), `text` (stray characters,
markup, fields that disagree), `col` (the crosscheck). Boundaries follow what the observation is
about, not what the Action chip says, because that is what tells a reader whether a block is theirs.
The `col` block lists only `ColIndexHighlights` (`col-close-match`, `col-classification`) and links
to `col-crosscheck.html`, the crosscheck's own entry page (`BuildFamilyPage`): its pages record two
catalogues disagreeing, not IUCN errors, and listed beside whitespace findings they read as though the
site thought otherwise. The entry page and the you-are-here table on every member page share one
ordering (`FamilyRank`) and set `IsAppendix` reports apart under an "Appendix" row. The other two
sections use `Producers()` order. A report naming no section, or an unknown one, is listed in the last
section rather than dropped. There is no legend: the chip labels are instructions, and the site
disclaimer appears once, in the footer.

**"Since <release>" column.** `rules/audit/release-counts.yml` records headline counts per report per
release; `AuditReleaseCounts` picks the most recent earlier release and the index and family tables
print "up from N" / "down from N" / "unchanged" / "fixed (was N)", or nothing when that release
recorded no count for the report. The build never writes the file (a `--limit` run would record partial
counts); it prints the current release's block, and saves it as `release-counts.yml` in the output
directory, for pasting in once the release is final.

- **`ActionClass`** â€” the `Action` chip on the index and on each report heading: what a reader would
  do about the rows, not what kind of data they are. Four values, no per-report overrides:
  `Mechanical` → "Fix by script" (green; every row carries a replacement value), `ByHand` → "Fix by
  hand" (red), `Policy` → "Decide policy" (amber; e.g. `no-latest`, which is a question about how the
  dataset is meant to work), `Informational` → "No action" (grey). The labels are instructions, so
  there is no legend. Adding a fifth label is almost always the wrong fix. `CsvIsPatch` marks the
  reports whose CSV carries taxon id, field, current and replacement values on every row; the report
  page says so under the download link. Every CSV starts with an `id` column
  (`AuditCsvWriter.StableId`: `{report}:{Key}`) so a row can be cited and tracked across releases.
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
  (orchestrates index, per-report detail pages, full-list pages, family entry pages, assets), `HtmlText`
  (escaping, a whitespace visualiser, a tiny Markdown subset), `AuditAssets` (embedded CSS + JS).
  A summary table with more than 8 rows is written in full but carries `data-collapse="6"`; `audit.js`
  clamps it to 6 rows with a fade over the clipped row and a "Show all N rows" toggle, so a 15-row
  class breakdown no longer pushes the findings preview off the screen. With JS off the whole table
  shows.
- **`RedlistAuditSiteCommand`** — the `redlist audit-site` command; runs the producers and writes the bundle.

The reusable seams already in the codebase that producers call directly: `IucnTaxonomyRepository`,
`IucnDataCleanupAnalyzer`, `IucnScientificNameVerifier`, `IucnHtmlUtilities`,
`IucnTaxaTaxonomyExtractor`, `IucnRedlistStatus`, `TaxonFilterSql`, `ColTaxonRepository`,
`ScientificNameDifference`, `AuthorityNormalizer`, and the shared `Infrastructure/IucnUrls.Species(...)`
and `Infrastructure/ColUrls.Taxon(...)` link helpers.

## Reports

IUCN-owned (the body): assessments with no geographic scope, historical assessments missing from the
API, taxonomy field cleanup, synonym
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
`EmptyScopeProducer` and `FailedAssessmentsProducer` split what used to be one page. Until August
2026 the API answered HTTP 500 for every assessment carrying an empty `scopes` array, so those
records could only be seen as download failures in `failed_requests`; that server fault was fixed
without the scope being filled in, the downloads then succeeded, and the rows vanished from
`failed_requests` along with the observation. `EmptyScopeProducer` therefore reads the condition
itself rather than the symptom: an empty `scopes` array in the API cache (a ~10 second JSON scan of
the whole cache, so API-cache-optional and build-time only) unioned with blank `scopes` rows in the
CSV export, which is where the taxon's *current* blank-scope assessments show up. `failed-assessments`
keeps its id and URL but now reports only ids a taxon lists that return HTTP 404; its title and
summary widen automatically if a non-404 status ever returns.

`FieldHygieneProducer` profiles the taxonomy table column by column: for each text column, the share
of values with surrounding whitespace, repeated spaces, non-breaking or control characters, or non-NFC
normalisation. Plain non-ASCII content is deliberately not counted. Names, authorities, and place
names are expected to carry accented letters, so that count is large, uniform, and nothing can be done
with it; the page does not mention the omission, because a reader who never sees the row has no
question to answer.

`NameChangesProducer` finds nothing in current data: amended assessments keep the taxon's present name
and record the former name in the errata text rather than in a field, so a field comparison cannot see
a rename. It briefly published an always-empty page (then badged "Nothing found"); that told the reader
only that a check they never asked about found nothing, so it is suppressed again and publishes only
when the count is above zero. The command's skip line reads "(data source unavailable, or nothing to
report)" so the message stays true for both reasons a producer can return null.

**Page order.** Within each index section, `Producers()` order; the CoL set is always `FamilyRank`
order. There is no methodology page: the report pages carry their own source line and scope, and the
one-time methodology text said nothing the reader could act on.

### Catalogue of Life crosscheck (`Producers/ColCrosscheck/`)

`ColCrosscheckEngine` matches the release against the Catalogue of Life in one scan and sorts the
findings into nine separate report pages (most actionable first, noisiest last):

| Report id | Page | What it lists |
| --- | --- | --- |
| `col-close-match` | Names with a close CoL match | the IUCN name is in no CoL usage at all (accepted or synonym), but a near CoL name exists (likely spelling/encoding), with that near name checked against CoL's own status and IUCN's synonym list |
| `col-synonym` | Species and subspecies treated as synonyms in CoL | an assessed taxon whose name CoL records as a synonym of another accepted name, where IUCN does *not* list that accepted name among its own synonyms (with the accepted name's authority/year) |
| `col-accepted-differs` | Name pairs where IUCN and CoL differ on which name is accepted | a reversed pair: IUCN already lists the CoL accepted name among its synonyms, so the two records join and only the choice of accepted name differs. Fed from both `col-close-match` and `col-synonym` |
| `col-synonym-higher` | Higher-rank names treated as synonyms in CoL | a genus/family/order/class name CoL records only as a synonym (with the accepted name's authority/year) |
| `col-classification` | Higher-rank placement differences that look like spelling variants | a higher taxon whose parent differs like a typo (fuzzy/encoding), same phylum only |
| `col-reorg` | Higher-rank names placed differently in CoL | a higher taxon under a genuinely different parent (not a typo), same phylum only |
| `col-authority` | Minor naming authority differences | an exact name match whose author name differs like a typo (spelling/diacritic/encoding); differences only in spacing, punctuation, or the year are dropped |
| `col-via-wiki` | Names not in CoL, but a Wikidata or Wikipedia name is | the IUCN name is in no CoL usage, no near spelling, no IUCN synonym in CoL, but Wikidata or English Wikipedia record another name for the taxon that CoL holds (usually a genus transfer CoL adopted). Split out of `col-not-found` in `AddOtherSources`, which reassigns `ReportId`. The ask: check whether it is a valid synonym and, if so, add it, since no name currently joins the two records |
| `col-other-name` | Names not in CoL, but an IUCN synonym is | the IUCN name is in no CoL usage and has no near spelling, but another name IUCN records for the taxon is in CoL as a synonym. A lead, not a match: the chain can end on an unrelated name |
| `col-not-found` | Names not in CoL under any known name | no exact match, no near candidate, and no other IUCN-listed name for the taxon in CoL either |

Each report carries every one of its rows on the full-list page and the CSV (there is no
HTML-subset / CSV-superset split), shows the IUCN status badge like the other reports, and links each
row to its Catalogue of Life entry (`ColUrls.Taxon`, `catalogueoflife.org/data/taxon/{id}`). Columns
are ordered IUCN side first (name, rank, status, assessment year), then the CoL side (matched value,
authority, `CoL year`, link, cross-checks), then taxonomy context. The `CoL year` is the name's
`namePublishedInYear` when present, otherwise the year parsed from its authority string.

**Synonym cross-check.** `IucnSynonymIndex` scans the IUCN API cache once (the only place IUCN
carries synonyms, `taxon.synonyms[]`, reconstructing the bare name from the structured fields) and
answers, for a given CoL name, whether IUCN already records it as a synonym "of same taxon" (the
catalogues are reversed on which name is accepted) or "of other taxon". The close-match and synonym
reports both consult it, and an "of same taxon" answer routes the row out to `col-accepted-differs`,
so the value left in their own columns is only "no" or "of other taxon". The
column is blank when the API cache is unavailable; under `--limit` the index is partial, so run
without a limit for a complete answer.

Both synonym reports show the CoL accepted name's authority (which carries the year, a hint at when
the name was established). `col-synonym` runs the synonym cross-check on the CoL accepted name.

`col-close-match` runs two checks on the near name it suggests, one lookup each, because both change
what the row means: whether CoL itself treats that name as accepted or as a synonym of something else
(`CoL status` column), and whether IUCN already lists it as a synonym (`Closest name in IUCN synonyms`
column). A pair IUCN already records is a known relationship, not a spelling slip, so "align the
spelling" would be the wrong suggestion for it. On 2026-1 about a fifth of the suggested near names
are CoL synonyms, so the checks are not hypothetical. Note what the report does *not* claim: the near
name is chosen by spelling similarity, and a row is a candidate for review, not a confirmed pair.

**The reversed pairs.** Where IUCN's own synonym list already holds the name CoL accepts, both
catalogues carry both names and each points at the other, so the records join and the only
disagreement is which name heads them. Those rows are pulled out of `col-close-match` and
`col-synonym` into `col-accepted-differs` (130 + 2,516 = 2,646 on 2026-1), because "align the
spelling" is wrong advice for them and because leaving them in overstated both source pages. The
page says plainly that nothing on it needs fixing: the one practical consequence is that IUCN
publishes synonyms only through the API, so a match run against the CSV export alone will not see
the link. Both source pages link across to it.

`Authority years` on that page is a flag column, blank on all but ~51 rows. It fills only where the
two authority strings credit the **same author** (letters only, brackets and digits stripped) and
give **different years**; a different author is a different name, not a date discrepancy. Those rows
get +10 severity so `OrderSpecies` lifts them to the top. The year is read from the two authority
strings, not from `ColYear`, which prefers CoL's `namePublishedInYear`.

A "CoL has the more recent authority year" split was considered and rejected on the data: of the
1,657 reversed pairs with a parseable year on both sides, 1,489 have the *same* year (a recombination
carries the original author and year over), and of the rest the split is roughly even in both
directions. The differences that do exist are mostly the two catalogues disagreeing on the
publication year of one author's name, which is what `Authority years` reports.

**The crosscheck is a partition, and says so.** Every assessed name lands on exactly one of the nine
pages, or on none of them when it matches cleanly. Nothing on the site used to say that, so a reader
on `col-not-found` could not tell what the other eight covered. Each report carries `FamilyId = "col"`,
a one-line `FamilyScope`, and a `FamilyRank`; `AuditSiteRenderer.AppendFamilyTable` prints the whole
family as a "you are here" table under each member's description, with the current row marked.
`FamilyRank` orders by **how likely a row is to need a change**: genuine gaps first, then probable
fixes, then bulk review, then cleanup, with the pages that list differences needing no action last.
It ranks by what the reader does, not by taxonomic level, so a species-level page and its higher-rank
twin sit together. An earlier version ordered by closeness to a clean match, which opened with a page
whose own title says "minor" and buried "names not found" in sixth place. Counts come from the document,
so they cannot drift from the pages they link to. Heading and intro live in `FamilyHeadings`, keyed by
family id; add a second family there rather than special-casing the renderer.

**Three "not in CoL" outcomes, three pages.** They share a headline fact and differ in what lead was
found, which is a different job each time, so they are not merged: `col-close-match` (a near spelling
exists, so align it), `col-other-name` (another IUCN-listed name for the taxon is in CoL as a synonym,
so follow the chain and confirm), `col-not-found` (no route at all, so spot-check against literature).
Merging them would put three suggestions on one page. `col-classification` and `col-reorg` are left
split for the same reason: one says confirm a spelling, the other says be aware of a difference.

**ColDP shape.** This ColDP `nameusage` table has no `acceptedNameUsageID` column; a synonym's
`parentID` points at its accepted taxon, and every accepted name carries its higher-rank ancestors
(kingdom..family) inline. So synonymy is resolved through `parentID` (this fixed a silent zero-count
bug where the missing column always read NULL), and the higher-rank placement comparison reads the
inline ancestor columns instead of walking the tree.

**Name in use on `col-synonym` and `col-other-name`.** After the main pass, `AddNameInUse` asks
`OtherSourceIndex` which of the two names Wikidata and English Wikipedia use for each taxon
(`OtherSourceHit.NameInUse`: "CoL name" / "IUCN name" / "both" / "neither" / blank when the taxon is
in neither source). Neither source is an authority, but each is a third party that picked one name,
so both pages sort "CoL name" rows first and carry a "Name used on Wikidata and Wikipedia" summary
table. On 2026-1 (20k-row sample) the synonym page split 53 CoL / 271 both / 32 neither / 1,433 IUCN.

**`no-latest` breakdown.** None of the 4,218 taxa is in the 2026-1 CSV export (the page computes and
states this, so it stays true), so they are genuinely dropped taxa, not current taxa missing a flag.
The page sorts most recently assessed first and adds "By year of last assessment" (2020+ are recent
taxonomic changes; 1996/1998 have been in this state for every release) and "By scope of last
assessment" (230 Europe-only and 48 Mediterranean-only taxa were never globally assessed).

**Wikidata and Wikipedia on `col-not-found` (and the `col-via-wiki` split).** `OtherSourceIndex` reads the Wikidata cache
(`wikidata_p627_values` joined to `wikidata_entities` / `wikidata_scientific_names`, keyed on the IUCN
taxon id) and the Wikipedia cache (`taxon_wiki_matches` + `wiki_pages`), and runs as a second pass
over the `col-not-found` bucket alone, after the main scan. That bucket is a few hundred rows out of
~190,000, so a handful of indexed queries each costs nothing, and it is the one page whose claim
("no route into CoL") those two sources can qualify. Two columns are added to `col-not-found`: the Wikidata item and the
English Wikipedia article. A row where either source records a name that **is** in CoL moves to
`col-via-wiki` (with a Name in CoL column, its CoL status, and the CoL link). That is usually a genus
transfer CoL made and the Red List has not (`Idiopoma javanica` / `Filopaludina javanica`); on 2026-1
about 20 of 745 move, while about half of the rest have a Wikidata item and 109 have an article.

Both caches are optional and sit outside the Red List entirely. When neither is present the columns
are **left out of the table** rather than rendered empty, and the intro says the check did not run:
three blank columns read as "checked, found nothing", which is a different claim. `ColCrosscheckData`
carries `OtherSourcesChecked` plus the three counts so the page copy can state them.

**Two passes.** The first pass matches each assessed taxon (exact name, then genus/species/infra
components). No match runs two further checks in order: a fuzzy pass over the same genus and epithet
(`ScientificNameDifference` + `FindByGenericName`/`FindBySpecificEpithet`) gives `col-close-match`,
and failing that the taxon's own IUCN synonyms (`IucnSynonymIndex.SynonymsOf`) are looked up in CoL:
an accepted hit is `col-accepted-differs`, a synonym-only hit is `col-other-name`, nothing is
`col-not-found`. That third check is what catches a taxon CoL holds under a different genus, which no
amount of fuzzy spelling can reach (`Aethalodelphis obliquidens` / `Lagenorhynchus obliquidens`); a
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
  index.html                 overview, Start here, report tables (records, text, CoL highlights)
  col-crosscheck.html        the crosscheck's entry page: what it compares, every page, appendix
  release-counts.yml         this release's headline counts, for rules/audit/release-counts.yml
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
