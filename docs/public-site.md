# Public species site (Beastie Bot Species Status)

Beastie Bot Species Status is an unofficial, free, read-only website for looking up IUCN Red List
taxa, aimed at Wikipedia editors. Each taxon page shows the latest global assessment (category,
criteria, population trend, dates), the assessment history, regional assessments, common names and
synonyms, links to Wikipedia, Wikidata, the Catalogue of Life and SPRAT, and wikitext to copy:
`{{cite iucn}}` with the assessment's authors and DOI, `{{IUCN status}}`, and the taxobox status
parameters. Instructions for deploying it to an Oracle Cloud Always Free VM are in
`deploy/oracle/README.md`.

## Parts

| Part | Where | What it does |
| --- | --- | --- |
| Shared library | `BeastieBot3.Shared/` | Code both the CLI and the site use. Keep it on net9.0 with no NuGet packages, so both can reference it. `Wikitext/`: `IucnCitationParts` (one assessment's citation, parsed by `site build-db` and stored as JSON in `citation_json`) and the renderers `CiteIucnRenderer`, `IucnStatusTemplate`, `SpeciesboxStatus`, `ScientificNameMarkup`. `SiteData/`: `SiteDbSchema` (the site database's DDL, `Version` and meta keys) and `SiteNameKey.Fold` (the folded form of a name used for exact lookups). |
| GBIF checklist | `BeastieBot3/Iucn/Gbif/` | `iucn gbif-download` downloads, and `GbifIucnChecklistReader` reads, GBIF's CC BY 4.0 copy of the IUCN checklist. |
| Site build | `BeastieBot3/SiteBuild/` | `site build-db` builds the site database; `site check-citations` writes a read-only report. Citation code: `IucnCitationPartsParser`, `IucnAuthorNameParser` and `AssessorNamePool` (parsing), `IucnDoiSelector` (choosing a DOI), `IucnTaxaHeaders` (each taxon's list of assessments). |
| Site | `BeastieBot3.Site/` (net10.0, Razor Pages) | Opens the site database read-only and reads no other data. Tests in `BeastieBot3.Site.Tests/`. |
| Deployment | `deploy/oracle/` | Server setup, app and database deploys, rollback, status. |

`BeastieBot3.Site` references only `BeastieBot3.Shared`, never the `BeastieBot3` project, because
that project contains the local web UI (`BeastieBot3/Web`, `serve`), which runs CLI commands and
must never be reachable from outside the machine.

## Updating the site after a Red List release

1. Import the release CSV zips (`iucn import`) and refresh the API cache (`iucn api refresh-start`,
   then `iucn api cache-all --full`). See the CLAUDE.md sections "IUCN SQLite Rules" and
   "Re-importing the API cache for a new release". Wait until `iucn api refresh-status` shows that
   the refresh session has closed: the site's history and citations come from the API cache.
2. Refresh the other inputs, so new taxa get names and links: `wikipedia update` (Wikidata items and
   Wikipedia articles), `common-names aggregate` (English names), and `col build-placement`
   (Catalogue of Life ids; the placement file only covers the species of the IUCN release it was
   built from). SPRAT changes rarely; run `sprat download` and `sprat import --force` when you want
   newer EPBC listings.
3. Run `iucn gbif-download`. It keeps the new checklist zip only when it differs from the newest
   zip (by the date in the file name) in `Datasets:GBIF_IUCN_dir`; `site build-db` reads the newest.
4. Run `site build-db`. For release 2026-1 it takes about 65 seconds and writes a database of about
   410 MB. It writes `<Datastore:site_sqlite>.building` and replaces `Datastore:site_sqlite` only
   when the build finishes; a failed or cancelled build leaves the previous database in place.
5. Optional: run `site check-citations`. It writes a Markdown report to `reports_dir` listing parse
   failures and comparing the parsed citations with the `{{cite iucn}}` templates in the cached
   English Wikipedia articles (the local cache, not live Wikipedia).
6. Run `deploy/oracle/deploy-db.sh` to deploy the new database.

## The site database

`SiteDbSchema.Ddl` is the contract between `site build-db` and the site. Increase
`SiteDbSchema.Version` whenever you add, remove or rename a table or column, or change what a column
holds, then run `site build-db` again. The site answers 503 (on `/healthz` and every page) for a
database with any other version, so deploy the new site and the rebuilt database together.

Rules the site depends on (pinned by `SiteDbBuildTests` and the site tests):

- Every taxon in the IUCN CSV export is a row of the `taxon` table, including subspecies, varieties
  and subpopulations. Taxa that are in the API cache but not in the CSV export are left out: old or
  merged ids, and taxa with no current assessment, such as the Amur leopard (taxon id 15957).
- The latest assessments come from the CSV export, which holds exactly one Red List release. Earlier
  assessments come from the list of assessments in each taxon's cached API response (the
  `assessments` array of the taxa JSON, read by `IucnTaxaHeaders`). Take the `latest` flag from that
  list, not from the `latest` field inside an assessment's own cached JSON, which can be out of date.
  A global row's scope is the literal `Global`.
- `category` is IUCN's code exactly as published, including the pre-1994 codes (`V`, `Ex`, `nt`).
  Codes are compared case-sensitively: `nt` (Not Threatened, pre-1994) and `NT` (Near Threatened)
  are different. `criteria_version` is `3.1`, `2.3`, or NULL for IUCN's "Earlier Version". A row
  whose code also exists in the current system (such as `NT` or `EX`) but whose `criteria_version`
  is NULL was assessed under a pre-1994 system; the site shows no `{{IUCN status}}` or taxobox
  wikitext for it.
- `citation_json` is `IucnCitationParts.ToJson()`. It never holds assessment narrative text.
  `replaced_by_assessment_id` links an assessment to the errata or amended version that replaced it.
- Every taxon's scientific name is in the `name` and `name_key` tables, and the `name_fts` index is
  rebuilt after the bulk insert. The finished file is in rollback-journal mode, not WAL, so a
  read-only process can open it.
- Language codes are ISO 639-1 where one exists, otherwise IUCN's ISO 639-2 code; `und`, `zxx`,
  `mis`, `mul` and the local-use range (`qaa` to `qtz`) become NULL.
- `common_name_en`, the English name shown on each page, is chosen exactly as the Wikipedia lists
  choose it: `CommonNameStore.ChooseBest` with the store's taxon id and the ambiguity rule
  (`AmbiguousNames`, which gives a name that several taxa have to the taxon with the best source
  for it), the capitalisation rules, `rules-list.txt` overrides and
  `SpeciesLineFormatter.IsUnusableCommonName`. So a wrong English name appears both on the site and
  in the lists.

## Citations

`IucnCitationPartsParser` reads the assessors in each cached assessment's `credits` array (the
entry with `credit_type_name` "assessor"; when there is none, the author part of IUCN's citation
text) and IUCN's citation text:

- Title annotations become fields: `(Europe assessment)` is `RegionalScope`,
  `(errata version published in YYYY)` is `ErrataYear`, `(amended version of YYYY assessment)` is
  `AmendsYear`. `{{cite iucn}}` shows an error when an errata or amended annotation is left in
  `|title=`.
- `SplitCreditNames` is told how many people to expect: the number of distinct entries in the
  credit's `value[]` array, which lists each person with their affiliation. Two assessors with the
  same short name ("Alemu, S., Alemu, S.") therefore stay as two authors.
- Each name is a Person (`Last` + `Initials`), an Organisation, or Verbatim (kept as IUCN wrote it;
  the site asks the editor to check these). A name that looks like a surname followed by given names
  (not initials) is a Person only when `value[]` has 2 or more entries and the split matched that
  count; otherwise it is Verbatim.
- A name with a letter lost to an encoding error ("Kry�tufek, B.", "U?ur Kaya") is repaired when
  exactly one undamaged assessor name in the cache matches it (`AssessorNamePool`). Assessments with
  a damaged name are parsed last, after the names of all the others have been collected.
- `IucnDoiSelector` reads the taxon id and assessment id inside a DOI and accepts the DOI only when
  the taxon id is the taxon's and the assessment id is the assessment's own. For an errata version
  (`ErrataYear` set) the id of the assessment it replaced is also accepted, because errata versions
  published from 2015 to 2018 kept their predecessor's DOI. Priority: IUCN's citation text, then
  GBIF (latest global assessments only), then Wikidata. IUCN's citation text has a DOI for only
  about 12% of latest assessments, so most DOIs come from GBIF. A DOI is never built from a year:
  the release part of a DOI cannot be predicted.

`CiteIucnRenderer` writes one line in `{{make cite IUCN}}` order and never writes `|page=` or
`|url=`. It escapes `|`, removes braces and CS1's invisible characters, wraps names that CS1 would
misread in `((...))`, writes generational suffixes as `|first=P.P. II` in the last/first style, and
prefixes an all-digit ref name with `iucn-`. To check output against Wikipedia's live CS1 and
Cite IUCN modules, send it to `https://en.wikipedia.org/w/api.php` with `action=parse`,
`title=Test` (so mainspace categories apply) and `prop=text|categories`, and wait about 3 seconds
between anonymous POSTs. Use a User-Agent such as
`BeastieBot3-site-dev/0.1 (https://en.wikipedia.org/wiki/User:Beastie_Bot)`, never one with an
email address.

## The site

- UI strings are in `BeastieBot3.Site/Display/SiteText.cs`, the About page text in
  `Pages/About.cshtml`, and category labels and badge colours (from en-wiki Module:IUCN status) in
  `Display/IucnCategories.cs`. All SQL is in `Data/SiteQueries.cs`; user input reaches FTS5 only
  through `Data/FtsQuery.cs`.
- Settings (`appsettings.json`, or environment variables such as `Site__DatabasePath`):
  - `Site:DatabasePath`.
  - `Site:BaseUrl`: the address used in canonical links, such as `https://species.example.org`.
  - `Site:ContactText` and `Site:ContactUrl`: who to contact about problems. Until they are set,
    pages say "the person who runs this site".
  - `Site:SourceUrl`: when set, the footer links to the source code.
  - `Site:RateLimits`: per client IP, `PagesPerMinute` (default 60), `SearchPerMinute` (30) and
    `SuggestPerMinute` (30); for the whole site, `ConcurrentSearches` (4) and `SearchQueueLength` (8).
- The site checks the database file when a request arrives, no more than once every 30 seconds. A
  replaced file is used from the next check, without a restart (connection pools are cleared and the
  output cache is emptied). A missing file or a wrong schema version makes `/healthz` answer 503 with
  a reason.
- Species pages are output-cached for an hour, keyed on the query parameters the page reads.
- The IUCN Red List Terms of Use limit what the site may hold and offer: no assessment narrative
  text, no coded threats, habitats or countries, no downloads, and no API that returns assessment
  fields (`/api/suggest` returns names, ids and the category only). Every page with IUCN data shows
  the Red List version and links to the About page, which credits every source with its licence and
  citation.
- Run locally with `dotnet run --project BeastieBot3.Site`. `appsettings.Development.json` points to
  `~/datasets/beastiebot/site.sqlite`; set `Site__DatabasePath` to use another file.

## Known gaps

- `common_name_en` follows the lists' ambiguity rule, which skips any English name that another taxon
  also has. "Lion" is also IUCN's name for the subspecies *Panthera leo leo*, and "Tiger" is a
  Catalogue of Life name for a grouper, so *Panthera leo* shows "Lioness" and *Panthera tigris*
  shows "Malayan tiger".
- About 4,200 taxa in release 2026-1 that are in the API cache but not in the CSV export (old or
  merged ids, and taxa with no current assessment such as the Amur leopard) have no page.
- About 300 subpopulation assessments have never been downloaded from the API, so their pages have
  no `{{cite iucn}}`. No command fetches them yet.
- Regional assessments have DOIs only when IUCN's citation text or Wikidata gives one.
