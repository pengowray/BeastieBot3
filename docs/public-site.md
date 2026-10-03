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
| Shared library | `BeastieBot3.Shared/` | Code both the CLI and the site use. Keep it on the same target framework as both (net10.0) and with no NuGet packages, so both can reference it. `Wikitext/`: `IucnCitationParts` (one assessment's citation, parsed by `site build-db` and stored as JSON in `citation_json`) and the renderers `CiteIucnRenderer`, `IucnStatusTemplate`, `SpeciesboxStatus`, `ScientificNameMarkup`. `SiteData/`: `SiteDbSchema` (the site database's DDL, `Version` and meta keys) and `SiteNameKey.Fold` (the folded form of a name used for exact lookups). |
| GBIF checklist | `BeastieBot3/Iucn/Gbif/` | `iucn gbif-download` downloads, and `GbifIucnChecklistReader` reads, GBIF's CC BY 4.0 copy of the IUCN checklist. |
| IUCN citations | `BeastieBot3/Iucn/Citations/` | Code the site build and the Wikidata dry run share: `CreditNameSplitter` (splits a credit's `full` string into names), `IucnAuthorNameParser` (reads one name as a person, an organisation, or a name kept as IUCN wrote it), `AssessorNamePool` (repairs names with a letter lost to an encoding error) and `IucnCitationText` (removes IUCN's "Accessed on" sentence and reads the DOI in IUCN's citation text). |
| DOI lookup | `BeastieBot3/Iucn/Doi/` | `iucn resolve-dois` looks for DOIs that IUCN's citation text, GBIF and Wikidata do not give, in Crossref's list of IUCN DOIs and at doi.org, and saves them in the DOI cache (`Datastore:IUCN_doi_cache_sqlite`). See [Missing DOIs](#missing-dois-iucn-resolve-dois). |
| Site build | `BeastieBot3/SiteBuild/` | `site build-db` builds the site database; `site check-citations` writes a read-only report. Citation code: `IucnCitationPartsParser` (title annotations, and putting the parts together), `IucnDoiSelector` (choosing a DOI), `IucnTaxaHeaders` (each taxon's list of assessments). `SiteApiTaxaReader` reads the taxa that are only in the API cache. `SiteBuildRules.ClassifySpratName` matches SPRAT profiles to taxa, both whole-taxon profiles and population profiles. |
| Site | `BeastieBot3.Site/` (net10.0, Razor Pages) | Opens the site database read-only and reads no other data. `wwwroot/theme.js` and the tokens at the top of `wwwroot/site.css` make the light and dark themes (see [Theme](#theme)). Tests in `BeastieBot3.Site.Tests/`. |
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
4. Run `iucn resolve-dois --refresh-crossref`, then `iucn resolve-dois --scope latest-regional`.
   The first run downloads Crossref's list of IUCN DOIs again, so that it includes the new
   release's DOIs, and checks the latest global assessments. See
   [Missing DOIs](#missing-dois-iucn-resolve-dois).
5. Run `site build-db`. For release 2026-1 it takes about 80 seconds and writes a database of about
   450 MB. It writes `<Datastore:site_sqlite>.building` and replaces `Datastore:site_sqlite` only
   when the build finishes; a failed or cancelled build leaves the previous database in place. It
   reads the DOI cache from `Datastore:IUCN_doi_cache_sqlite` (`--doi-cache` gives another path),
   and uses the DOIs found in every scope.
6. Optional: run `site check-citations`. It writes a Markdown report to `reports_dir` listing parse
   failures and comparing the parsed citations with the `{{cite iucn}}` templates in the cached
   English Wikipedia articles (the local cache, not live Wikipedia).
7. Run `deploy/oracle/deploy-db.sh` to deploy the new database.

In the local web UI (`serve`), the "Update the public species site" workflow (`public-site`) has
these steps in the same order. The steps in its "1 · Inputs" group are the same steps as in the
"Import IUCN data" workflow and the "Wikipedia reports pipeline", with the same lights.
`PublicSiteStateReader` reads the files for the lights of the steps after them:

- "Download GBIF's copy of the IUCN checklist" compares the newest checklist's Red List release
  with the release in the IUCN Red List database.
- "Find missing DOIs" counts the latest global assessments that have no DOI from IUCN's citation
  text, the GBIF checklist or Wikidata, and how many of them the DOI cache has not checked yet. Its
  buttons run `iucn resolve-dois`, `iucn resolve-dois --scope latest-regional` and
  `iucn resolve-dois --status`.
- "Build the site database" compares the site database's schema version and Red List release with
  the current ones, and names each input that changed after the build.

The Data sources page has cards for the GBIF checklist folder, the DOI cache and the site database
(data source ids `gbif-checklist`, `iucn-doi-cache` and `site-sqlite`). The workflow shows them as
the files that the GBIF download, "Find missing DOIs" and "Build the site database" steps write.
The "Upload the site database to the server (manual)" step (step 7 above) shows the site
database as its input, and is blocked when that file is missing.

## The site database

`SiteDbSchema.Ddl` is the contract between `site build-db` and the site. Increase
`SiteDbSchema.Version` whenever you add, remove or rename a table or column, or change what a column
holds, then run `site build-db` again. The site answers 503 (on `/healthz` and every page) for a
database with any other version, so deploy the new site and the rebuilt database together.

Rules the site depends on (pinned by `SiteDbBuildTests` and the site tests):

- Every taxon in the IUCN CSV export is a row of the `taxon` table with `in_release = 1`, including
  subspecies, varieties and subpopulations.
- A taxon that has its own record in the API cache but is not in the CSV export is a row with
  `in_release = 0`: an old or merged id, or a taxon that IUCN no longer assesses, such as the Amur
  leopard (taxon id 15957). The build of 3 October 2026 has 4,223 of them. `SiteApiTaxaReader`
  reads their names, ranks and authority from the record's taxon object, and their kind from its
  infrarank and subpopulation flags (a name with "var." is a variety). The parent is the record's
  species (`species_taxa`) when that species is in the release, otherwise a taxon found by name.
  Every assessment of such a taxon has `is_latest = 0`, including one the API flags as latest, and
  the taxon's `latest_global_assessment_id` is always NULL.
- `current_taxon_id` (only when `in_release = 0`) is the taxon in the release with the same
  scientific name: one in the same kingdom first, then one of the same kind, then the lowest id.
  The `taxon_current` index lets a taxon page find the old ids that have its name.
- `epbc_listing` has one row per SPRAT profile of a taxon (schema version 3; version 2 had the
  columns `taxon.sprat_taxon_id` and `taxon.epbc_status` instead). `applies_to` is `taxon` for the
  profile of the whole taxon (matched by the taxon's scientific name, or by one of the IUCN names
  that the profile lists) and `population` for each profile named "<taxon name> (<population>)", with the
  text in brackets in `population`. In
  `SiteBuildRules.ClassifySpratName`, the brackets must be one group at the end of the name and
  must not contain a digit (digits mean a voucher in a phrase name such as "Acacia sp. Castletower
  (N.Gibson TOI345)"); a sense in brackets, such as "sensu lato", means the whole taxon. When a
  taxon in the release and an old id have the same name, the profile goes to the taxon in the
  release.
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
  For an errata version, the build looks for the replaced assessment among the ids that
  `IucnTaxaHeaders.PredecessorIds` returns. If none of those ids is an earlier assessment row of
  the taxon, and the errata version's DOI came from the DOI cache, the build uses the assessment
  id inside that DOI (see [Citations](#citations)). This finds replaced assessments that IUCN now
  lists with another scope or year. For example, *Pinus pinea* assessment 2977175 is now scoped
  Europe, and its row is marked as replaced by the global errata version 129160976. The build
  summary row "Errata versions whose replaced assessment was found from their DOI (another scope
  or year)" counts these cases: 6 in release 2026-1.
- Every taxon's scientific name is in the `name` and `name_key` tables, and the `name_fts` index is
  rebuilt after the bulk insert. The finished file is in rollback-journal mode, not WAL, so a
  read-only process can open it.
- Language codes are ISO 639-1 where one exists, otherwise IUCN's ISO 639-2 code; `und`, `zxx`,
  `mis`, `mul` and the local-use range (`qaa` to `qtz`) become NULL.
- `common_name_en`, the English name shown on each page, is chosen exactly as the Wikipedia lists
  choose it, by `CommonNameChooser`: a `rules-list.txt` override, otherwise the best of the store's
  names for the taxon (by source priority, skipping junk names and names another taxon keeps under
  the ambiguity rule `AmbiguousNames`, repaired where `CommonNameQuality` can repair them, with the
  capitals of the taxon's Wikipedia article title or else the capitalisation rules), and none when
  that name is not usable as a common name. So a wrong English name appears both on the site and
  in the lists.
- The `name` table leaves out common names that `CommonNameQuality` finds to be junk (wiki markup,
  author citations, OCR errors, names cut off at a bracket), in every language, and stores
  repairable ones repaired (`SiteNameSet`). The `site build-db` summary counts both: "Common names
  left out as junk" (once per name, language and source) and "Common names repaired before
  storing" (rows stored; the HTML and leading-backslash tidying in `SiteBuildRules.CleanName` is
  not counted). In an October 2026 build of release 2026-1, about 450 common names were left out
  and about 110 were repaired.
- When the build reads the DOI cache, it sets the meta key `iucn_doi_checked_to` to the newest
  `checked_at` date in the cache's `doi_check` table.

Start new build tests from `BeastieBot3.Tests/SiteBuild/SiteBuildSourceFixture`, imported with
`using static`. The fixture has the temporary folder, writers for the IUCN CSV database and the API
cache, the `Header` and `Payload` JSON builders (an assessment in a taxon record, and an
assessment's own payload), and SQL helpers.

## Citations

`IucnCitationPartsParser` reads the assessors in each cached assessment's `credits` array (the
entry with `credit_type_name` "assessor"; when there is none, the author part of IUCN's citation
text) and IUCN's citation text. The author-name code is in `BeastieBot3/Iucn/Citations/`, shared
with the Wikidata dry run, and so is `IucnCitationText`, which removes IUCN's "Accessed on ..."
sentence from the citation text and reads the DOI it links to.

- Title annotations become fields: `(Europe assessment)` is `RegionalScope`,
  `(errata version published in YYYY)` is `ErrataYear`, `(amended version of YYYY assessment)` is
  `AmendsYear`. `{{cite iucn}}` shows an error when an errata or amended annotation is left in
  `|title=`.
- `CreditNameSplitter.Split` is told how many people to expect: the number of distinct entries in
  the credit's `value[]` array, which lists each person with their affiliation. Two assessors with
  the same short name ("Alemu, S., Alemu, S.") therefore stay as two authors.
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
  GBIF (latest global assessments only), then Wikidata, then the DOI cache that
  `iucn resolve-dois` writes (`DoiSource.Resolved`: found in Crossref's list of IUCN DOIs or at
  doi.org). `IucnDoiSelector` checks a DOI from the DOI cache like the others, with one
  addition for an errata version. If the DOI contains the errata version's taxon id and the id of
  another assessment, that assessment is accepted as one the errata version replaced, even when
  `IucnTaxaHeaders.PredecessorIds` does not return it (`IucnDoiSelector.ErrataPredecessorNamedBy`).
  This is safe because `iucn resolve-dois` saves such a DOI only when the DOI resolves to the
  errata version's page. In release 2026-1 this rule gives 8 more assessments a DOI. For example,
  the global errata version 129160976 of *Pinus pinea* gets the DOI
  `10.2305/IUCN.UK.2013-1.RLTS.T42391A2977175.en`, which contains the id of assessment 2977175.
  IUCN's citation text has a DOI for only about 12% of latest assessments, so most DOIs come from
  GBIF.
- `site build-db` never builds a DOI from a year: the release part of a DOI cannot be predicted.
  `iucn resolve-dois` builds candidate DOIs from the year, but saves one only when doi.org confirms
  that it exists.
- A missing DOI cache file, or one without the `doi_check` table, gives a warning, and the build
  continues without those DOIs.

`CiteIucnRenderer` writes one line in `{{make cite IUCN}}` order and never writes `|page=` or
`|url=`. It escapes `|`, removes braces and CS1's invisible characters, wraps names that CS1 would
misread in `((...))`, writes generational suffixes as `|first=P.P. II` in the last/first style, and
prefixes an all-digit ref name with `iucn-`. To check output against Wikipedia's live CS1 and
Cite IUCN modules, send it to `https://en.wikipedia.org/w/api.php` with `action=parse`,
`title=Test` (so mainspace categories apply) and `prop=text|categories`, and wait about 3 seconds
between anonymous POSTs. Use a User-Agent such as
`BeastieBot3-site-dev/0.1 (https://en.wikipedia.org/wiki/User:Beastie_Bot)`, never one with an
email address.

### Missing DOIs (`iucn resolve-dois`)

`iucn resolve-dois` works on the assessments in a scope that have no DOI from IUCN's citation
text, GBIF or Wikidata (checked the way `site build-db` checks them), and saves what it finds in
the DOI cache (`Datastore:IUCN_doi_cache_sqlite`). `--scope` takes `latest-global` (the default:
the CSV export's global assessments, including subspecies, varieties and subpopulations),
`latest-regional`, `all-latest`, or `history` (assessments in the API cache that the CSV export
does not include). It has two sources:

1. Crossref's list of IUCN DOIs (`api.crossref.org/prefixes/10.2305/works`, saved in
   `crossref_works`). In October 2026 the download was 258 requests and about 3 minutes, for
   255,060 assessment DOIs. A run downloads the list again when the saved copy is more than 7 days
   old, or with `--refresh-crossref`. Each entry in the list (a Crossref "work") gives the ids in
   the DOI and the ids in the URL of the page the DOI points to. For an errata version published from 2015 to 2018 they differ: the DOI has the id
   of the assessment it replaced and points to the errata version's page.
2. doi.org (`https://doi.org/api/handles/<doi>?type=URL`). For an assessment missing from
   Crossref's list, `IucnDoiCandidates` builds candidate DOIs from the releases of the year it was
   published (ordered by how many known DOIs of that year use each release) and, for an amended
   version, the releases of the year it amends; doi.org says whether each one exists. For an
   errata version published from 2015 to 2018, the DOIs of the assessments it replaced are tried
   first. An assessment that is in the current release's CSV export but not the previous
   release's was new in the current release, so that release is tried first. An assessment
   published before 1996 gets no candidates.
   `--doi-org recent` (the default) checks only assessments published in the year Crossref's list
   was downloaded or the year before, or new in the current release; the others are saved with no
   DOI. `--doi-org all` checks every assessment, and `--doi-org never` checks none. Requests start
   at least 300 ms apart (`--delay`); after a 429 answer the command waits for Retry-After and
   slows the pace.

Every DOI saved passes `IucnDoiSelector.Check`. For an errata version, the check also accepts the
DOI of another assessment of the same taxon when Crossref's list links that DOI to the errata
version's page (`IucnDoiResolution.ChooseFromCrossref`). A run skips the assessments already in
the cache; `--recheck-missing-after <DAYS>` checks again the assessments whose last check found no
DOI and is at least DAYS days old. `--status` prints the counts for a scope, sends no requests and
creates no cache file.

The DOI cache's tables:

- `doi_check`: one row per assessment checked (`assessment_id`, `taxon_id`, `doi` or NULL,
  `checked_at`, `candidates_tried`). `site build-db` reads this table.
- `doi_check_detail`: `found_by` (`crossref` or `doi.org`), `scope`, `year_published`, `note`.
- `doi_lookup_log`: every doi.org request.
- `crossref_works` and `crossref_listings`: the saved copy of Crossref's list.

Results for release 2026-1 (October 2026). Crossref's list had every DOI that IUCN's citation
text, GBIF and Wikidata give. doi.org found no DOI for any assessment missing from Crossref's
list, at about 3.3 requests per second with no 429 answers. The table counts the assessments in
each scope that have no DOI from IUCN's citation text, GBIF or Wikidata:

| Scope | DOI found | No DOI |
| --- | --- | --- |
| latest-global | 1,703 | 190 |
| latest-regional | 6,377 | 10,965 |
| history | 30,555 | 98,262 |

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
- The page of a taxon that is not in the release (`in_release = 0`) says "No current assessment in
  IUCN Red List version X", names the taxon in the release with the same name (`current_taxon_id`)
  when there is one, and lists the taxon's earlier assessments with the wikitext (such as
  `{{cite iucn}}`) for each. Its Regional assessments table lists every regional assessment, by
  region and then newest first. On the page of a taxon in the release, that table lists the latest
  assessment in each region, and the history section links to each old id that has the same
  scientific name. The page of a taxon in the release with no global assessment says "No global
  assessment. This taxon has been assessed in N regions."
- Search, `/name/{name}` and `/api/suggest` rank taxa in the release before taxa that are not,
  within each group of matches (exact name, name that starts with the text, any other match).
  Search and `/name/{name}` go straight to a taxon page when the text names one taxon exactly: the
  only exact match among the taxa in the release or, when no taxon in the release matches exactly,
  the only exact match among all taxa (`SearchModel.SingleExactMatch`). The search box lists each
  suggested name once, ignoring letter case, because two suggestions with the same name open the
  same search result. When an old id and a taxon in the release have the same scientific name,
  `/api/suggest` returns both, and `site.js` keeps only the first, which is the taxon in the
  release.
- The IUCN Red List Terms of Use limit what the site may hold and offer: no assessment narrative
  text, no coded threats, habitats or countries, no downloads, and no API that returns assessment
  fields (`/api/suggest` returns names, ids and the category only). Every page with IUCN data shows
  the Red List version and links to the About page, which credits every source with its licence and
  citation. Crossref is one of those sources, for the DOIs that `iucn resolve-dois` finds; its
  licence is CC0, and its Version cell gives the date of the newest DOI check
  (`iucn_doi_checked_to`).
- Run locally with `dotnet run --project BeastieBot3.Site`. `appsettings.Development.json` points to
  `~/datasets/beastiebot/site.sqlite`; set `Site__DatabasePath` to use another file.

### Theme

- The header has a Theme control: System (the default, which follows the system setting), Light
  and Dark.
- `wwwroot/theme.js` loads in `<head>` without `defer`, and sets `data-theme` on `<html>` from
  `localStorage` (key `theme`) before the page is drawn.
- `site.css` has the light tokens on `:root`, and the dark tokens twice: under
  `@media (prefers-color-scheme: dark)` for `:root:not([data-theme="light"])`, and under
  `:root[data-theme="dark"]`. Keep the two dark blocks identical.
- The control is hidden until `theme.js` runs, so without JavaScript the site follows the system
  setting.
- The server never writes `data-theme` and always marks System as selected, so an output-cached
  page is the same for every visitor. `theme.js` is a file from the site, as the Content Security
  Policy (`script-src 'self'`) requires; it only sets attributes.
- `--control-border` gives text box borders at least 3:1 contrast, and the category badge colours
  pass 4.5:1 in both themes. `ThemeControlTests` pins the theme.

## Known gaps

- Most taxa that are not in the release have no Wikipedia article and no English name on the site:
  in the build of 3 October 2026, 195 of the 4,223 have an `enwiki_title` and 104 have a
  `common_name_en`. `wikipedia match-taxa` matches the taxa in the IUCN Red List database, so such
  a taxon has an article only when the Wikipedia cache still has a match made for it earlier, and
  it has an English name only when the common names store has one for it. The Amur leopard
  (*Panthera pardus* ssp. *orientalis*, 15957) has neither.
- The ambiguity rule gives a Wikipedia article title or taxobox name priority over an IUCN main
  name. So for about 445 taxa in the release, the name that IUCN gives as the taxon's main name is
  used for another taxon instead, one that has the name as its Wikipedia article title or taxobox
  name. The count leaves out a taxon whose IUCN main name is used for its own species or for one
  of its own subspecies. A decision on changing the source priority is pending.
- The Wikipedia matcher (`wikipedia match-taxa`) can match a taxon to the article about a taxon of
  the same name in another kingdom. It matches the plant *Ficus variegata* to "Ficus variegata
  (gastropod)", and the palm *Gaussia princeps* to "Gaussia princeps (crustacean)". The site's
  article link (`taxon.enwiki_title`, read from the matcher's `taxon_wiki_matches`) still goes to
  those pages. The Wikipedia lists link the taxon's own scientific name when English Wikipedia has
  that title, so they link "Ficus variegata" and "Gaussia princeps". "Ficus variegata" is a
  disambiguation page, and the plant's article is "Ficus variegata (plant)". English Wikipedia
  has the titles "Gaussia princeps (plant)" and "Gaussia princeps (crustacean)", so "Gaussia
  princeps" may also be a disambiguation page; the Wikipedia cache has not downloaded it, so this
  was not checked.
- Regional assessments have DOIs only when IUCN's citation text, Wikidata or the DOI cache
  (`iucn resolve-dois --scope latest-regional`) gives one.
