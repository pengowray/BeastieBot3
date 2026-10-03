# Public species site (Beastie Bot Species Status)

Beastie Bot Species Status is an unofficial, free, read-only website for looking up IUCN Red List
taxa, aimed at Wikipedia editors. Each taxon page shows the latest global assessment (category,
criteria, population trend, dates), the assessment history, regional assessments, common names and
synonyms, links to Wikipedia, Wikidata, the Catalogue of Life and SPRAT, and wikitext to copy:
`{{cite iucn}}` with the assessment's authors and DOI, `{{IUCN status}}`, the taxobox status
parameters, and `{{cite Q}}` for the assessment's Wikidata item (or, when the assessment has no
item, QuickStatements commands to create one). Instructions for deploying it to an Oracle Cloud Always Free VM are in
`deploy/oracle/README.md`.

## Parts

| Part | Where | What it does |
| --- | --- | --- |
| Shared library | `BeastieBot3.Shared/` | Code both the CLI and the site use. Keep it on the same target framework as both (net10.0) and with no NuGet packages, so both can reference it. `Wikitext/`: `IucnCitationParts` (one assessment's citation, parsed by `site build-db` and stored as JSON in `citation_json`; each author is a `CitationAuthor`, with `GivenNames` when they are known), the renderers `CiteIucnRenderer` (with `CiteIucnOptions.FullGivenNames`), `IucnStatusTemplate`, `SpeciesboxStatus`, `ScientificNameMarkup`, and `WikidataCitation` (`{{cite Q}}`, and QuickStatements commands and links for an assessment's Wikidata item, following `WikidataItemModel`; see [Wikidata items of assessments](#wikidata-items-of-assessments)). `SiteData/`: `SiteDbSchema` (the site database's DDL, `Version` and meta keys) and `SiteNameKey.Fold` (the folded form of a name used for exact lookups). |
| GBIF checklist | `BeastieBot3/Iucn/Gbif/` | `iucn gbif-download` downloads, and `GbifIucnChecklistReader` reads, GBIF's CC BY 4.0 copy of the IUCN checklist. |
| IUCN citations | `BeastieBot3/Iucn/Citations/` | Code the site build and the Wikidata dry run share: `CreditNameSplitter` (splits a credit's `full` string into names), `IucnAuthorNameParser` (reads one name as a person, an organisation, or a name kept as IUCN wrote it), `AssessorNamePool` (repairs names with a letter lost to an encoding error), `AssessorGivenNames` (finds a person's full given names in the assessor credit's `value[]` list; see [Full given names](#full-given-names)) and `IucnCitationText` (removes IUCN's "Accessed on" sentence and reads the DOI in IUCN's citation text). |
| DOI lookup | `BeastieBot3/Iucn/Doi/` | `iucn resolve-dois` looks for DOIs that IUCN's citation text, GBIF and Wikidata do not give, in Crossref's list of IUCN DOIs and at doi.org, and saves them in the DOI cache (`Datastore:IUCN_doi_cache_sqlite`). See [Missing DOIs](#missing-dois-iucn-resolve-dois). |
| Site build | `BeastieBot3/SiteBuild/` | `site build-db` builds the site database; `site check-citations` writes a read-only report. Citation code: `IucnCitationPartsParser` (title annotations, and putting the parts together), `IucnDoiSelector` (choosing a DOI), `IucnTaxaHeaders` (each taxon's list of assessments). `SiteApiTaxaReader` reads the taxa that are only in the API cache. `SiteBuildRules.ClassifySpratName` matches SPRAT profiles to taxa, both whole-taxon profiles and population profiles. `SiteWikidataItems` chooses each assessment's Wikidata item. `SiteBuildRules.DescribesAnotherKingdom` decides whether a Wikidata item matched to a taxon by name is left out of `taxon.wikidata_qid`: it is left out when the item's English description names a group in another kingdom. |
| Site | `BeastieBot3.Site/` (net10.0, Razor Pages) | Opens the site database read-only and reads no other data. `Pages/WikitextOptions.cs` reads and writes the citation options; `Pages/WikidataCite.cs` builds the `{{cite Q}}` part of the wikitext section; `wwwroot/site.js` updates the wikitext when an option changes (see [Citation options](#citation-options)). `wwwroot/theme.js` and the tokens at the top of `wwwroot/site.css` make the light and dark themes (see [Theme](#theme)). Tests in `BeastieBot3.Site.Tests/`, and a Playwright check of the wikitext updates in `BeastieBot3.Site.Tests/browser/live-update.cjs`. |
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
   newer EPBC listings. Run `wikidata iucn-assessment-items` to find the Wikidata items of
   assessments, including the items readers created with the site's QuickStatements commands (see
   [Wikidata items of assessments](#wikidata-items-of-assessments)). It is a step of the "Update
   IUCN statuses on Wikidata" workflow (`wikidata-iucn-status`); `wikipedia update` and the
   `public-site` workflow do not run it. Without it, `site build-db` uses the items from the last
   time it ran.
3. Run `iucn gbif-download`. It keeps the new checklist zip only when it differs from the newest
   zip (by the date in the file name) in `Datasets:GBIF_IUCN_dir`; `site build-db` reads the newest.
4. Run `iucn resolve-dois --refresh-crossref`, then `iucn resolve-dois --scope latest-regional`.
   The first run downloads Crossref's list of IUCN DOIs again, so that it includes the new
   release's DOIs, and checks the latest global assessments. See
   [Missing DOIs](#missing-dois-iucn-resolve-dois).
5. Run `site build-db`. For release 2026-1 on 3 October 2026 it took about 100 seconds and wrote a
   database of about 463 MB. It writes `<Datastore:site_sqlite>.building` and replaces `Datastore:site_sqlite` only
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
- `taxon_link` links each taxon not in the release (an old id) to taxa in the release, in two ways
  (`SiteTaxonLinks`):
  - `same-name`: the taxon in `current_taxon_id`;
  - `iucn-synonym`: the one taxon in the release, in the same kingdom, whose IUCN synonyms (from its
    API taxon record) include the old id's scientific name exactly. Both kingdoms must be known. A
    name that two or more such taxa list gives no link.

  An old id can have both links, to two taxa: after a split, its name stays with one part and is a
  synonym of the other (*Platanista gangetica*, old id 41758, is linked to 41756 by its name and
  to *Platanista minor*, 41757, as a synonym). The build of 3 October 2026 has 1,723 `same-name`
  links and 1,298 `iucn-synonym` links (1,238 species, 7 subspecies and 1 variety with no
  `same-name` link, and 52 old ids that have both); 21 old ids have a name that two or more taxa
  list as a synonym and are not linked by it. Matching the names with case and accents folded
  would add 6 links. Of 20 synonym links checked by eye, all were IUCN synonymies (a species
  lumped into another, a genus change, a misspelled old name). The build summary rows are "Not in
  the release, linked to the one taxon in the release whose IUCN synonyms list its name" and "Not
  in the release, with a name that two or more taxa in the release list as a synonym (not
  linked)". The `taxon_link_current` index lets a taxon page find the old ids linked to it.
- `assessment.has_taxonomic_notes` is 1 when the cached payload's `documentation.taxonomic_notes`
  has a letter or digit once its HTML tags are removed (`SiteBuildRules.HasText`; IUCN also stores
  empty notes as markup such as `<em><br/></em>`), 0 when it has none, and NULL when the payload is
  not cached. The notes are narrative text, so the database holds only this flag. In the build of
  3 October 2026, 93,587 of the 366,258 assessments have notes.
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
- `assessment.wikidata_item_qid` is the assessment's Wikidata item and
  `assessment.wikidata_item_properties` lists the properties that item has (schema version 4). The
  meta key `wikidata_item_model` is the item model that the site's QuickStatements commands
  follow. See [Wikidata items of assessments](#wikidata-items-of-assessments). `site build-db`
  always writes `wikidata_item_properties` for a row that has `wikidata_item_qid`. The site relies
  on this: for a row with an item and NULL properties, it cannot tell which statements the item
  lacks, so it offers no commands to add them.
- `taxon.wikidata_qid` is the item that states the taxon's IUCN taxon id (P627,
  `wikidata_qid_source = 'p627'`), otherwise an item that `wikidata backfill-iucn` matched by the
  taxon's name (`'name-match'`). The build leaves out a name-matched item whose English
  description names a group in another kingdom, such as the insect item matched to the plant
  *Clusia flava* ("species of insect"). `SiteBuildRules.DescribesAnotherKingdom` takes the group
  from a description of the form "species of <group>" (or "genus of", "subspecies of" and other
  ranks) and looks it up in the word table of `WikiPageKingdom`, the table the Wikipedia matcher
  uses. The build of 3 October 2026 left out 13 items this way; the build summary row is
  "Wikidata items matched by name but left out: the item is a taxon in another kingdom".
- `taxon.enwiki_title` is the article that `wikipedia match-taxa` matched the taxon to. The
  matcher rejects a page about a taxon in another kingdom, and tries the taxon's name followed by
  a bracketed word for its kingdom, such as "Ficus variegata (plant)" or "Orestias elegans (fish)"
  (see the CLAUDE.md section "Wikidata / Wikipedia cache priority"). In the build of 3 October
  2026, the plants *Ficus variegata* and *Gaussia princeps* link "Ficus variegata (plant)" and
  "Gaussia princeps (plant)"; before the check they were matched to the articles about a gastropod
  and a crustacean with the same names. The plant *Beilschmiedia madagascariensis*, matched
  before through a synonym to "Long-billed bernieria" (a bird), now has no article.

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

### Full given names

IUCN's citations print authors with initials ("Sayer, C."), but the assessor credit's `value[]`
list often names each person in full ("Catherine Sayer (IUCN Red List Unit)"). For each person
that IUCN wrote with initials, `IucnCitationPartsParser` asks `AssessorGivenNames` for the
person's given names and stores them in `CitationAuthor.GivenNames` when exactly one `value[]`
entry fits. An entry fits when:

- after its trailing notes in brackets and a trailing Jr., Sr., II, III or IV are removed, it ends
  with the author's surname as a whole word, ignoring case and accents ("Chris van Swaay" for
  "van Swaay, C.");
- each initial, in order, is the start of the next given name ("J.-P." and "J.P." both fit
  "Jean-Pierre", and "Th." fits "Thomas"); particles among the initials, as in "C. de C.", are
  skipped;
- the given names contain no digit, "@", bracket, comma, semicolon, slash, "&" or letter lost to
  an encoding error ("Jos? Ralison"), no word that marks an organisation ("University", "Museum",
  "Specialist" and the other words in `IucnAuthorNameParser`), and no name that starts with a
  lower-case letter ("hai-Ning Qin"), and they are not only initials (an entry "D.R. Paulson" does
  not fit).

When two entries fit, the person gets no given names: "Alemu, S." appears twice, and the entries
for Shambel Alemu and Sisay Alemu both fit. A person known by a middle name gets none either: the
entry for "Liddle, T.A." is "Adam Liddle", and "Adam" does not start with T. When the number of
given names differs from the number of initials:

- fewer given names than initials: the remaining initials are kept as IUCN printed them, so
  "Paulson, D.R." gets "Dennis R." and "Nogueira, C. de C." gets "Cristiano de C.";
- more given names than initials: only the given names that the initials stand for are kept, so
  "Reppucci, J." gets "Juan", not "Juan Ignacio".

The build of 3 October 2026 has given names for 384,383 of the 430,197 persons in the author
lists of latest assessments, global and regional (89.4%), and for 446,501 of the 561,765 persons in
the author lists of all assessments (79.5%). Each count is of author entries, so a person who
assessed several taxa is counted once for each assessment. `site check-citations` has a section
"Full given names from value[]" with the number of authors for each outcome (given names found,
and each reason for finding none), examples, and a random sample of 100 matches (fixed seed) to
check by eye.

With `CiteIucnOptions.FullGivenNames`, `CiteIucnRenderer` writes a person with given names as
`|author=Sayer, Catherine`, or `|last1=Sayer |first1=Catherine` in the last/first style. A
generational suffix follows the given names ("Lowry, Porter P. II"), and a name IUCN wrote with
the initials first, "N.H. Rakotoarivelo", becomes "Rakotoarivelo, Nirina Hasina". Organisations,
names kept as IUCN wrote them, and persons without given names are written as in IUCN's citation.
The site's option for this is "Full given names instead of initials" (see
[Citation options](#citation-options)).

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

## Wikidata items of assessments

Wikidata has items for about 6,600 IUCN assessments as publications. The site gives `{{cite Q}}`
for an assessment that has an item, and QuickStatements commands that add the statements the item
lacks, or that create an item for an assessment that has none. The site never edits Wikidata: a
reader runs the commands in QuickStatements with their own Wikidata account.

### Which item an assessment gets (`SiteWikidataItems`)

`site build-db` reads the items from the Wikidata cache's `wikidata_iucn_assessment_items` table,
which `wikidata iucn-assessment-items` fills. That command reads Wikidata with SPARQL queries and
edits nothing. It finds items that have an IUCN Red List DOI or assessment URL, and reads the
taxon id and assessment id from that DOI or URL. The table was last filled on 13 September 2026
and has 6,578 items. An item is kept when one of its classes (P31) is scholarly article
(Q13442814), data set (Q1172284) or evaluation (Q1379672) and none is taxon (Q16521):

| Class (P31) | Items | Kept |
| --- | --- | --- |
| Q13442814 scholarly article | 6,571 | yes |
| Q1172284 data set | 5 | yes |
| Q1379672 evaluation | 1 | yes |
| Q16521 taxon and Q55808 seabird | 1 | no: Q1272830, the taxon item of Zino's petrel, which has an assessment DOI |

The build summary names any other class it leaves out, so a new class shows up there and can be
added to `SiteWikidataItems.PublicationClasses`. When the Wikidata cache has no such table, the
build prints a warning and continues with no assessment items and no DOIs from Wikidata.

An assessment gets the item whose taxon id and assessment id are the assessment's own. An errata
version with no item of its own gets the item of the assessment that its DOI names. The two rows
then share one item, just as they share one DOI, and the site never offers to create a second item
with that DOI. In the build of 3 October 2026, 6,786 assessments have an item: 6,518 have their own
item and 268 errata versions share one. The other 59 kept items are for assessments that are not
in the site database. Of the 6,518 items, 6,501 have no author statement (P50 or P2093), 2,063
have no main subject (P921), and 6 have a URL.

`wikidata_item_properties` lists the properties that the item has, as recorded in the Wikidata
cache, in the order of `WikidataCitation.JudgedProperties`: P31, P1476, P1433, P921, P953, P577,
P356, P2093 and P50. It also has the token `Len` when the item has an English label; `Len` is the
QuickStatements command that sets an English label. The cache's URL column has the item's P953,
P854 and P856 values, and any of them counts as P953. A typical value is
`P31 P1476 P1433 P921 P577 P356 Len`.

### The item model

The commands follow the assessment item model of the Wikidata status dry run
(`docs/wikidata-iucn-status.md`). `site build-db` reads `rules/wikidata/iucn-status.yml` with
`WikidataIucnEditConfig.LoadFromRules` and stores `ToItemModel()` as JSON in the meta key
`wikidata_item_model`; the build summary row "Wikidata assessment item model" names the file it
read. The defaults are in `WikidataItemModel` in `BeastieBot3.Shared`, and
`WikidataCitationTests.ShippedYaml_MatchesTheSharedModelDefaults` checks that the YAML file has the
same values. The site uses the defaults when the meta key is missing, and offers no commands when
the stored JSON cannot be read.

### What `WikidataCitation` writes

`CiteQ` writes `{{cite Q|Q56227924}}`. When the ref option is on, it wraps the template in a
`<ref>` and cleans the ref name as `{{cite iucn}}` does. It writes `|access-date=` only when
`CiteQOptions.ItemHasUrl` is true, because `{{cite Q}}` takes its URL from the item and CS1 reports
an access date without a URL as an error.

`CreateItemCommands` writes QuickStatements v1 commands that create an item. The first command is
`CREATE`; each command after it starts with `LAST` (the item just created) and adds, in this order:

- the English label and description, from the model's templates (each is left out when it is over
  250 characters);
- instance of (P31), from the model;
- title (P1476): the scientific name, as monolingual text;
- published in (P1433): the IUCN Red List;
- publisher (P123): IUCN;
- main subject (P921): the taxon's item, when the site has one (`taxon.wikidata_qid`, from P627 or
  a name match);
- language (P407): from the DOI's last part (`.en` English, `.es` Spanish, `.fr` French, `.pt`
  Portuguese), or the model's language when there is no such DOI;
- full work available at URL (P953): the assessment's page on iucnredlist.org;
- publication date (P577): the year the assessment was published, as `+2023-00-00T00:00:00Z/9`;
- DOI (P356), in capitals, only when the DOI names the assessment's own taxon and assessment ids
  (an errata version's DOI belongs on the item of the assessment it corrects);
- one author name string (P2093) for each author, as IUCN's citation prints the name (never the
  full given names), with a series ordinal (P1545) qualifier. A second author with the same
  printed name is written with `!P2093`, so that its ordinal goes on a new statement and not on
  the first author's.

No statement has a reference.

`AddMissingCommands` writes commands that add to an existing item what it lacks, judged from
`wikidata_item_properties`: the English label when `Len` is missing, and each statement above
whose property is missing. An item with an author item (P50) gets no author name strings (P2093).
The commands never add publisher (P123), language (P407) or a description, because the cache does
not record whether the item has them, and they never remove or change a statement. When the item
lacks nothing, there are no commands.

`QuickStatementsUrl` writes a link to `https://quickstatements.toolforge.org/#/v1=` with the
commands joined by `||` and each tab written as `|`, percent-encoded. A command whose values
contain `|` keeps its tabs (`%09`). `QuickStatementsUrlFits` checks that a link is no longer than
`MaxQuickStatementsUrlLength`, 8,000 characters, a length every common browser accepts. The
longest create batch in release 2026-1 is for an assessment with 59 authors, and its link was
measured at 4,430 characters.

### On the site (`Pages/WikidataCite.cs`)

The subsection "{{cite Q}} citation from Wikidata" is the last part of the wikitext section, after
the citation options.

- When the assessment has an item, the subsection shows a link to the item, the `{{cite Q}}` box,
  and a one-line summary of English Wikipedia's guidance on `{{cite Q}}`, linked to WP:Citing
  sources#Wikidata. When `AddMissingCommands` returns commands, it also lists what they add (such
  as "main subject (P921)"), shows the commands in a box, and links to QuickStatements with the
  commands filled in.
- When the assessment has no item, the subsection says "No Wikidata item found for this
  assessment." It links to a Wikidata search, so that a reader can find an item made after the
  site's data was downloaded: by the DOI (`haswbstatement:P356=`), or, when there is no DOI, by the
  article number (`e.T<taxon id>A<assessment id>`) that assessment items have in their labels. It
  then shows the create commands and a link that opens QuickStatements with them.
- When a `WikidataCitation` call throws, the page leaves out only the box that call makes, and logs
  a warning.

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
  IUCN Red List version X", and names each taxon in the release it is linked to in `taxon_link`:
  "IUCN Red List version X lists *name* under IUCN id N" (`same-name`), or "IUCN lists *old name*
  as a synonym of *current name* (IUCN id N)" (`iucn-synonym`). It lists the taxon's earlier
  assessments with the wikitext (such as `{{cite iucn}}`) for each. Its Regional assessments table
  lists every regional assessment, by region and then newest first. On the page of a taxon in the
  release, that table lists the latest assessment in each region. The page of a taxon in the
  release with no global assessment says "No global assessment. This taxon has been assessed in N
  regions."
- Combined assessment history (`Pages/CombinedHistory.cs`, `Pages/Shared/_CombinedHistory.cshtml`):
  when a taxon has a linked taxon (an old id linked to it, or, on an old id's page, a taxon in the
  release it is linked to) with at least one global assessment, the history section is headed
  "Combined assessment history" and replaces the taxon's own history table. The combined table has
  every global assessment of the page's taxon and of each linked taxon (one link away, not the
  other old ids of a linked taxon), newest first, so the reader can see where one id's assessments
  stop and the other's start. A legend line for each id gives its scientific name, whether it is in
  the release, the number and years of its global assessments, and how it is linked ("Same
  scientific name as IUCN id N", or "IUCN lists X as a synonym of Y"). Taxa in the release come
  first, then old ids, each by id, so a taxon in the release has the same colour on its own page
  and on an old id's page. Columns: year published, IUCN id (linked to that id's page; the page's
  own id is marked "This page"), a Name column only when the ids have different names, category,
  criteria, date assessed, wikitext, and the IUCN Red List link. A row of another id links to that
  id's page with `?assessment=` and the visitor's citation options (`OtherIdOptionsUrl`, with that
  page's default ref name for the assessment), labelled "See IUCN id N".
  - Each id's rows have a background and a mark on their left edge (`--id-tint-N` and
    `--id-mark-N` in `site.css`, six of them), and the legend has a swatch of the same colours. The
    colours only help: the IUCN id column names each row's id. A table with more than six ids has
    no colours, so that no colour stands for two ids; in release 2026-1 that is 4 pages, such as
    *Bythiospeum acicula* (292912196, 23 ids). The row whose wikitext is shown has an outline
    instead of the usual background. Contrast, checked with a script: in both themes, text, links,
    visited links, muted text and grey badges are at least 5.6:1 on every background (4.5:1
    needed), and each mark is at least 3.9:1 against its background and 4.4:1 against the page.
  - When the newest global assessment of an id has taxonomic notes (`has_taxonomic_notes = 1`), a
    line under the table says that IUCN's taxonomic notes may explain why the assessments are under
    more than one IUCN id, and links to those assessments on the IUCN Red List website. The site
    never shows the notes and never says why an id changed (split, lump or new name): IUCN's data
    has no field for the reason.
  - Regional assessments stay in each taxon's own Regional assessments table: none of an old id's
    regional assessments is current, and the table of a taxon in the release lists the latest
    assessment in each region. Under the combined table, "Regional assessments of IUCN id N are on
    its page" links to that section of each other id that has any (364 of the 2,969 linked old ids
    in release 2026-1).
  - When no linked taxon has a global assessment, the page of a taxon in the release keeps its own
    history table and has a line for each old id: "Earlier assessments of a taxon with this name are
    under IUCN id N." or "Earlier assessments of X are under IUCN id N. IUCN lists X as a synonym
    of Y." (`_EarlierIds.cshtml`).
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

### Citation options

The options form in a taxon page's wikitext section is read from and written to the query string
(`Pages/WikitextOptions.cs`): `authors=author|lastfirst`, `fullnames=1`,
`access=download|today|none`, `ref=1`, `refname=...`, `amp=1` and `opts=1`. A browser does not
send an unticked checkbox, so the form also sends `opts=1`: with it, a missing `ref` or `amp`
means off; without it (a plain link), the defaults apply. The output cache stores a separate copy
of the page for each combination of these parameters (`SiteCachePolicies.SpeciesQueryKeys`), and
links to other assessments of the taxon keep the options.

- `fullnames=1` turns on "Full given names instead of initials" (`CiteIucnOptions.FullGivenNames`;
  see [Full given names](#full-given-names)). It is off by default, so it needs no `opts=1`. The
  checkbox is shown only when at least one author of the selected citation has given names, with a
  help line that gives how many authors have them and an example from the citation. When the
  assessment shown has no authors with given names, a `fullnames=1` already chosen is kept in a
  hidden field, so it still applies on the next assessment.
- The `{{cite Q}}` box uses the same ref options as `{{cite iucn}}` (`ToCiteQOptions`). The access
  date option has no effect on it, because the site never sets `CiteQOptions.ItemHasUrl` (see
  [Known gaps](#known-gaps)).

`wwwroot/site.js` updates the wikitext when an option changes:

- An update starts when a radio button or checkbox changes, 400 ms after typing in the ref name
  box stops, or when Enter is pressed in that box. When the page loads, the script hides the
  Update wikitext button, which submits the form when JavaScript is off.
- The script requests the page with the form's query, the same GET the form would send, so the
  output cache answers a repeated set of options. It replaces each element marked
  `data-live-region` (`#wikitext-output` and `#wikidata-cite`) with the element of the same id in
  the new page. The form is not replaced, so the focus and the text in the ref name box stay.
- It keeps the form at the same place on the screen when the boxes above it change height, keeps
  open each `details` element (with an id) that was open, sets the address bar to the new page's
  address for these options (`data-options-url`, with `history.replaceState`), updates the "Show
  wikitext" links in the assessment tables (`data-options-link`), and shows "Wikitext updated" in
  a status line for 4 seconds.
- A new update cancels a request still running. When the server answers 429 (too many requests),
  the script writes "Too many requests: the wikitext was not updated. Wait a minute, then select
  Update wikitext." in the status line and shows the Update wikitext button again. The next change
  to an option starts a new update. After any other failure, the browser goes to the page with the
  new options, as it would without JavaScript. Typing a ref name can reach the limit of 60 pages a
  minute for each client IP address (`Site:RateLimits:PagesPerMinute`).
- Copy buttons use one click listener on the document, so they work on the replaced boxes.
- The page has no inline script. The Content Security Policy (`Web/SiteMiddleware.cs`) has
  `connect-src 'self'`, which allows the script's request.

`BeastieBot3.Site.Tests/browser/live-update.cjs` checks the updates in a browser. It is not part
of `dotnet test`. To run it:

1. `SITE_FIXTURE_DB_OUT=/tmp/site-fixture.sqlite dotnet test BeastieBot3.Site.Tests --filter FixtureExport`
   writes the test fixture database to a file.
2. `ASPNETCORE_ENVIRONMENT=Production Site__DatabasePath=/tmp/site-fixture.sqlite Site__RateLimits__PagesPerMinute=10000 dotnet run --project BeastieBot3.Site --urls http://127.0.0.1:5391`
   runs the site on that file.
3. `node BeastieBot3.Site.Tests/browser/live-update.cjs` runs the checks. Playwright installed
   globally is enough. `SITE_URL` sets another address, and `SHOTS` names a folder for
   screenshots.

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
- A taxon can still be linked to the Wikipedia article or the Wikidata item of a taxon in another
  kingdom when the page or item does not name its group. `wikipedia match-taxa` finds that a page
  is about another kingdom only when its taxobox, its title or its "<group> described in <year>"
  categories name a group in the tables of `WikiPageKingdom`. `site build-db` finds that a
  name-matched Wikidata item is about another kingdom only when its English description has the
  form "species of <group>" (or another rank) with a group in those tables.
- Regional assessments have DOIs only when IUCN's citation text, Wikidata or the DOI cache
  (`iucn resolve-dois --scope latest-regional`) gives one.
- For 6,569 of the 6,578 cached assessment items, the title (P1476) has the form SourceMD gave
  them, "Name: author list" ("Rusa unicolor: Timmins, R., ..."), and 6,566 have an English label of
  that form too. `{{cite Q}}` shows that whole text as the title of the work. The site's commands cannot correct it, because they
  never remove or replace a statement.
- The commands that add missing statements never add publisher (P123), language (P407) or the
  English description, because the Wikidata cache does not record whether an item has them. To
  offer them, `wikidata iucn-assessment-items` would have to record them.
- The site keeps offering the commands to create an item for an assessment until
  `wikidata iucn-assessment-items` finds the new item and `site build-db` runs again. The search
  link beside the commands is there so a reader can check first.
- The site never sets `CiteQOptions.ItemHasUrl`, so its `{{cite Q}}` never has `|access-date=`,
  even for the 6 items that have a URL.
- The site does not check a QuickStatements link with `QuickStatementsUrlFits` before showing it.
  The longest link in release 2026-1 is about 4,430 characters, under `MaxQuickStatementsUrlLength`
  (8,000 characters).
