# Public species site (Beastie Bot Species Status)

Beastie Bot Species Status is an unofficial, free, read-only website for looking up IUCN Red List
taxa, aimed at Wikipedia editors. Each taxon page shows the latest global assessment (category,
criteria, population trend, dates), the assessment history, regional assessments, common names and
synonyms, links to Wikipedia, Wikidata, the Catalogue of Life and SPRAT, and wikitext to copy:
`{{cite iucn}}` with the assessment's authors and DOI, `{{IUCN status}}`, the taxobox status
parameters, and `{{cite Q}}` for the assessment's Wikidata item (or, when the assessment has no
item, QuickStatements commands to create one). For the latest global assessment of a species or
subspecies, it also gives QuickStatements commands that bring the IUCN conservation status (P141)
on the taxon's Wikidata item up to date. Instructions for deploying it to an Oracle Cloud Always Free VM are in
`deploy/oracle/README.md`.

## Parts

| Part | Where | What it does |
| --- | --- | --- |
| Shared library | `BeastieBot3.Shared/` | Code both the CLI and the site use. Keep it on the same target framework as both (net10.0) and with no NuGet packages, so both can reference it. `Wikitext/`: `IucnCitationParts` (one assessment's citation, parsed by `site build-db` and stored as JSON in `citation_json`; each author is a `CitationAuthor`, with `GivenNames` when they are known), the renderers `CiteIucnRenderer` (with `CiteIucnOptions.FullGivenNames`), `IucnStatusTemplate`, `SpeciesboxStatus`, `ScientificNameMarkup`, `WikidataCitation` (`{{cite Q}}`, and QuickStatements commands and links for an assessment's Wikidata item, following `WikidataItemModel`; see [Wikidata items of assessments](#wikidata-items-of-assessments)), `WikidataStatusEdit` (QuickStatements commands for a taxon item's IUCN conservation status; see [IUCN conservation status on Wikidata](#iucn-conservation-status-on-wikidata)) and `WikidataStatusValues` (the P141 value for each IUCN category, also used by the Wikidata status dry run). `SiteData/`: `SiteDbSchema` (the site database's DDL, `Version` and meta keys) and `SiteNameKey.Fold` (the folded form of a name used for exact lookups). |
| GBIF checklist | `BeastieBot3/Iucn/Gbif/` | `iucn gbif-download` downloads, and `GbifIucnChecklistReader` reads, GBIF's CC BY 4.0 copy of the IUCN checklist. |
| IUCN citations | `BeastieBot3/Iucn/Citations/` | Code the site build and the Wikidata dry run share: `CreditNameSplitter` (splits a credit's `full` string into names), `IucnAuthorNameParser` (reads one name as a person, an organisation, or a name kept as IUCN wrote it), `AssessorNamePool` (repairs names with a letter lost to an encoding error), `AssessorGivenNames` (finds a person's full given names in the assessor credit's `value[]` list; see [Full given names](#full-given-names)) and `IucnCitationText` (removes IUCN's "Accessed on" sentence and reads the DOI in IUCN's citation text). |
| DOI lookup | `BeastieBot3/Iucn/Doi/` | `iucn resolve-dois` looks for DOIs that IUCN's citation text, GBIF and Wikidata do not give, in Crossref's list of IUCN DOIs and at doi.org, and saves them in the DOI cache (`Datastore:IUCN_doi_cache_sqlite`). See [Missing DOIs](#missing-dois-iucn-resolve-dois). |
| Site build | `BeastieBot3/SiteBuild/` | `site build-db` builds the site database; `site check-citations` writes a read-only report. Citation code: `IucnCitationPartsParser` (title annotations, and putting the parts together), `IucnDoiSelector` (choosing a DOI), `IucnTaxaHeaders` (each taxon's list of assessments). `SiteApiTaxaReader` reads the taxa that are only in the API cache. `SiteBuildRules.ClassifySpratName` matches SPRAT profiles to taxa, both whole-taxon profiles and population profiles. `SiteWikidataItems` chooses each assessment's Wikidata item. `SiteBuildRules.DescribesAnotherKingdom` decides whether a Wikidata item matched to a taxon by name is left out of `taxon.wikidata_qid`: it is left out when the item's English description names a group in another kingdom. `SiteLinkReaders` reads the taxon items' IUCN conservation status (P141) statements, and `P141JsonReferences` reads the parts of their references that the Wikidata cache's index does not record: later stated in items and reference URLs (see [IUCN conservation status on Wikidata](#iucn-conservation-status-on-wikidata)). |
| Site | `BeastieBot3.Site/` (net10.0, Razor Pages) | Opens the site database read-only and reads no other data. `Pages/WikitextOptions.cs` reads and writes the citation options; `Pages/WikidataCite.cs` builds the `{{cite Q}}` part and the IUCN conservation status part of the wikitext section, which the partials `Pages/Shared/_WikidataName.cshtml`, `_WikidataItemChanges.cshtml`, `_WikidataMainSubject.cshtml`, `_WikidataStatus.cshtml` and `_WikidataStatusPlan.cshtml` show; `wwwroot/site.js` updates the wikitext when an option changes (see [Citation options](#citation-options)). `wwwroot/theme.js` and the tokens at the top of `wwwroot/site.css` make the light and dark themes (see [Theme](#theme)). Tests in `BeastieBot3.Site.Tests/`, and a Playwright check of the wikitext updates in `BeastieBot3.Site.Tests/browser/live-update.cjs`. |
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
   IUCN statuses on Wikidata" workflow (`wikidata-iucn-status`) and, as "Find the Wikidata items
   of assessments", of the `public-site` workflow; `wikipedia update` does not run it. Without it,
   `site build-db` uses the items from the last time it ran. The command also stores three things that `site build-db` reads:
   - the title (P1476) statements of each assessment item, with their language and rank (column
     `title_statements` of `wikidata_iucn_assessment_items`; NULL when not recorded yet, `[]` when
     the item has no title). The commands that replace an item's title need them;
   - the editions of the IUCN Red List on Wikidata (table `wikidata_iucn_red_list_editions`). A
     status reference stated in one of these editions cites IUCN;
   - the items that state an IUCN taxon ID (P627) only at deprecated rank (table
     `wikidata_deprecated_iucn_taxon_ids`, 11 items on 3 October 2026).

   The command replaces the rows of each of the two tables only when that table's query succeeds.
   See [IUCN conservation status on Wikidata](#iucn-conservation-status-on-wikidata) for how the
   build uses the tables.
   Run `wikidata sweep-taxa --refresh-days 30` for the species on Wikidata that IUCN does not have
   (about 10 minutes for a full pass; see
   [Species from the Catalogue of Life and Wikidata](#species-from-the-catalogue-of-life-and-wikidata)).
3. Run `iucn gbif-download`. It keeps the new checklist zip only when it differs from the newest
   zip (by the date in the file name) in `Datasets:GBIF_IUCN_dir`; `site build-db` reads the newest.
4. Run `iucn resolve-dois --refresh-crossref`, then `iucn resolve-dois --scope latest-regional`.
   The first run downloads Crossref's list of IUCN DOIs again, so that it includes the new
   release's DOIs, and checks the latest global assessments. With `--refresh-crossref`, the
   command downloads the list even when no assessment needs checking. The list also gives the
   title registered with Crossref for each DOI. `site build-db` uses the name in that title for
   the title and label of a Wikidata item for an assessment, when the item has no title of its
   own. See
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
these steps in the same order. Apart from "Find the Wikidata items of assessments", which runs
`wikidata iucn-assessment-items`, the steps in its "1 · Inputs" group are the same steps as in the
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
database with any other version, so deploy the new site and the rebuilt database together. The
current version is 13 (version 12 is used on a branch). Version 13 added the species from the
Catalogue of Life and Wikidata that IUCN does not have (see
[Species from the Catalogue of Life and Wikidata](#species-from-the-catalogue-of-life-and-wikidata)). Schema versions 6 and 7 were used only on a branch before it was merged
into main, and no database built from main has them. Version 9 added the tree of groups (see
[Groups and lists](#groups-and-lists)), version 10 `assessment.population_size`, and version 11
`name.authority`, `assessment.api_not_found` and assessments with no scope (`scope = ''`).

Rules the site depends on (pinned by `SiteDbBuildTests` and the site tests):

- An assessment that IUCN published with no geographic scope (an empty scopes column in the CSV
  export, an empty `scopes[]` in the API) is stored with `scope = ''` (`SiteBuildRules.NoScope`).
  It is never global, so it never becomes `latest_global_assessment_id`. Until schema 11 these rows
  were left out, and the 7 taxa whose only current assessment has no scope had no assessment on
  the site. The build of 6 October 2026 has 33 such rows: 28 current ones from the CSV export and
  24 rows from API taxon records that are not in the CSV export (the build summary rows "Stored
  with no scope", which overlap). These are the assessments of the audit site's "Assessments with
  no geographic scope" page.
- `assessment.api_not_found` is 1 for an assessment that the IUCN API answered 404 for, although
  the taxon's record lists it (`failed_requests` in the API cache, endpoint `assessment`): 3 rows,
  the audit site's "Historical assessments missing from the API". The rows keep the citation from
  the payload the cache downloaded before the 404.
- Synonyms are stored once per source that gives them (`iucn`, `col`, `wikidata`,
  `wikipedia-taxobox`), with
  `name.authority` when the source has one. IUCN's authority is the synonym's `infrarank_author`
  for an infraspecific name, else `species_author`, else the text after the name in its full name
  (`SiteBuildRules.IucnSynonymAuthority`; 150,611 of 151,808). CoL's is the `authorship` of the CoL
  synonym row whose `parentID` is the taxon's `col_id`, matched by folded name
  (`SiteLinkReaders.ReadColSynonymAuthorities`; 406,281 of 449,171). Wikidata's are the scientific
  names of the items the taxon's item names as taxon synonym (P1420), when the Wikidata cache has
  downloaded that item (285 names from 8,570 synonym items, read in about 50 seconds), with no
  authority. `wikipedia-taxobox` synonyms come from the taxobox of the taxon's Wikipedia article
  (`SiteLinkReaders.ReadWikipediaTaxoboxSynonyms`): the taxobox's own scientific name with its
  authority, when Wikipedia uses another name than IUCN ("Nycticeinops crassulus" for
  *Pipistrellus crassulus*), and the names in its `synonyms` parameter (`TaxoboxSynonymsParser`,
  which finds names in 96% of the 43,461 cached taxoboxes with a value). A page about a genus or a
  higher taxon gives none; a page matched to several taxa gives them only to the taxon whose name is
  the taxobox's. The authority is never part of `name_key` or `name_fts`, and `SiteTaxonLinks` still
  compares IUCN synonym names without it.

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
  `IucnCitationParts.RegisteredName` is the name part of the title registered with Crossref for
  the citation's DOI ("Canis mesomelas" from "Canis mesomelas: Hoffmann, M."), read from the DOI
  cache's `crossref_works.title`. The build sets it only when the DOI names the row's own
  assessment id, so an errata version that has the DOI of the assessment it corrects gets none.
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
- `name.source` for an English name from the common names store is `wikipedia` for a Wikipedia
  article title, `wikipedia-taxobox` for the English name in an article's taxobox (since October
  2026; before, both were `wikipedia`), `wikidata` for a Wikidata label or other Wikidata name,
  `col` or `iucn`. The names table on a taxon page shows these as "Wikipedia", "Wikipedia
  taxobox", "Wikidata", "Catalogue of Life" and "IUCN Red List" (`SiteText.SourceLabel`), so a
  taxobox name that differs from the name the page shows is listed with its source.
  `common-names aggregate` stores a taxobox name only when it differs from the article title. A
  site older than this change shows the source `wikipedia-taxobox` as it is written, so deploy the
  site before or with a database built by the new `site build-db`.
- When the build reads the DOI cache, it sets the meta key `iucn_doi_checked_to` to the newest
  `checked_at` date in the cache's `doi_check` table.
- `assessment.wikidata_item_qid` is the assessment's Wikidata item and
  `assessment.wikidata_item_properties` lists the properties that item has (schema version 4). The
  meta key `wikidata_item_model` is the item model that the site's QuickStatements commands
  follow. See [Wikidata items of assessments](#wikidata-items-of-assessments). `site build-db`
  always writes `wikidata_item_properties` for a row that has `wikidata_item_qid`. The site relies
  on this: for a row with an item and NULL properties, it cannot tell which statements the item
  lacks, so it offers no commands to add them.
- Three more columns describe the assessment's item (schema version 8):
  - `assessment.wikidata_item_titles`: the item's title (P1476) statements of every rank, as
    `WikidataTitle` JSON with each title's text, language and rank, copied from the Wikidata
    cache's `title_statements` column. NULL when there is no item, or when
    `wikidata iucn-assessment-items` has not recorded the titles. For an item whose titles are not
    recorded, the site offers no commands that replace the title.
  - `assessment.wikidata_item_label_en`: the item's English label.
  - `assessment.wikidata_item_assessment_id`: the assessment that the item is for. It is the row's
    own `assessment_id`, except for an errata version that shares the item of the assessment it
    corrects, where it is that assessment's id.
- `taxon.wikidata_qid` is the item that states the taxon's IUCN taxon id (P627,
  `wikidata_qid_source = 'p627'`), otherwise an item that `wikidata backfill-iucn` matched by the
  taxon's name (`'name-match'`). When two or more items state the id,
  `SiteBuildRules.ChooseP627Item` chooses one of them. An item that states the id only at
  deprecated rank (listed in `wikidata_deprecated_iucn_taxon_ids`) is chosen only when every item
  that states the id does. From the items left, it chooses the one whose English label or taxon
  name (P225) is the IUCN scientific name (with or without its rank marker), and then the lowest
  item number. For IUCN taxon ID 96251644, the build chooses Q122932761, which states the id at
  normal rank, over Q3008560, which has it at deprecated rank. When the Wikidata cache has no
  `wikidata_deprecated_iucn_taxon_ids` table, the build warns and treats every item that states
  an id as stating it at a rank that is not deprecated.

  The build leaves out a name-matched item whose English
  description names a group in another kingdom, such as the insect item matched to the plant
  *Clusia flava* ("species of insect"). `SiteBuildRules.DescribesAnotherKingdom` takes the group
  from a description of the form "species of <group>" (or "genus of", "subspecies of" and other
  ranks) and looks it up in the word table of `WikiPageKingdom`, the table the Wikipedia matcher
  uses. The build of 3 October 2026 left out 13 items this way; the build summary row is
  "Wikidata items matched by name but left out: the item is a taxon in another kingdom".
- Four columns describe the taxon's item when `wikidata_qid_source = 'p627'` (schema version 8).
  The [IUCN conservation status](#iucn-conservation-status-on-wikidata) part of a taxon page reads
  them:
  - `taxon.wikidata_p141`: every IUCN conservation status (P141) statement on the item, at any
    rank, as `WikidataStatusStatement` JSON. Each statement has its id, value and rank, and, from
    its references, the stated in (P248) items (only the first one of each reference), the IUCN
    taxon IDs (P627), the number of references, and `citesIucn`, which is true when a reference
    cites IUCN. The column is `[]` when the item has no P141 statement, and NULL when the Wikidata
    cache has not downloaded the item. A statement with no value or an unknown value is not in the
    cache, so it is not in this column either.
  - `taxon.wikidata_item_downloaded`: the day (`yyyy-MM-dd`) that the Wikidata cache downloaded
    the item. NULL when `wikidata_p141` is NULL.
  - `taxon.wikidata_p627_deprecated`: 1 when the item states the taxon's IUCN taxon ID only at
    deprecated rank, else 0.
  - `taxon.wikidata_other_items`: the other items that state the taxon's IUCN taxon ID, as
    `WikidataOtherTaxonItem` JSON: each item's id, whether it states the id only at deprecated
    rank, and its P141 statements (NULL when the cache has not downloaded it). The column is NULL
    when no other item states the id.
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

## Groups and lists

Schema 9 adds the groups the taxa are in (`higher_taxon`), so the site can link every rank on a
taxon page and has a page for each group, with a Wikipedia list of its taxa.

### The tree of groups (`SiteTaxonTree`)

- Each taxon in the release (`in_release = 1`) goes under IUCN's kingdom, phylum, class, order,
  family and genus. IUCN's upper-case names are stored in title case ("Carnivora").
- Between class and order, order and family, and family and genus, the build adds the Catalogue of
  Life groups that the placement file keeps for the IUCN database it reads (`col build-placement`;
  see "Catalogue of Life groups in headings" in CLAUDE.md). These are the groups the Wikipedia list
  headings can use, so a CoL group never takes a taxon out of its IUCN order or family. A CoL group
  of the same rank as the IUCN taxon above it, such as order Cetacea in order Artiodactyla, has
  `show_rank = 0` and is shown by name only. The placement has nothing above class, so there is no
  subphylum Vertebrata. The build uses the placement built from this IUCN database and the current
  `rules/iucn-not-assigned.yml`; when the file has only an older one, it uses that and warns
  (`meta.col_placement_state` = `out-of-date`).
- An order or family that IUCN gives as "NOT ASSIGNED" takes its value from
  `rules/iucn-not-assigned.yml`, as in the lists (`source = 'iucn-rule'`); with no rule, the taxa
  go directly under the group above. The `taxon` row keeps IUCN's own values.
- Groups and taxa are numbered depth-first, children in alphabetical order. The taxa in a group
  have consecutive `taxon.tree_pos` values, from the group's `first_pos` to its `last_pos`, so one
  indexed range query finds them. In a genus each species comes before its own subspecies,
  varieties and subpopulations.
- `higher_taxon_count` has the counts by `{{IUCN status}}` code of each group's latest global
  assessments: species, subspecies and varieties, and subpopulations.
- Node ids change with every build, so pages address a group by rank and name:
  `/taxa/family/felidae`. When two groups have the same rank and name (204 groups in the build of
  5 October 2026, nearly all genera in two kingdoms), `link_query` holds what tells them apart:
  `kingdom=plantae`, or `parent=Moraceae` when both are in one kingdom. A bare address that matches
  two groups shows both to choose from.

The build of 5 October 2026 has 33,559 groups, 4,526 of them from the Catalogue of Life. 677 taxa
take their order from `iucn-not-assigned.yml`, and 43 have a rank still "NOT ASSIGNED".

### Names and links of groups (`SiteGroupNames`)

- `common_name_en` is the name the Wikipedia list headings use (`HeadingFormatter.ResolveCommonName`):
  `taxon-rules.yml`, then `rules-list.txt`, then the common names store (which has no groups
  above species today), then the title of the article the scientific name redirects to
  (Araneae to Spider). Only 1,182 groups have one, nearly all of them families and orders, and
  the forms differ: "cetaceans", "mammal", "Orchid". A heading's "Members of ..." line uses it.
- The Catalogue of Life's English vernacular names are stored apart, in `higher_taxon_name`, as
  CoL writes them (names that differ only in case are listed once). They are not checked, and some
  name only part of the group ("cattle", "goats" for Bovidae), so the site lists them under their
  source and never uses one as the group's name. 10,651 groups have some.
- `enwiki_title`: a wikilink from the rules, else the group's name when English Wikipedia has a
  page or redirect with that title that is not about another kingdom (`EnwikiTitleCheck`), else
  the name with a bracketed word for its kingdom when the name is a disambiguation page.
- `col_id`: the placement's id for a CoL group; for an IUCN group, the accepted CoL name usage with
  the same name, rank and kingdom (for a genus with two, the one in the same family). The lookup
  forces the `scientificName` index (`+rank`): without statistics SQLite chose the rank index, and
  the phase took over 20 minutes instead of about a minute.

### List lines (`SpeciesListLine`)

The site's lists use `SpeciesListLine` in `BeastieBot3.Shared`, the renderer `wikipedia
generate-lists` uses, so a line on the site is the line in the generated lists. The build stores
what the line needs that only the CLI can work out: `taxon.list_article_title` (the article a line
links, from `SpeciesLineFormatter.ResolveArticleTitle`, which can differ from `enwiki_title`: a
redirect with the taxon's own name is linked as it is) and, for a subspecies or variety with no
article, `list_parent_article_title` (its species' article). The English name is `common_name_en`,
chosen by the same `CommonNameChooser`.

### The group page (`Pages/Group.cshtml`)

- The page shows the classification above the group (CoL groups marked), counts (species,
  threatened, extinct, subspecies and varieties, subpopulations), the counts by category, CoL's
  English names, links, and a table of the groups directly in it.
- "Wikipedia list": the list as wikitext, with a preview, and the options beside it
  (`Lists/GroupListQuery.cs` reads and writes them as query parameters, so a list can be linked):
  line format (the lists' styles A, B and C; the default is the style the generated lists use for
  such a group), `{{IUCN status}}` on or off, a section for each category (off by default), a
  heading for each of the ranks the reader ticks (any rank found inside the group, CoL ranks
  included; default: two of class, order and family below the group's rank), the "Members of ..."
  line under headings (off by default), the top heading level (2 to 4; ranks that would go below
  level 6 are left out and the page says so), categories, subspecies and varieties (none, after the
  species, or under their species as `**` lines), subpopulations, and the order of names.
  `site.js` updates the list as options change, as it does the citation options (`#wikitext`,
  `data-live-region`).
- Categories: every category except NE is ticked by default; ticking none means all of them, NE
  included. NE holds the taxa with no global assessment (IUCN assessed them only regionally; 9,142
  species in 2026-1). Their lines have no `{{IUCN status}}`. `GroupList.CountLines` counts them as
  the group's totals less the counts in `higher_taxon_count`, so no table holds them.
- The preview is the wikitext rendered by `Lists/WikitextPreview.cs`: only the wikilinked part of a
  line links (to the Wikipedia article), `''`/`'''` are italics and bold, and `{{IUCN status}}` is
  the category code linked to its Wikipedia article plus a superscript "IUCN <year>" link to the
  assessment, as the template renders it.
- A list is made only up to `GroupList.MaxLines` (3,600) lines, about as many `{{IUCN status}}`
  templates as one Wikipedia page holds. The page counts the lines from `higher_taxon_count` first
  and reads no taxa for a longer list. The cap also keeps the site from handing out the categories
  of a whole kingdom at once.
- List type "Species tables" (`type=table`; `Lists/SpeciesTable.cs`, options in
  `Lists/SpeciesTableQuery.cs`, extra columns read by `Data/SpeciesTableQueries.cs`): one
  `{{Species table}}` per genus with a `{{Species table/row}}` per species, the format of featured
  lists such as "List of vespertilionines", below the rank headings the reader ticks (genus is never
  a heading). Species only; NE is ticked by default and its rows have `iucn-status=NE` and no
  reference. The site database has no range, size, habitat, diet or image (the IUCN terms), so those
  parameters are written blank, or switched off with `no-diet=yes` / `no-ecology=yes` (on the table
  and every row; range and image are always written, because the row template prints a missing one
  as `{{{range}}}`). The genus authority is unknown, so the caption has none; `species-count` is the
  genus's species in the release, in words below 100, not the number of rows. A row gets the
  authority split into name and year (brackets: `authority-not-original=yes`), the population
  (`PopulationValues.Display`: "Unknown", or IUCN's number of mature individuals as the tables write
  it) and the trend template (`PopulationTrendTemplate`, shared with `/update`). The row's
  `{{IUCN status|option=23|...}}` has no ids, so the reference after the trend is its only link to
  the assessment. Options: references (none; list-defined in a `{{reflist|refs=}}` at the end, the
  default; full citation in the row), ref names ("IUCN" + English name without spaces or
  punctuation as the featured lists write them, the default; `iucn-<taxon id>`; the scientific
  name; a second taxon with the same name gets `-<taxon id>`), citation template (`{{cite iucn}}`,
  or `{{cite Q}}` when the assessment has a Wikidata item), columns, and the `{{IUCN statuses}}` box
  (counts of the rows). The bullet-only options are hidden while tables are chosen (`site.js`
  switches `data-list-type` elements). The preview (`Lists/SpeciesTablePreview.cs`) is built from the
  rows, not from the wikitext.
- Tables are capped at `SpeciesTable.MaxRowsWithReferences` (400) rows with references and
  `MaxRowsWithoutReferences` (1,000) without. Measured with `action=parse` (October 2026) on
  subfamily Vespertilioninae, 281 rows: post-expand include size 1,078,884 bytes with list-defined
  `{{cite iucn}}` references (3.8 KB a row; inline `{{cite Q}}` the same), 360,827 bytes with no
  references and no "Size and ecology" column (1.3 KB a row; about 1.9 KB with the column, from
  family Felidae). Wikipedia's limit is 2 MB a page, so 546 and about 1,100 rows would fill it; the
  caps leave about a quarter for the article's own text and references. Lua time was 0.9 s of the
  10 s limit. The parse showed no error categories, also for NE rows, `no-ecology` and `{{cite Q}}`.
- Search lists the groups whose name is the search text, and goes straight to the group when it is
  the only match and no taxon has the name exactly.
- On a taxon page, each rank links to its group page, with the group's English name, or else up to
  three of CoL's names (muted, with a tooltip naming the source and saying they are unchecked). CoL groups are hidden until the
  reader ticks "Show N ranks from the Catalogue of Life"; the toggle is CSS only (`:has`). A taxon not in the
  release has no place in the tree and shows IUCN's ranks as text, as before.

### Species from the Catalogue of Life and Wikidata

Schema 13 lets a group's list include species from the Catalogue of Life and Wikidata that are not
on the IUCN Red List. Species rank only: no subspecies, varieties or populations from those sources.

**Wikidata taxon sweep** (`wikidata sweep-taxa`, `Wikidata/WikidataSweepTaxaCommand.cs`). The
Wikidata cache only holds the ~196,000 items linked to IUCN, so the command reads a short record of
every item with a taxon name (P225), about 4 million, into `wikidata_taxon_sweep` in the Wikidata
cache: taxon name, rank (P105), parent taxa (P171), CoL IDs (P10585), IUCN taxon IDs (P627),
English Wikipedia sitelink, English label (only when it is not a taxon name), instance of (P31)
values other than taxon, and the items that name the item as taxon synonym (P1420). It queries the
QLever Wikidata endpoint (`https://qlever.dev/api/wikidata`, `--endpoint` or
`WIKIDATA_QLEVER_ENDPOINT`), 100,000 result rows per page ordered by Q-number after a cursor; a
full pass takes 6 to 12 minutes. The Wikidata Query Service cannot run the query in its 60-second
limit. Each page is stored with the cursor in one transaction, so a stopped run carries on from the
last item. When a pass reaches the last item it deletes the rows it did not see (deleted or merged
items) and records the finish time (`taxon_sweep_completed` in `wikidata_sync_state`). A later run
does nothing unless `--restart` or `--refresh-days N` starts a new pass; `--status` prints the
counts without a query. The table adds about 430 MB to the cache. The `public-site` workflow runs it
as "Download the Wikidata taxon list" (`--refresh-days 30`), with a light from the sync table.

**Build** (`SiteBuild/ExtraSpecies/`, `site build-db --extra-species genus|family|none`, default
`genus`). After the tree of groups and before the names are written:

- CoL: the accepted and provisionally accepted species of each IUCN genus (one indexed query per
  genus on the CoL database; with `family`, also per IUCN family). Left out: `extinct = true`,
  usages that are an IUCN taxon's `col_id` or have an IUCN species' name, and species whose name or
  CoL ID is a Wikidata item that is a fossil or extinct taxon (CoL leaves `extinct` empty for many
  fossil species, such as the Panthera fossils from ZooBank).
- Wikidata: the species items of the sweep. Left out: instance of fossil taxon, synonym,
  unavailable combination, original combination or extinct taxon; names that are not a plain
  binomial; items that are an IUCN taxon (the taxon's item, P627 of a taxon in the database, P10585
  of an IUCN species' CoL usage, or an IUCN species' name). An item whose P10585 is a CoL entry's id,
  or with a CoL entry's name, is merged into it (one row in both sources; Wikidata's name kept when
  it differs). The kingdom and family come from the parent taxa (P171); with no kingdom, the genus
  name must be in one kingdom only.
- Placement: under the IUCN genus with the same name in the same kingdom; with `family`, else under
  the IUCN family. A species in a genus or family IUCN does not have is left out (counted in the
  summary).
- English name: the Wikidata label when it is not a taxon name and not junk (`CommonNameQuality`);
  CoL's vernacular names are unchecked and never used. Article: the Wikidata enwiki sitelink.
- `taxon_source_name`: CoL's accepted name of an IUCN species that the placement file matched
  through a CoL synonym, and the Wikidata taxon name of an IUCN species' item when it differs.
- Overlaps (`extra_overlap`, `ExtraSpeciesNameRules`, `OverlapReason`): an entry's name is an IUCN,
  CoL or Wikidata synonym of an IUCN species; a Wikidata-only entry's name (or P10585) is a CoL
  synonym of another entry, of a subspecies of it, or of an IUCN species; a Wikidata-only entry's
  item is named as taxon synonym (P1420) by an IUCN species' item or another entry's item; an IUCN
  species' name is a CoL synonym of a CoL entry; same genus and epithets that differ by a Latin
  gender ending (likely); same genus and epithets one or two letters apart (possible); the same
  epithet in another genus of the same family with the same author and year, or for a Wikidata
  species an epithet no other species in the family has (possible). Entries from one source are not
  compared with each other.
- `higher_taxon_extra` counts the extra species under each group by source, with the group's last
  descendant node id (node ids are depth-first), so the page checks the line cap with one lookup
  and reads a group's extra species by a node-id range. `sort_pos` puts an entry after the IUCN
  taxon before it in name order (after that species' subspecies), or after a family's taxa.

Build of 6 October 2026 (release 2026-1, CoL 26.7 XR): with `genus`, 998,573 extra species
(60,939 only in CoL, 477,256 only in Wikidata, 460,378 in both) and 318,647 overlap rows, adding
about 80 MB to the site database (572 MB to 651 MB) and about 75 seconds to the build. Most
Wikidata-only entries are old combinations that Wikidata keeps as separate items; 287,217 of the
overlaps are of this kind (CoL synonym). With `family` the build had 2,386,895 extra species
(1,387,673 of them under a family) and, before the columns were made smaller, added 305 MB.

**Site** (`Lists/ListSources.cs`, `Lists/ListSourceMerge.cs`, `Lists/GroupListSources.cs`,
`Data/SiteQueries.Extra.cs`, strings in `Display/GroupSourceText.cs`). The list option "Species
sources" has a checkbox for each source (`src=iucn|col|wd`, IUCN only by default) and an order of
preference (`prefer=icw|iwc|ciw|cwi|wic|wci`, IUCN, then CoL, then Wikidata by default):

- An IUCN species is listed when IUCN is ticked, or when it has a CoL usage (`col_id`) and CoL is
  ticked, or a Wikidata item and Wikidata is ticked; subspecies, varieties and subpopulations come
  from IUCN only. An extra species is listed when one of its sources is ticked. Extra species have
  no assessment, so they go in the NE section with no `{{IUCN status}}`; ticking CoL or Wikidata
  also ticks NE.
- Each entry's name comes from the most preferred ticked source that has it (`taxon_source_name`
  for IUCN species).
- A likely duplicate: the entry whose most preferred ticked source comes later in the order is left
  out, with a notice under the list ("Left out of the list"). When both entries have the same best
  source, or the reason is only possible, both stay ("Kept in the list"). An entry outside the group
  counts when one of its sources is ticked: it can leave out an entry here, but a less preferred
  entry outside the group gets no notice here.
- The line cap uses the IUCN counts plus `higher_taxon_extra`, before duplicates are left out, so
  the count can be higher than the final list; the "too many taxa" note says so.
- Preview lines of extra species end with links to their CoL page and Wikidata item (preview only,
  `WikitextPreview.ToHtml`'s line suffixes).
- No overlap notice goes in the wikitext.

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
   255,060 assessment DOIs. A run that has assessments to check downloads the list again when the
   saved copy is more than 7 days old. With `--refresh-crossref`, a run downloads the list every
   time, even when no assessment needs checking. Each entry in the list (a Crossref "work") gives
   the ids in the DOI, the ids in the URL of the page the DOI points to, and the title registered
   for the DOI. For an errata version published from 2015 to 2018 the ids differ: the DOI has the
   id of the assessment it replaced and points to the errata version's page.
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
  `crossref_works.title` is the title registered for the DOI, in the form "Name: author list"
  ("Canis mesomelas: Hoffmann, M."). A cache made before October 2026 gets the `title` column
  when `iucn resolve-dois` next opens it, and the column stays NULL until the command downloads
  the list again (after 7 days, or at once with `--refresh-crossref`). `site build-db` reads the titles for `RegisteredName` (see
  [The site database](#the-site-database)). When the cache has no titles, the build warns and new
  Wikidata items use IUCN's citation name.

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
lacks and replace a title of the form "Name: author list" and an English label that differs from
the item model, or that create an item for an assessment that has none. The site never edits Wikidata: a reader runs the commands in
QuickStatements with their own Wikidata account.

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

- the English label and description, from the model's templates with the name from `TitleNameFor`
  (see [The name in titles and labels](#the-name-in-titles-and-labels)); each is left out when it
  is over 250 characters;
- instance of (P31), from the model;
- title (P1476): the name from `TitleNameFor`, as monolingual text;
- published in (P1433): the IUCN Red List;
- publisher (P123): IUCN;
- main subject (P921): the taxon's item, when the caller passes one. The site passes
  `taxon.wikidata_qid` (from P627 or a name match), except in the two cases described under
  [On the site](#on-the-site-pageswikidatacitecs): two or more items state the taxon's IUCN taxon
  ID, or the taxon's item states it only at deprecated rank;
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

No statement has a reference. When `TitleNameFor` finds no usable name, `CreateItemCommands`
returns no commands, because an item with no title or label could not be found again.

`AddMissingCommands` writes commands that add to an existing item what it lacks, judged from
`wikidata_item_properties`: the English label when `Len` is missing, and each statement above
whose property is missing. The label and the title use the name from `TitleNameFor`, given the
item's own titles (`wikidata_item_titles`). An item with an author item (P50) gets no author name
strings (P2093). `AddMissingCommands` never adds publisher (P123), language (P407) or a
description, because the cache does not record whether the item has them, and it never removes or
changes a statement. When the item lacks nothing, it returns no commands.

`FixCommands` writes commands that replace an existing item's title (P1476) and English label.
Most items that SourceMD made in 2017 and 2018 have "Name: author list" as both, and `{{cite Q}}`
shows the title as the title of the work.

- Title: when the item has exactly one title that is not deprecated, and that title has the form
  "Name: author list", the commands add "Name" as a new title in the old title's language, then
  remove the old title by its exact text and language
  (`-Q1<TAB>P1476<TAB>en:"Canis mesomelas: Hoffmann, M"`), which is how QuickStatements finds a
  statement to remove. The name stays as the title has it, never IUCN's newer citation name. The
  title is not changed when `wikidata_item_titles` is NULL, when any title of any rank already has
  the new text in that language, when a deprecated title has the old title's text and language
  (QuickStatements could remove that one), when QuickStatements cannot match the old text exactly
  (a control character, `||`, or a space at either end), or when the name part of the old title
  (`NameFromTitle`) is an internal IUCN name with `_`.
- English label: when the label differs from the model's label template filled with the name from
  `TitleNameFor`, the commands set it with `Len`, which replaces the old label. A missing label is
  added by `AddMissingCommands`.

The site shows the commands from `AddMissingCommands` and `FixCommands` in one box.

`QuickStatementsUrl` writes a link to `https://quickstatements.toolforge.org/#/v1=` with the
commands joined by `||` and each tab written as `|`, percent-encoded. A command whose values
contain `|` keeps its tabs (`%09`). `QuickStatementsUrlFits` checks that a link is no longer than
`MaxQuickStatementsUrlLength`, 8,000 characters, a length every common browser accepts. The
longest create batch in release 2026-1 is for an assessment with 59 authors, and its link was
measured at 4,430 characters.

### The name in titles and labels

`WikidataCitation.TitleNameFor` gives the name for an assessment item's title, English label and
English description, and says which title it read the name from (`TitleNameSource`). It takes the
first of these:

1. the name part of the item's own title (`ItemTitle`), when the item has exactly one title that is
   not deprecated;
2. the name part of the title registered with Crossref for the assessment's DOI (`Crossref`, from
   `IucnCitationParts.RegisteredName`);
3. IUCN's citation name (`IucnCitation`).

For a new item, `TitleNameFor` has no item title to read, so it tries Crossref's title first.
IUCN's citation gives the taxon's current name even for an older assessment: the title registered
for the 2014 assessment of the black-backed jackal is "Canis mesomelas: Hoffmann, M.", and IUCN's
citation now gives *Lupulella mesomelas*. Neither the item's title nor Crossref's title proves the
name an assessment first appeared under. Crossref's records for some 2008 and 2010 DOIs were made
in 2015 and deposited again with the names current in 2015 (the code comments cite the items
Q29037714 and Q29393952), and SourceMD, the tool that made most assessment items, copied most of
their titles from Crossref. The page says only which title a name was read from.

A name with `_` is IUCN's internal name for a taxon that IUCN has replaced ("Larus
glaucoides_old"), and `TitleNameFor` never uses one (`IsIucnInternalName`). When `TitleNameFor`
finds no usable name, there are no create commands, and the add commands leave out the title and
the label.

`NameFromTitle` reads the name part of a title: the text before the last colon outside round
brackets, or the whole title when it has no such colon. A subpopulation's name can have a colon
inside brackets, as in "Oncorhynchus nerka (COLUMBIA RIVER: Redfish Lk): Rand, P.S." (66 of
Crossref's 255,060 titles in October 2026). HTML tags are removed, entities decoded and runs of
spaces collapsed.

`SameName` decides whether two names differ, for the name note on the page and for the build
summary. It counts "ssp." and "subsp." as the same, ignores the bracket characters `(`, `)`, `[`
and `]` (the text inside them still counts), and counts a run of spaces as one space. So
"Apollonias barbujana ssp. ceballosi" (Crossref) and "Apollonias barbujana subsp. ceballosi"
(IUCN) are the same name. The commands keep each name as it is
written.

### On the site (`Pages/WikidataCite.cs`)

The subsection "{{cite Q}} citation from Wikidata" follows the citation options in the wikitext
section. On the page of a taxon in the release, when the assessment shown is its latest global
assessment, the subsection "IUCN conservation status on Wikidata" follows it (see
[IUCN conservation status on Wikidata](#iucn-conservation-status-on-wikidata)).

- When the assessment has an item, the subsection shows a link to the item, the `{{cite Q}}` box,
  and a one-line summary of English Wikipedia's guidance on `{{cite Q}}`, linked to WP:Citing
  sources#Wikidata. When `AddMissingCommands` or `FixCommands` returns commands, it also lists the
  statements the item is missing (such as "main subject (P921)"), shows a table of each title and
  English label that the commands replace beside its new value (`_WikidataItemChanges.cshtml`),
  shows all the commands in one box, and links to QuickStatements with the commands filled in.
- When the item is for another assessment (`wikidata_item_assessment_id` is not the row's own id),
  the row is an errata version that shares the item of the assessment it corrects. The subsection
  then shows only the link to the item, the `{{cite Q}}` box and a line that links to the page of
  that assessment, where the commands for the item are. When the site has no page for that
  assessment, the line says so. Commands built from the errata row would give the item the errata
  version's article number, URL and authors.
- When the assessment has no item, the subsection says "No Wikidata item found for this
  assessment." It links to a Wikidata search, so that a reader can find an item made after the
  site's data was downloaded: by the DOI (`haswbstatement:P356=`), or, when there is no DOI, by the
  article number (`e.T<taxon id>A<assessment id>`) that assessment items have in their labels. It
  then shows the create commands and a link that opens QuickStatements with them. When the only
  name the site has is an internal IUCN name with `_`, there are no create commands, and a line
  says why.
- A line about the name (`_WikidataName.cshtml`) appears in two cases:
  - The name from `TitleNameFor` comes from the item's title or Crossref's title, and `SameName`
    finds it different from IUCN's citation name. The line says "The name in the item's title
    (P1476) is X" or "The name in the title registered with Crossref for this assessment's DOI is
    X", then that IUCN's citation gives a different name (and, when that name has `_`, that IUCN
    uses it as an internal name), then "The commands use X" when the commands set the item's title
    or label.
  - The commands set a title or label with IUCN's citation name, because neither title is known.
    The line names IUCN's citation name and says that IUCN's citation gives the taxon's current
    name, even for an older assessment.
- When two or more Wikidata items state the taxon's IUCN taxon ID (P627), at any rank, or the
  taxon's item states it only at deprecated rank (`TaxonItemDoubt.Of`, from `wikidata_other_items`
  and `wikidata_p627_deprecated`), the create and add commands leave out main subject (P921). A
  line beside the commands box says why (`_WikidataMainSubject.cshtml`). The line is shown for
  every assessment of the taxon whose commands would otherwise have added P921. The IUCN
  conservation status part gives no commands in these two cases either; the Wikidata status dry
  run puts such links in its tiers C and D. An item matched to the taxon by name is still written
  as main subject.
- When a `WikidataCitation` call throws, the page leaves out only the box that call makes, and logs
  a warning.

## IUCN conservation status on Wikidata

The subsection "IUCN conservation status on Wikidata" compares the IUCN conservation status (P141)
statements on the taxon's Wikidata item with the latest global assessment, and gives
QuickStatements commands that bring the item up to date with a reference to the assessment.
`WikidataStatusEdit.Plan` (in `BeastieBot3.Shared`) makes the commands, `WikidataCite.BuildStatus`
decides when to offer them, and `_WikidataStatus.cshtml` and `_WikidataStatusPlan.cshtml` show
them. The commands are based on the Wikidata status dry run. "The public site's status commands"
in `docs/wikidata-iucn-status.md` lists where they differ from it, both where QuickStatements
cannot do what the dry run plans and where the site follows its own rules. The site never edits
Wikidata.

### When the commands are offered

The subsection appears only on the page of a taxon in the release (`in_release = 1`), and only
when the assessment shown is the taxon's latest global assessment. The subsection says why it
gives no commands when:

- the taxon is a variety or a subpopulation;
- the taxon has no Wikidata item;
- the taxon's item was matched by name (`wikidata_qid_source = 'name-match'`): no item states
  the taxon's IUCN taxon ID (P627);
- two or more items state the taxon's IUCN taxon ID, at any rank (`wikidata_other_items`), or the
  taxon's item states it only at deprecated rank (`wikidata_p627_deprecated = 1`). The dry run
  holds such links for review (tiers C and D). The subsection still shows the comparison table,
  with the P141 statements of every item that states the id. For IUCN taxon ID 96251644, the other
  item Q3008560 states the id only at deprecated rank, and the taxon still gets no commands;
- the Wikidata cache has not downloaded the item (`wikidata_p141` is NULL).

### Which statements are compared

Only P141 statements with a reference that cites IUCN (`citesIucn`) are compared with the
assessment. This section calls them IUCN statements. A reference cites IUCN when it has one of
these:

- an IUCN taxon ID (P627);
- stated in (P248) the IUCN Red List (Q32059), IUCN (Q48268), an edition of the Red List (from
  `wikidata_iucn_red_list_editions`), or the item of an IUCN assessment (from
  `wikidata_iucn_assessment_items`);
- a reference URL (P854) on iucnredlist.org or a subdomain of it, such as a pre-publication PDF on
  nc.iucnredlist.org (`WikidataStatusStatement.IsIucnRedListUrl`).

Statements with no reference to IUCN, such as one referenced only to a national red book, are
never removed. The table marks each of them "no reference" or "no reference to IUCN", and a note
under the table says that they are not compared.

`site build-db` reads the statements and references from the Wikidata cache's index tables
`wikidata_p141_statements` and `wikidata_p141_references`, which the cache fills each time it
downloads an item (`wikidata cache-entities`). The build takes about a second to read them.
Reading every item's JSON instead took 94 seconds in October 2026. The index records the first
stated in of each reference and its IUCN taxon IDs, but no later stated in and no reference URL.
So for each statement that has references but none that the index shows citing IUCN, the build
reads the item's cached JSON and checks every stated in and every reference URL
(`P141JsonReferences`). In the build of 3 October 2026 it read 87 items, and 34 of their
statements cite IUCN by a reference URL. When the cache has no `wikidata_iucn_red_list_editions` table, the build warns, and a reference stated in an
edition cites IUCN only when it also has an IUCN taxon ID or an IUCN reference URL.

### The comparison (`WikidataStatusEdit.Plan`)

- The IUCN value is `WikidataStatusValues.QidForCode` of the category as published, compared
  case-sensitively: "nt" (Not Threatened, before 1994) is not NT. LR/nt and LR/lc give near
  threatened and least concern. LR/cd has no P141 value, so the page gives no commands (`NoValue`). Possibly
  extinct is a flag on CR, so CR(PE) and CR(PEW) give critically endangered, and a line under the
  table says that Wikidata has no value for possibly extinct.
- The item's best rank is taken over every statement that is not deprecated, whatever its
  references cite: preferred when any statement is preferred, else normal. The IUCN statements at
  that rank are compared with the IUCN value. When every IUCN statement is at normal rank under a
  preferred statement from another source, all the IUCN statements are compared.
- The outcome is `Agrees` when the compared statements all have the IUCN value, `Differs` when
  any has another value, and `Missing` when no statement that is not deprecated cites IUCN (other
  statements can still have a value).
- The outcome is `Blocked`, with no commands, when a deprecated statement has the IUCN value or two
  or more statements have it. QuickStatements finds the statement that a reference goes on by its
  value, taking the last statement of any rank with that value, so the reference could go on the
  wrong statement.

### The commands

- One command adds the IUCN value with the reference. When a statement already has the IUCN
  value, the command adds the reference to that statement instead. The reference has stated in
  (P248) the assessment's Wikidata item when the site has one, IUCN taxon ID (P627), reference URL (P854) of the assessment's page on
  iucnredlist.org, and retrieved (P813), the day the IUCN API cache downloaded the assessment. The
  page lists what the reference leaves out: stated in the Red List release, because the site has no
  Wikidata item for the release, and stated in the assessment's own item when the site has none.
- The statement with the IUCN value gets no reference when it already has a reference with this
  taxon's IUCN taxon ID, of any date, or one stated in the assessment's item. `wikidata_p141`
  does not hold a reference's URL or retrieved date, and a reference that differs only in its
  retrieved date would otherwise be added again each time the IUCN API cache downloads the
  assessment again.
- For a changed status (`Differs`), there are two choices (`StatusEditChoice`):
  - Replace also removes the IUCN statements at the item's best rank that have another value, by
    statement id (`-STATEMENT<TAB>Q140$...`). A normal-rank statement under a preferred one is
    never removed, whatever source the preferred one cites.
  - Keep removes nothing.

  QuickStatements v1 cannot set a statement's rank. When statements with another value are left
  on the item, the page lists the ranks to set by hand on the item's page: the statement with the
  IUCN value to preferred rank, unless it is preferred already, and each preferred statement with
  another value to normal rank.
- The choice shown first is Keep when the item has a statement at preferred rank, else Replace
  (`WikidataStatusEdit.RecommendedChoice`), and a line gives that reason. The other choice is in a
  details element. The page leaves it out when its commands are the same as the first choice's.
  That happens when every IUCN statement is at normal rank under a preferred statement from
  another source, so Replace can remove no statement.
- The add command comes before the removals, so a batch that stops part way never leaves the item
  without a status.

The `site build-db` summary has these rows for the status part: "Of those, items downloaded to the
Wikidata cache (IUCN status statements known)", "Of those, items with no IUCN status (P141)",
"Items whose JSON was read for P141 references that the cache's index does not record", and two
rows that count the statements found to cite IUCN in that JSON, by a reference URL and by a second
or later stated in. When the Wikidata cache has the matching tables, it also has "Of those, items
that state the id only at deprecated rank (no status commands)" and "Editions of the IUCN Red List
on Wikidata (a P141 reference stated in one cites IUCN)".
For the names in titles and labels it has "Citations with a title registered with Crossref for
their DOI", "Of those, titles with a name other than IUCN's citation name (ssp./subsp., brackets
and spaces ignored)", and, when the cache has not recorded some items' titles, "Items kept whose
title statements are not recorded (run wikidata iucn-assessment-items)".

## The site

- UI strings are in `BeastieBot3.Site/Display/SiteText.cs` (with `SiteText.Taxon.cs` and `SiteText.Wikidata.cs`), the About page text in
  `Pages/About.cshtml`, and category labels and badge colours (from en-wiki Module:IUCN status) in
  `Display/IucnCategories.cs`. All SQL is in `Data/SiteQueries.cs`; user input reaches FTS5 only
  through `Data/FtsQuery.cs`.
- Settings (`appsettings.json`, or environment variables such as `Site__DatabasePath`):
  - `Site:DatabasePath`.
  - `Site:BaseUrl`: the address used in canonical links, such as `https://species.example.org`.
  - `Site:ContactText` and `Site:ContactUrl`: who to contact about problems. Until they are set,
    pages say "the person who runs this site".
  - `Site:SourceUrl`: when set, the footer links to the source code.
  - `Site:RateLimits`: per client IP, `PagesPerMinute` (default 60), `SearchPerMinute` (30),
    `SuggestPerMinute` (30) and `UpdatesPerMinute` (10, texts sent to `/update`); for the whole site,
    `ConcurrentSearches` (4) and `SearchQueueLength` (8), which count status updates too. Taxon, group and name pages
    (`/species`, `/taxa`, `/name`) also have `TaxonPagesPerHour` (600) and `TaxonPagesPerDay` (3,000)
    per client, so a scraper under the per-minute limit cannot copy the site in a few days (one
    walked the group pages on 5 October 2026); the 429 page then gives the wait from Retry-After.
    Caddy from Ubuntu's apt repository has no rate limiting module, so the limits are in the app.
- All SQL of the status update page is in `Data/SiteStatusLookup.cs`, over one connection per request.
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
- Search and `/api/suggest` also find IUCN ids (`Data/IdQuery.cs`, read before the name search
  and before the minimum length check): `T22823A14871490` with or without `e.` and `.en`, `T22823`
  (taxon id), `A14871490` (assessment id), a plain number (looked up as both), an IUCN DOI
  (`10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en`, also with `doi:` or a doi.org address) and a
  Red List address (`iucnredlist.org/species/22823/14871490`). One match redirects to the taxon
  page; an assessment that is not the taxon's latest global one opens with that assessment's
  wikitext shown (`?assessment=N#wikitext`). Several matches are listed with "Matched IUCN taxon
  ID" or "Matched IUCN assessment ID". A DOI is read for the ids in it, so the DOI of an errata
  version that names the assessment it corrects finds that assessment. When the text names a
  taxon and an assessment and the site has only the taxon, the taxon is listed under a line
  saying the assessment was not found. No id matches: the text is searched as a name.
- The table under the regional assessments (`_RelatedTaxaTable.cshtml`) lists, on a species page,
  its subspecies, varieties and subpopulations with the category, criteria and year of each one's
  latest global assessment, or a line saying IUCN has assessed none (animals: "subspecies or
  subpopulations"; other kingdoms add varieties). On a subspecies, variety or subpopulation page it
  lists the species, then the species' other subspecies, varieties and subpopulations. A subspecies
  or variety in the release with no parent (347 in 2026-1, the audit site's "Subspecies and
  varieties with no assessed parent species") says that IUCN has not assessed its species as a
  whole, and lists the other assessed taxa with the same genus and species epithet, found through
  the genus's `first_pos`..`last_pos` range (`SiteQueries.GetUnassessedSpeciesSiblings`).
- Synonyms are a table of synonym and source. The authority beside the name is the first source's
  that has one (IUCN, Wikidata, then CoL); a source whose authority differs, ignoring case and
  spacing, has it in brackets after its name ("Catalogue of Life (authority: Phipps, 1774)").
- A taxon assessed under a working name (`sp. nov.`, `ssp. nov.`, `subsp. nov.`, `var. nov.`;
  `SiteFormat.IsProvisionalName`, 168 taxa in 2026-1) has a line under its heading saying the name
  is provisional. The site does not look for the published name; the audit site's "Provisional
  (sp. nov.) names with a described name in another source" does.
- An assessment with no scope is listed in the Regional assessments table as "No scope given"
  (headed "Assessments with no geographic scope" when every row has no scope). The status summary
  of a taxon with no global assessment counts regions without it and says how many of its
  assessments have no scope.
- An assessment with `api_not_found = 1` has "Not found in the IUCN API" beside its IUCN Red List
  link.
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
  date option applies to it only when the item has a URL: `WikidataCite.Build` sets
  `CiteQOptions.ItemHasUrl` when the item's `wikidata_item_properties` include P953.
- "Citation in taxobox status_ref" (`cite=q`, off by default): the taxobox lines' `status_ref` holds
  `{{cite Q}}` instead of `{{cite iucn}}` when the assessment has a Wikidata item. The `{{cite iucn}}`
  and `{{cite Q}}` boxes do not change. `Pages/IucnReference.cs` makes the choice for every page
  that writes a reference (`cite=q` on the group page's species tables, `citeq` on `/update`).

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

### Status update page (`/update`)

An editor pastes the wikitext of an article or list and gets the same text back with the IUCN
statuses changed to match the latest global assessments, and a report with one row per item (line,
result, item, text before and after, taxon, notes). Strings are in `Display/UpdateText.cs`; the
logic is pure, in `Update/` (`WikitextScanner` masks comments, nowiki, pre, syntaxhighlight, source
and math, and finds templates by counting braces; `WikiTables` reads wikitables line by line with
colspan and rowspan; `StatusUpdater` decides the edits, and `StatusTaxonResolver` finds each item's taxon by name, synonym, IUCN citation or common name), and reads the database through
`IStatusLookup` (`Data/SiteStatusLookup.cs`), so `StatusUpdaterTests` run over a fake.

- `{{IUCN status}}` with a taxon id: the code (`IucnStatusTemplate.ToTemplateCode`), the ids, and
  `|year=` or a `|label=` that is a year are replaced; for EX and EW those parameters are removed. A
  template with neither keeps having neither, and a template with a taxon id only ("2467", as List
  of cetaceans writes it) keeps that form, unless the reader asks for them (options below). An id
  with `in_release = 0` and a `current_taxon_id` uses the current taxon. An id the site does not
  have, or one not in the release with no current taxon (List of birds of Hawaii has old BirdLife
  ids), falls back to the scientific name in the template's table row or on its list line.
- `{{IUCN status}}` with no ids on a list line (`*`, `#`, `:` or `;` first), as the lists by country
  write it ("**** [[Aye-aye]], ''Daubentonia madagascariensis'' {{IUCN status|EN}}"): the taxon is
  the one the scientific names on the line before the template name. An abbreviated name
  ("''G. aurita''") takes its genus from the nearest line above that says "Genus ''[[Geogale]]''",
  and a plain binomial in brackets counts.
- `{{Species table/row}}`'s `iucn-status` (the family lists: List of felids, canids, mustelids,
  hominoids, pinnipeds, 28 to 64 rows each): the taxon is the row's `binomial`, whose abbreviated
  genus is the link text of the `genus` of the `{{Species table}}` above it, or the row's `name`.
  The row's `direction` gets the latest global assessment's population trend in the form the
  family lists use (`{{decrease|Population declining}}`, `{{steady|Population steady}}`,
  `{{increase|Population increasing}}`, `{{population change unknown}}`, which were 1,241 of the
  1,245 `direction` values in 10 family lists in October 2026). Only the trend template is replaced,
  so the `<ref>` after it stays; a template with the same trend (any label, capitals or redirect,
  such as `{{Down}}`) is kept as written; an empty `direction` is filled; a `direction` with no trend
  template, or a taxon with no trend, is left with a note. `population` is not changed. A row whose
  `population` differs from IUCN's number of mature individuals (`assessment.population_size`, from
  the payload's `supplementary_info.population_size`: a range, a number, a range and a best estimate
  `500000-999999,800000`, two ranges, or `U`) is listed under "Population differences" with a
  suggested value (`PopulationValues`): the first part of IUCN's value written as the tables write
  numbers ("500,000–999,999"), or "Unknown" for `U`. A row agrees when its number or range matches
  any part of IUCN's value end by end, allowing the row's own rounding within 5% (2,200 for 2177) and
  a band's upper end one lower (2,500–10,000 for `2500-9999`); references and `{{efn}}` notes are
  ignored. A taxon with no population size gives no row. In October 2026 this listed 0 to 8 rows in
  each of 6 family lists. `range`, `size`, `habitat` and `diet` are not checked; the page says so.
- `{{cite iucn}}` anywhere outside a taxobox's `status_ref`: found by the T…A… id in
  `|article-number=`, `|id=`, `|url=` or `|doi=`. A citation of a regional assessment is skipped. A
  citation of an older global assessment is reported, and replaced only when the reader asks.
- Wikitable cells in a column whose header mentions IUCN or the Red List, or says "status" without
  naming another list (EPBC, CITES, ...): a bare code or `{{IUCN status|X}}` with no ids. The taxon is
  the one taxon in the release named in the same row (italics, links, `{{sp}}`, `{{taxlink}}`; a
  rowspan cell above counts), by an exact `name_key` match on scientific names, trying a trinomial
  with `ssp.`, `subsp.` and `var.`; synonyms (from any source in the `name` table: IUCN, the Catalogue
  of Life, Wikidata and Wikipedia taxoboxes) only when no scientific name matches and they name one
  taxon, with a note naming the synonym; then the taxon id in an IUCN citation in the row or line
  (a `{{cite iucn}}` written there, or a `<ref name="X"/>` whose definition anywhere in the text has
  a T…A… id), when the citations name one taxon in the release, which also settles a name that
  matches several taxa. Only the references on the status count: the status cell, the text from an
  `{{IUCN status}}` to the end of its line, a species table row's `iucn-status`, `direction` and
  `population`, and a taxobox's `status_ref`. Other cells cite other assessments: List of
  vespertilionines cites the broad-headed serotine's assessment for the habitat of Happolds'
  pipistrelle, which was split from it and is not evaluated, and reading the whole row gave the split
  species the old species' status. With the status references only, 22 of that list's 323 items
  are found this way (it uses newer genus names such as *Afropipistrellus*), all with the status
  the list already gives, and its 45 NE rows are left as is. English common names only when none of
  these matches and the item is not NE (the "Kruger serotine" of the same list, described in 2026, is
  IUCN's English name for *Neoromicia melckorum*), they name
  one taxon, and the reader asks (option below). Without the option the item says which common name
  would have found it. Only the code is changed (and ids and a year added when asked), and a bare `CR` is kept
  for a possibly extinct taxon unless the reader asks for CR(PE) and CR(PEW).
- Taxoboxes ({{Speciesbox}}, {{Taxobox}}, {{Automatic taxobox}}, {{Subspeciesbox}},
  {{Infraspeciesbox}}) with a `status` parameter: `status` and `status_system`
  (`SpeciesboxStatus.ToStatusCode` / `ToStatusSystem`; `status_system` is added when missing), and the
  `{{cite iucn}}` inside `status_ref` when it cites another assessment (by the assessment id in
  `|article-number=`, `|id=`, `|url=` or `|doi=`, else by `|year=` / `|volume=`). The new citation
  uses the taxon page's default options, without the ref wrapper, which is kept.
- Options (`StatusUpdateOptions`, form fields `pe`, `ids`, `year`, `cites`, `common`, all off by
  default): CR(PE) or CR(PEW) instead of a kept CR in table cells and species table rows; ids added
  to templates with none or with a taxon id only; `|year=` added to templates with no year or label
  (not EX or EW); older `{{cite iucn}}` citations replaced; a taxon found by an English common name
  in the row or line. When an option that is off would change
  items, the result lists it with the count and a button that sends the same text again with it on
  (`StatusUpdateResult.CountNotes`).
- `citeq` (off by default): every citation the page replaces, in `status_ref` or elsewhere, is
  `{{cite Q|<item>}}` when the latest assessment has a Wikidata item, else `{{cite iucn}}`
  (`StatusUpdater.ReplacementCitation`, through `IucnReference`). An existing `{{cite Q}}` is not
  checked.
- Checked against a sample of 9 English Wikipedia lists on 5 October 2026: what is left as is is
  taxa IUCN does not assess (subfossil lemurs, the domestic cat), names IUCN spells differently,
  and legend tables. Two forms the first version left out did not occur at all, in the sample or in
  the 93,530 cached articles: linked codes (`[[Endangered species|EN]]`) and `data-sort-value` on
  status cells. Not handled: `[[File:Status iucn3.1 EN.svg]]` images (4 in the
  cached articles).
- The result starts with an edit summary line (`Update/EditSummary.cs`) for the reader to copy into
  Wikipedia's edit summary: each taxon whose category changed with the old and new codes
  ("Ursus maritimus EN→VU"), read from the item's text before and after; when that list would pass
  300 characters (MediaWiki keeps 500, and the rest of the line takes about 130), the changes counted by new category in IUCN's order ("40 IUCN statuses changed
  (20 to EN, 20 to LC)"); then counts of the other status entries and the citations that changed,
  and "(assisted by Beastie Bot Species Status)". It is left out when nothing changed.
- The updated wikitext is in a read-only box of fixed height (24rem, at most 70% of the window)
  that scrolls, with its own colours (`--output-bg`, `--output-border`) and "(read only)" in its
  label, so it is not taken for the box text is pasted into; `site.js` grows every other wikitext
  box to fit. The report shows the changed items first, with radio buttons for the items already up to
  date, the items left as is and all items; when nothing changed, it starts with the items left as is. The filter is CSS
  (`:has`), with no script and no state.
- Only the values that change are replaced; everything else comes back byte for byte. At most 3,600
  items (`GroupList.MaxLines`) are checked; the rest are counted and left as they are.
- The page is the only one that answers POST (`SiteMiddleware.UseGetAndHeadOnly` allows it on
  `/update` and raises the request body limit to 2 MB plus 64 KB for it; Caddy's
  `deploy/oracle/Caddyfile.template` allows the same size there and 1 MB elsewhere). The form is
  multipart/form-data, so wikitext is not percent-encoded; text over 2 MB (UTF-8) gets 413 with a
  message on the page. The page sets no cookies and has no antiforgery token, is never cached, and
  the error page also answers POST, so a 429 or 500 after a POST shows the usual error page.

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

- Wikidata gives few synonyms until the synonym items are downloaded: the taxa's items name 8,686
  synonym items (P1420). `wikidata queue-synonyms` queues the ones not in the cache, and
  `wikidata cache-entities` downloads them (the `public-site` workflow's step "Download the Wikidata
  synonym items"). Once downloaded, a synonym item is in the cache's name index, so
  `wikidata backfill-iucn` can link a taxon whose IUCN name is a synonym item's name to that item.
- `TaxoboxSynonymsParser` gives nothing for an epithet written with a capital (`''Coluber Aurora''`),
  an abbreviated genus (`''U. clandestina''`) or a genus in square brackets, and keeps publication
  details and `sensu`/`auct. non` comments in some authorities.

- Few groups have an English name of their own (1,182 of 33,559 in the build of 5 October 2026).
  The rules files name the groups the Wikipedia lists needed; the common names store has names for
  species only. Adding plurals to `rules-list.txt` or `taxon-rules.yml` names a group on the site
  and in the lists at once.

- Most taxa that are not in the release have no Wikipedia article and no English name on the site:
  in the build of 3 October 2026, 195 of the 4,223 have an `enwiki_title` and 104 have a
  `common_name_en`. `wikipedia match-taxa` matches the taxa in the IUCN Red List database, so such
  a taxon has an article only when the Wikipedia cache still has a match made for it earlier, and
  it has an English name only when the common names store has one for it. The Amur leopard
  (*Panthera pardus* ssp. *orientalis*, 15957) has neither.
- The ambiguity rule gives a Wikipedia article title priority over an IUCN main name (see
  "Ambiguous common names" in `docs/common-names.md`). So for 286 taxa in the release, the name
  that IUCN gives as the taxon's main name is used for another taxon instead, one that has the
  name as its Wikipedia article title. For 127 more, the other taxon also has the name as its IUCN
  main name and wins because it also has the name from its taxobox (89) or as its Wikidata label
  (38). The counts leave out a taxon whose IUCN main name is used for its own species or for one
  of its own subspecies, and are from the common names store of 3 October 2026. Until October
  2026 a taxobox name and a Wikidata label also had priority over an IUCN main name, and the first
  count was about 445.
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
  that form too. `{{cite Q}}` shows that whole text as the title of the work until a reader runs the
  site's commands that replace the title and label (`FixCommands`). The site offers no title change
  for an item whose title statements `wikidata iucn-assessment-items` has not recorded.
- The commands that add missing statements never add publisher (P123), language (P407) or the
  English description, because the Wikidata cache does not record whether an item has them. To
  offer them, `wikidata iucn-assessment-items` would have to record them.
- The site keeps offering the commands to create an item for an assessment until
  `wikidata iucn-assessment-items` finds the new item and `site build-db` runs again. The search
  link beside the commands is there so a reader can check first.
- The reference that the status commands add has no stated in (P248) for the Red List release.
  `WikidataStatusEdit` never cites a release item: when it was written, Wikidata had no item for
  release 2026-1. The dry run's release reference cites the release item.
- A QuickStatements link longer than `MaxQuickStatementsUrlLength` (8,000 characters) is left out
  (`QuickStatementsUrlFits`). The reader can still copy the commands from the box. The longest
  link in release 2026-1 is about 4,430 characters.
