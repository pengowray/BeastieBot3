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
   Then run `wikipedia fetch-group-titles`, which downloads the English Wikipedia pages of new
   groups and the redirects to their articles (see
   [Names and links of groups](#names-and-links-of-groups-sitegroupnames)).
   Run `iucn api green-status` before `site build-db`. It downloads all published IUCN Green Status
   of Species assessments into the IUCN API cache in one request (about 780 KB). Rows already
   stored keep the date and Red List version of the download that first stored them, and rows that
   are no longer published are deleted. The rows of the first download (2026-10-08, release
   2026-1) have `baseline = 1`, because the release each one was first published in is not known.
   Run `iucn summary-tables` once IUCN has published the new release's summary statistics. It
   downloads the release's Table 7 when IUCN has put it under one of the file names it has used
   before; otherwise add its URL to `rules/iucn-summary-tables.yml` (see
   [Reasons for category changes](#reasons-for-category-changes-iucn-summary-tables)).
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
current version is 17 (version 17 counts the email addresses left out of each credit group). Version 14 has the Wikipedia names of groups (`higher_taxon_name` source `wikipedia`, built
on a branch as version 12) together with the species from the Catalogue of Life and Wikidata that
IUCN does not have (version 13, deployed on 6 October 2026 without the Wikipedia names) (see
[Species from the Catalogue of Life and Wikidata](#species-from-the-catalogue-of-life-and-wikidata)). Schema versions 6 and 7 were used only on a branch before it was merged
into main, and no database built from main has them. Version 9 added the tree of groups (see
[Groups and lists](#groups-and-lists)), version 10 `assessment.population_size`, and version 11
`name.authority`, `assessment.api_not_found` and assessments with no scope (`scope = ''`).
Version 15 (never deployed on its own) added `extra_species.authority` and
`higher_taxon_extra_count` for the group page lists, and version 16 adds `assessment.credits` and the
`credit_name` table (see [Credits of an assessment](#credits-of-an-assessment)).

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
- `other_status` (schema 23) has one row per status of a taxon in a list other than the IUCN Red
  List. Rows from NatureServe and ECOS are described after this item. From SPRAT: the EPBC Act listing (system `au-epbc`, also in
  `epbc_listing`, with the date the listing took effect from `EPBC_Threatened_Species_Date_Effective`)
  and the eight state and territory columns (`au-act` to `au-wa`, `SiteLinkReaders.SpratStateColumns`)
  of each profile in `epbc_listing`. `population` is the population of a population's profile.
  `listed_name` is set only when the listing's name is not the taxon's own name
  (`SiteBuildRules.OtherListedName`). State statuses are tidied by `SiteBuildRules.ListStatusText`
  (spacing, capitals of known categories, a value repeated after a comma). The species page shows
  them in the "Other conservation statuses" section, one table per country, in the order of
  `OtherStatusSystems.All` (in `BeastieBot3.Shared`), with the date of the SPRAT report. The SPRAT
  report's state columns are headed with the acts SPRAT names ("NSW TSC Act and FM Act", "Vic. FFG
  Act (Advisory Lists)", "WA WC Act"), some of them since replaced, so the page names the state and
  says the statuses may differ from the states' current lists.
- `other_status` rows from the status lists store (`docs/status-lists.md`), read by
  `SiteLinkReaders.ReadStatusLists`. NatureServe, NZTCS and SALVE rows are matched by
  `StatusListMatcher.OnePerTaxon` (pinned by `StatusListMatcherTests`): every row by its own name
  first, then the unmatched rows by the source's synonyms and by IUCN's synonyms, one row per
  taxon, earlier rows first. A NatureServe record goes to the taxon with its scientific
  name in the same kingdom (`StatusListNameIndex` ignores "ssp.", "subsp." and "var.", so
  NatureServe's bare trinomials find IUCN's subspecies; a subpopulation is never found), else the
  one taxon that one of NatureServe's synonyms names, else the one taxon whose IUCN synonyms
  include its name. A taxon gets one record, and Standard records are tried before Provisional
  and Nonstandard ones with the same name. Each record gives `natureserve-global` (the rank as
  published in `status`, the rounded rank in `status_code`; GNR, TNR, GNA and TNA are left out),
  `ca-cosewic` (the code as COSEWIC's words, `OtherStatusSystems.CosewicLabel`) and `ca-sara` (the
  English part). Each ECOS listing goes to the taxon of its scientific name, of another name its
  brackets give, or of an IUCN synonym, as `us-esa` with the listing date; its entity description
  is the population unless it is "Wherever found". ECOS's listing date is the date the taxon or
  population was first listed, not the date of its current status (the humpback chub, Threatened
  since 2021, has 11 March 1967), so a table with ECOS dates heads the column "First listed". The heading is chosen by
  `OtherStatusSection.DateHeadingFor`: ECOS's heading whenever any date in the column is ECOS's
  (`DateHeadingRule.AnyDatedRow`); SALVE's "Assessed" only when every date in the column is
  SALVE's (`AllDatedRows`); otherwise "In effect from". In the build of 8 October 2026: 14,058 taxa
  matched to NatureServe records (10,928 global ranks, 440 COSEWIC, 270 SARA) and 1,611 of the
  2,478 ECOS listings matched. The COSEWIC and SARA statuses are NatureServe's copy, because
  Canada's Species at Risk Public Registry has no bulk download; the page says so. The rank's cell
  gives what the rounded rank means and which part of the rank that is
  (`SiteText.NatureServeRankMeaning`): "Vulnerable (rounded rank G3)" for G3G4, "Imperiled
  (subspecies rank T2)" for G5T2.
- `natureserve-national` and `natureserve-subnational` rows (schema 26): for each matched NatureServe
  record, its national ranks (`natureserve_nation`) and its state, province and territory ranks
  (`natureserve_subnation`), with the nation in `other_status.country` and, for a subnational rank,
  the place's name in `population` (`NatureServePlaces`; the search gives only codes: NF is the
  island of Newfoundland, LB Labrador, NN the Navajo Nation). `status` and `status_code` are the
  rounded rank as NatureServe gives it, with breeding (B), non-breeding (N) and migrant (M) parts
  ("N5B,N5N"). A rank is kept when one of its parts is a rank (1 to 5, H, X or U,
  `OtherStatusSystems.IsRankedLocally`), or when it is NA and NatureServe says the taxon is exotic
  there and not native (`qualifier` 'exotic'); NR and other NA ranks are left out, as GNR and GNA
  are. The national rank is a row of the country's table; the state ranks are a collapsed table
  under it (`OtherStatusTable.PlaceRanks`), opened by a line with the number of places and how many
  rank the taxon S1, S2, SH or SX. In the test build of 9 October 2026: 14,578 national ranks of
  11,819 taxa, 98,561 state, province and territory ranks of 12,390 taxa.
- `cites` rows (schema 26), under the "International" heading, which comes before the countries:
  each species, subspecies and variety of the Checklist of CITES Species goes to the taxon with its
  name in its kingdom, else to the taxon of one of its Checklist synonyms, else of an IUCN synonym;
  CITES-accepted names first. Each of its current listings is a row: `status` "Appendix II", or
  "Appendix III (Nepal)" with the Party; `status_code` the appendix; `population` the populations
  the listing's own note names, as plain text ("Populations of Botswana, Namibia, South Africa and
  Zimbabwe are included in Appendix II ..."); `listed_on` the date it took effect; `url` the
  taxon's Species+ page. A listing inherited from a genus, family or order has the higher taxon in
  `listed_under` ("family Trochilidae", shown under the appendix) and the higher taxon's note that
  applies to the taxon, unless the note is about the higher taxon's other members ("Except the
  species included in Appendix I"). A site species the Checklist does not name takes the listing of
  its IUCN genus, else family, else order, when the Checklist has that taxon with a listing of its
  own and no note. The note under the tables gives the citation the Checklist asks for, with the
  download date (meta `cites_fetched`). In the test build of 9 October 2026: 11,070 site taxa matched
  to Checklist taxa and 358 covered through a higher taxon; 11,460 rows.
- `gb-jncc` rows (schema 26), under "United Kingdom": JNCC's designations for the UK, Great Britain
  or one of its countries (`jncc_designation.scope` 'uk' or 'country'); international ones are left
  out because JNCC lists them for UK taxa only. A JNCC taxon goes to the taxon with its UKSI
  recommended name in its kingdom (a subgenus in brackets taken out), else of one of its names as
  designated, else of an IUCN synonym. `JnccLists.Classify` gives each designation code its list
  (an `other_status_list` row: Great Britain and England red lists, Birds of Conservation Concern 5,
  rare and scarce species, the wildlife laws, the priority species lists) and the status to show;
  `population` is the area it applies to. A red list keeps one category per season, the newest by
  `designated_on`; another list joins its statuses ("Schedule 5, section 9.4b; Schedule 5, section
  9.5a"). `report` is the source publication of red list and rarity rows. In the test build of 9
  October 2026: 3,370 taxa, 7,266 rows.
- `other_status_list` (schema 26) holds the lists of a system with several (JNCC's, and the national
  red lists from GBIF): its name (the row label), country, region, order among the country's lists,
  publisher, licence, citation, version and download date. The note under the tables is built from
  the lists of the page's rows.
- `br-salve` rows: each current SALVE assessment goes to the animal taxon with its name, else the
  one animal taxon whose IUCN synonyms include it; one per taxon. `status` is the category in
  English (`OtherStatusSystems.SalveLabel`; CR with SALVE's flag is "Critically Endangered (Possibly
  Extinct)"), `status_code` the code, `listed_on` the end of the assessment (the Brazil table heads
  it "Assessed"), `url` the assessment's DOI, else the PDF of its sheet.
- `nz-nztcs` rows: each current NZTCS assessment with a scientific name and a status other than
  "Not assessed" goes to the one taxon with that name in any kingdom (NZTCS gives none), else the
  one taxon whose IUCN synonyms include it; one assessment per taxon. `status` is the category and
  the status within it as NZTCS writes them ("Threatened - Nationally Vulnerable"), `report` the
  report it was published in, shown in the New Zealand table's "Published in" column.
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
- Language codes are ISO 639-1 where one exists, otherwise the ISO 639-3 code (IUCN's names keep
  IUCN's ISO 639-2 or 639-5 code, such as `phi`); for IUCN's names, `und`, `zxx`, `mis`, `mul` and
  the local-use range (`qaa` to `qtz`) become NULL. The other sources' codes are described under
  "Common names in other languages" below.
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
- Common names in other languages (since October 2026) come from IUCN (the API's taxon records,
  kept as they are), and from three sources that `SiteOtherLanguageNames` reads after every synonym
  list is in, because its rules compare names with the synonyms:
  - the Catalogue of Life: the `vernacularname` rows of the taxon's `col_id`, source `col`;
  - Wikidata: the labels, aliases and taxon common name (P1843) statements, other than deprecated
    ones, of the taxon's item (`taxon.wikidata_qid`), source `wikidata`. One scan of
    `wikidata_entities` reads the JSON of every downloaded item and parses only the taxa's items;
  - Wikipedia: the titles of the item's sitelinks to a Wikipedia (`<code>wiki`; not Commons,
    Wikispecies, Meta and the other `...wiki` sites, `OtherLanguageNameRules.WikipediaLanguage`),
    with a bracketed disambiguation at the end removed ("Tigre (animal)" is "Tigre"), source
    `wikipedia`.

  English names from these three are not read here: English names come only from the common
  names store, as before.
  - Language codes (`SiteLanguageCodes.Normalise`): ISO 639-1 where one exists, else ISO 639-3.
    Wikimedia's codes are mapped: `zh-hans`, `zh-hant`, `zh-tw` and the other `zh-` codes to `zh`,
    `zh-yue` to `yue`, `zh-min-nan` to `nan`, `zh-classical` to `lzh`, `pt-br` to `pt`, `sr-ec`
    and `sr-el` to `sr`, `be-tarask` and `be-x-old` to `be`, `als` (Alemannic Wikipedia) to `gsw`,
    `sh` to `hbs`, `bh` to `bho`, `bat-smg` to `sgs`, `fiu-vro` to `vro`, `roa-rup` to `rup`,
    `cbk-zam` to `cbk`, and any other code with a hyphen to the part before it. `simple`, `mul`,
    `nrm`, `roa-tara`, `map-bms` and `eml` are left out. The Catalogue of Life's codes `dnj`,
    `thy`, `mlf` and `fqs` are its source 2036's (and a few others') Danish, Thai, Malayalam and
    Persian names, as their scripts and words show, and are stored as `da`, `th`, `ml` and `fa`;
    individual languages that Wikipedia and Wikidata label with their macrolanguage are stored as
    it (`cmn` as `zh`, `zlm` and `zsm` as `ms`, `swh` as `sw`, `arb` as `ar`, `pes` as `fa` and a
    few more). Kotava (`avk`) is left out: Kotava Wikipedia titles its species articles with one
    word for the group and the scientific name in brackets ("Vesnol (Myotis horsfieldii)"), so
    without the brackets 960 bats would share "Vesnol". A code is kept only when
    `LanguageNameTable` (in `BeastieBot3.Shared`) has an English name for it, so every language the
    species page lists has a name; `isv` (Interslavic) and `rrm` (Moriori, newer than the ISO
    tables used) are left out.
  - `LanguageNameTable` has every ISO 639-3 and 639-5 code (8,026), keyed as stored, with CLDR's
    English name where CLDR keeps the code as it is, else ISO's reference name; when two codes
    would get one name, the CLDR-named one takes ISO's ("tw" is "Twi", not CLDR's "Akan").
    `node BeastieBot3.Shared/SiteData/generate-language-names.mjs` writes it
    (`LanguageNameTable.Data.cs`) from the iso-codes package's JSON (`/usr/share/iso-codes/json`)
    and Node's `Intl.DisplayNames`. The site's `LanguageNames.Name` reads the same table and asks
    the server's ICU only about a code it does not have.
  - `OtherLanguageNameRules.Check` leaves out a name that, compared by `SiteNameKey.Fold`, is the
    taxon's scientific name (also without IUCN's rank marker), a synonym from any source or a
    scientific name (P225) of the item, or the genus; and a name that starts with the genus as
    written, a space and a lower-case letter, or with the genus's initial, a full stop, a space and
    the species epithet ("P. tigris"). Direction marks (U+200E and the other bidirectional
    controls) are trimmed from the ends first: Wikidata's Asturian labels wrap the scientific name
    in them. The binomial rule also takes out real names that begin with the genus, such as the
    French "Tragopan de Cabot" and the Italian "Aquila minore di Cassin". Bots titled hundreds of
    thousands of articles in the Cebuano, Waray, Swedish, Dutch and Vietnamese Wikipedias with the
    scientific name, and most languages' Wikidata label of a taxon item is its scientific name; the
    rules take out nearly all of these. Of the 916 Cebuano and Waray Wikipedia titles left in the
    build of 9 October 2026, about 380 have the shape of a binomial: synonyms the build does not
    know and misspelt names ("Karpatiosorbus barthae", "Cyperus alleizettei").
  - The build of 9 October 2026 (release 2026-1, CoL 26.7 XR) read 1,077,531 CoL names, 7,141,312
    Wikidata names and 1,476,957 sitelink titles for the site's taxa, and wrote 868,549 `col`,
    864,240 `wikidata` and 443,013 `wikipedia` rows beside IUCN's 101,117: 1,382,883 names after
    merging, in 888 languages, for 113,785 taxa. The rules left out as scientific names 635 CoL
    names, 5,700,562 Wikidata names (nearly all labels) and 931,481 titles; the binomial rule
    alone 4,009 CoL names, 24,706 Wikidata names and 3,060 titles, of which about 2,400 CoL names
    and 3,000 Wikidata names and titles contain a preposition or a letter outside ASCII and so are
    probably real names. The name tables, `name_key` and `name_fts` grew from 206 MB to 489 MB, and
    the file from 1,040 MB (schema 25) to 1,346 MB. Reading the Wikidata JSON takes about 2 minutes.
  - `SiteNameSet` keeps one row per name, language and source. It compares common names in
    languages other than English by `SiteNameKey.CaseFold` (Unicode compatibility normalisation,
    lower case, spaces), so names that differ in an accent or a mark are two names ("Ñandú" and
    "Nandu", or the Japanese "ガエル" and "カエル"); English names are still compared by `Fold`.
    IUCN's names in other languages go through the same set.
  - The species page's "Common names in other languages" is a table (Language, Name, Source) built
    by `TaxonNames.OtherLanguageNames`: one row per name with case and Unicode normalisation
    ignored (`CaseFold`), in the spelling most of its sources give (IUCN's on a tie), with its
    sources in the order IUCN Red List, Catalogue of Life, Wikidata, Wikipedia; a language's names
    with most sources first, then by name; languages by their English name, "Language not given"
    last. Each language is one `tbody` whose first row has the language in a `th` with `rowspan`;
    the languages after the first 10 are hidden until the reader ticks "Show all languages".
  - These names are in `name_key` and `name_fts`, so a search in another language finds the
    taxon, but not in `name_word` (spelling suggestions use the English names only).
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
- The Catalogue of Life's English vernacular names are stored apart, in `higher_taxon_name`, with
  the caps rules applied as for a group name (`CommonNameNormalizer.ApplyGroupCapitalization`: the
  first word is lower-cased too, so "Typical Big Cats" is "typical big cats" and "Old World
  Monkeys" is "Old World monkeys"), each name listed once. They are not checked, and some
  name only part of the group ("cattle", "goats" for Bovidae), so the site lists them under their
  source and never uses one as the group's name. 10,651 groups have some.
- Names from English Wikipedia (`SiteGroupWikipediaNames`, `higher_taxon_name` rows with source
  `wikipedia`): the title of the group's article and the titles of the redirects to it, which
  `wikipedia fetch-group-titles` downloads (below). Search finds a group by them, and the group
  page lists them ("Names in English Wikipedia (article title and redirects):"), each linked to its
  Wikipedia page, on a line of their own before CoL's names. The table does not say which title is
  the article, so the list cannot mark it; every title is a page or redirect, so every link works.
  The article (from `enwiki_title`, else the group's name, followed through
  redirects) counts only when it is about the group: not a disambiguation page, nothing in it about
  another kingdom (`WikiPageKingdom`), and its taxobox taxon is the group's name, or, with no
  taxobox name, the group's title redirects to it and no taxon is matched to it. Genus
  *Orycteropus* redirects to "Aardvark", whose taxobox is *Orycteropus afer*, so the genus takes
  none of its names. Left out: the group's own name, the scientific name of any group or taxon in
  the site, the `common_name_en` of a taxon in the group ("Pirarucu" redirects to "Arapaima" and is
  the English name of *Arapaima gigas*, so the search still goes to the species), redirects to a section ("Dobsoniini" to "Megabat#List of genera"), titles with
  brackets, digits, colons or slashes, possessives, all capitals, "-ology"/"-ologist", and close
  misspellings of the article title or the group's name ("Chiroptra"). Scientific synonyms of the
  group itself stay ("Megachiroptera"). `higher_taxon_name.name_key` (`SiteNameKey.Fold`) is
  indexed for the search. The build summary counts the groups whose article is about the group,
  those of them with no redirect list downloaded yet, and the groups with no English name that
  get a name from Wikipedia that is not a scientific name (a candidate for `common_name_en`, which
  is still chosen as above).
- `wikipedia fetch-group-titles` builds the groups as `site build-db` does (`SiteGroupTree`, from
  the IUCN CSV export and the CoL placement) and, for each group's name (and a rules wikilink),
  downloads the English Wikipedia page into `wiki_pages` (50 titles per action API request, no REST
  HTML, saved through `WikipediaPageFetcher.SaveQueried`, so redirects, categories and taxoboxes are
  stored as for any page), then the kingdom-qualified titles of names that are disambiguation pages
  ("Morus (plant)"), then the redirects to each article reached (`prop=redirects`, into
  `wiki_incoming_redirects`, with `wiki_incoming_redirect_fetches` recording which titles were asked
  about). Families and above go first, then subfamilies and tribes, then genera. Names that are not
  in the all-titles list are not asked for. `--status` prints what is left; each run writes its
  counts to `wiki_group_title_status`, which the workflow light reads. The first full run in October
  2026 sent 1,038 requests and took under an hour: 24,063 pages and 27,510 redirect lists (60,154
  redirects).
- Once the genus pages are downloaded, the last fallback of `common_name_en` (the title the
  scientific name redirects to) gave 5,530 groups an English name instead of 1,182, many of them
  wrong: a monotypic genus took its species' name (genus *Ashbyia* redirects to "Gibberbird"), and
  genera took the titles of unrelated pages (genus *Thera* redirects to "Santorini") or of other
  taxa ("Paspalum"). `StoreBackedCommonNameProvider.GetWikipediaRedirectTitleByScientificName`
  therefore gives no name when the downloaded target has no taxobox, or when its taxobox is another
  taxon and that taxon is a species of the genus or the target's title is that taxon's scientific
  name. With that rule, 1,705 groups have an English name (October 2026). It also removes about
  300 names that were scientific names of other taxa ("Psilotaceae" for order Psilotales).
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

Two bullet list options add to a line (`Lists/ListLineOptions.cs`, the partial
`Pages/Shared/_ListLineOptions.cshtml`, strings in `Display/GroupLineText.cs`). Their query keys differ
from the species tables' `refs` and `cite`, because both sets of options are in one form.

- **Taxon authority** (`auth=small|plain`, none by default): `SpeciesListLineOptions.Authority` puts
  `SpeciesListEntry.Authority` right after the scientific name, before the `, common name` of the
  scientific-name-first style and inside the brackets of the common-name-first style:
  `* ''[[Panthera leo]]'' <small>(Linnaeus, 1758)</small>, Lion`,
  `* [[Lion]] (''Panthera leo'' <small>(Linnaeus, 1758)</small>)`. A line in the common-name-only
  style shows no scientific name, so no authority, unless the taxon has no English name. `<small>` is
  the form most species lists on English Wikipedia use (featured list "List of Banksia species";
  also the Conus, Anolis, Eucalyptus, Cortinarius and Tipula lists, the last with `{{small}}`); plain
  text is the other choice. Authorities with wikitext markup characters are wrapped in `<nowiki>`.
  The CLI never sets the option, so the generated lists are unchanged. IUCN taxa use
  `taxon.authority`, species from CoL use `extra_species.authority`; Wikidata-only species have none
  (the author and year are qualifiers P405/P574 on the taxon name, and the author is an item, so the
  sweep would need a qualifier join and label lookups and a full new pass). A line shown under
  another source's name has no authority: an IUCN taxon under CoL's or Wikidata's name, or a species
  in both sources under Wikidata's spelling, since the brackets of an authority depend on the genus.
- **References** (`lrefs=list|inline`, none by default; `lcite=q` for `{{cite Q}}`):
  `Lists/ListReferences.cs` gives each line a reference to the source it is listed from (the most
  preferred ticked source, `ListSourceMerge`): an IUCN taxon its latest global assessment, as the
  species tables cite it (`IucnReference`, ref name "IUCN" + English name, `SpeciesTable.RefNamer`;
  NE taxa get none); a species from CoL `{{Catalogue of Life |id=... |title=''Name'' Authority}}`
  (the template English Wikipedia uses, a CS1 wrapper in Module:Cite taxon; ref name `col-<id>`); a
  species from Wikidata `{{cite Q|Q...}}` of the taxon item (`wd-Q<n>`), which renders as
  "Name, Wikidata Q...". English Wikipedia treats Wikidata as user-generated and not a reliable
  source (WP:UGC), so the help line asks editors to replace those references. The `<ref>` goes after
  the `{{IUCN status}}` template, as in "List of mammals of Madagascar". List-defined references
  are in a `{{reflist|refs=}}` at the end; inline ones are written in full the first time a ref name
  is used. As with the species tables, inline references add no `{{reflist}}`. No external-link
  form: MOS:EL keeps external links out of article text. `Data/ListReferenceQueries.cs` reads the
  citation of every kind of taxon in the group (the species tables read species only). The preview
  shows each reference as a superscript number and lists the citations as written.
- Lists with references are capped at `ListReferences.MaxLines` (550). Measured with `action=parse`
  (October 2026) on genus Pristimantis, 531 lines with authorities and `{{IUCN status}}`: post-expand
  include size 1,522,686 bytes with `{{cite iucn}}` references, list-defined or inline (2.9 KB a
  line), and 299,422 bytes without references (0.56 KB a line). 731 lines would fill Wikipedia's
  2 MB; the cap leaves about a quarter for the article, as the species tables' caps do. Lua time was
  1.8 s of 10 s. The parse showed no error or maintenance categories for any of the forms, including
  `{{Catalogue of Life}}` with no access date and `{{cite Q}}` of a taxon item.

### The group page (`Pages/Group.cshtml`)

- The page shows the classification above the group (CoL groups marked), counts (species,
  threatened, extinct, subspecies and varieties, subpopulations), the counts by category, the names
  from English Wikipedia, CoL's English names, links, and a table of the groups directly in it.
- Help for an option that takes more than a line is behind a small "i" button beside the option's
  name (`Pages/Shared/_InfoTip.cshtml`, model `InfoTipModel(Id, Label, Text)`; Label is the
  button's accessible name, "Help for Red List categories"): Red List categories, list type,
  species sources, order of preference, and the species table options, references and ref names.
  One-line hints stay visible. The text is a popover (`popover="auto"`), so without JavaScript a
  click or tap opens it and Escape or a click outside closes it; a browser without popovers shows
  the text in place. `site.js` (`setUpInfoTips`) places it below the button (above when there is
  no room), opens it on mouse hover and on keyboard focus (closing when the pointer or focus
  leaves), keeps it open after a click, and sets `aria-expanded`. The partial is phrasing content,
  so it can go inside a `<legend>`, but never inside a `<label>`. A fieldset whose legend has one
  gets `aria-labelledby` pointing at a span around the legend's text, so screen readers name the
  group "Red List categories", not "Red List categories Help for Red List categories".
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
  templates as one Wikipedia page holds, or 550 with references (`ListReferences.MaxLines`, see
  "List lines"). The page counts the lines from `higher_taxon_count` first and reads no taxa for a
  longer list. The cap also keeps the site from handing out the categories
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
- Search lists the groups whose name is the search text, and the groups that have it as one of
  their names from English Wikipedia (below), with the matched title ("Matched English Wikipedia
  title: Fruit bat"), groups found by their own name first. It goes straight to the group when
  exactly one group is found and no taxon matches the text strongly (`SearchModel.GroupToGoTo`).
  A strong match is an exact match on a taxon's scientific name, a synonym, its `common_name_en` or
  its article title (`SearchHit.IsStrongExactMatch`). An exact match on any other common name is
  weak: "fruit bat" goes to family Pteropodidae, although "Fruit Bat" is a CoL vernacular of
  *Epomophorus pusillus*. When a group is found and a taxon matches strongly, or two or more groups
  are found, the results are listed, groups first. With no group found, search goes to the taxon
  as before (`SingleExactMatch`). `all=1` always lists. When search goes to a group and taxa
  matched too, the address has `?q=` and the group page links to all the results ("See all search
  results for “fruit bat”", for the species *Epomophorus pusillus*), as a taxon page does. The
  link is shown only when the text is the group's name or one of its names from English Wikipedia
  (`GroupModel.ArrivalText`), so it cannot put other text on the page; `q` is part of the group
  pages' output cache key.
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
- Schema 15: `higher_taxon_extra_count` splits those counts by sources, by placement (under a family
  because IUCN does not have the genus, or under the genus) and by whether a likely `extra_overlap`
  row pairs the species with an IUCN taxon (`SiteExtraSpeciesBuild.SplitCounts`); and
  `extra_species.authority` holds CoL's authorship for species in CoL (the list option "Taxon
  authority").

Build of 6 October 2026 (release 2026-1, CoL 26.7 XR): with `genus`, 998,573 extra species
(60,939 only in CoL, 477,256 only in Wikidata, 460,378 in both) and 318,647 overlap rows, adding
about 80 MB to the site database (572 MB to 651 MB) and about 75 seconds to the build. Most
Wikidata-only entries are old combinations that Wikidata keeps as separate items; 287,217 of the
overlaps are of this kind (CoL synonym). With `family` the build had 2,386,895 extra species
(1,387,673 of them under a family) and, before the columns were made smaller, added 305 MB.

Schema 15 build of 6 October 2026 (`family`, the default): 2,386,524 extra species (209,864 only in
CoL, 943,136 only in Wikidata, 1,233,524 in both; 1,387,670 under a family), 1,432,284 of the
1,443,388 in CoL with an authority, and 80,515 `higher_taxon_extra_count` rows (1 MB). The database
is 787.7 MB (761.3 MB at schema 14; the authorities are most of the difference) and the build took
267 s (270 s before).

**Site** (`Lists/ListSources.cs`, `Lists/ListSourceMerge.cs`, `Lists/GroupListSources.cs`,
`Data/SiteQueries.Extra.cs`, strings in `Display/GroupSourceText.cs`). The list option "Species
sources" has a checkbox for each source (`src=iucn|col|wd`, IUCN only by default) and an order of
preference (`prefer=icw|iwc|ciw|cwi|wic|wci`, IUCN, then CoL, then Wikidata by default):

- An IUCN species is listed when IUCN is ticked, or when it has a CoL usage (`col_id`) and CoL is
  ticked, or a Wikidata item and Wikidata is ticked; subspecies, varieties and subpopulations come
  from IUCN only. An extra species is listed when one of its sources is ticked. Extra species have
  no assessment, so they go in the NE section with no `{{IUCN status}}`; ticking CoL or Wikidata
  also ticks NE. The NE box carries `data-live-sync`: after a live update `site.js` copies its
  ticked state from the new page's form (only when the form still sends the query the page was made
  for), so the box shows what the server used without a reload.
- Each entry's name comes from the most preferred ticked source that has it (`taxon_source_name`
  for IUCN species).
- A likely duplicate: the entry whose most preferred ticked source comes later in the order is left
  out. When both entries have the same best source, or the reason is only possible, both stay. An
  entry outside the group counts when one of its sources is ticked: it can leave out an entry here,
  but a less preferred entry outside the group gets no notice here.
- The "Possible duplicates" panel under the list (`_ListNotices.cshtml`, arranged by the pure
  `Lists/ListNoticeGroups.cs`; strings in `GroupSourceText`). A line under the list's size gives
  the counts and links to it ("Possible duplicates: 278 entries left out, 1 pair with both entries
  kept"). Two sections: "Left out of the list (N entries)", with a line on how the order of
  preference decides, and "Both entries kept (N pairs)". Each section has a collapsible group per
  reason ("299 pairs: Catalogue of Life lists one name as a synonym of the other."), open when it
  has at most 10 rows, in a fixed reason order (synonyms, gender endings, spellings, other genus).
  Each group is a two-column table: the entry left out, or in the list, and the entry it is likely
  (or may be) the same species as. After each name come its source and, where the column heading
  does not say it, its state in this list: "in this list", "left out", or "in another genus"
  (the page's rank; "outside this group" on a Catalogue of Life group). The state is read from the
  merged rows, not from the notice, because an entry kept by one pair can be left out by another.
  Three or more pairs with one reason between the same two genera, from the same sources and in the
  same states, are one row ("*Rana* names Wikidata | *Lithobates* names IUCN · in another genus")
  with a "Show 41 pairs" checkbox that shows them (CSS `:has`; without it every pair shows). These
  runs are the old names of species moved to another genus. "Both entries kept" leaves out a pair
  in which either entry was left out by another pair (that entry is in the first section with its
  reason, and the pair puts no duplicate in the list), and puts the entry in the list first. The
  details and run boxes have ids, so a live update keeps them open and ticked (`data-keep-checked`).
  Genus *Rana* with all three sources (October 2026): 318 notices, 313 of them left-out pairs with
  278 entries left out, nearly all
  of them old Wikidata combinations; the 299 CoL synonym pairs show as 26 run rows and 58 single
  rows, and the second section has 1 pair (it had 5 before pairs with a left-out entry were left
  out of it).
- The line cap uses the IUCN counts plus `higher_taxon_extra_count` (`ExtraSpeciesCounts.For`):
  with `genera=0` it leaves out the species placed under a family, and when IUCN is ticked and first
  in the order it leaves out the species that are likely an IUCN taxon, since the merge always
  leaves those out then (with IUCN not first or not ticked, an IUCN taxon listed through its CoL
  usage or Wikidata item can tie with the extra species, and both stay). Likely duplicates between
  CoL and Wikidata entries are still counted, so the count can be higher than the final list; the
  "too many taxa" note says so. With all three sources ticked (October 2026), the count is 173 for a
  list of 169 in family Felidae (254 before schema 15), 1,058 for 1,022 in genus Conus (1,704),
  1,769 for 1,553 in family Conidae (3,480), and exact for Rattus, Ursidae and Panthera; with
  `genera=0`, 126 for 122 in Felidae (254).
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

**Pages of extra species** (schema 22; `Pages/ExtraSpecies.cshtml`, queries in
`Data/SiteQueries.ExtraPages.cs`, strings in `Display/SiteText.Extra.cs`). Each extra species has a
page at `/col/{CoL ID}` and, when it has a Wikidata item, at `/wikidata/{QID}` (`SiteUrls.Extra`
links the CoL address when there is one); `extra_id` is numbered at build time, so it is never in
an address. The page gives the name and author, the English name, the groups above it on this
site, that it is not on the Red List and which sources list it, the family it is shown under when
IUCN has no genus of its name, the IUCN taxon with the same CoL ID (an IUCN subspecies that CoL
treats as a species; 43 in 2026-1), its `extra_overlap` pairs in both directions, and links. A
`/wikidata/` address of an IUCN taxon or assessment goes to search, a `/col/` address that only an
IUCN taxon has goes to its page, and a property or an item the site uses goes to search, which
names it. The pages are noindex. Search lists extra species after the IUCN taxa from
`extra_name_fts` (an external-content FTS5 table over the scientific name, Wikidata's spelling and
the English name, rebuilt by `site build-db` after the rows are in; about 86 MB for 2.4 million
species): it reads at most 500 matches and ranks them in C# (a name equal to the text, then a name
starting with it, then shorter names). It goes straight to the page when no IUCN taxon and no group
matches exactly and exactly one extra species does. IUCN taxon pages list the extra species that
may be the same ("Possible duplicates in the Catalogue of Life and Wikidata"), and the group page's
duplicates panel links the extra species' pages. `/api/suggest` does not suggest extra species.

### Credits of an assessment

The taxon page has a closed section "Credits as given by IUCN" under the `{{cite iucn}}` box
(`Pages/Shared/_AssessmentCredits.cshtml`, `Pages/CreditsView.cs`, strings in
`Display/SiteText.Credits.cs`). It lists the people and organisations that the payload's
`credits[]` array names for the assessment the wikitext is for, so an editor can see the full
names and affiliations behind the citation's initials. The section has `id="iucn-credits"`, so it
stays open when `site.js` replaces the wikitext after an option changes. It changes with the
assessment picked from the history tables.

Each credit type is listed under the heading IUCN's assessment pages use (checked on the tiger's
page on iucnredlist.org in October 2026), in IUCN's order: `assessor` "Assessor(s)", `evaluator`
"Reviewer(s)", `contributor` "Contributor(s)", `facilitators` "Facilitator(s) / Compiler(s)" and
`institutions` "Partner(s) / Institution(s)". A type IUCN adds later is shown after these under
its own name. Each list has its number of entries after the heading, and the summary gives the
number of different names, counting a name once when it appears under two headings (compared
without its bracketed affiliation, `CreditsView.NameKey`).

What `site build-db` stores (`AssessmentCreditsReader`, pinned by `SiteDbBuildCreditsTests`):

- One group per credit type with the `value[]` entries as IUCN wrote them, with whitespace
  collapsed to single spaces ("Pranav  Chanchani"). Entries that are not strings are skipped,
  an entry listed twice in a type is kept once, and a type that a payload repeats (4 payloads in
  2026-1) is one group.
- Email addresses are left out: an entry that is only an address is dropped, and an address inside
  an entry is removed with its brackets. About 0.75% of entries in 2026-1 have one, nearly all of
  them address-only. An "@" that is not part of an address is kept.
  The group records how many different address-only entries it had (`"emails"` in the stored JSON,
  schema 17), and the page says so under the group: "IUCN's list also has 2 email addresses, not
  shown here."
- `value[]` has no fixed order: in two of every three blocks with two or more names it differs from
  the order of the `full` string, which IUCN's pages and the citation use. The entries are put in
  the order of `full` when each entry's surname (its last word, after notes in brackets and a
  generational suffix) is found there exactly once as a whole word; otherwise `value[]`'s order is
  kept.
- When `value[]` is empty, or holds only email addresses, the group is the `full` string (the
  citation form, "Tolley, K. & Menegon, M."), shown as one line with no count and no note. When any group is like this the summary has no total. In 2026-1
  this applies to 69,554 of the 346,877 assessments with credits, mostly older ones.
- `assessment.credits` holds the groups as ids into `credit_name` (`StoredCredits` in
  `BeastieBot3.Shared`): `[{"type":"assessor","names":[12,45]},{"type":"evaluator","full":77}]`.
  The same people are credited on thousands of assessments: the build of 6 October 2026 has
  2,215,723 entries and 34,119 distinct ones. Stored this way the credits add about 41 MB to the
  database (39.4 MB of `credits` text and 1.9 MB of `credit_name`), where JSON with the names in
  each row would have added about 136 MB. The site reads them only for the assessment shown: the
  row by its primary key, then the names with one `json_each` parameter.

Credits are names and affiliations, not narrative text, so the IUCN Terms of Use rule above does not
keep them out of the database.

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
  the first author's. When `AssessorGivenNames` found a person's full given names, the statement
  also has author last names (P9688) and author given names (P9687) qualifiers ("Sayer",
  "Catherine"). `{{cite Q}}` passes these to the citation as `|last=` and `|first=`, so it shows
  the full given names, or the initials with `|name-list-style=apa` (which writes "R. L." where
  IUCN prints "R.L."). A name with a suffix ("Lowry, P.P., II") gets neither qualifier, because
  `{{cite Q}}` would leave the suffix out;
- author (P50) instead of an author name string for an organisation listed in `IucnAuthorItems`
  (`BeastieBot3.Shared/Wikitext/IucnAuthorItems.cs`: BirdLife International, BGCI, UNEP-WCMC,
  ICMBio, NatureServe and a few others, checked on Wikidata on 2026-10-08), with the series
  ordinal and object named as (P1932) holding the name as IUCN prints it. `{{cite Q}}` shows the
  P1932 name and links the organisation's English Wikipedia article. Most IUCN SSC specialist
  groups had no Wikidata item then; add a row to the table when one is created.

No statement has a reference. When `TitleNameFor` finds no usable name, `CreateItemCommands`
returns no commands, because an item with no title or label could not be found again.

`AddMissingCommands` writes commands that add to an existing item what it lacks, judged from
`wikidata_item_properties`: the English label when `Len` is missing, and each statement above
whose property is missing. The label and the title use the name from `TitleNameFor`, given the
item's own titles (`wikidata_item_titles`). An item with any author statement (P50 or P2093) gets
no author statements. `AddMissingCommands` never adds publisher (P123), language (P407) or a
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
  `Display/IucnCategories.cs`. All SQL is in `Data/`, most of it in `SiteQueries` (`SiteQueries.cs`
  and the partial files `SiteQueries.*.cs`: `Search`, `Groups`, `ListTaxa` and others); user input
  reaches FTS5 only through `Data/FtsQuery.cs`.
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
- The home page's search examples are picked again on every visit (`Display/HomeExamples.cs`, not
  output-cached): up to 5 from lists of scientific names, a large well-known animal first, always a
  plant, a bat in half of the visits, and threatened species (CR, EN, VU) four times as likely to be
  picked as LC ones (NT twice). Names the site database does not have as a species in the release
  are left out (`SiteQueries.GetExampleTaxa`, read once per database file); each example shows the
  common name, or the scientific name 30% of the time and always at least once. Each links straight
  to the taxon page (with `?q=` for a common name, the address a search that finds only that taxon
  goes to), because a search for many of these names lists other taxa too ("Tiger", "Dodo", and
  *Balaenoptera musculus*, a synonym of the fin whale).
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
  A Wikidata item (`Q33609`, or a `wikidata.org/wiki/Q33609` or `/entity/Q33609` address) finds
  the taxa whose item it is (`taxon.wikidata_qid`) and the assessments whose item it is
  (`assessment.wikidata_item_qid`), listed with "Matched Wikidata item". When one taxon in the
  release has the item, search redirects to that taxon even when an old IUCN id also has the item.
  When no taxon or assessment has the item, the item of an extra species goes to its page (below),
  an item the site uses itself (a P141 value, the assessment item model's items, an organisation
  cited as an author) is named with its English label and linked ("Q32059 is the Wikidata item
  “IUCN Red List”."), and any other item gets a line saying no taxon or assessment has it; the text
  is not searched as a name. A Wikidata property (`P31`, `wikidata.org/wiki/Property:P31`) is
  named the same way when the site uses it, from the labels in `Display/WikidataTerms.cs` (checked
  2026-10-08). Not searched: the other items that state a taxon's IUCN id
  (`taxon.wikidata_other_items`).
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
  Next comes the kind of name matched: a scientific name, then an English common name, then a
  common name in another language, then a synonym. Among
  exact matches on a common name, a taxon whose English name (`common_name_en`) is the text comes
  before a taxon that has the text only as another common name, so "Dodo" lists *Raphus cucullatus*
  before *Euphorbia drupifera* (a common name "dodo"), and "Axolotl" lists *Ambystoma mexicanum*
  before *Ambystoma bombypellum*. Search and `/name/{name}` go straight to a taxon page when the text names one taxon exactly: the
  only exact match among the taxa in the release or, when no taxon in the release matches exactly,
  the only exact match among all taxa (`SearchModel.SingleExactMatch`). Taxa whose only exact
  matches are common names in other languages (`SearchHit.IsExactInOtherLanguageOnly`) count only
  when no other taxon matches exactly, so an English name, scientific name or synonym of one taxon
  still goes to it when the same words are another taxon's name in another language. The matched
  name shown for a hit is the one with the best score: the query takes it as a bare column beside a
  single MIN(), so the other per-taxon flags are SUMs, not MAXes. The search box lists each
  suggested name once, ignoring letter case, because two suggestions with the same name open the
  same search result. When an old id and a taxon in the release have the same scientific name,
  `/api/suggest` returns both, and `site.js` keeps only the first, which is the taxon in the
  release.
- The IUCN Red List Terms of Use limit what the site may hold and offer: no assessment narrative
  text, no coded threats or habitats, no downloads, and no API that returns assessment fields. The
  countries and areas of each taxon's latest global assessment (`area`, `taxon_area`, schema 18) are
  held only to compare a list with the one area chosen on `/update` (owner's decision, October 2026,
  being confirmed with IUCN); no page lists a taxon's areas.
  `/api/suggest` returns names, ids and the category only. Every page with IUCN data shows
  the Red List version and links to the About page, which credits every source with its licence and
  citation. Crossref is one of those sources, for the DOIs that `iucn resolve-dois` finds; its
  licence is CC0, and its Version cell gives the date of the newest DOI check
  (`iucn_doi_checked_to`).
- Run locally with `dotnet run --project BeastieBot3.Site`. `appsettings.Development.json` points to
  `~/datasets/beastiebot/site.sqlite`; set `Site__DatabasePath` to use another file.

### Comparative classification (`ladder_node`)

The species page's comparative classification (headed "Classification" on the page; the hidden
heading of the line of groups under the taxon's name is "Short classification") lines up the
taxon's classification in IUCN, on this site (IUCN with the
Catalogue of Life groups between its ranks), in the Catalogue of Life, Wikidata, English
Wikipedia and Wikispecies, row by main rank (kingdom to species; `Display/ClassificationComparison.cs`).
`site build-db` writes each source's nodes to `ladder_node` (`SiteBuild/SiteLadders.cs`); the page
climbs `parent_id` from the taxon's start node.

| Source | Start node | Downloaded by |
| --- | --- | --- |
| col | the taxon's `col_id` | `col import` |
| wikidata | the taxon's `wikidata_qid` | `wikidata sweep-taxa` |
| wikipedia | `article:<enwiki_title>`, then the taxonomy templates its taxobox climbs | `wikipedia fetch-taxonomy-templates` |
| wikispecies | `page:<IUCN scientific name>`, then the taxonavigation templates its page climbs | `wikispecies fetch` |

`wikispecies fetch` keeps its pages in a cache of their own (`Datastore:wikispecies_cache_sqlite`,
else `wikispecies_cache.sqlite` in the datastore folder), with the same tables as the Wikipedia
cache. It asks for the Wikispecies page of each IUCN name (animal subspecies without "ssp.", plant
"subsp." and "var." kept; redirects followed), then, round by round, the templates the pages and
templates call. `Taxonomy/WikispeciesTaxonavigation.cs` reads a page's Taxonavigation section and a
template's "Rank: name" lines (Latin ranks in English, as for Wikipedia's templates), and leaves
out comments, `<noinclude>` parts and the templates' "summary" blocks. A taxon gets a Wikispecies
column only when its page's last taxonavigation line names the page itself.

### Subspecies and varieties (`infraspecific_name`, schema 27)

A species page has a section after the names that lists the species' subspecies and varieties from
three sources, one row per name with its authority and its sources, each source linked to its
record (`Pages/SubspeciesRows.cs`, `Pages/Shared/_Subspecies.cshtml`, strings in
`Display/SiteText.Subspecies.cs`). The section is left out for a taxon that is not a species in the
release, and when no source lists a subspecies or variety of it.

| Source | Rows | Link |
| --- | --- | --- |
| IUCN Red List | the subspecies and varieties in the release whose `parent_taxon_id` is the species, read from `taxon` when the page is shown | the taxon's page on this site |
| Catalogue of Life | accepted and provisionally accepted name usages of rank subspecies or variety whose `parentID` is the species' `col_id` | the CoL record |
| Wikidata | items of `wikidata sweep-taxa`'s table with rank subspecies (Q68947) or variety (Q767728) whose parent taxa (P171) include the species' item | the item |

`site build-db` writes the CoL and Wikidata rows to `infraspecific_name` (`SiteBuild/SiteSubspecies.cs`):
one indexed CoL query per species with a CoL ID (`nameusage` has an index on `parentID`), and one
read of the sweep's subspecies and variety rows by rank. It leaves out Wikidata items that are an
instance of synonym, fossil taxon, unavailable combination or original combination, and items that
another item names as a taxon synonym (P1420). An item of an extinct taxon (Q98961713) is kept: the
Cape lion is still a subspecies of the lion. CoL's `extinct` flag is not used, because it is set on
both living lion subspecies. A name is kept only when `InfraspecificNames.Split` (`BeastieBot3.Shared`)
reads it as a genus, a species epithet and one more epithet, with at most one rank marker (a
subgenus in brackets after the genus, as CoL writes many insect names, is dropped from the name).
Hybrid names ("×", "nothosubsp.") and names with a capitalised last word are left out.

The page merges the rows by `InfraspecificNames.Key`: the rank, then the three words folded, so
"Panthera pardus ssp. orientalis" (IUCN) and "Panthera pardus orientalis" (CoL) are one row, and a
subspecies and a variety of the same name are two. A row shows the name as the lists write it (no
rank marker for an animal subspecies, "subsp." and "var." for other kingdoms), the authority of the
first source that gives one, and the sources in the order of the names tables (IUCN, Wikidata,
Catalogue of Life), each with its own authority when that differs. A source with two records of one
name (Wikidata has two "Panthera leo leo" items) links each record by its id. Rows are sorted by
name, and those after the first 10 are hidden behind a "Show all" box.

Known limits:

- Wikidata often states a synonym in both directions: the item for Panthera leo leo names
  P. l. persica as a taxon synonym and the persica item names P. l. leo, so both items are left out.
- Names are merged only when they are spelled the same: "melanochaita" and "melanochaitus" are two
  rows, and so are a CoL subspecies whose species part differs from IUCN's (CoL's accepted name of an
  IUCN species may be spelled differently or be in another genus) and IUCN's or Wikidata's name for
  it. In the October 2026 build, 4,014 CoL rows (1,394 species) and 1,355 Wikidata rows (477
  species) have another species part than IUCN's name, such as CoL's "Acerodon macklotii alorensis"
  under IUCN's Acerodon mackloti.

In the October 2026 build (2026-1, COL26.7 XR), 25,624 species have a list (14,100 with names from
two or more sources): 3,076 IUCN subspecies and varieties, 55,184 CoL rows (46,789 subspecies and
8,395 varieties, 16,792 species) and 75,048 Wikidata rows (54,655 subspecies and 20,393 varieties,
22,436 species; 1,009 items named as a synonym and 178 synonym or fossil items left out). Both
readers take about 4 seconds.
- The checklists store keeps no subspecies. The Mammal Diversity Database's species file has a
  `subspecies` column (1,429 of 6,904 species in v2.5: each subspecies with its authority, its
  synonyms and a fossil or recently extinct note), and the Reptile Database's ColDP export has 7,667
  subspecies names, but `checklists import` reads neither. Wikispecies pages and the `subdivision`
  lists of English Wikipedia taxoboxes are other possible sources.

### Citation options

The options form in a taxon page's wikitext section is read from and written to the query string
(`Pages/WikitextOptions.cs`): `authors=author|lastfirst` (default `lastfirst`, `|last1=Surname
|first1=I.`, since 6 October 2026; a sole author that is not a person, such as BirdLife International,
is `|author=`; the species tables and `/update` use the same default), `fullnames=1`,
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

`BeastieBot3.Site.Tests/browser/live-update.cjs` checks the updates in a browser, on a taxon page
and on a group page's list (ticking CoL updates the list in place and ticks Not Evaluated; an info
button opens its help text and Escape closes it). It is not part of `dotnet test`. To run it:

1. `SITE_FIXTURE_DB_OUT=/tmp/site-fixture.sqlite dotnet test BeastieBot3.Site.Tests --filter FixtureExport`
   writes the test fixture database to a file.
2. `ASPNETCORE_ENVIRONMENT=Production Site__DatabasePath=/tmp/site-fixture.sqlite Site__RateLimits__PagesPerMinute=10000 dotnet run --project BeastieBot3.Site --urls http://127.0.0.1:5391`
   runs the site on that file.
3. `node BeastieBot3.Site.Tests/browser/live-update.cjs` runs the checks. Playwright installed
   globally is enough. `SITE_URL` sets another address, and `SHOTS` names a folder for
   screenshots.

### Status update page (`/update`)

A Wikipedia URL (desktop, mobile, or `index.php` with `title=` and `oldid=`) or a wikilink typed
into the search box (`WikipediaPageInput` in `BeastieBot3.Shared`; either may be wrapped in
quotation marks, guillemets, backticks or angle brackets, as pasted from a message) redirects to
`/update?page=Title`, which loads the wikitext from English Wikipedia's action API
(`Update/WikipediaPageSource.cs`: redirects followed, answers kept for 5 minutes, at most 2 MB)
and runs the update as a POST would, with the categories its title names and, for a title that
names Australia, the EPBC Act column. The page says which page and revision the text is; hidden
fields keep that line on later POSTs while the text is unchanged (`pagekey`). The site asks
Wikipedia only when `Site:WikipediaUserAgent` is set. A load counts against the client's
`UpdatesPerMinute` like a POST and the shared limit on updates at once, and loads for all clients
together are capped at `WikipediaLoadsPerMinute` (default 30; a page held from the last 5 minutes
does not count), after which the page answers 429 and says so. The systemd unit allows connections to Wikimedia's address ranges
for it. The option "Show the EPBC Act status" (`epbc`) adds each taxon's EPBC Act listing
(`epbc_listing`, so only taxa IUCN also has) to the report and the comparison's tables, and lists
the missing taxa that have one.

An editor pastes the wikitext of an article or list and gets the same text back with the IUCN
statuses changed to match the latest global assessments, and a report with one row per item (line,
result, item, text before and after, taxon, notes). Strings are in `Display/UpdateText.cs`; the
logic is pure, in `Update/` (`WikitextScanner` masks comments, nowiki, pre, syntaxhighlight, source
and math, and finds templates by counting braces; `WikiTables` reads wikitables line by line with
colspan and rowspan; `StatusUpdater` decides the edits, and `StatusTaxonResolver` finds each item's taxon by name, synonym, IUCN citation, a link to its English Wikipedia article (the title the site stores as a `name` with source `wikipedia`; not for an item given NE) or common name), and reads the database through
`IStatusLookup` (`Data/SiteStatusLookup.cs`), so `StatusUpdaterTests` run over a fake.

- `{{IUCN status}}` with a taxon id: the code (`IucnStatusTemplate.ToTemplateCode`), the ids, and
  `|year=` or a `|label=` that is a year are replaced; for EX and EW those parameters are removed. A
  template with neither keeps having neither, and a template with a taxon id only ("2467", as List
  of cetaceans writes it) keeps that form, unless the reader asks for them (options below). An id
  with `in_release = 0` and a `current_taxon_id` uses the current taxon. An id the site does not
  have, or one not in the release with no current taxon (List of birds of Hawaii has old BirdLife
  ids), falls back to the scientific name in the template's table row or on its list line. A
  template whose id is of another taxon than the one its row or line names is left as it is, with
  a note to check the id; so is a template with such an id whose row names no taxon the site has.
  List of animals in the Galápagos Islands had one mockingbird's id on 433 rows, including bats
  IUCN lists under other names; List of mammals of Europe has rows naming a species with the id of
  one of its subpopulations.
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
- Statuses added where there are none (`Update/StatusUpdater.Add.cs`, form fields `addlines` and
  `addcols`, both off by default). With an option off, the lines or tables are counted
  (`StatusUpdateResult.ListLinesWithoutStatus`, `TablesWithoutStatus`) for an offer above the
  result and are not items, so they do not count toward the item limit.
  - `addlines`: `{{IUCN status|EN}}` on a `*` or `#` line with no `{{IUCN status}}`, outside
    templates (except list layout templates such as `{{columns-list}}` and `{{div col}}`), tables and
    sections such as References, External links, See also, Further reading and Synonyms, whose text
    before its first `<ref>` writes exactly one scientific name outside external link labels, naming
    one taxon (a synonym counts, with a note). A name counts when it is in italics or is a scientific
    name or synonym of a taxon, so a common name in a link ("[[Magnificent frigatebird]]", which has
    the shape of a binomial) does not count as a second name. A line with no scientific name is
    found by a link to the taxon's English Wikipedia article (`MatchedByArticle`, below). Lines that
    name a rank first ("* Family [[Basking shark|Cetorhinidae]]", "** Genus ..."), lines that head a
    species group ("''S. vagrans'' complex") and links labelled with a group name (one word in
    italics, or ending -idae, -inae and so on) are left out. It goes after the name, after a
    closing bracket when the name is in brackets ("[[Tiger]] (''P. tigris'')"), and after an
    authority straight after it: in brackets with a year, `{{small}}` or `<small>`. A line that gives
    a status as "(EN)" or a status image is reported and left. The `ids` and `year` options apply to
    the new template. In a sample of 51 cached articles in October 2026 this added a status to 313
    lines of List of Acer species, 375 of List of Phyllanthus species and 254 of the 280 lines of
    List of Carex species that name an IUCN taxon (the other 26 name a taxon IUCN has not assessed
    globally), and to the species lines of genus articles such as Alseodaphne and Bulinus. Fossil
    and hybrid lines name no IUCN taxon and are not items.
  - `addcols`: an "IUCN status" column after the column with the most scientific names, in a
    wikitable with no header that names a status and no data cell holding a code or
    `{{IUCN status}}`, not nested, with at least 3 data rows of which at least half name one taxon.
    Only a table with one header row at the top, the same number of cells in every row and no
    rowspan or colspan gets the column; another is reported with the first row that stops it
    (`ColumnLayout`). Each new cell is written the way its row writes cells (after `||` or `!!` on
    the same line, or on a line of its own); a row whose taxon is not found gets an empty cell. The
    table's findings are one item, so a table is never half changed.
  - The edit summary counts added statuses ("12 IUCN statuses added") and new columns ("IUCN status
    column added (14 statuses)", or "IUCN status columns added to 2 tables (30 statuses)"). Feeding the result back
    through the page finds every added status already up to date.
- Comparison with the group (`Update/ListScope.cs`, report only, the text is never changed;
  section "Comparison with Family Felidae", `Pages/Shared/_ListScope.cshtml`, strings in
  `Display/UpdateText.Scope.cs`). The taxa the text lists (`StatusUpdateResult.Members`: list lines,
  table rows, species table rows and `{{IUCN status}}` templates, with or without a status; not
  taxoboxes or citations) are compared with one group (`higher_taxon`), read through
  `IListScopeLookup` (`Data/SiteListScopeLookup.cs`):
  - The group is the deepest that holds 95% of the listed taxa; a Catalogue of Life group must hold
    all of them (a list of CR mammals with two monotremes is compared with class Mammalia, not
    subclass Theria). The reader can choose a group above it, or below it when that group holds
    half the listed taxa (form field `scope`, "rank/name").
  - Categories: when every code the text writes (5 or more) is one category, or all are CR, EN or
    VU, or all EX or EW, only taxa in those categories are compared. A text with no codes is
    compared in the category (or with the threatened categories) that 80% of its species are in
    now. The written codes are used first because they still show where taxa that have moved were.
    The reader can choose the categories instead (form field `cats`, a `ListCategories` key in
    `BeastieBot3.Shared`: all, threatened, extinct (EX, CR(PE), CR(PEW), as the recently extinct
    lists), or one category); a page loaded from Wikipedia takes them from its title
    (`ListCategories.FromTitle`, "List of threatened birds of Brazil" is threatened). Chosen
    categories are kept when the text lists less than half of their taxa, and the list is then
    partial. A taxon matches a category by its code ("CR(PE)" for the extinct choice) or by the
    code's category (CR(PE) is CR).
  - Country or area (`area`, an area code) and Origin (`areamode`: native or reintroduced, the
    default; endemic; native, reintroduced or introduced; any origin): with an area, the group's
    taxa whose latest global assessment codes the area with a matching origin stand in for the group
    in the counts, the partial test and the missing taxa (`IListScopeLookup.TaxaInArea`, a join of
    the group's tree_pos range with `taxon_area`). Presence never leaves a taxon out: country lists
    keep extirpated species. A page loaded from Wikipedia takes the area from its title
    (`AreaNames.FromTitle` in `BeastieBot3.Shared`: the words after the last " of " or " in ",
    matched with IUCN's names, the part before a comma, English Wikipedia's names for the rest, and
    "Hawaii"; continents name no area) and endemic when the title says so. Of 466 downloaded bird
    and mammal country lists, 360 mention introduced species and about 200 accidental or vagrant
    ones, and endemic-only lists are separate pages (28 titles), so native is the default. Missing
    taxa whose record needs a word (introduced, vagrant, possibly extinct...) are in a table "to
    check before adding", and listed taxa the area's records leave out in another; both say what
    the records say about that one area only. Extra species (Catalogue of Life) are not offered with
    an area: their distributions are not read.
  - The text is a list of the group when it lists half the group's species (in the categories).
    Otherwise (a regional list) no missing taxa are listed unless the reader asks (`anyway`), and the
    section is shown only for 10 or more listed taxa; under 3 listed taxa there is no section.
    Subspecies and varieties are compared only when the text lists half of them.
  - It lists the missing taxa with a latest global assessment as list lines (`SpeciesListLine`, the
    group's default style from `GroupListQuery.DefaultStyle`, ids and year), at most 3,600; the taxa
    listed that are now in another category; the taxa IUCN places outside the group (a genus move);
    and taxa that appear under two or more names on different lines (a lump).
  - Checked in October 2026 on 12 Wikipedia lists and the 51 cached articles of the check above (14 of
    them got a comparison): List of
    endangered amphibians (no codes) gave 567 EN species missing and 38 listed taxa now in another
    category; List of canids, List of cetaceans and Genus Fulica (Coot) one missing species each;
    List of felids none; List of Acer species 2 missing and 5 lumps; the regional lists (mammals of
    India and Madagascar, birds of Hawaii) are partial. A 320 KB list takes under 0.3 s. Lines that
    give only a common name link are found by the article (the live List of critically endangered
    mammals: 187 of IUCN's 236 CR species). A missing taxon whose article is a list (its own name
    redirects there) is written without a link.
- Missing taxa put into the list (`Update/ListPlacement.cs`, `ListPlacement.SpeciesTables.cs`,
  `ListPlacement.Tables.cs`; form field `addmissing`, off by default; a button under the missing
  taxa sends the text again with it on, then a ticked checkbox keeps it on). Each missing taxon
  goes where the list has a place for it, next to a taxon the comparison found
  (`ListMember.Source` says where that is):
  - a species with a species of its genus on a list line: a new line among the genus's lines (the
    lines with fewest markers), after the lines under its neighbour (more markers, or a ":" note),
    copying the neighbour's markers and name style, with `{{IUCN status}}` (ids and year when those
    options are on) when the neighbour has one;
  - a subspecies or variety whose species is on a list line: a line one marker deeper, after the
    lines already under the species, in their style;
  - a species with a species of its genus in a `{{Species table}}`: a new `{{Species table/row}}` in
    the neighbour row's layout, with name, binomial, authority, status, population and trend filled
    in (`SpeciesTable.Row`), the other values (image, range, size, habitat, diet, subspecies) left
    blank, layout switches such as `no-diet=yes` kept, and an inline reference, or
    `<ref name="X"/>` when the text already defines a reference citing the taxon (`ReferenceIndex`);
  - a species whose genus has no table, in a list of `{{Species table}}`s that has a table of a genus
    of the same family: a new `{{Species table}}` for the genus (no authority, species count in
    words) with a row for each missing species, among the family's tables in order of genus name;
  - a species with a species of its genus in a row of a simple wikitable (the same layout rule as
    `addcols`): a new row copying the neighbour row's cells, with the scientific name, common name
    and status cells filled in and the others empty;
  - in a text whose taxa are mostly on list lines, any other species (`ListPlacement.Sections.cs`):
    each heading's section gets a group, the group its heading names (a link target or label, the
    text, a capitalised word: "==[[Galliformes]]==", "Order Galliformes") when that group holds at
    least half its taxa, else the deepest group holding nearly all of them; sections beside each
    other whose groups are of different ranks are raised to the rank most of them have (a family
    heading with one species is the family's, "== Landfowl ==" among order headings is the order's).
    A section titled "Other ...", "Miscellaneous ..." or "Unplaced ..." is of its parent's group.
    The species goes among the list lines of the section of the deepest group that holds it (looking
    inside sections of no group of their own, such as "===[[Lemuroidea|Lemurs]]===" with lines of
    several families). When that section has sections below it for groups of one rank and none for
    the species' group of that rank, the species goes in an "Other ..." section under it, or among
    its own lines when they are of several groups of that rank, else in a new section for its group:
    a heading at the level of the section beside it, with the group's scientific name (linked when
    that heading has a link, after its rank word when it has one), its `{{gray}}` line with the
    group's English name, and the lines in its `{{columns-list}}`. New sections go in alphabetical
    order when the sections keep it, else after the last section of the group's nearest relatives
    (most of its path shared), which for birds (no CoL groups between order and family) is the last
    section. A text with no headings is one section: the species goes in alphabetical place, or after
    the last line. A line copies its neighbour's link form: "*[[Anas bernieri|Bernier's teal]]"
    gives "*[[Arizelopsar femoralis|Abbott's starling]]".
  Among its neighbours a taxon goes in alphabetical order by the scientific names as written (a
  synonym sorts where the list put it; lines of species IUCN does not have count), or by common
  names when the list keeps that order instead (List of canids sorts its rows by common name); one
  pair in ten may be out of order; a list in neither order gets it after the genus's last taxon.
  Missing taxa with no place stay in the copy box, as do all of them for a list that may be
  regional. The insertions are applied with the updater's edits on the pasted text
  (`StatusUpdater.TextWith`), and the edit summary says "2 species added" ("2 taxa added" when a subspecies or variety is among them; the button and option say "taxa" in the same case). In October 2026 it put the
  missing species of List of Acer species, List of Carex species, Bulinus and Citharexylum in
  alphabetical place, 78 of the 92 missing EN species into List of endangered amphibians, the red
  wolf between the golden jackal and the wolf in List of canids, and all 8 missing species into List
  of vespertilionines. List of cetaceans gets none: its tables use rowspan. With headings (October
  2026): List of endangered birds 33 of 33 (10 by genus alone), with new headings for Acanthizidae,
  Campephagidae and Hylocitreidae; List of vulnerable mammals 171 of 171 (107), with one new heading
  (Galagidae, after Lorisoidea) and a sheath-tailed bat in "Other microbat species"; List of
  critically endangered amphibians 6 of 6 (4). The report gives each placed taxon's heading
  ("(new)" for a new one) and each taxon left out its reason (`UnplacedReason`).
- Taxa now in another category taken out (`Update/ListPlacement.Removal.cs`; a button under their
  table, field `rmall`, ticks all of them; then each row has a checkbox, field `rm` with the taxon id,
  sent with `rmshown`, the text's key, so that a form with none ticked means none and a new text
  drops the choice). A taxon is taken out only when every place the text names it can be: its list
  lines with the ":" notes under them, and its `{{Species table/row}}` rows. It stays (`KeptReason`)
  when it is in a wikitable or running text, when its line names another taxon or has list lines
  under it that are not taken out (List of near threatened reptiles lists *Caretta caretta*, VU,
  with its NT subpopulations under it), or when its line defines a named reference other lines use.
  The first line of a `{{columns-list|...|*line` loses only its list part, and the last line of
  `...]]}}` takes the line break before it, so the `}}` stays. A section left with no taxa and
  nothing but templates, comments and blank lines goes too (heading, `{{gray}}`, empty
  `{{columns-list}}`), and so does a `{{Species table}}` with no rows left, unless a missing taxon
  goes in it: a missing taxon whose neighbours are all taken out goes where the first of them was.
  A text put in inside a removed span would be lost, so that taxon stays in the copy box
  (`UnplacedReason.RemovedLine`). The updater's own edits inside a removed line are dropped
  (`TextRemoval.Owned`), and the edit summary adds "16 species in other categories removed" ("taxa" when a subspecies or variety is among them).
- The list rebuilt (`Update/ListRebuild.cs`; field `rebuild`, a button under the comparison, then a
  ticked checkbox; only for a text whose taxa are mostly on list lines, refused for a list that may be
  partial unless `anyway` is on, and above `GroupList.MaxLines` taxa). Every taxon the comparison
  counts in the group (`ListScope.ComparedTaxa`: the group's taxa in the list's categories, of the area
  or region when the comparison is of one, subspecies and varieties when they were compared) gets a
  line, and the taxa now in another category are left out as with `rmall` (the same checkboxes and
  `KeptReason` rules; a rebuild starts with all of them ticked). The sections come from
  `ListSections`. With headings as in the wikitext (the default), a listed taxon stays in its section
  while the section's group holds it; a missing species goes into the section of the line of its
  genus that sorts just before it (so into the right "===C===" of a list in sections by letter); a
  taxon with no line of its genus, or one IUCN has moved, goes where `ListPlacement` would put it
  (deepest section, an "Other ..." section, the section's own lines of several groups, or a new
  section placed as in placement). A new heading's `{{gray}}` line and bracketed English name come
  only from the rules files' plural names (`GroupList.HasSentenceName`); a name from a Wikipedia
  title is singular ("Hummingbird"). A taxon's line is the first line that
  writes its own name (a synonym line comes second); another line writing the same name is a
  duplicate and goes (`DuplicateLines`) unless it defines a used reference; a line writing another
  name stays where it is. A line under another taxon's line (a subspecies under its species) goes
  with it. Each section is read once (`Read`): text before the list, list blocks by part (a part
  starts at a label such as `'''Subspecies'''`), the part's layout template (opening and closing as
  written, on the first or last line or on lines of their own), and text after the list. A section
  keeps its heading line, its text before and after the list and its template; its lines keep their
  wording with the updater's changes (`StatusUpdater.TextWithin`). Lines that name no taxon of the
  list stay in their section and part (`OtherLines`). A section of a group with no line left is
  dropped and its other text listed (`Dropped`). Sections of no group (See also, References) and the
  text before the first heading stay as they are. Lines go in the order the section keeps, found from
  its old lines (scientific names without "×" or "†", or common names; one pair in ten may be out of
  order); in no order, the old lines keep theirs and the others go after them, unless that whole
  arrangement reads as sorted, in which case it is sorted, so that rebuilding the rebuilt text changes
  nothing (checked on five Wikipedia lists). A new section's lines follow the order of the sections
  they came from, and its layout template is that of the section its first old line came from.
  `RebuildOptions` (form fields named as on the group pages where they mean the same: `hmode` and
  `h`, `wording`, `style`, `sort`, `order`, `infra`): headings for chosen ranks in place of the text's
  (in IUCN's order; a heading keeps the heading line, re-levelled, and text of the text's section of
  its group; the other sections of groups go and their other lines go to the heading of the nearest
  group; a heading with no section to copy is written like a section of the same rank, else as
  `GroupList.HeadingText`); lines written anew in a chosen style (lines under them kept); a chosen
  order of lines; sections in IUCN's order; subspecies and varieties left out, listed after the
  species under a `'''Subspecies'''` label, or under their species. The rebuilt list is a tree of
  `OutNode`s for both modes. In October 2026, List of endangered birds gave 33 taxa added, 17 removed
  and 3 new headings; List of vulnerable mammals 171 added, 4 moved, 128 removed; List of Carex
  species kept its 2,073 lines of species IUCN has not assessed in place.
- Catalogue of Life species (form field `extra`, off by default; shown for a list of every
  category): the species of the Catalogue of Life in the group that are not in the IUCN Red List
  (`extra_species` with a CoL id, less likely IUCN duplicates; the species only on Wikidata are left
  out because most are fossil species), less every name the wikitext writes, are listed in their
  own box with no `{{IUCN status}}`, and put into the list with the missing IUCN taxa when
  `addmissing` is on. The group choice shows how many of each group's species the wikitext
  includes.
- More options for added statuses: `addend` puts a status added to a list line at the end of the
  line, before references and footnotes at the end; `addrefs` adds a reference after each added
  status (the named reference the text defines for the latest assessment, else `{{cite iucn}}` or
  `{{cite Q}}` in a `<ref>`); `colhead` is the heading of an added column (IUCN status, Conservation
  status, Red List status or Status); and each table that gets a column has a checkbox in the
  report (`cols`, with `colsshown` holding a key of the pasted text, so a new text starts with every
  table). A status added after a name also goes after a plain-text authority ("Kosterm.",
  "(C.K.Allen) Kosterm.", "Brown & Wright, 1978"). Status cells with references after the code
  ("VU<ref name=a/>") are status cells: the code is read and replaced without the references.
- Round-trip check: `BeastieBot3.Site.Tests/browser/update-roundtrip.py <folder>` sends each page in
  a folder of wikitext to a running site twice, with two sets of options, in LF and CRLF, and
  reports any page whose second run changes the text (a second run must find every added status up
  to date and every added taxon listed), whose CRLF output has a bare LF, or that takes over 1.5 s.
  Run it against `site-preview-nolimit` (no update rate limit) after changing `Update/`. In October
  2026 it passed on 12 Wikipedia lists and 51 cached articles.
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

## Reasons for category changes (`iucn summary-tables`)

IUCN records why a taxon's Red List category changed, but neither the Red List API (checked on
2026 payloads: no field for it) nor the CSV export has it. Assessors code it in SIS as genuine
(recent, or since the first assessment) or non-genuine (new information, knowledge of the criteria,
incorrect data used previously, taxonomy, criteria revision, other), or no change (BGCI's
2026 reassessment guidelines list the options). IUCN publishes only a summary: Table 7 of each
release's summary statistics, "Species changing IUCN Red List Status", with a reason code for each
change, G (genuine), N (non-genuine) or E (the previous listing was an error, from 2014-2). The
2008 table lists only genuine changes, in "Genuine improvements" and "Genuine deteriorations"
sections. Table 9, "Possibly Extinct and Possibly Extinct in the Wild Species" (2014-1 to 2020-2),
lists every species tagged PE or PEW in the release, with the year of its first such assessment
and the date last recorded in the wild.

`rules/iucn-summary-tables.yml` lists the files. IUCN's summary statistics page links the current
Table 7 and a zip of the end-of-year Table 7s; the zip is behind a Cloudflare check, but each PDF in
it downloads with a plain request. The tables of the other releases of each year and every Table 9
were found in the Internet Archive's list of files on `nc.iucnredlist.org` and
`cmsdocs.s3.amazonaws.com`; three (2010-3, 2011-1 and 2013-1) exist only in the Internet Archive.
Each release's Table 7 includes the changes of the earlier releases of its year, with the version
each was first published in (from 2010-4; 2010-3 has no version column), so the end-of-year tables
alone cover nearly everything; the others add changes that a later release of the same year
changed again, and corrections (`2020-2_RL_Stats_Table7_corrected.pdf`,
`2024-1_RL_Table_7_corrected_20240916.pdf`). A table later in the list wins when two give one change
different reasons.

`site build-db` links the rows (`SiteSummaryTables`): the taxon by scientific name, else by IUCN
synonym, in the kingdom of the row's group heading when it names one; the assessment is the
taxon's global assessment with the new category published in the version's year (else the year
after: Bos javanicus's 2024-2 change is on its amended assessment published in 2025), the first one
whose previous global assessment has another category. LR/nt counts as NT and LR/lc as LC, as
Table 7's legend says. On release 2026-1, 24,745 of 25,027 rows link and 16,224 assessments get a
reason. Most of the 262 that do not link name a species whose earlier assessments are not in the
IUCN API's history (Heloderma horridum has only its 2021 assessment there), or a name that matches
no taxon (41).

Possibly Extinct: in the IUCN API, `possibly_extinct` is set on CR assessments published as early as 2000,
before the first of these tables. Of the 1,369 assessments the tables list as PE
or PEW, 37 have no tag in their own record; `possibly_extinct_listing` keeps them all and the site
marks only those 37. Table 7 prints "CR(PE)" for some tagged assessments and plain "CR" for others
(in the 2023-1 table, 3 against 27), so a plain "CR" in Table 7 says nothing about the tag.

On the species page (`Pages/HistoryTableNotes.cs`, `_ReasonCell`, `_FootnoteRef`,
`_HistoryTableNotes`; strings in `Display/SiteText.SummaryTables.cs`), the Assessment history and
Combined assessment history tables get a "Reason for change" column after Category when at least
one row has a reason. A row with a reason shows IUCN's name for the code ("Genuine status change
(G)", "Non-genuine status change (N)", "Previous listing was an error (E)") and a numbered footnote
that links the Table 7 PDF it is from (one footnote per table). The other rows say why there is
none, in light italic: "—" for an assessment published before 2007 (a line under the table
says that reason for change reports were not published before 2007), "first assessment" for the oldest row, "no change" when the category is the
same as the row below it (LR/nt counts as NT and LR/lc as LC), "no reason given" with the 2008
table's footnote for a change published in 2008 (that table lists genuine changes only), and "not
found" for any other change. On release 2026-1, over the pages with the column, that is 22,202
dashes, 16,224 reasons, 10,198 "no change", 4,352 "first assessment", 459 "no reason given" and
125 "not found". An assessment that the tables list as PE or PEW and that is flagged as neither
(IUCN's word: Table 9 says assessments are "flagged as 'Possibly Extinct' (PE)") shows the badge
CR (PE) or CR (PEW) with a footnote that names the tables and versions; an assessment flagged with
the other tag keeps its own badge and gets the footnote. The About page lists the tables as a
source (meta keys `table7_first_version` and so on) and explains both.

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
  and in the lists at once. `site report-group-names` lists the groups with no English name,
  largest first, with candidate names (English Wikipedia redirects to the group's article and the
  Catalogue of Life's English names) and a `rules-list.txt` line for each; the first 42 were added
  in October 2026.

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
