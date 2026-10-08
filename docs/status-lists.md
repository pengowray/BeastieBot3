# Status lists: conservation statuses from other systems

The status lists store holds conservation statuses from systems other than the IUCN Red List, for the public species site (`site build-db` reads it into `other_status`; see `docs/public-site.md`):

- NatureServe Explorer: the NatureServe global rank (G rank), the national and subnational (state, province and territory) ranks in the United States and Canada, and the US Endangered Species Act, COSEWIC and SARA statuses that NatureServe records (`statuses natureserve-fetch`).
- ECOS, the US Fish and Wildlife Service's Environmental Conservation Online System: the list of species, subspecies and populations listed under the US Endangered Species Act (`statuses ecos-import`).
- The New Zealand Threat Classification System database (nztcs.org.nz, Department of Conservation, CC BY 4.0): the current assessments (`statuses nztcs-import`).
- SALVE (salve.icmbio.gov.br), ICMBio's system for the national assessments of the extinction risk of Brazil's fauna: the current assessment of each species and subspecies (`statuses salve-import`).
- JNCC's Conservation Designations for UK Taxa (Joint Nature Conservation Committee, Open Government Licence v3.0): one row per taxon and designation, for the GB and England red lists, Birds of Conservation Concern, Nationally Rare and Scarce, the UK and country priority species lists, the Wildlife and Countryside Act and other UK legislation, and the international conventions and EU directives as they apply to UK taxa (`statuses jncc-import`).
- The Checklist of CITES Species (checklist.cites.org, compiled by UNEP-WCMC for the CITES Secretariat): the current CITES Appendix listings of every taxon in the Appendices, with the listings each taxon inherits from a higher taxon (`statuses cites-import`).
- National and subnational red lists published on GBIF as Darwin Core checklists: 29 lists of 16 countries, chosen in `rules/status-lists/national-red-lists.yml` (`statuses red-lists-import`).
- The Red List of Japan's Ministry of the Environment (環境省, Public Data License 1.0): one row per taxon or threatened local population, from the 5th Red List (birds, reptiles, amphibians, vascular plants, bryophytes, algae, lichens, fungi) and the Red List 2020 (mammals, brackish and freshwater fishes, insects, molluscs, other invertebrates) (`statuses japan-import`).
- PatriNat's BDC Statuts (base of species statuses in France, Licence Ouverte 2.0): the French national red list (Liste rouge nationale, by UICN France, MNHN and OFB) of metropolitan France and the overseas territories, the regional red lists, and the national, overseas, regional and departmental protection lists, with each listed taxon's TAXREF names and its IUCN, BirdLife, Catalogue of Life and GBIF ids from TAXREF (`statuses france-import`).

Code: `BeastieBot3/StatusLists/`. The schema is `StatusListStore.Ddl`, with a comment on every column.

The seven imports (`statuses ecos-import`, `nztcs-import`, `salve-import`, `jncc-import`, `cites-import`, `japan-import` and `france-import`) share one run, `StatusListImport.RunAsync`. It imports the file given with `--file`, or downloads the source into the status lists folder (written as a `.part` file and renamed when complete), as `<stem>-<yyyy-MM-dd>.<extension>` or, when the spec has `FindFileName`, under the name the source gives its file. It then reads the file, stops without changing the store when the file has no rows, replaces the source's rows and its `status_source` row in one transaction, and prints a table of counts. Each command passes a `StatusListImportSpec` with its download, reader, store method and messages, and `StatusListDownload` creates the HTTP client and writes the downloaded files. A spec's `SourceForFile` changes the `status_source` row for the file imported (JNCC uses it for the file's URL and the year in its attribution). The store's methods for each source are in `StatusListStore.NatureServe.cs`, `StatusListStore.Ecos.cs`, `StatusListStore.Nztcs.cs`, `StatusListStore.Salve.cs`, `StatusListStore.Jncc.cs`, `StatusListStore.Cites.cs` and `StatusListStore.RedLists.cs`. `statuses red-lists-import` imports many datasets in one run, so it has its own run (see its section).

## Files

| What | Where |
| --- | --- |
| Store | `Datastore:status_lists_sqlite` in paths.ini, else `status_lists.sqlite` in the datastore folder (`PathsService.GetStatusListsPath`, `ResolveStatusListsPath`) |
| Downloaded files | `Datasets:status_lists_dir`, else a `status-lists` folder in the datastore folder (`GetStatusListsDownloadDir`) |
| Red list archives | the `red-lists` folder in the downloaded files folder, as `<key>-<yyyy-MM-dd>.zip` |
| List of red lists | `rules/status-lists/national-red-lists.yml` (`RedListManifest`, read from `RulesPaths.Resolve(paths).SourceRulesDir`) |

## Tables

| Table | One row per |
| --- | --- |
| `status_source` | source (`natureserve`, `ecos`, `nztcs`, `salve`, `jncc`, `cites`, `japan`, `france`, `taxref`, and `redlist:<key>` for each red list): title, URL, licence, citation with the access date, version, when it was last fetched, row count. The site's credits can be built from it. |
| `status_sync_state` | key of the NatureServe download's progress (`natureserve_pass_*`, `natureserve_completed*`) |
| `natureserve_species` | NatureServe record (`element_global_id`): a species, subspecies, variety or population |
| `natureserve_synonym` | synonym NatureServe lists for a record |
| `natureserve_nation` | national rank of a record (`element_global_id`, `nation_code`: US or CA) |
| `natureserve_subnation` | subnational rank of a record in a US state or a Canadian province or territory (`element_global_id`, `nation_code`, `subnation_code`) |
| `natureserve_partition` | name prefix of the NatureServe download under way, with the next page to ask for. Empty between downloads. |
| `ecos_listing` | ESA listing (`entity_id`, the ECOS Listed Species ID) |
| `ecos_name` | name an ECOS listing's scientific name gives, the main name included |
| `nztcs_assessment` | current NZTCS assessment (`assessment_id`), with the scientific name `NztcsApi.ChooseName` gives |
| `salve_assessment` | current SALVE assessment of a species or subspecies (`ficha_id`, SALVE's sheet id) |
| `jncc_designation` | row of the Master List sheet of JNCC's Conservation Designations for UK Taxa: one taxon and one of its designations (`row_number`, the row's number in the sheet) |
| `cites_taxon` | taxon of the Checklist of CITES Species (`taxon_concept_id`, Species+'s id): a species, subspecies, variety or higher taxon, with Species+'s summary of its listing (`current_listing`) |
| `cites_listing` | current CITES listing of a taxon (`taxon_concept_id`, `listing_change_id`): its own, or inherited from a higher taxon |
| `cites_note` | long note of the CITES listings (a full note, the text of an annotation such as #4), stored once and referred to by id |
| `cites_synonym` | synonym the Checklist gives for a taxon, with and without its author |
| `red_list_dataset` | national or subnational red list from GBIF (`dataset_key`, the key in `national-red-lists.yml`) |
| `red_list_taxon` | status of a taxon in one of those lists (`dataset_key`, `taxon_id`, `seq`) |
| `red_list_synonym` | synonym a list gives for one of its taxa with a status (`dataset_key`, `taxon_id`) |
| `japan_listing` | taxon or threatened local population in the latest Red List of Japan's Ministry of the Environment for its group (`row_id`, the row's place in the import) |
| `france_status` | row of the BDC Statuts of a stored status type: a taxon (`cd_nom`, the TAXREF id of the name the list used, and `cd_ref`, the id of its accepted name), its status in one list and one place (`row_number`, the row's number in the BDC's CSV) |
| `france_status_type` | status type of the BDC (`type_code`: LRN, LRR, PN ...), stored or not |
| `france_territory` | place of the stored statuses (`territory_code`, the BDC's CD_SIG), with its English name |
| `france_document` | document that gives the stored statuses (`cd_doc`): a red list chapter, a regional red list, a decree; its year, title and citation |
| `france_taxref_name` | TAXREF name (`cd_nom`) whose accepted name is the `cd_ref` of a stored status: the accepted name, its synonyms and the other names under it |
| `france_taxref_link` | id of one of those names in the IUCN Red List, BirdLife, the Catalogue of Life or GBIF, from TAXREF |

## NatureServe Explorer: `statuses natureserve-fetch`

Licence: CC BY 4.0. Citation form (stored in `status_source.citation` with the date the last download finished):

> NatureServe. 2026. NatureServe Explorer [web application]. NatureServe, Arlington, Virginia. Available https://explorer.natureserve.org/. (Accessed: October 8, 2026).

The command asks the species search (`POST https://explorer.natureserve.org/api/data/speciesSearch`, documented at https://explorer.natureserve.org/api-docs/) for 100 records a page, one request at a time, at least 0.5 seconds apart. It tries a request again after 5, 15, 30, 60 and 120 seconds when the answer is 429, 408, a 5xx or a network error, then stops; the next run carries on.

### Why it goes by name prefix

The search answers HTTP 500 for any page past its first 10,000 records (page 100 at 100 records a page), so one query cannot reach all 113,530 records (October 2026). The command asks for one scientific name prefix at a time with a `textSearch` `startsWith` on `scientificName`:

- It starts with every record. When a query has more than 10,000 records, the command stores the first page and replaces the query with longer prefixes: "A" to "Z" for every record, then, for example, "Aa" to "Az" for "A". In October 2026 A (11,822 records), C (14,955) and P (15,302) were split; S (9,413) was not.
- Checked on 2026-10-08: the prefix is a prefix of the whole name, case does not matter, the counts of "A" to "Z" add up to the total, and the counts of "Aa" to "Az" add up to the count of "A". Within one query the order is fixed (informal group, then name): pages do not overlap, and the same page asked twice gives the same records.
- Each page is stored with its prefix's progress in one transaction, so a stopped run carries on with the next page.

The first full download (2026-10-08) asked about 101 prefixes (29 had no records) in 1,212 requests, took 11 minutes with no failed requests, and stored 113,530 records, the same number as NatureServe's total.

### Full downloads and refreshes

| Run | What it does |
| --- | --- |
| First run, or `--restart` | Full download of every record. |
| A download under way | Carries on from the next page. |
| After a finished download | Nothing, unless `--refresh-days N` and the last download finished more than N days ago. |
| `--refresh-days N` | A refresh: only the records modified since the last download started, less an hour (the search's `modifiedSince`), then the records NatureServe unpublished since then (`POST /api/data/unpublishedTaxa`), which it deletes. |
| `--limit N` | Stops after N page requests. |
| `--status` | Prints the progress and the number of records with national and with state or province ranks, and sends no requests. |

When a full download finishes, it deletes the stored records it did not see, but only when it stored at least as many records as NatureServe gave as its total. When it stored fewer, it keeps the old records and says so, because a record that paging missed is not a deleted record.

NatureServe last modified 113,413 of the 113,530 records on 2026-10-02 or 2026-10-03, so a refresh after a bulk update by NatureServe is close to a full download.

### What is stored

Per record: `element_global_id`, `unique_id`, `elcode`, `scientific_name`, the primary common name and its language, `g_rank` (as published, for example G3G4, G2T1, G3TNRQ) and `rounded_g_rank`, `classification_status`, kingdom to genus, `informal_taxonomy`, `infraspecies`, `usesa_code`, `cosewic_code`, `sara_code` (the English part of NatureServe's bilingual SARA status, "Endangered" from "Endangered/En voie de disparition") and `sara_code_raw`, `us_n_rank` and `ca_n_rank` (the rounded national ranks of the US and Canada), `nsx_url`, `last_modified`, `fetched_at`, the synonyms, and the national and subnational ranks (see "National and subnational ranks" below).

Not stored: taxonomic comments and every other narrative text, other common names, and distribution. The national and subnational ranks as published (`nrank`, `srank`, not rounded), the year each was last reviewed, and the names of the nations and subnations are not stored either: the species search does not give them. They are only in each record's own API page (`GET https://explorer.natureserve.org/api/data/taxon/<unique_id>`), which would take one request per record.

Values in the first full download (2026-10-08, 113,530 records: 66,070 animals, 32,888 plants, 14,572 fungi):

| Column | Values |
| --- | --- |
| `rounded_g_rank` | GNR 58,705; G5 17,108; G4 9,045; G3 5,547; G1 4,114; G2 3,650; GNA 1,010; GU 828; GH 404; GX 203; and for the 12,916 subspecies, varieties and populations, T ranks: TNR 4,234, T5 2,325, T4 1,997, T3 1,794, T2 1,190, T1 1,039, TU 185, TH 62, TX 58, TNA 32 |
| `g_rank` | as published: G5, G4G5 (a range), G5T5, G5TNR, G3G4T2Q; 9,198 are ranges, 3,630 contain "?" |
| `usesa_code` (2,089 records) | E 1,210; T 369; UR 276; DL 99; "E, XN" 36; PE 30; PT 23; SAT 8; "T, XN" 8; "E, T" 7; "E, PDL" 6; C 4; others under 6. A record with several codes has them joined by ", ". |
| `cosewic_code` (1,175) | E 379; SC 269; T 203; NAR 201; DD 63; X 25; XT 21; Non-active/Nonactive 14 |
| `sara_code` (672) | Endangered 296; Special Concern 207; Threatened 146; Extirpated 23 |
| `us_n_rank` | NNR 46,031; none 33,757; then NNA, N5, N3, N1, N4, N2; migratory birds have ranks such as N5B,N5N |
| `ca_n_rank` | none 48,025; NNR 19,217; NU 18,107; then N5, N4, NNA, N3 |
| `classification_status` | Standard 111,530; Provisional 1,959; Nonstandard 41 |
| `primary_common_name_language` | EN 112,965; HAW 373; ES 184; OTHER 8 |

Scientific names, as NatureServe writes them:

- an animal subspecies is a trinomial without a rank word: "Lithobates areolatus circulosus" (3,880 records); 16 animal records have "ssp." and a number for an undescribed subspecies ("Oncorhynchus virginalis ssp. 1");
- a plant or fungus subspecies or variety has "ssp." or "var.": "Atriplex cordulata var. cordulata" (5,496 var., 2,938 ssp.);
- a population has "pop." and a number: "Ambystoma californiense pop. 1" (583 records), with a T rank such as G3TNRQ;
- an undescribed species has "sp." and a number: "Gasterosteus sp. 1" (47 records);
- a hybrid has "x" before the epithet, "Physalis x elliottii" (1,001 records), or before the genus, "x Agropogon littoralis" (37 records, among the names starting with X);
- names of species not yet described have "cf.", "aff.", "nr.", "clade" or "complex": "Steiroxys cf. trilineata", "Plethodon websteri clade A", "Hellinsia stramineus complex";
- other forms: "Ambystoma pop. 3", "Salmo salar (landlocked)", "Argynnis zerene myrtleae sensu lato", "Physalis x elliottii nothovar. elliottii", "Cortinarius grosmorneënsis".

238 scientific names belong to two or more records, nearly all a Standard record and a Provisional (224) or Nonstandard (12) record with the same name, such as Abies lasiocarpa (144815 Standard, 137855 Provisional). Matching to IUCN names should prefer the Standard record. One name is used in two kingdoms: Pilophorus clavatus, a fungus (121859) and an animal (907882).

### National and subnational ranks

Each search result has `nations[]`, one object per nation with `nationCode`, `roundedNRank`, `native`, `exotic` and `subnations[]`, and each subnation object has `subnationCode`, `roundedSRank`, `native` and `exotic`. The search gives no other fields for nations and subnations (checked 2026-10-08 with Ambystoma californiense, Haliaeetus leucocephalus, Danaus plexippus and Puma concolor). The command stores these values unchanged, with `native` and `exotic` as 1 or 0, in two tables:

| Table | Columns |
| --- | --- |
| `natureserve_nation` | `element_global_id`, `nation_code`, `rounded_n_rank`, `native`, `exotic` |
| `natureserve_subnation` | `element_global_id`, `nation_code`, `subnation_code`, `rounded_s_rank`, `native`, `exotic` |

- `native` and `exotic` are NULL when the search leaves the field out. Both are 1 for a taxon that is native in one part of the nation or subnation and exotic (introduced) in another.
- A rank can be several ranks joined by a comma, one each for the breeding (B), nonbreeding (N) and migrant (M) populations: `N5B,N5N`; `N3B,NUM`; `S4B,S5N,S4M`; `S2N,SXB`. `NNRB` means that the breeding population is not ranked, and `NNRN` that the nonbreeding population is not ranked.
- Other rank codes: NNR and SNR not ranked, NU and SU unrankable, NNA and SNA not applicable (usually an exotic taxon), NH and SH possibly extirpated, NX and SX presumed extirpated.
- Only the nation codes US and CA appeared in `nations[]` in the test run below and the four species above, in either order. The subnation codes are NatureServe's own, and the tables key them with the nation code (CA under US is California). Under US: the 50 states, DC and NN (the Navajo Nation). Under CA: AB, BC, MB, NB, NS, NT, NU, ON, PE, QC, SK and YT, with Newfoundland and Labrador as two codes, NF (the island of Newfoundland) and LB (Labrador). The search gives no names, so the public site has to map the codes to names.
- When a record is stored, its rows in both tables are deleted and written again, in the transaction that stores the record. They are deleted with the record (ON DELETE CASCADE). A record whose `nations[]` is empty has no rows.
- A nation object or subnation object without a code is left out. When one record has two nations with the same code, or one nation has two subnations with the same code, only the first in the array is stored.

Counts from a test run of 20 pages of 100 records on 2026-10-08 (1,611 records: the first 100 records of the whole search, the first 100 of the names starting A, every name starting Aa, Ab or Ac, and 100 starting Ad; a record read twice counts once):

| What is counted | Count |
| --- | --- |
| Records with at least one `natureserve_nation` row (NNR included) | 1,591; the other 20 records have an empty `nations[]` |
| Records with at least one `natureserve_subnation` row | 1,432 |
| `natureserve_nation` rows by `nation_code` | US 1,187; CA 911; total 2,098 |
| `natureserve_nation` rows by `rounded_n_rank` | NNR 983; NU 233; N5 229; N4 189; NNA 164; N3 100; N1 76; N2 73; NX 21; NH 18; rows with breeding, nonbreeding or migrant ranks 12 (`N5B,N5N` 5; `N3N` 2; `N2B,NUN,NUM` 1 ...) |
| `natureserve_subnation` rows by `rounded_s_rank` (8,141 rows, 54 values) | SNR 3,402; SU 1,145; SNA 868; S4 731; S5 623; S3 504; S1 351; S2 283; SH 63; SX 36; rows with breeding, nonbreeding or migrant ranks 135 (`S4B` 15; `SNRB` 11; `S5B` 11; `S3B,S4N` 10 ...) |
| `natureserve_nation` rows by `native`, `exotic` | native only 1,926; exotic only 154; both 18; no row had neither |
| `natureserve_subnation` rows by `native`, `exotic` | native only 7,342; exotic only 793; both 6; no row had neither |

The `natureserve_species` columns `us_n_rank` and `ca_n_rank` are filled as before. They come from the first US object and the first CA object in `nations[]`, so they match the record's US and CA rows in `natureserve_nation` (they did for every record in the test run).

#### A full download is needed once

A store whose records were downloaded by an earlier version of the command has no rows in `natureserve_nation` or `natureserve_subnation`. A refresh (`--refresh-days`) adds rows only for the records that NatureServe changed. To add rows for every record, run `statuses natureserve-fetch --restart` once (about 15 minutes). The old records are kept until the new download finishes. A run without `--restart` does not start a full download.

The status table, which `--status` prints (a run also prints it when a download finishes, when it stops at `--limit`, and when there is nothing to do), then has the row "National, state and province ranks", which says to run the command once with `--restart`. The table shows that row while `NatureServePlan.NeedsFullDownloadForFields` is true:

- Each pass (one full download or one refresh, which can take several runs) writes the number `NatureServePlan.FieldsVersion` (2 since the ranks were added) to the `status_sync_state` key `natureserve_pass_fields_version` when it starts.
- When a full download finishes, the command copies that number to the key `natureserve_full_completed_fields_version`.
- `NeedsFullDownloadForFields` is true when the store has records, `natureserve_full_completed_fields_version` is missing or less than `FieldsVersion`, and no full download that started with the current `FieldsVersion` is under way.

When a later change makes the command store another field, increase `FieldsVersion`.

## ECOS: `statuses ecos-import`

Licence: public domain (work of the US federal government).

The command downloads the ECOS "pullreports" species report as CSV, filtered to listed species (`status_category = 'Listed'`), with more columns than the default export: `id`, `sid`, `dps`, `is_foreign`, `country` and `gn` from the species table, and `tsn`, `kingdom`, `family` and `group` from its taxonomy table. The report's column list is at `https://ecos.fws.gov/ecp/pullreports/catalog/species/report/species` (ask for JSON). It keeps the file as `ecos-listed-species-<yyyy-MM-dd>.csv` in the status lists folder (a second run on the same day replaces that day's file), and replaces every ECOS row in the store in one transaction. `--file` imports a file already downloaded.

The number at the end of a species page URL (`https://ecos.fws.gov/ecp/species/1470`) is the ECOS Species ID, and a species listed as several populations has several listings with that ID (Lampsilis virescens is listed as Endangered and as an experimental population). The table's key is the ECOS Listed Species ID (`entity_id`); the species page's ID is `species_id`.

In October 2026: 2,478 listings of 2,327 species pages; 1,866 Endangered, 519 Threatened, 77 "Experimental Population, Non-Essential" and 16 "Similarity of Appearance (Threatened)". 195 listings have the common name "No common name", stored as NULL; 27 have no listing date. One listing has kingdom UNKNOWN and group Algae (Isogomphodon oxyrhynchus, a shark), stored as ECOS gives it.

### Names in brackets

ECOS writes earlier names in brackets after the word they replace. `EcosScientificName` (pinned by `EcosScientificNameTests`) reads them; `ecos_listing.scientific_name` is the name without brackets, and `ecos_name` has every name:

| ECOS name | Names |
| --- | --- |
| Papasula (=Sula) abbotti | Papasula abbotti, Sula abbotti |
| Harrisia (=Cereus) aboriginum (=gracilis) | Harrisia aboriginum, Cereus aboriginum, Harrisia gracilis, Cereus gracilis |
| Pediocactus (=Echinocactus,=Utahia) sileri | Pediocactus sileri, Echinocactus sileri, Utahia sileri |
| Hemileuca maia menyanthevora (=H. iroquois) | Hemileuca maia menyanthevora, Hemileuca iroquois |
| Andrias japonicus (=davidianus j.) | Andrias japonicus, Andrias davidianus japonicus |
| Icaricia (Plebejus) shasta charlestonensis | Icaricia shasta charlestonensis, Plebejus shasta charlestonensis |
| Avahi laniger (entire genus) | Avahi laniger, with the note "entire genus" in `name_note` |

The rules are mechanical, so a few names are not real combinations: "Otus magicus (=insularis) insularis" also gives "Otus insularis insularis" (the earlier name was the species Otus insularis). Rank words ("ssp.", "var.", "spp.") are kept as written.

## NZTCS: `statuses nztcs-import`

The NZTCS website's search pages call two JSON endpoints that answer without a login: `POST
/rest/assessmentSearch` (with `reportEditStatusList: ["PUBLISHED"]`, `reportPublishedStatusList:
["CURRENT"]`) and `POST /rest/species/findByCriteria`. Both page from `pageNumber` 1, and 1,000 rows
a page works: on 2026-10-08, 17 pages of assessments (16,331) and 23 of species records (22,633),
in under a minute. The command keeps both lists in `nztcs-<date>.json` in the downloads folder
(`--file` imports a kept file) and replaces every stored assessment in one transaction.

An assessment's name is HTML with the authority ("<i>Apteryx haastii</i> Potts, 1872") and has no
kingdom. Its species record has a plain scientific name, but it can be out of date: the record
that holds the current assessment of *Apteryx australis australis* is named *Apteryx australis
lawryi*. `NztcsApi.ChooseName` decides the name stored:

- an informal name in the title (quotes, "aff.", "cf.", "sp.", "nr.") gets none, so an undescribed
  or informal taxon ("Apteryx australis \"southern Fiordland\"", "Aciphylla aff. glaucescens")
  never takes the status of the species it is named after (1,557 assessments);
- the record's name, when the title gives the same name or only the start of it;
- otherwise the name read from the title (`NameFromTitle`: genus, epithet and an infraspecific
  epithet with its rank; none when a rank comes after the authority).

Nothing narrative is stored: the species records' notes, descriptions and habitat fields are not
downloaded (the species search returns only id, name and authority), and the assessments' text
fields are left out.

## SALVE: `statuses salve-import`

SALVE's public search, `GET /salve-api/public/search`, gives one row per species or subspecies
with its current assessment: the name as HTML (`<i>Aaptos glutinans</i>&nbsp;<span>Moraes,
2011</span>`), the category code, a "possibly extinct" flag, the criteria, the end date of the
assessment and its DOI (10.37002/salve.ficha...). With no filter it answers 500, so the command
asks for every category id of `/salve-api/public/selectOptions` (`categoriaIds=127,...,136`, EX to
NA). Pages are at most 500 rows (`paginationPageSize`), numbered from 1 (`paginationPageNumber`);
with a filter the answer has no total, so the command reads pages until one is short. On
2026-10-08: 15,409 rows in 31 pages (LC 12,170, DD 1,194, VU 508, EN 508, NT 443, CR 361 of them 57
possibly extinct, NA 215, EX 6, RE 3, EW 1), 14,366 with a DOI. The rows are kept in
`salve-<date>.json`.

ICMBio's data policy for the assessments (Instrução Normativa 05/2017) makes them public once the
category is validated and asks that authorship and source be cited; the site cites SALVE in its
own form ("ICMBio, 2026. Sistema de Avaliação do Risco de Extinção da Biodiversidade – SALVE...")
and links each assessment's DOI. Precise localities can be restricted under that policy; the store
holds no places, states or biomes. SALVE's numbers do not always match the official list of
threatened species (Portaria MMA 148/2022), as SALVE's own home page says. SALVE covers animals
only: no bulk source for the national assessments of Brazil's plants (CNCFlora) was found.

## JNCC: `statuses jncc-import`

Source: Conservation Designations for UK Taxa, published by the Joint Nature Conservation Committee
at https://jncc.gov.uk/resources/478f7160-967b-4366-acdf-8941fd33850b. It is an Excel workbook
(8.1 MB) whose "Master List" sheet has one row per taxon and designation. JNCC collates the lists
from their sources and matches every name to the recommended name in the UK Species Inventory
(UKSI), kept by the Natural History Museum.

Licence: Open Government Licence v3.0. The resource page asks for this attribution statement, with
the year of the release:

> Contains JNCC/NE/NRW/NatureScot/NIEA data © copyright and database right 2026

`status_source.citation` holds that statement with the year of the date in the file's name,
`status_source.url` the URL of the file and `status_source.version` the file's name.

### How the file is found

The workbook's name has the date of its release (`taxon-designations-20260609.xlsx` on
2026-10-08) and changes with every release. The command reads the resource page and takes the link
to `taxon-designations-<yyyyMMdd>.xlsx` with the newest date (`JnccDesignations.FindSpreadsheetLink`;
if no name has a date, the first `.xlsx` in the resource's folder on data.jncc.gov.uk). It keeps the
file in the status lists folder under JNCC's name. When the folder already has a file of that name,
the command reads that file again and does not download it. No JNCC API for the resource was found.
JNCC publishes no CSV of the workbook. The page's other downloads are a zip file of the same
workbook with two PDFs of guidance, the two PDFs, and a link to the "GB Red List Dataset", a
separate workbook of the red lists only (`gb-red-list-data-20260609.xlsx`). `--file` imports a workbook already downloaded; the year in the
attribution then comes from the date in its name, else from the year the command runs.

The workbook is read with ExcelDataReader (MIT licence). Its default settings need the Windows-1252
code page, so the reader registers .NET's `CodePagesEncodingProvider` first.

### Columns of the Master List

The 2026-06-09 file has 27,152 rows of 15,211 taxa (distinct taxon version keys); the workbook's
own pivot table also counts 27,152 rows, in 20 columns, with a line of text above the column headings. `JnccDesignations.Read` finds
the heading row by the heading "Recommended taxon version" and reads the columns by their headings.

| JNCC column | Stored as |
| --- | --- |
| Category | `category`: Bird, Mammal, Fish, Reptile, Amphibian, Invertebrate, Vascular plant, Non-vascular plant, Fungi, Algae, Slime mould |
| Taxon group | `taxon_group`: UKSI's informal group, such as "insect - beetle (Coleoptera)", "lichen" |
| Recommended taxon name, authority, qualifier | `scientific_name`, `authority`, `qualifier` ("s.l.", "agg.", "sensu stricto"; 239 rows) |
| Recommended taxon version | `taxon_version_key`: the UKSI key (NBNSYS..., NHMSYS..., BMSSYS...) |
| Designated name | `designated_name`: the name the source published; it differs from the recommended name in 5,232 rows |
| Common name | `common_name`; empty in 16,037 rows |
| Source, URL source | `source` (the document), `source_url` |
| Date designated | `designated_on` (yyyy-MM-dd); often 1 January of the year of the source |
| Reporting category | `reporting_category`: the list, such as "Wildlife and Countryside Act 1981" |
| Designation | `designation`: the schedule, annex or category, such as "Schedule 5 Section 9.4b", "Vulnerable" |
| Designation abbreviation | `designation_code`, such as WACA-Sch5_sect9.4b, RedList_GB_post2001-VU |
| IUCN version | `iucn_version`: 2001, 1994 or "pre 1994" |
| Reporting category sort order | `sort_code`: A, C, C1 ... M |
| Source description, designation description | not stored: a description of each list, up to 2,315 characters |
| Criteria description | not stored: IUCN criteria codes on some red list rows ("B2ab(ii,iv)"), sentences on others, and the Scottish Biodiversity List's category and criteria ("Category: Watching brief only; Criterion: S4 - <6 Scottish 10km sqs") |
| Comments | not stored: notes of up to 5,126 characters |

Read from the other columns (`JnccClassification`):

- `scope` and `area`, from the designation code: `uk` (the whole UK or Great Britain), `country`
  (part of the UK) or `international` (a convention, an EU directive or regulation, or IUCN's global
  or European red list). A code that no rule knows gets no scope, and the import names it. Two
  corrections apply to Great Britain designations: the 20 Extinct and Extinct in the Wild rows of
  the Vascular Plant Red List for England have the codes RedList_GB_post2001-EX and -EW, and are
  stored as England; and 200 Wildlife and Countryside Act rows whose comment says that the
  designation no longer applies in Scotland ("Designation does not apply in Scotland since 2007")
  are stored as England and Wales, and 5 whose comment starts "England only" as England. The
  Conservation of Habitats and Species Regulations 2010 extend to England and Wales only (regulation
  2), so their rows are stored as England and Wales.
- `status_code`, from a red list's code: the category after the hyphen (RedList_GB_post2001-CR(PE):
  CR(PE)), WL for the Waiting List of the Vascular Plant Red List for England (taxa it did not
  assess, waiting for taxonomic or mapping work), and Red or Amber for Birds of
  Conservation Concern and the spider list. Other designations have none.
- `population`: breeding or non-breeding, for the bird red list (its codes end _Breeding or
  _NonBreeding).
- `kingdom`, in IUCN's spelling: ANIMALIA for the six animal categories, PLANTAE for vascular
  plants, mosses, liverworts, hornworts and stoneworts, FUNGI for fungi and lichens, CHROMISTA for
  the group "chromist", and none for algae (red, green and brown) and slime moulds.
- `rank`, from the form of the name: species 26,169 rows, subspecies 686, variety 121, form 62,
  aggregate 50 (names with "agg." or a slash, "Anser fabalis/serrirostris"), above species 46 (one
  word: Cetacea, Sphagnum, Orchidaceae), hybrid 5, section 3, none 10 ("Mycetoporus 'species A'",
  "Cantharis nigra (=thoracica)", "Mine site community"). A subgenus in brackets after the genus
  ("Lithobius (Monotarsobius) crassipes", 202 names) and a trailing "s. lat." or "s.l." are
  left out when the rank is read; `scientific_name` keeps them.

### Designations in the 2026-06-09 file

| `sort_code` | Reporting category | Rows | Taxa (taxon version keys) | Scope: area |
| --- | --- | ---: | ---: | --- |
| A | Bern Convention: Appendix 1, 2, 3 (Bern-A1 16, A2 290, A3 73) | 379 | 371 | international: Europe |
| C | Birds Directive: Annex 1 (111), 2.1 (22), 2.2 (51) | 184 | 180 | international: European Union |
| C1 | Convention on Migratory Species: Appendix 1 (20), Appendix 2 (215), AEWA Annex II (152), ASCOBANS (11), EUROBATS Annex I (32) | 430 | 286 | international: World; AEWA Africa-Eurasia; ASCOBANS North-East Atlantic and Baltic; EUROBATS Europe |
| C2 | OSPAR | 34 | 34 | international: North-East Atlantic |
| D | Habitats Directive: Annex 2 priority species (5), Annex 2 non-priority species (47), Annex 4 (83), Annex 5 (37) | 172 | 139 | international: European Union |
| E | EC Cites: Annex A (82), B (42), C (5), D (8) | 137 | 137 | international: European Union |
| F | Global Red list status: IUCN global categories, 2001 and 1994 criteria (293), and IUCN's European red list (6) | 299 | 294 | international: World; Europe |
| Fa | Red Listing based on pre 1994 IUCN guidelines: Rare 352, Insufficiently known 271, Endangered 214, Vulnerable 178, Indeterminate 90, Extinct 71 | 1,176 | 1,163 | uk: Great Britain |
| Fb | Red Listing based on 1994 IUCN guidelines: DD 114, NT 73, VU 24, EN 4, EX 2, CR 1 | 218 | 218 | uk: Great Britain |
| Fc | Red listing based on 2001 IUCN guidelines: GB red lists 10,666 (the bird red list 357 of them); the Vascular Plant Red List for England 1,935 (1,839 coded RedList_ENG, 20 coded RedList_GB, 76 Waiting List) | 12,601 | 10,957 | uk: Great Britain; country: England |
| Fd | Birds of Conservation Concern 5: Red (70), Amber (103) | 173 | 173 | uk: United Kingdom |
| Fe | Spider Amber List | 43 | 43 | uk: Great Britain |
| Ga | Rare and scarce species: Nationally Rare (1,639) and Nationally Scarce (1,360), red-listed taxa included | 2,999 | 2,986 | uk: Great Britain |
| Gb | Rare and scarce species (not based on IUCN criteria): Nationally Notable 542, Notable A 200, Notable B 419, Nationally Rare 201 and Scarce 334 (red-listed taxa excluded), rare marine 62, scarce marine 53 | 1,811 | 1,805 | uk: Great Britain |
| Ha | UK Biodiversity Action Plan priority species (BAP-2007) | 1,150 | 1,150 | uk: United Kingdom |
| Hb | England: NERC Act section 41 | 943 | 943 | country: England |
| Hc | Scottish Biodiversity List | 2,103 | 2,085 | country: Scotland |
| Hd | Wales: Environment (Wales) Act section 7 | 569 | 568 | country: Wales |
| He | Northern Ireland Priority Species | 483 | 482 | country: Northern Ireland |
| I | Wildlife and Countryside Act 1981: Schedule 1 Part 1 (94) and Part 2 (3), Schedule 5 by section (670), Schedule 8 (183) | 950 | 438 | uk: Great Britain (745 rows); country: England and Wales (200 rows), England (5 rows) |
| J | Wildlife (Northern Ireland) Order 1985: Schedules 1, 5 and 8 | 133 | 133 | country: Northern Ireland |
| K | Conservation of Habitats and Species Regulations 2010: Schedules 2, 4 and 5 | 97 | 97 | country: England and Wales |
| L | Conservation (Natural Habitats, etc.) Regulations (Northern Ireland) 1995: Schedules 2, 3 and 4 | 67 | 67 | country: Northern Ireland |
| M | Protection of Badgers Act 1992 | 1 | 1 | uk: Great Britain |

In all: 18,982 rows for the UK or Great Britain, 6,535 for part of the UK and 1,635 international.

Values of the main designations:

- GB red lists (2001 criteria, codes RedList_GB_post2001-*, area Great Britain; `designation` and
  `status_code`): Least concern (LC) 6,543, Vulnerable (VU) 755, Data Deficient (DD) 748, Near
  Threatened (NT) 697, Not Evaluated (NE) 468, Endangered (EN) 369, Not Applicable (NA) 307,
  Critically Endangered (CR) 216, Regionally Extinct (RE) 111, Critically Endangered (possibly
  extinct) (CR(PE)) 54, Extinct (EX) 38, Extinct in the Wild (EW) 3. The bird red list (codes
  Bird_RedList_GB_post2001-*, 357 rows) assesses the breeding (258) and non-breeding (99)
  populations of a species separately (`population`), with the same categories. The Great Britain
  red lists of the 2001 criteria come from 44 documents (`source`), dated from 2004 to 2025.
- Birds of Conservation Concern: "Bird Population Status - red" and "- amber"; green-listed birds
  are not in the workbook.
- Wildlife and Countryside Act and the other legislation: the schedule and section is the
  designation; there is no status apart from being listed.
- NERC section 41, section 7, the Scottish Biodiversity List, Northern Ireland Priority Species and
  the UK BAP list: the designation is the name of the list.

One taxon can have the same designation more than once: 80 pairs of taxon version key and
designation code appear two or three times, from two published names under one recommended name
("Anas crecca" and "Anas crecca crecca") or from two source documents. In the GB red lists of the
2001 criteria, 23 taxa have two different categories for the same population (or for the whole
taxon), mostly from two published names in one source document.

## CITES Checklist: `statuses cites-import`

The Checklist of CITES Species (https://checklist.cites.org/) lists every taxon in the CITES Appendices with its current listing. UNEP-WCMC compiles it for the CITES Secretariat from the Species+ database (https://speciesplus.net/), so the two sites have the same listings and the same taxon concept ids.

### Terms and citation

The Checklist's terms of use (the "Terms of Use" link on checklist.cites.org) and the Species+ terms of use (https://speciesplus.net/terms-of-use) cover the data and the "Species+/CITES Checklist API", and say the same things. The owner of this project accepted the Species+ terms for the public species site in October 2026. The terms:

- allow publishing the data online when it cannot be downloaded, with the citation clearly visible, the date of download visible, and a clear link to the source (checklist.cites.org in the Checklist's terms, www.speciesplus.net in the Species+ terms);
- ask that the most recent version is used;
- forbid commercial use, sub-licensing and redistribution (web downloads, web services), and any application that replicates or tries to replace the essential user experience of the Checklist or Species+;
- strongly recommend that the CITES Secretariat (info@cites.org) or UNEP (species@unep-wcmc.org) review the published material before publication, and the Species+ terms ask for two electronic copies of published material, sent to species@unep-wcmc.org.

The command stores the Checklist's citation form, with the date of the download, in the `citation` column of the `cites` row of `status_source`. It uses the Checklist's form because the data comes from the endpoint that checklist.cites.org reads:

> UNEP-WCMC (Comps.) 2026. The Checklist of CITES Species Website. CITES Secretariat, Geneva, Switzerland. Compiled by UNEP-WCMC, Cambridge, UK. Available at: http://checklist.cites.org. [Accessed 08/10/2026].

The Species+ form is "UNEP (2026). The Species+ Website. Nairobi, Kenya. Compiled by UNEP-WCMC, Cambridge, UK. Available at: www.speciesplus.net. [Accessed dd/mm/yyyy]." Each taxon's `cites_taxon.url` is its page on Species+ (`https://speciesplus.net/species#/taxon_concepts/<id>/legal`), which shows the same listings with their notes.

### The endpoint

The Checklist's web app (https://checklist.cites.org/js/app.js) reads the Species+ checklist API at `https://www.speciesplus.net/checklist/`, which answers without a token. This API is not documented and could change without notice; the token-based Species+ API (https://api.speciesplus.net) is the documented one. The command asks for:

```
GET https://www.speciesplus.net/checklist/taxon_concepts?output_layout=alphabetical&level_of_listing=0
    &show_synonyms=1&show_author=1&show_english=0&show_spanish=0&show_french=0&locale=en&page=1&per_page=1000
```

| Parameter | What it does |
| --- | --- |
| `page`, `per_page` | Pages are numbered from 1. Pages of 1,000 rows worked; larger pages were not tried. A page of plants takes the server about 20 seconds, because every orchid row repeats the full note of the Orchidaceae listing and the text of annotation #4. |
| `output_layout` | `alphabetical`. The Checklist's About page says that `taxonomic` changes only the order of the taxa on the web page. |
| `level_of_listing` | `0`: every taxon. `1` gives only the taxa at the level at which they are listed (Felidae, not its species). |
| `show_synonyms`, `show_author` | With both set to 1, each row has `synonyms_with_authors` and `author_year`. With `show_author=0`, each row has `synonyms` (the names without authors) and no `author_year`. |
| `show_english`, `show_spanish`, `show_french` | `0`: no common names. |
| `locale` | `en`: the language of the notes and country names. |
| `scientific_name`, `country_ids[]`, `cites_region_ids[]`, `cites_appendices[]` | Filters of the web page; not used. |

A page is `[{"result_cnt", "total_cnt", "animalia": [...], "plantae": [...]}]`: `result_cnt` is the number of rows on the page and `total_cnt` the number of taxa in the whole list. The rows come animals first, then plants, each in name order. The command asks for one page at a time, 1 second apart, until it has `total_cnt` rows. The download fails, and the store is not changed, when `total_cnt` changes between pages or when the rows do not have `total_cnt` different taxon ids; the partly written file stays as `.part` and is never imported.

The rows are kept as one gzip-compressed JSON array, `cites-<date>.json.gz`, in the status lists folder (5.3 MB on 2026-10-08; as plain JSON it is about 140 MB, most of it notes repeated on every row). `--file` imports a kept file. It also imports the Checklist's own full JSON download (below), whose rows call the listings `current_listing_changes` instead of `current_additions`; the import of the 2026-10-08 full download gave the same taxa, listings and synonyms, without the inherited notes. `--limit N` downloads only the first N pages, into `cites-firstNpages-<date>.json.gz`. It still replaces every CITES row in the status lists store, so give `--store` the path of a test store.

Other endpoints that the web app uses, and that the command does not use:

- `checklist/geo_entities?geo_entity_types_set=2` (countries; `1` for regions): the id, name and ISO code of each place that the `countries_ids` of a row name.
- `checklist/timelines?taxon_concept_ids[]=...`: the history of listings of the taxa shown on the web page.
- `checklist/downloads/download_index?format=json` (also `csv` and `pdf`): the whole Index of CITES Species in one answer. On 2026-10-08 the JSON was 153 MB and took the server about 2 minutes. Its rows have the same listings as the paged endpoint, with the ISO codes of each taxon's countries, but without the inherited notes (`inherited_short_note`, `inherited_full_note`). `checklist/downloads/download_history` is the history of every listing.

This API has CITES only. The EU Wildlife Trade Regulation annexes are in Species+ and its token-based API, not in these answers.

### What a row says

- `current_listing` is Species+'s summary of the taxon and its descendants. On Antigone canadensis, `I/II` means that some of its subspecies are in Appendix I, while the species itself is listed in Appendix II. `NC` in a combination means that part of the taxon is in no appendix: the genus Agapornis is `II/NC` because Agapornis roseicollis is excluded from the listing of the order Psittaciformes. `NC` alone means that the taxon is in no appendix (Agapornis roseicollis).
- `current_additions` are the listings that apply to the taxon itself, one per row of `cites_listing`: those made for the taxon and those it inherits from a higher taxon. A taxon has one listing per appendix, and one per Party for Appendix III (Crax rubra is listed in Appendix III by Colombia, Honduras and Guatemala).
- A taxon with listings in more than one appendix is split listed by population, and only the notes say which populations are in which appendix. Loxodonta africana has an Appendix I listing inherited from the genus Loxodonta, with the genus's note "Except the populations of *Loxodonta africana* of Botswana, Namibia, South Africa and Zimbabwe ...", and an Appendix II listing of its own, with the note "Populations of Botswana, Namibia, South Africa and Zimbabwe are included in Appendix II subject to annotation A11 (see full note); all other populations are included in Appendix I". Each listing has its own `countries_ids` field, but it was empty in every row, so the store does not keep it.
- An inherited listing has an `auto_note` such as "FAMILY listing Trochilidae spp.". The command stores the rank and the name of the higher taxon (`inherited_rank`, `inherited_name`), and its id (`inherited_from_id`) when the download has exactly one taxon of that name and rank; on 2026-10-08 all 41,744 inherited listings had one. `inherited_short_note` is the higher taxon's note that applies to the taxon ("Excludes fossils." for the corals of the order Scleractinia). The inherited listing can also have a note of its own (`short_note` "Included in SCLERACTINIA spp."). Its `listing_change_id`, Species+'s id of the listing, is often the id of the higher taxon's listing.
- The notes are HTML as Species+ gives them: `short_note`, `full_note`, `hash_full_note` (the text of an annotation such as #4; not a hash value), `inherited_short_note`, `inherited_full_note` and `nomenclature_note`. They have `<i>` for names, `<p>` for line breaks, `\r\n`, no-break spaces and entities such as `&amp;`, and two full notes have an `<img>` tag. The site has to make them safe before showing them. Full notes and annotation texts are stored once each in `cites_note`.
- `cites_accepted` is 0 for 2,097 taxa (2,018 of them species): names that Species+ marks as not CITES accepted, which the Checklist shows in plain type instead of bold. Some of these taxa have listings made for them (Agrias amydon boliviensis is in Appendix III, listed by Bolivia), so the store keeps them.

Not stored: common names (not asked for), the countries of each taxon (the row's `countries_ids`), the `countries_ids` of each listing (always empty), `change_type_name` (always ADDITION) and `is_current` (always true) of the listings, `recently_changed`, and the history of listings.

### Names

`full_name` is one of these:

- a species binomial;
- a subspecies trinomial without a rank word ("Achillides chikae chikae");
- a variety with "var." ("Euphorbia decaryi var. robinsonii");
- a genus, subfamily, family or order name (the Checklist adds "spp." on its web page; the store does not);
- an informal name in quotes, for 15 New Zealand geckos ("Woodworthia "Pygmy"", "Dactylocnemis "Matapia"").

No name is a hybrid. One name ended in a no-break space, which the command trims.

The endpoint gives synonyms with their authors ("Ornismya abeillei Lesson & DeLattre, 1839"). `CitesChecklist.SplitSynonym` splits each into the name (`cites_synonym.name`) and the author (`author`):

- The name is the first word (the genus), then a subgenus in brackets if one follows the genus (sometimes in lower case: "Phyllomedusa (agalychnis) callidryas"), then every lower-case epithet ("d'albertisii"), rank word ("var.") and qualifier ("aff.", "cf.") that follows.
- The author is the rest, from the first word that is none of those. A lower-case particle such as "de", "van" or "la" starts the author when the words after it lead to a capitalised word ("Trochilus tzacatl de la Llave, 1833"), and so do "sensu", "auct." and "hort.".

The endpoint gives the same synonyms without authors when it is asked with `show_author=0`. On two pages of 1,000 taxa, all 2,272 synonyms split into the same names. In the whole download, the name of 3 synonyms of species is only the genus, because the words after the genus have another form: "Dactylocnemis "Mokohinau" Nielsen, Bauer, ...", "Paphiopedilum 'victoria' De Vogel" and "Varanus (subgen. inc. sed.) spinulosus Böhme & Ziegler, 2007".

### Counts (2026-10-08)

Taxa: 43,310 (7,885 animals, 35,425 plants): 41,075 species, 70 subspecies, 8 varieties, 2,034 genera, 112 families, 1 subfamily and 10 orders. 22 scientific names belong to two taxa each, all of them plants in Appendix II, most with two different authors (Cyathea parva (Maxon 1944) R.Tryon 1976 and Cyathea parva Copel. 1942), so a match by name has to allow for a name that gives two taxa.

`current_listing` of the 41,153 species, subspecies and varieties:

| `current_listing` | Taxa |
| --- | --- |
| II | 39,330 |
| I | 1,130 |
| III | 532 |
| NC | 44 |
| I/II | 36 |
| I/NC | 24 |
| II/NC | 22 |
| (none) | 17 |
| III/NC | 14 |
| I/II/NC | 2 |
| I/III | 1 |
| I/II/III/NC | 1 |

The appendices of the listings in `cites_listing` (inherited ones included) of the same 41,153 taxa: Appendix II only 39,360, Appendix I only 1,148, Appendix III only 533, Appendices I and II 21. 91 of them have no row in `cites_listing`: the 44 with `NC`, the 17 with no `current_listing` (species of Dicksonia, a genus listed only for its populations in the Americas), and 30 species whose `current_listing` comes from their listed subspecies or varieties (Agrias amydon is `III/NC` because Agrias amydon boliviensis is in Appendix III). Of all 43,310 taxa, 54 have no `current_listing` and no row in `cites_listing`: those 17 species and 37 genera, most of them orchid and cactus genera such as Odontoglossum and Neobuxbaumia. 257 taxa have a `current_listing` with two or more parts but listings in only one appendix (Aonyx capensis is `I/II`, its listing is Appendix II, and its subspecies Aonyx capensis microdon is in Appendix I).

Listings: 43,246 (Appendix I 1,257, Appendix II 41,421, Appendix III 568). 41,744 are inherited: from a family (33,328), a genus (4,774), an order (3,624), a subfamily (11) or a species (7, varieties of Euphorbia).

Split listings, with listings in Appendices I and II: 22 taxa. Balaenoptera acutorostrata, Caiman latirostris, Canis lupus, Caracal caracal, Ceratotherium simum simum, Crocodylus acutus, C. moreletii, C. niloticus, C. porosus, Falco newtoni, Herpailurus yagouaroundi, Loxodonta africana, Melanosuchus niger, the genus Moschus, Moschus chrysogaster, M. fuscus, Panthera leo, Prionailurus bengalensis bengalensis, P. rubiginosus, Puma concolor, Ursus arctos and Vicugna vicugna.

Appendix III: 568 listings of 557 taxa by 31 Parties (South Africa 148, Australia 144, India 33, Ukraine 32, New Zealand 32, Cuba 26, Brazil 19, Colombia 16, Honduras 16, Pakistan 12, United States 10, European Union 9, and 19 others with fewer). 10 taxa are listed by two or more Parties.

Annotations: #4 on 34,023 listings, #15 on 291, #17 on 172, #5 on 125, #2 on 95, #14 on 32, #6 on 19, #9 on 13, #18 on 7, #1 on 5, #3 on 2, and #7, #8, #10, #11, #12, #13, #16 and #19 on one listing each. `cites_note` has 141 notes (33 KB).

Synonyms: 42,423 rows in `cites_synonym`, for 16,910 taxa. They have 41,294 different names (`name`, without the author); 536 of these names are synonyms of two or more taxa, so a match by synonym has to allow for a name that gives several taxa. 1,281 synonyms have no author.

A full download took 44 requests and 14 minutes on 2026-10-08, and no request had to be sent again. Importing the kept file takes about 7 seconds, and the CITES tables take about 25 MB of the status lists store.

### The endpoint may change

The command uses the endpoint that the Checklist's web app uses, not a published API, so UNEP-WCMC can change its parameters or its answers without notice. The download fails, and the store is not changed, when a page is not JSON, when `total_cnt` changes during the download, or when the rows do not have `total_cnt` different taxon ids. A renamed field would show only as missing values, so compare the counts that the command prints after a run with the counts above.

## National and subnational red lists: `statuses red-lists-import`

Many national and subnational red lists are published on GBIF as Darwin Core Archive (DwC-A) checklists, with the category of each taxon in the `threatStatus` field of the Distribution extension. `statuses red-lists-import` imports the ones listed in `rules/status-lists/national-red-lists.yml`: 29 lists of 16 countries in October 2026, 60,168 statuses of 60,160 taxa, and 6,472 synonyms.

### How the lists were chosen

On 2026-10-08, GBIF's species search faceted by dataset (`/v1/species/search?facet=datasetKey&threat=...`) found 214 checklist datasets with at least one threat status that GBIF could read as an IUCN category. A dataset search for "red list" in English and other languages ("rote Liste", "lista roja", "liste rouge", "rødliste", "Red Data Book" and others) found the lists whose categories GBIF cannot read (Germany's, Ukraine's). Each candidate's registry entry was checked, and its archive downloaded and read. A list is in the file when:

- it is an official or authoritative national or subnational red list, or a legal list of threatened species;
- it gives a category per taxon in `threatStatus`;
- its licence is CC0, CC BY or CC BY-NC (the site is free and non-commercial);
- its categories are the list's own, not copies of IUCN global categories;
- the publisher has not replaced it with a later full edition. A partial update is listed beside the list it updates (Luxembourg's bryophytes: the 2003 list and its 2008 update of 40 taxa; for a taxon in both, the 2008 status replaces the 2003 one).

The Danish statuses come from the national checklist behind arter.dk, the Danish agency's species portal, which is not a red list dataset as such. Its categories are the Danish Red List's: of the 1,654 names that it shares with Hortus botanicus Leiden's export of the Danish red list of vascular plants (assessed 2018), 1,644 have the same status, and the other 10 are NE or NA on one side.

### The file

`rules/status-lists/national-red-lists.yml` has one entry per list. Its header explains each field: the store key (`<country>-<list>[-<year>]`), the GBIF dataset key, the country (ISO 3166-1 alpha-2) and, for a subnational list, the region with its ISO 3166-2 code, the list's name as its publisher gives it (with an English name when it is in another language), the year of the edition, the publisher, the licence, the citation of the original list (as the archive's metadata cites it, else the GBIF dataset), a kingdom for archives that give none, an archive URL that replaces the registry's endpoint, English labels for statuses that are not IUCN categories (`categories`), and notes. `RedListManifest` reads it and refuses an entry with no key, GBIF key, country, name, publisher, licence or citation, a licence other than CC0 1.0, CC BY 4.0 and CC BY-NC 4.0, or a key used twice.

### The run

For each list, one request at a time and at least a second apart:

1. The GBIF registry (`https://api.gbif.org/v1/dataset/<key>`) gives the title, licence, pubDate, DOI, GBIF's citation and the DWC_ARCHIVE endpoint. A dataset whose registry licence is not one of the three is not imported; a licence that differs from the file's is stored and named in a warning.
2. When the registry's pubDate and the archive URL are those of the last import and the archive is still in the folder, nothing is downloaded (`RedListPlan.NeedsDownload`). GBIF updates the pubDate when it crawls a new version, so a dataset that the publisher rebuilds every night with the same content (Denmark's) is not downloaded every night.
3. Otherwise the archive is downloaded to `<key>-<yyyy-MM-dd>.zip.part` and renamed when complete. A file that does not start as a zip (an error page) is deleted and refused. When its SHA-256 is that of the archive last imported, the new copy is deleted and the old one kept.
4. The archive is read and the list's rows replaced in one transaction, unless it is the archive last imported and was read by the same `RedListArchiveReader.Version` (`RedListPlan.NeedsImport`). Increase the version when what the reader stores changes: the next run reads every kept archive again without downloading it.

`--force` downloads and imports every list again; `--dataset <key>` imports one list; `--limit N` imports the first N lists of the file; `--status` prints the lists, their last import and their counts, and sends no requests. A run over the whole file (no `--dataset`, no `--limit`) deletes the stored lists that the file no longer lists. A list that fails (no answer, not a zip, no Distribution extension, no rows with a status) is reported and the others carry on; the run then exits with 1, and the failed list's stored rows are kept.

A full run with nothing changed takes 29 registry requests and about 45 seconds. The first run downloaded 6.1 MB of archives.

The Ecuador ministry's IPT (patrimonio.ambiente.gob.ec) does not answer, so the four Ecuador lists come from ChecklistBank's copies (`https://api.checklistbank.org/dataset/<id>/archive.zip`, datasets 264127, 54652, 53732 and 54007, imported by ChecklistBank on 2026-09-21). ChecklistBank has no archive for the Colombian list (dataset 288059 answers 404), so it comes from the Colombian IPT, as the registry gives it.

### The archive format

A DwC-A is a zip with `meta.xml`, which names the core file (rowType Taxon) and each extension file, and for each file its field separator, quote character, header lines, encoding, the column of the row id (`id` in the core, `coreid` in an extension) and the term of each column. `DwcArchive` reads `meta.xml` and nothing is assumed about column order: terms are matched by their local name (`threatStatus` is `http://iucn.org/terms/threatStatus` in some archives), and a field with a default and no index gives every row that value. The files are read with CsvHelper's parser. Every archive here is tab-separated with `fieldsEnclosedBy=''`, which means no quoting at all, so an empty enclosure is read in CsvHelper's NoEscape mode: a quote at the start of a value does not open a quoted field that swallows the next lines. Some archives have a header line and some (Denmark's) do not.

### What is stored

`red_list_dataset`: one row per list, with the file's fields, the registry's title, licence, pubDate, DOI and citation, the archive's URL, file name, size and SHA-256, the reader version, when the list was last checked and last imported, and its counts. Each list also has a `status_source` row with source `redlist:<key>` (title, GBIF dataset page, licence, citation, pubDate as the version, fetch time, row count), so the site's credits can be built from `status_source` as for the other sources. The existing readers of `status_source` ask for their own sources by name, so these rows do not reach them.

`red_list_taxon`: one row per Distribution row with a status, joined to its core taxon (`taxon_id` is the core row's id, `seq` numbers a taxon's statuses). Stored as given: the scientific name, authorship, rank, taxonomic status, accepted name, kingdom to genus, `threatStatus` (trimmed, otherwise verbatim), countryCode, locality, locationID, establishmentMeans, occurrenceStatus, eventDate (or temporal) and source. Derived:

- `iucn_code`: the IUCN category when the status is one (`RedListCategories.ToIucnCode`): the codes EX, EW, RE, CR, EN, VU, NT, LC, DD and NA in any case ("En"), the names ("Least Concern"), a name with its code ("Least Concern (LC)"), the categories before 2001 (LR/nt and LR/cd are NT, LR/lc is LC), and a code with a mark of its list: CR(PE), CR(PEW), CR-PE and CR* are CR, VU° and VUº (a category lowered for immigration from outside the country) are VU, NAa and NAb are NA. Every other status has none (NULL): Germany's categories, Ukraine's, Luxembourg's R, the Netherlands' REW.
- `status_label`: the English label the file's `categories` give the status, matched without regard to case or to the kind of space. Every status in the German and Ukrainian lists has one, and so do CR*, NAa, NAb (France), VU° and the like (Norway), CR-PE (Ecuador's birds) and R (Luxembourg's dragonflies).
- `canonical_name`: the archive's `canonicalName` when it gives one (none of these does), else the scientific name less the authorship column when the name ends with it, then `BareScientificName.Strip`. A nothospecies keeps the hybrid sign ("Mentha × gracilis", "Salix x rubens" is "Salix × rubens"). It is NULL when the name could be mistaken for another taxon's: a hybrid formula ("Populus alba × tremula"), a row of rank unranked, hybrid, section or another rank above species other than genus (Sweden's "Phocoena phocoena (Baltic population)", Denmark's "Rubus sect. Rubus"), or a species or infraspecific name that strips to one word ("Ramaria aff. strasseri", "Oenothera biennis-Gruppe"). 83 of the 60,168 rows have none.
- `accepted_taxon_id`: the accepted taxon's id, when acceptedNameUsageID names another taxon. Uruguay's 42 and Venezuela's 102 rows with one are synonyms with their own status; their accepted taxon is usually not in the archive (`accepted_name` gives its name).
- `url`: the taxon's page at the publisher, when the core's `references` or `bibliographicCitation`, or the row's `source`, is a URL and nothing else: Denmark's arter.dk pages (all 12,222 rows) and Cuba's assessment pages at caribea.planta.ngo (all 4,117).

Left out of `red_list_taxon`: Distribution rows with no status (empty, or the text "NULL"), rows whose status is NE (Denmark's checklist gives NE to 42,822 taxa, genera and families included; Flanders 143, Ecuador's birds 315), and rows whose taxon is not in the core.

`red_list_synonym`: the core taxa whose acceptedNameUsageID names a taxon with a status, for name matching (`accepted_taxon_id` is that taxon's `taxon_id`). Misapplied names are left out (Sweden's 342): they are names used for another taxon. Sweden gives 6,367 synonyms, Venezuela 103, Switzerland's beetles 1, Uruguay 1.

Not stored: occurrenceRemarks (Cuba's are narrative: endemism and protected areas; Flanders' are the Dutch names of the categories), taxonRemarks, the Description, Reference, VernacularName and other extensions.

### For the site: matching and showing

- Match a status row by `canonical_name` within its `kingdom`, then by `red_list_synonym.canonical_name` to its `accepted_taxon_id`. For a row with an `accepted_taxon_id`, prefer the accepted taxon's own row when the list has one, and try `accepted_name` too. A row with no canonical name cannot be matched safely.
- The country is `red_list_dataset.country_code`; a row's own `country_code` is as given ("Ecuador", "fr", "nl"). Flanders is the one subnational list (`region` Flanders, `region_code` BE-VLG). Ecuador's birds have two statuses for 8 taxa: `locality` "Ecuador continental" and "Ecuador insular | Islas Galápagos".
- `iucn_code` says which statuses are IUCN categories; show `threat_status` as written, with `status_label` when it has one. RE and NA are regional categories; Germany's and Ukraine's are their own systems, and the German list's metadata says they cannot be compared directly with IUCN's.
- Two lists can cover one taxon in one country: Luxembourg's bryophytes (show the 2008 status), France's species and subspecies lists (different taxa), Switzerland's plants, butterflies and beetles (different taxa).
- Citation: `red_list_dataset.citation` is the original list; `gbif_citation` cites the GBIF dataset with its DOI. Both are in English or the publisher's language as given.
- The archives give taxon pages only for Denmark and Cuba (`url`). Sweden's taxon ids are Dyntaxa ids (`urn:lsid:dyntaxa.se:Taxon:N`), whose page is `https://artfakta.se/taxa/N`.

### The lists

Imported on 2026-10-08 into a test store. Statuses as the archives write them, with the number of rows; `*`, `0` to `3`, `D`, `G`, `nb`, `R` and `V` are Germany's categories.

| Key | Country | Year | Licence | Statuses | Synonyms | Statuses as written |
| --- | --- | --- | --- | ---: | ---: | --- |
| `se-redlist-2025` | SE | 2025 | CC0 1.0 | 6,156 | 6,367 | NT 1,966, VU 1,667, DD 1,079, EN 920, CR 323, RE 201 |
| `dk-redlist` | DK | | CC BY 4.0 | 12,222 | 0 | LC 6,565, DD 1,590, NA 1,139, VU 848, EN 675, NT 628, RE 394, CR 383 |
| `co-mads-2024` | CO | 2024 | CC0 1.0 | 2,104 | 0 | VU 838, EN 800, CR 466 |
| `be-vlg-validated` | BE-VLG | | CC0 1.0 | 2,889 | 0 | LC 1,320, NT 460, VU 338, CR 271, EN 234, RE 209, DD 57 |
| `ec-amphibians` | EC | 2019 | CC BY 4.0 | 635 | 0 | LC 168, EN 147, VU 129, CR 85, NT 78, DD 26, En 2 |
| `ec-freshwater-fish` | EC | 2019 | CC BY 4.0 | 163 | 0 | DD 66, LC 62, VU 15, NT 13, EN 6, CR 1 |
| `ec-birds` | EC | 2018 | CC BY 4.0 | 1,508 (1,500 taxa) | 0 | LC 1,142, NT 162, VU 107, EN 63, CR 15, DD 12, CR-PE 4, RE 3 |
| `ec-palms` | EC | 2018 | CC BY 4.0 | 21 | 0 | EN 8, VU 6, LC 3, CR 2, DD 2 |
| `lu-plants-2025` | LU | 2025 | CC0 1.0 | 1,423 | 0 | LC 540, NA 167, EN 162, VU 152, NT 116, RE 109, CR 98, DD 79 |
| `lu-birds-2019` | LU | 2019 | CC0 1.0 | 64 | 0 | NT 24, EX 13, VU 11, EN 8, CR 7, DD 1 |
| `lu-odonata-2006` | LU | 2006 | CC0 1.0 | 60 | 0 | LC 35, RE 12, R 6, EN 2, NT 2, VU 2, CR 1 |
| `lu-orthoptera-2003` | LU | 2003 | CC0 1.0 | 46 | 0 | LC 23, R 8, RE 4, NT 3, VU 3, CR 2, DD 2, EN 1 |
| `lu-bryophytes-2003` | LU | 2003 | CC0 1.0 | 587 | 0 | LC 316, VU 77, NT 63, CR 61, EN 52, DD 10, EX 8 |
| `lu-bryophytes-2008` | LU | 2008 | CC0 1.0 | 40 | 0 | CR 13, EN 12, VU 9, DD 3, NT 3 |
| `cu-plants-2023` | CU | 2023 | CC BY 4.0 | 4,117 | 0 | LC 1,614, CR 768, DD 696, EN 471, VU 396, NT 145, EX 23, RE 4 |
| `uy-fauna` | UY | | CC BY 4.0 | 574 | 1 | LC 363, NA 86, VU 37, NT 35, DD 22, EN 22, CR 6, RE 2, EX 1 |
| `is-plants-2018` | IS | 2018 | CC BY 4.0 | 840 | 0 | Not Applicable 414, Least Concern 359, Vulnerable 31, Data Deficient 10, Near Threatened 10, Critically Endangered 8, Endangered 7, Regionally Extinct 1 |
| `fr-plants-2018` | FR | 2018 | CC BY 4.0 | 6,070 | 0 | LC 3,843, NAa 1,083, DD 373, NT 321, VU 238, EN 132, CR 42, RE 22, CR\* 9, NAb 5, EX 2 |
| `fr-plants-2018-subspecies` | FR | 2018 | CC BY 4.0 | 960 | 0 | LC 671, DD 103, NAa 75, NT 59, VU 23, EN 19, CR 8, CR\* 1, RE 1 |
| `no-plants-2021` | NO | 2021 | CC BY 4.0 | 3,839 | 0 | NA 2,130, LC 1,145, NT 200, VU 147, EN 136, CR 54, RE 13, VU° 6, DD 4, EN° 2, LC° 1, NT° 1 |
| `nl-plants-2012` | NL | 2012 | CC BY 4.0 | 1,432 | 0 | LC 739, NT 253, VU 244, EN 89, CR 51, RE 37, REW 15, DD 4 |
| `nl-bryophytes-2012` | NL | 2012 | CC BY 4.0 | 246 | 0 | NT 102, EN 52, VU 43, CR 27, RE 22 |
| `ch-plants-2016` | CH | 2016 | CC BY 4.0 | 2,915 | 0 | LC 1,643, NT 437, VU 368, EN 200, CR 113, DD 99, RE 35, CR(PE) 19, EX 1 |
| `de-plants-2018` | DE | 2018 | CC BY 4.0 | 5,255 | 0 | `*` 2,153, `3` 567, `D` 513, `nb` 477, `2` 438, `R` 379, `V` 374, `1` 241, `0` 85, `G` 28 |
| `ch-butterflies-2014` | CH | 2014 | CC BY 4.0 | 224 | 0 | LC 102, NT 44, VU 38, EN 27, CR 10, RE 3 |
| `ch-beetles-2016` | CH | 2016 | CC BY 4.0 | 287 | 1 | LC 88, NT 46, EN 44, VU 42, DD 34, CR 31, RE 2 |
| `ve-fauna-2015` | VE | 2015 | CC BY 4.0 | 3,947 | 103 | LC 2,991, DD 391, NT 257, VU 150, EN 124, CR 31, EX 2, RE 1 |
| `ua-redbook-plants-2021` | UA | 2021 | CC0 1.0 | 857 | 0 | вразливий 313, рідкісний 231, зникаючий 205, неоцінений 65, недостатньо відомий 26, зниклий в природі 13, зниклий 4 |
| `ua-redbook-animals-2021` | UA | 2021 | CC0 1.0 | 687 | 0 | вразливий 279, рідкісний 201, зникаючий 160, недостатньо відомий 28, неоцінений 11, зниклий 7, зниклий в природі 1 |

Notes on the lists (more in the file):

- Sweden lists only red-listed taxa (RE to DD); taxa assessed LC or NA are not in the dataset. Colombia's legal list has only CR, EN and VU, and the Dutch bryophyte list only RE to NT.
- Flanders' validated list holds the red lists of 16 groups, each from its own year (1996 to 2017); each row's `source` names the group's list and `event_date` its year. Uruguay's dataset holds two national lists: birds (2012, 454 rows) and amphibians and reptiles (2015, 120 rows).
- Hortus botanicus Leiden republished the French, Norwegian, Dutch, Swiss and German plant lists on GBIF in 2026 (CC BY 4.0); the file cites the original lists. Their archives give no kingdom (the file gives Plantae). The full Norwegian Red List 2021 is not on GBIF.
- Luxembourg's "R" (dragonflies 6, grasshoppers 8) is not an IUCN category; the dragonfly archive's remarks call those species extremely rare, and the grasshopper archive does not explain it. The Netherlands' "REW" (15 species) is not explained in its archive.
- Ukraine's lists are the legal lists of the Red Data Book of Ukraine (2021 orders of the Ministry of Environmental Protection and Natural Resources), with the categories of the Law "On the Red Book of Ukraine". One is written with a no-break space ("зниклий в<U+00A0>природі").
- Venezuela's Libro Rojo de la Fauna Venezolana (2015) is published by Provita, a non-governmental organisation; it is the national red list of animals.

### Lists left out

| Dataset | Reason |
| --- | --- |
| The Swedish Red List 2020 | Replaced by the 2025 list. |
| Norwegian Red List 2015 (all groups) | Replaced by the 2021 list, which is on GBIF only for vascular plants (Leiden's export). |
| Red List Vascular Plants (Denmark), Leiden's export | The same red list as the Danish national checklist, which is current and covers every group (1,644 of 1,654 shared names have the same status). |
| Checklist of Danish Fungi (svampe.databasen.org) | CC BY-NC, and its statuses are the Danish Red List's, which the national checklist already has for fungi. |
| Dyntaxa, Svensk taxonomisk databas | Its statuses are the Swedish Red List's; the archive URL needs a subscription key. |
| Non-validated red list of Flanders | Not validated by the Flemish government (INBO publishes the validated lists separately). |
| Red list of dragonflies in Flanders | Its 66 species are in the validated lists. |
| Red list of Lycopodiaceae of Luxembourg 2019 | The 2025 vascular plant list covers the family and replaces it (Lycopodium annotinum is R in 2019, CR in 2025). |
| Checklist of Amphibia species of Luxembourg 2003 | 14 species from a distribution atlas, not a red list; uses "V" with no explanation. |
| National checklists and red lists for European butterflies (INBO) | A 2019 compilation of 40 countries' lists, with each country's categories translated into IUCN categories and rows for Europe and the EU; not the lists themselves. |
| Lista roja de los árboles endémicos de Venezuela | IUCN global assessments (nameAccordingTo cites the IUCN Red List 2022). |
| The National Checklist of Taiwan (TaiCOL) and the Taiwan Wildlife Conservation List | Taiwan's own red lists mixed with IUCN global categories ("CD", IUCN's LR/cd, on giant clams), with nothing to tell them apart; the conservation list's statuses are TaiCOL's. |
| Lista de referencia de especies de aves de Colombia 2022 | IUCN global categories (Tinamus tao, T. osgoodi, Crypturellus kerriae VU). |
| Especies de Fauna Reportadas en los Libros Rojos de Colombia 2010; the Valle del Cauca lists; local Colombian inventories | Replaced by Resolution 0126 of 2024, or copies of national or IUCN categories for one area; most are CC BY-NC project lists. |
| Checklist of the mammals of Central Asia | IUCN global categories (Marmota menzbieri VU in all four countries). |
| Checklist of the vascular plants of the Democratic Republic of the Congo | IUCN global categories, including those before 2001 ("Lower Risk/near threatened"). |
| NZCS Endangered Fauna Suriname | IUCN global categories ("iucnStatus=vulnerable"). |
| CONABIO's Mexican lists (Lista de la herpetofauna con distribución en México) | IUCN global categories. |
| Checklist and distribution of the species of Seychelles for conservationists | No source for its categories. |
| Endangered Species from the Coastal Region of Kenya; IUCN Red list of some Plants and Animals along the Nigerian Coast | IUCN global categories. |
| The IUCN Red List of Threatened Species, Catalogue of Life, GBIF Backbone, WCVP | Global, not national. |
| Red lists of flora and fauna of Ukraine's regions (26 datasets of oblasts and cities) | No Distribution extension in the three checked (Cherkasy, Kyiv city, Lviv): lists of names with no category. |
| Endangered species list of Fungi in Japan 2020; Togo Plant Red List; Red List of Polish Fungi 2006; Red List of Fungi, Russia; Red List of Altai Mountain Country | No `threatStatus` (Togo puts its categories in taxonomicStatus). |
| Lista Vermelha da Flora Vascular de Portugal Continental; Rote Liste der Heuschrecken der Schweiz; The Red Data Book of Rare and Threatened Plants of Greece | Plazi treatment archives with no `threatStatus`. |
| Catálogo de Plantas das Unidades de Conservação do Brasil | No `threatStatus`. |
| Red Data Book of the Komi Republic | The archive URL does not return a zip. |
| South African national checklists (SANBI) | The IPT answers 403, and the plant checklist has 35 statuses. |
| Doñana Biosphere Reserve; Askania Nova | Protected areas, not national or regional lists. |

## Japan's Red List: `statuses japan-import`

The Ministry of the Environment of Japan (環境省) publishes the national Red List for thirteen groups. It is revising the list group by group: the 5th Red List (環境省第５次レッドリスト) has replaced the Red List 2020 (環境省レッドリスト2020) for eight groups so far, and the Red List 2020 is still the latest list for the other five. The command stores the latest list of each group, one row per taxon or threatened local population, in `japan_listing`.

Code: `JapanImportCommand.cs`, `JapanRedList.cs` (the groups, their files and the category codes), `JapanRedListCsv.cs`, `JapanRedList2020Pdf.cs`, `StatusListStore.Japan.cs`.

### Sources

| Group (`group_key`) | Japanese name | Latest list | File | Rows |
| --- | --- | --- | --- | ---: |
| Mammals (`mammals`) | 哺乳類 | Red List 2020 | `900515981.pdf` | 89 |
| Birds (`birds`) | 鳥類 | 5th Red List, 2026 | `redlist2026_birds.csv` | 170 |
| Reptiles (`reptiles`) | 爬虫類 | 5th Red List, 2026 | `redlist2026_reptiles.csv` | 63 |
| Amphibians (`amphibians`) | 両生類 | 5th Red List, 2026 | `redlist2026_amphibian.csv` | 77 |
| Brackish and freshwater fishes (`fishes`) | 汽水・淡水魚類 | Red List 2020 | `900515981.pdf` | 260 |
| Insects (`insects`) | 昆虫類 | Red List 2020 | `900515981.pdf` | 877 |
| Molluscs (`molluscs`) | 貝類 | Red List 2020 | `900515981.pdf` | 1,190 |
| Other invertebrates (`other-invertebrates`) | その他無脊椎動物 | Red List 2020 | `900515981.pdf` | 152 |
| Vascular plants (`vascular-plants`) | 維管束植物 | 5th Red List, 2025 | `redlist2025_ikansoku.csv` | 2,222 |
| Bryophytes (`bryophytes`) | 蘚苔類 | 5th Red List, 2025 | `redlist2025_sentairui.csv` | 289 |
| Algae (`algae`) | 藻類 | 5th Red List, 2025 | `redlist2025_sorui.csv` | 178 |
| Lichens (`lichens`) | 地衣類 | 5th Red List, 2025 | `redlist2025_chiirui.csv` | 153 |
| Fungi (`fungi`) | 菌類 | 5th Red List, 2025 | `redlist2025_kinrui.csv` | 110 |

- The 5th Red List's CSV files are at `https://ikilog.biodic.go.jp/rdbdata/files/redlist2026/<file>` (birds, reptiles, amphibians) and `.../redlist2025/<file>` (the other five), on the site of the Ministry's Biodiversity Center of Japan. The e-Gov data portal lists them as the dataset "レッドリスト/レッドデータブック_第５次レッドリスト" (https://data.e-gov.go.jp/data/dataset/env_20260420_1111, added 2026-04-20). In October 2026 the ikilog site's own pages showed a maintenance notice (until about the end of the month), but on 2026-10-08 every file URL answered with the file (HTTP 200, `text/csv`).
- The Red List 2020 is only a PDF: https://www.env.go.jp/content/900515981.pdf (131 pages, "別添資料３" of the Ministry's announcement of the Red List 2020). It has every group of 2020; the import keeps its rows of the five groups that the 5th Red List does not cover yet. The Ministry's Red List page is https://www.env.go.jp/nature/kisho/hozen/redlist/index.html. Its press releases for the 5th Red List link PDF versions of the lists (with list numbers such as VP0001 for the plants), which the import does not use.
- When the Ministry publishes the 5th Red List of the other groups, add their CSV files to `JapanRedList.Groups` and the PDF stops being needed.

### Terms and citation

The Ministry's terms of use (https://www.env.go.jp/mail.html, "利用規約・免責事項・著作権") apply the Public Data License (Version 1.0) (PDL1.0, https://www.digital.go.jp/resources/open_data/public_data_license_v1.0), Japan's government open data licence, which is compatible with CC BY 4.0. PDL1.0 asks for the source to be named (the Ministry's example: 出典：「○○動向調査」（環境省）（URL）) and, when the content is edited, for a separate statement that it was edited and by whom, without presenting the edited version as the government's (example: 「○○動向調査」（環境省）（URL）を加工して作成). The e-Gov portal's API gave no licence for the dataset on 2026-10-08 (`license_id` empty), so the licence stored is the Ministry's.

The `japan` row of `status_source` has the title "Red List of the Ministry of the Environment, Japan (環境省レッドリスト)", the URL of the Ministry's Red List page, the licence "Public Data License (Version 1.0) (PDL1.0)", the folder imported as `version`, and this citation, which the site shows with the download date (each row's `list_version` and `list_year` say which edition the row comes from):

> Source: Red List 2020 (環境省レッドリスト2020) and 5th Red List (環境省第５次レッドリスト), Ministry of the Environment, Japan. The lists are used under the Public Data License (Version 1.0). Beastie Bot Species Status edited them: it converted the categories to letter codes and matched the names to species on this site. 出典：「環境省レッドリスト2020」（環境省）（https://www.env.go.jp/content/900515981.pdf）及び「環境省第５次レッドリスト」（環境省）（https://ikilog.biodic.go.jp/）を加工してBeastie Bot Species Statusが作成

### The download

The command downloads the eight CSV files and the PDF, one request each, 1 second apart, into a folder `japan-redlist-<yyyy-MM-dd>` in the status lists folder, under the names in their URLs (1.2 MB in all). A file that is already in the folder is read again and not downloaded again, so a run that stopped on a failed download carries on from the next file. A failed download names the URL and the HTTP status, and an answer that is a web page (`text/html`, as a site under maintenance may give) is refused, never saved under the file's name. When a file has moved, save it by hand in the folder under the name in the table above and run the command with `--dir <folder>`.

`--dir` imports a kept folder. The import needs all nine files, because it replaces every stored row of Japan's Red List; a folder without one of them is refused, and the store is not changed.

### The CSV files

The files do not share an encoding: the files of April 2026 for vascular plants, bryophytes, algae and fungi are Shift_JIS, and those for birds, reptiles, amphibians and lichens are UTF-8 with a byte order mark. `JapanRedListCsv.Decode` reads a file as UTF-8 when it starts with a byte order mark or is valid UTF-8, else as Shift_JIS (code page 932, from .NET's `CodePagesEncodingProvider`).

Two layouts:

- Birds, reptiles and amphibians: 178 columns under three heading rows. The first row numbers the columns. The second names the block each column is in: `5thRL`, then `RL2020`, `RL2019`, `RL2018`, `RL2017`, `RL2015`, `4thRL`, `3rdRL`, `2ndRL` and `1stRL` (the names and categories each earlier list gave), then habitat (生息・生育環境区分), region (国土地域区分) and threat (存続を脅かす要因) blocks of 0/1 flags and free text. The third has the column names. The reader takes the columns of the `5thRL` block only: 掲載No. (`list_number`, BI0001), 分科会名 (the committee, 爬虫類・両生類 for both reptiles and amphibians, so the group comes from the file), カテゴリーJPN (`category_ja`), カテゴリーENG (EX, EW, CR, EN, VU, NT, DD, LP), 目名 and 科名 (order and family, `higher_taxa`), 和名, 学名 and 判定基準 (`criteria`: IUCN-style criteria such as B2ab, or the Ministry's numbered qualitative criteria ①②③④). The two category columns must give the same category.
- Vascular plants, bryophytes, algae, lichens and fungi: one heading row, カテゴリー (with the code in brackets: 絶滅危惧ⅠＡ類（CR）), 分類群 (the group), 和名 and 学名.

"－" (full-width hyphen-minus) and "―" (horizontal bar) mean "none" in every column.

### The Red List 2020 PDF

`JapanRedList2020Pdf` reads the PDF with PdfPig through `PdfLines` (the summary tables' line reader), which gives each line's words with their x positions. A group starts with its title ("【哺乳類】環境省レッドリスト2020"), and a category with a heading that states the number of rows under it ("●絶滅危惧IA類（CR） 12種", or "26集団", populations, under LP; "―" when there are none). Each page has a running header ("別添資料３", "【哺乳類】") and a footer ("5 / 131 ページ"), which are skipped. Each row has these columns:

- insects: the order (コウチュウ目), then the Japanese name;
- other invertebrates: the phylum, class and order (節足動物門 甲殻綱 エビ目), then the Japanese name;
- the other groups: the Japanese name;
- then the scientific name without authors, subspecies as trinomials ("Prionailurus bengalensis euptilurus"), undescribed species with "sp." and a letter or number ("Assiminea sp. D", "Pungitius sp. 1").

The scientific name's column starts where the heading's count starts (0.2 to 0.5 points to its left), so every word that starts at or after that x, less 3 points, is part of the scientific name. A row whose scientific name has Japanese letters in it, does not start with a letter or is missing, or that has no Japanese name, is not stored and the command lists it. Words before the name that end in 門, 綱 or 目 are the classification (`higher_taxa`). pdftotext's `-layout` mode puts two names on the line above or below their row (オキナワキムラグモ（広義）, アユミコケムシ); PdfLines puts them on their row.

On 2026-10-08 the parser read 5,811 rows under the 103 category headings of all thirteen groups, the number the headings state, and 2,568 rows under the 41 headings of the five groups the import keeps (mammals 89, fishes 260, insects 877, molluscs 1,190, other invertebrates 152), again the number the headings state. No line was left unread. After every import the command prints the two numbers for the five groups it keeps, and a line for each of their headings where the numbers differ. Spot checks: the LP row "房総半島のシロバネカワトンボ（f. edai）を含むアサヒナカワトンボ" (a Japanese name with Latin letters in it) is one row of Mnais pruinosa; the PDF itself misprints "Haemaphysalis pentalagi" as "Haemaphy salispentalagi" (page 62, stored as printed) and, in the birds section that the import does not keep, "Histrionicus histrionicus" as "Histrionicus histrionicu".

### Categories

| `category` | Japanese (as the lists write it) | Meaning |
| --- | --- | --- |
| EX | 絶滅 | Extinct |
| EW | 野生絶滅 | Extinct in the Wild |
| CR | 絶滅危惧IA類 | Critically Endangered (threatened category IA) |
| EN | 絶滅危惧IB類 | Endangered (threatened category IB) |
| CR+EN | 絶滅危惧I類 | Threatened category I, not split into IA and IB: Critically Endangered or Endangered |
| VU | 絶滅危惧II類 | Vulnerable (threatened category II) |
| NT | 準絶滅危惧 | Near Threatened |
| DD | 情報不足 | Data Deficient |
| LP | 絶滅のおそれのある地域個体群 | Threatened local population: a population of the taxon in one region of Japan, not the whole taxon |

`JapanRedListCategory.Code` reads every form the lists write: the plant CSVs use Roman numerals and a full-width letter (絶滅危惧ⅠＡ類（CR）, but 絶滅危惧ⅠB類（EN）), the animal CSVs ASCII (絶滅危惧IA類) with the code in a column of its own, and the PDF's headings ASCII with the code in brackets (絶滅危惧I類（CR+EN）, 情報不足 （DD）). It normalises the text (NFKC, spaces removed), and when a Japanese name and a code in brackets are both given they must agree. `category_ja` keeps the category as written.

The 5th Red List splits 絶滅危惧I類 into IA and IB for every taxon. The Red List 2020 does not for some groups: molluscs have CR 39, EN 28 and CR+EN 234; other invertebrates EN 2 and CR+EN 20 (and in the groups the import takes from the CSV files instead, bryophytes, algae, lichens and fungi used CR+EN in 2020).

### Columns of `japan_listing`

| Column | What it holds |
| --- | --- |
| `row_id` | the row's place in the import: the groups in the Ministry's order (mammals first, fungi last), then the rows in their file's order; it changes when a list changes |
| `group_key`, `group_en`, `group_ja` | the group: key, English name, Japanese name as the list writes it |
| `kingdom` | IUCN's spelling, from the group: ANIMALIA; PLANTAE (vascular plants, bryophytes); FUNGI (lichens, fungi); NULL for algae, which IUCN puts in more than one kingdom |
| `list_version`, `list_year` | 'Red List 2020' and 2020, or '5th Red List' and 2025 or 2026 |
| `category`, `category_ja` | the code (table above) and the category as written |
| `japanese_name` | 和名 as written; for an LP row, the place and the name (九州地方のカワネズミ) |
| `scientific_name` | 学名 as written, without authors; full-width letters and punctuation made ASCII (NFKC: "Utricularia ｘ japonica" is stored as "Utricularia x japonica") and spaces collapsed; letters with diacritics kept ("Cladonia koyaënsis") |
| `population` | LP rows: the place, `japanese_name` before its last の (九州地方; 本州の太平洋側湖沼系群 for 本州の太平洋側湖沼系群のニシン) |
| `higher_taxa` | as written: order and family in the animal CSVs (カモ目 カモ科), the order for Red List 2020 insects (コウチュウ目), phylum, class and order for Red List 2020 other invertebrates (節足動物門 甲殻綱 エビ目); NULL for the other groups |
| `criteria` | 判定基準 of the animal CSVs as written (B2ab, A2 C1, ①②, "A2 ＋付加的事情"); NULL for the other groups |
| `list_number` | 掲載No. of the animal CSVs (BI0001, RE0044, AM0001); NULL for the other groups |
| `source_file`, `source_url` | the file the row was read from and its URL |
| `source_page`, `source_line` | PDF rows: the page and the line's number on it (top to bottom, header included); CSV rows: no page, and the row's number in the file (heading rows included) |

The import of 2026-10-08: 5,830 rows, 5,772 taxa and 58 threatened local populations.

| Group | EX | EW | CR | EN | CR+EN | VU | NT | DD | LP | Rows |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Mammals (2020) | 7 | 0 | 12 | 13 | 0 | 9 | 17 | 5 | 26 | 89 |
| Birds (2026) | 14 | 0 | 23 | 39 | 0 | 46 | 30 | 18 | 0 | 170 |
| Reptiles (2026) | 0 | 0 | 6 | 13 | 0 | 17 | 18 | 7 | 2 | 63 |
| Amphibians (2026) | 0 | 0 | 7 | 31 | 0 | 22 | 15 | 2 | 0 | 77 |
| Brackish and freshwater fishes (2020) | 3 | 1 | 71 | 54 | 0 | 44 | 35 | 37 | 15 | 260 |
| Insects (2020) | 4 | 0 | 75 | 107 | 0 | 185 | 351 | 153 | 2 | 877 |
| Molluscs (2020) | 19 | 0 | 39 | 28 | 234 | 328 | 440 | 89 | 13 | 1,190 |
| Other invertebrates (2020) | 1 | 0 | 0 | 2 | 20 | 43 | 42 | 44 | 0 | 152 |
| Vascular plants (2025) | 26 | 10 | 539 | 526 | 0 | 700 | 377 | 44 | 0 | 2,222 |
| Bryophytes (2025) | 4 | 0 | 25 | 73 | 0 | 71 | 41 | 75 | 0 | 289 |
| Algae (2025) | 4 | 1 | 22 | 40 | 0 | 17 | 26 | 68 | 0 | 178 |
| Lichens (2025) | 3 | 0 | 6 | 28 | 0 | 3 | 14 | 99 | 0 | 153 |
| Fungi (2025) | 20 | 0 | 2 | 8 | 0 | 3 | 8 | 69 | 0 | 110 |
| All | 105 | 12 | 827 | 962 | 254 | 1,488 | 1,414 | 710 | 58 | 5,830 |

Names: 507 rows are animal trinomials (subspecies without a rank word), 611 have "var.", "subsp." or "f.", 11 are hybrids with "x", and 142 are undescribed or unnamed taxa with "sp." or "gen. & sp." ("Marginellidae gen. &. sp.", "Stereophaedusa sp. (Sm)"). Scientific names do not repeat, except among LP rows: the 58 LP rows name 44 taxa (mammals: 26 populations of 12 taxa, such as five populations of Ursus thibetanus japonicus), and in the 2026-10-08 lists no taxon with an LP row also has a row for the whole taxon.

### What is not stored

- the habitat, region and threat columns of the animal CSVs, and their free text;
- the earlier lists' names and categories in the animal CSVs (Red List 2020 back to the 1st Red List);
- the Red List 2020's rows of the groups the 5th Red List now covers, and the authors in its plant sections;
- 分科会名 and 分類群 (the group comes from the file);
- the Red Data Books (the Ministry's descriptions of each taxon).

### Showing it on the site

- `category` is a code; the Japanese category is in `category_ja`, and the English meanings are in the table above. Show CR+EN as Critically Endangered or Endangered, not as one of the two.
- An LP row is a threatened local population, not a status of the whole taxon: show it as a threatened local population in the place `population` names (in Japanese; the list gives no English name for the place), of the taxon in `scientific_name`.
- `list_version` and `list_year` say which edition a row comes from: the Red List 2020 groups are five or six years older than the 5th Red List groups.

## Web UI

The Data sources page has a card for the store, with the number of NatureServe records and ECOS listings. The "Update the public species site" workflow has a step for each command before the site build (except `statuses red-lists-import`, which has none yet), each with a light: NatureServe is blue while a download is under way, and either step is amber when its last download is more than 30 days old. The site build's light counts the store as one of its inputs.

## France: `statuses france-import`

### Sources

- The BDC Statuts ("Base de connaissance « Statuts » des espèces"), kept by PatriNat (OFB, MNHN, CNRS and IRD): every status of the taxa of TAXREF in France, its overseas territories, regions and departments, one row per taxon, status type and place, with the document that gives it. Version 18 is built on TAXREF v18. `BDC.zip` (32.6 MB; the server's date for it is 2025-11-28) holds `BDC_18/bdc_18_01.csv` (329 MB, 447,664 rows, 30 columns, dated 24 July 2025) and the list of the 24 status types, as CSV and as xlsx.
- TAXREF v18, the French national taxonomic reference, also kept by PatriNat: `TAXREF_v18_2025.zip` (60.6 MB) holds `TAXREFv18.txt` (708,685 names) and `TAXREF_LIENS.txt` (2,016,747 links from a name to its id in another database), with 6 smaller files and the methodology report as a PDF.

Code: `FranceImportCommand.cs` (the command, the download and the summary), `FranceBdc.cs` (reading the BDC, the documents, which red list row is current, the BDC's citation), `FranceRedListRemark.cs` (the parts of a red list remark), `FranceTaxref.cs` (reading TAXREF, its citation), `FranceTerritories.cs` (English names of the places) and `StatusListStore.France.cs`. Tests: `FranceStatusesTests`.

### Licence and citations

The owner of this project decided in October 2026 that both datasets are used under the Licence Ouverte / Open Licence 2.0 (Etalab), https://www.etalab.gouv.fr/licence-ouverte-open-licence/, the licence that data.gouv.fr lists for them:

- BDC Statuts: https://www.data.gouv.fr/datasets/statuts-reglementaires-et-de-conservation-des-especes ("Statuts réglementaires et de conservation des espèces", published by Système d'information sur la biodiversité).
- TAXREF: https://www.data.gouv.fr/datasets/referentiel-taxonomique-taxref-1 ("Référentiel Taxonomique TaxRef", same publisher). A second data.gouv.fr page for TAXREF, https://www.data.gouv.fr/datasets/referentiel-taxonomique-taxref, published by the MNHN, lists the Licence Ouverte 1.0.

TAXREF's own terms of use (taxref.mnhn.fr, "Conditions d'utilisation", as archived in July 2025) put TAXREF under the Licence Ouverte, allow any use when TAXREF is cited, and do not forbid redistribution, though PatriNat prefers that people download TAXREF from its own site. GBIF's description of its TAXREF dataset still quotes older download conditions from INPN, which asked that no part of TAXREF be put online without PatriNat's permission.

`status_source` has two rows: `france` for the BDC and `taxref` for TAXREF. Each has its data.gouv.fr page in `url`, the licence's name and address in `licence`, and the kept file's name in `version`. Their citations, as stored after importing the files of November 2025:

> Gargominy, O. & Régnier, C. 2025. Base de connaissance "Statuts" des espèces en France. Version pour TAXREF v18.0. PatriNat (OFB-MNHN-CNRS-IRD). Archive contenant trois fichiers. [version du 24 juillet 2025]

> TAXREF [Eds] 2025. TAXREF v18.0, référentiel taxonomique pour la France. PatriNat (OFB-CNRS-MNHN-IRD), Muséum national d'Histoire naturelle, Paris. Archive de téléchargement contenant 8 fichiers générés le 9 janvier 2025. https://inpn.mnhn.fr/telechargement/referentielEspece/taxref/18.0/menu

Where the citation forms come from:

- The BDC's download page on INPN loaded its citation from DOCS-Web document 232324 (`https://inpn.mnhn.fr/docs-web/docs/DocJson/232324`). The Internet Archive has that answer for version 14 (2021), version 16 (3 April 2023) and version 17 (29 May 2024), in the same form since 2023: "Gargominy, O. & Régnier, C. 2024. Base de connaissance "Statuts" des espèces en France. Version pour TAXREF v17.0. PatriNat (OFB-MNHN-CNRS-IRD). Archive contenant deux fichiers. [version du 29 mai 2024]". No copy for version 18 was found, so `FranceBdc.Citation` fills in that form from the zip: the version from the CSV's name (`bdc_18_01.csv`), the year and the date from the CSV's date in the zip, and the number of files in the zip (three in version 18, which adds the xlsx). It is built on PatriNat's form; it is not PatriNat's wording for version 18.
- TAXREF's form is the one that taxref.mnhn.fr gave for TAXREF v18.0 (archived in July 2025). `FranceTaxref.Citation` fills it in from the zip: the version from `TAXREFv18.txt`, the date of that file in the zip, and the number of .txt and .csv files (8).
- The BDC's guide (Régnier, C. & Gargominy, O. 2018. Diffusion des statuts des espèces : principes et objectifs. Rapport Patrinat 2018-109) gives only the reference of the guide itself. INPN's pages ask for "MNHN & OFB [Ed]. 2003-2025. Inventaire national du patrimoine naturel (INPN), Site web : https://inpn.mnhn.fr" for the site's content.

Each list also has its own citation, in `france_document.citation` (plain text) and `citation_html` (as the BDC gives it). The chapters of the national red list are cited as, for example, "UICN Comité français, MNHN, LPO, SEOF & ONCFS. 2016. La Liste rouge des espèces menacées en France - Chapitre Oiseaux de France métropolitaine. 31 pp."

### Download

MNHN's sites (inpn.mnhn.fr and taxref.mnhn.fr) have been down or behind a Cloudflare challenge since a cyberattack in July 2025, and the download links on the data.gouv.fr pages point to inpn.mnhn.fr. PatriNat lists the files on a temporary download page, https://www.patrinat.fr/fr/page-temporaire-de-telechargement-des-referentiels-de-donnees-lies-linpn-7353, which says that they will be removed when INPN is back. The command's defaults are the addresses on that page:

| Option | Default |
| --- | --- |
| `--bdc-url` | https://assets.patrinat.fr/files/referentiel/BDC.zip |
| `--taxref-url` | https://assets.patrinat.fr/files/referentiel/TAXREF_v18_2025.zip |

The command sends a HEAD request for each file and names the kept copy after the file's name in the address and the date the server gives for the file (Last-Modified): `BDC-2025-11-28.zip` and `TAXREF_v18_2025-2025-11-28.zip`. When the status lists folder already has a file of that name, the command reads it and does not download it again. Each file is downloaded as a `.part` file and renamed when it is complete.

The command stops without changing the store when an address answers with an error (HTTP 404 when PatriNat removes the file), when the server answers with a web page, or when the file downloaded is not a zip file. Its message gives the address, the error, the temporary download page, the two data.gouv.fr pages, and the option that takes the file's new address (`--bdc-url` or `--taxref-url`).

`--file` and `--taxref-file` import kept copies and must be given together. Give the TAXREF version that the BDC was built on (TAXREF v18 for `BDC_18`): the BDC's `CD_REF` is the accepted name in that version. The summary warns when the two versions differ, and counts the names on which they differ (2 in version 18, both in protection lists).

Importing kept copies of both zips takes about 20 seconds. The French tables take about 66 MB of the store.

### What is stored

| Type (`type_code`) | What it is | Rows | Taxa (`cd_ref`) | Documents | Places |
| --- | --- | ---: | ---: | ---: | ---: |
| LRN | Liste rouge nationale: the national red list, for metropolitan France and each overseas territory | 23,020 | 20,615 | 35 | 13 |
| LRR | Listes rouges régionales: the regional red lists that UICN France has endorsed, by region (before and after the merger of regions in 2016) | 75,650 | 19,222 | 141 | 25 |
| PR | Protection régionale: regional protection lists | 4,632 | 2,402 | 26 | 25 |
| PD | Protection départementale: departmental protection lists | 4,372 | 3,002 | 41 | 52 |
| PN | Protection nationale: national protection, in metropolitan France, the overseas departments and collectivities, and the whole of France (ETATFRA: marine mammals, sea turtles, marine invertebrates and some fish) | 3,871 | 3,225 | 34 | 11 |
| POM | Protection COM: protection in the overseas collectivities (the provinces of New Caledonia, French Polynesia, Saint Pierre and Miquelon, Clipperton) | 3,036 | 2,292 | 6 | 6 |

In all, 114,581 rows of 34,816 taxa, 275 documents and 104 places.

Not stored:

- IUCN's global and European red lists (LRM, 19,461 rows; LRE, 5,831), which the site has from IUCN.
- The international conventions and EU directives (BERN, BONN, BARC, OSPAR, DH and DO; 3,750 rows). Their remarks say which populations a listing covers ("excepté la population estonienne ..."), so a row means little without its remark, and the remarks are long.
- ZNIEFF determinant species (ZDET, 173,912 rows), national action plans (PNA and exPNA), the regulations on introductions, control and trade (REGLII, REGLSO, REGL and REGLLUTTE; 125,345 rows), and the lists of taxa whose records are sensitive (SENSNAT, SENSREG and SENSDEP).
- The remarks of the protection lists: notes and comments of up to 599 characters, some with personal communications. The red lists' remarks are codes and are stored, except 5 regional remarks that are sentences ("Espèce erratique non autochtone dans la région").
- The columns NOM_COMPLET_HTML and NOM_VALIDE_HTML (names as HTML), GROUP1_INPN and GROUP2_INPN (INPN's informal groups), CD_SUP, THEMATIQUE and TYPE_VALUE (the same in every row). `france_status_type` keeps every type's label and group, the types not stored included.

From TAXREF, for the 34,814 accepted names of the stored rows (two accepted names of protection rows are under another name in TAXREF): 173,261 names in `france_taxref_name` (the accepted names, their synonyms and the other names under them), and 265,245 links in `france_taxref_link`: GBIF 163,822, Catalogue of Life 87,986, IUCN Red List 11,701, and BirdLife 1,736.

### The national red list

Rows by place, with the English name in `france_territory.name_en`:

| Place (`territory_code`) | `name` in the BDC | Rows | Taxa | EX | RE | CR* | CR | EN | VU | NT | LC | DD | NA | NE |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Metropolitan France (TERFXFR) | France métropolitaine | 12,278 | 11,903 | 6 | 44 | 28 | 122 | 377 | 687 | 746 | 6,855 | 1,612 | 1,801 | 0 |
| Guadeloupe (TER971) | Guadeloupe | 2,451 | 2,447 | 10 | 11 | 7 | 85 | 114 | 130 | 158 | 1,156 | 589 | 191 | 0 |
| Réunion (TER974) | Réunion | 2,235 | 2,235 | 22 | 44 | 15 | 154 | 165 | 171 | 93 | 969 | 482 | 120 | 0 |
| New Caledonia (TER988) | Nouvelle-Calédonie | 1,693 | 1,683 | 1 | 0 | 0 | 153 | 302 | 224 | 219 | 691 | 84 | 0 | 19 |
| French Guiana (TER973) | Guyane | 1,496 | 1,496 | 0 | 1 | 0 | 24 | 58 | 68 | 91 | 924 | 252 | 78 | 0 |
| Mayotte (TER976) | Mayotte | 998 | 998 | 0 | 0 | 0 | 36 | 47 | 208 | 96 | 399 | 139 | 73 | 0 |
| Martinique (TER972) | Martinique | 747 | 747 | 7 | 9 | 6 | 62 | 53 | 42 | 56 | 207 | 126 | 179 | 0 |
| French Polynesia (TER987) | Polynésie française | 693 | 693 | 37 | 1 | 14 | 125 | 164 | 73 | 60 | 102 | 63 | 54 | 0 |
| French Southern and Antarctic Lands: Scattered Islands (TER984B) | TAAF : Îles éparses | 244 | 244 | 0 | 2 | 0 | 1 | 7 | 10 | 6 | 65 | 118 | 35 | 0 |
| French Southern and Antarctic Lands: sub-Antarctic islands (TER984A) | TAAF : Îles sub-antarctiques | 154 | 154 | 1 | 1 | 0 | 9 | 12 | 5 | 4 | 50 | 14 | 58 | 0 |
| French Southern and Antarctic Lands: Adélie Land (TER984C) | TAAF : Terre-Adélie | 25 | 25 | 0 | 0 | 0 | 1 | 1 | 4 | 0 | 5 | 1 | 13 | 0 |
| Wallis and Futuna (TER986) | Wallis et Futuna | 4 | 4 | 0 | 0 | 0 | 0 | 1 | 2 | 0 | 0 | 1 | 0 | 0 |
| Saint Martin (TER978) | Saint-Martin | 2 | 2 | 0 | 0 | 0 | 0 | 2 | 0 | 0 | 0 | 0 | 0 | 0 |
| All places | | 23,020 | 20,615 | 84 | 113 | 70 | 772 | 1,303 | 1,624 | 1,529 | 11,423 | 3,481 | 2,602 | 19 |

The codes (`code`, with the French label in `label`) are IUCN's categories as UICN France applies them to a region: EX "Eteinte au niveau mondial" (extinct worldwide), RE "Disparue au niveau régional" (regionally extinct), CR* "On ne sait pas si l'espèce n'est pas éteinte ou disparue" (CR, and possibly extinct or regionally extinct; 70 rows), CR, EN, VU, NT, LC, DD, NA "Non applicable" and NE "Non évaluée". The regional red lists also use "RE?" (59 rows).

#### Remarks

The remark (`RQ_STATUT`) of a red list row is stored as given in `remark`, and `FranceRedListRemark.Parse` reads its parts. A remark has, in this order, each part optional: the criteria, the category and criteria before a regional adjustment, or the letter of the reason for NA; then " - " and the population or presence that the row assesses. 10,101 national rows have a remark (1,350 of them only "/"), and every other national remark has at least one part that the parser reads.

| Remark | Columns |
| --- | --- |
| `B2ab(iii)` | `criteria` B2ab(iii) |
| `pr. D2` | `criteria` pr. D2: an NT taxon that came close to meeting criterion D2 ("proche") |
| `VU D1 (-1) - Nicheur` | `criteria` D1, `adjusted_from` VU, `adjustment` -1 (the taxon met VU D1 and was moved down one category for the region, the code is NT), `population_fr` Nicheur, `population` breeding |
| `NT (pr. D1) (-1) - Visiteur régulier` | `criteria` pr. D1, `adjusted_from` NT, `adjustment` -1, `population_fr` Visiteur régulier, `population` visiting |
| `b - Visiteur` | `na_reason` b, `population_fr` Visiteur, `population` visiting |
| `Hivernant` | `population_fr` Hivernant, `population` wintering |

National rows by remark part: 5,112 have criteria, 144 have a regional adjustment (`adjusted_from`, `adjustment`), and 2,425 NA rows have a reason: a 1,486, b 688, c 105, d 146. UICN France's national red list defines NA as a species that is not assessed because it was introduced in recent times (generally after 1500; reason a) or occurs in the region only occasionally or marginally (reason b). Reasons c and d are used only on bird rows of wintering and passage populations; as far as is known (not checked against the lists), c is a species present regularly in winter or on passage that does not meet the criteria of a significant presence, and d one for which too few data are available to confirm that it does.

Populations (`population_fr` as the BDC writes it, `population` the key):

| `population_fr` | `population` | National rows | English, for the site |
| --- | --- | ---: | --- |
| Nicheur | breeding | 866 | breeding |
| Nicheur certain, Reproducteur certain | breeding | 137, 146 | breeding (confirmed) |
| Nicheur probable, Reproducteur probable | breeding | 2, 7 | breeding (probable) |
| Hivernant | wintering | 185 | wintering |
| Visiteur | visiting | 509 | on passage in metropolitan France (415 rows; the 2011 bird list calls them "de passage"); visiting in French Guiana (94) |
| Visiteur régulier | visiting | 172 | regular visitor |
| Visiteur occasionnel | visiting | 228 | occasional visitor |
| Visiteur et possiblement nicheur; Visiteur régulier et reproducteur probable; Visiteur régulier et nicheur probable | visiting | 9, 9, 2 | visitor, possibly or probably breeding |
| Inconnu | NULL | 9 | presence unknown (French Guiana) |

A row with no population is about the whole taxon. Most regional lists of birds are of breeding birds and give no population in their remarks; for a bird row (`taxclass` Aves) with none, `population` comes from its document's title when the title names one population ("Liste rouge des oiseaux nicheurs de Franche-Comté": breeding; `france_document.bird_population`, `FranceBdc.TitlePopulation`). 2,440 regional rows get their population this way; `population_fr` stays NULL. No national row does.

#### Birds of metropolitan France: two lists

Two national lists of the birds of metropolitan France are in the BDC: the 2011 list (`cd_doc` 31343, 600 rows) and the 2016 list (`cd_doc` 165208, 317 rows). In version 18, the 2016 list has only its breeding rows (Nicheur), and the 2011 list only its wintering rows (Hivernant, 185) and its passage rows (Visiteur, 415): the BDC does not have the 2016 list's wintering and passage assessments. 219 taxa have rows from both lists, always for different populations (Crex crex: EN breeding in 2016, NA passage in 2011), so the 2011 rows are the current rows for wintering and passage birds.

#### The current row

`is_current` (`FranceBdc.MarkCurrent`) marks the row to show for a taxon. Red list rows are grouped by type (LRN, LRR), accepted name (`cd_ref`), place and `population` (breeding, wintering, visiting, or none). In each group:

1. the rows of the newest document are current (by `france_document.year`; a document with no year is older than any);
2. of those, when one is the row of the accepted name itself (`cd_nom` = `cd_ref`), only the rows of the accepted name are current.

Other rows are 0, and protection rows are NULL (every protection row applies).

| | LRN | LRR |
| --- | ---: | ---: |
| Current rows | 22,976 | 71,676 |
| Rows of an older list for the same taxon, place and population | 0 | 3,146 |
| Rows of another name of a taxon that has a row under its accepted name in the same list | 44 | 828 |
| Taxa with two or more current rows for one place and population | 7 | 267 |

The 44 national rows are rows of two names that TAXREF now treats as one taxon, in one list: 35 in the vascular plants of metropolitan France, nearly all a species and its nominate subspecies (Pinus mugo subsp. mugo, NT, is a synonym of Pinus mugo, LC, in TAXREF v18, so the row of Pinus mugo is the current one), and 9 in the vascular plants of New Caledonia, two names of one palm or fern (Kentiopsis oliviformis and Chambeyronia oliviformis). Of the 7 national taxa with two current rows, 6 are a species and its nominate subspecies in metropolitan France, both LC and neither of them TAXREF's accepted name (Sarcocornia perennis and Sarcocornia perennis subsp. perennis); and Sarcochilus hillii has two rows in the 2024 list of the vascular plants of New Caledonia, DD and VU. Of the 267 regional taxa, 188 are birds in the 2020 list of Provence-Alpes-Côte d'Azur, which assesses breeding, passage and wintering birds but whose rows in the BDC do not say which population each is about (Pluvialis squatarola: LC and NA).

The rule compares lists only for the same place code. A region before the merger of 2016 (`admin_level` "Ancienne région", such as Alsace) has its own code, so the current rows of an old region's list stay current beside the list of the new region that contains it (Grand Est).

### Matching taxa to IUCN taxa

TAXREF's ids first, then names:

1. `france_taxref_link` rows with `source` "IUCN Red List" (an IUCN taxon id) or "IUCN Red List > BirdLife" (BirdLife's id, which is the IUCN taxon id of a bird). Many birds have only the BirdLife link (Crex crex: 22692543). Look them up by the status row's `cd_ref`: the links are on TAXREF names, and a link on a synonym counts for its accepted name. Of the 20,615 national red list taxa, 9,070 have an IUCN id this way, and 8,909 have an id that is a taxon id in the IUCN Red List 2026-1. 172 national taxa have two or more IUCN ids (131 with two or more in 2026-1), because TAXREF puts two names that IUCN assesses apart under one accepted name (Eliomys quercinus, 7618, and Eliomys melanurus, 7619). For those, prefer the link on the accepted name itself (`cd_nom` = `cd_ref`; 162 of the 172 have one), then the link on the status row's `cd_nom`, then any.
2. Names: the status row's `name` (the name the list used), then the `france_taxref_name` names with the same `cd_ref` (the accepted name and its synonyms). Matching only exact names, without kingdoms, 735 more national taxa match a scientific name of IUCN 2026-1 by their accepted name and 233 by another TAXREF name. The other 10,738 match no IUCN name; most are taxa that IUCN has not assessed.

The Catalogue of Life and GBIF links are there for matching by those ids.

### Using the rows on the site

- Label a place with `france_territory.name_en` ("Metropolitan France", "French Guiana", "French Southern and Antarctic Lands: Scattered Islands"), else its French `name` (the departments of metropolitan France have no English name).
- Show `population` (breeding, wintering, passage or visiting) beside a bird's category; a bird can have one current row for each population in one place.
- Show the rows with `is_current` = 1, and cite each row's document (`france_document`) beside the BDC itself.

## Web UI

The Data sources page has a card for the store, with the number of NatureServe records and ECOS listings. The "Update the public species site" workflow has a step for each command before the site build, each with a light: NatureServe is blue while a download is under way, and either step is amber when its last download is more than 30 days old. The site build's light counts the store as one of its inputs. `statuses france-import` has no step in the workflow yet, and `site build-db` does not read the French tables yet.
