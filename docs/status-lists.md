# Status lists: conservation statuses from other systems

The status lists store holds conservation statuses from systems other than the IUCN Red List, for the public species site (`site build-db` reads it into `other_status`; see `docs/public-site.md`):

- NatureServe Explorer: the NatureServe global rank (G rank), the national and subnational (state, province and territory) ranks in the United States and Canada, and the US Endangered Species Act, COSEWIC and SARA statuses that NatureServe records (`statuses natureserve-fetch`).
- ECOS, the US Fish and Wildlife Service's Environmental Conservation Online System: the list of species, subspecies and populations listed under the US Endangered Species Act (`statuses ecos-import`).
- The New Zealand Threat Classification System database (nztcs.org.nz, Department of Conservation, CC BY 4.0): the current assessments (`statuses nztcs-import`).
- SALVE (salve.icmbio.gov.br), ICMBio's system for the national assessments of the extinction risk of Brazil's fauna: the current assessment of each species and subspecies (`statuses salve-import`).
- JNCC's Conservation Designations for UK Taxa (Joint Nature Conservation Committee, Open Government Licence v3.0): one row per taxon and designation, for the GB and England red lists, Birds of Conservation Concern, Nationally Rare and Scarce, the UK and country priority species lists, the Wildlife and Countryside Act and other UK legislation, and the international conventions and EU directives as they apply to UK taxa (`statuses jncc-import`).
- The Checklist of CITES Species (checklist.cites.org, compiled by UNEP-WCMC for the CITES Secretariat): the current CITES Appendix listings of every taxon in the Appendices, with the listings each taxon inherits from a higher taxon (`statuses cites-import`).
- The Red List of Japan's Ministry of the Environment (環境省, Public Data License 1.0): one row per taxon or threatened local population, from the 5th Red List (birds, reptiles, amphibians, vascular plants, bryophytes, algae, lichens, fungi) and the Red List 2020 (mammals, brackish and freshwater fishes, insects, molluscs, other invertebrates) (`statuses japan-import`).

Code: `BeastieBot3/StatusLists/`. The schema is `StatusListStore.Ddl`, with a comment on every column.

The six imports (`statuses ecos-import`, `nztcs-import`, `salve-import`, `jncc-import`, `cites-import` and `japan-import`) share one run, `StatusListImport.RunAsync`. It imports the file given with `--file`, or downloads the source into the status lists folder (written as a `.part` file and renamed when complete), as `<stem>-<yyyy-MM-dd>.<extension>` or, when the spec has `FindFileName`, under the name the source gives its file. A source of several files (`ImportsFolder`: Japan's nine files) is kept as a folder, `<stem>-<yyyy-MM-dd>`, and its command names a kept folder with `--dir`. It then reads the file, stops without changing the store when the file has no rows, replaces the source's rows and its `status_source` row in one transaction, and prints a table of counts. Each command passes a `StatusListImportSpec` with its download, reader, store method and messages, and `StatusListDownload` creates the HTTP client and writes the downloaded files. A spec's `SourceForFile` changes the `status_source` row for the file imported (JNCC uses it for the file's URL and the year in its attribution). The store's methods for each source are in `StatusListStore.NatureServe.cs`, `StatusListStore.Ecos.cs`, `StatusListStore.Nztcs.cs`, `StatusListStore.Salve.cs`, `StatusListStore.Jncc.cs`, `StatusListStore.Cites.cs` and `StatusListStore.Japan.cs`.

## Files

| What | Where |
| --- | --- |
| Store | `Datastore:status_lists_sqlite` in paths.ini, else `status_lists.sqlite` in the datastore folder (`PathsService.GetStatusListsPath`, `ResolveStatusListsPath`) |
| Downloaded files | `Datasets:status_lists_dir`, else a `status-lists` folder in the datastore folder (`GetStatusListsDownloadDir`) |

## Tables

| Table | One row per |
| --- | --- |
| `status_source` | source (`natureserve`, `ecos`, `nztcs`, `salve`, `jncc`, `cites`, `japan`): title, URL, licence, citation with the access date, version, when it was last fetched, row count. The site's credits can be built from it. |
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
| `japan_listing` | taxon or threatened local population in the latest Red List of Japan's Ministry of the Environment for its group (`row_id`, the row's place in the import) |

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

The Data sources page has a card for the store, with the number of NatureServe records and ECOS listings. The "Update the public species site" workflow has a step for each command before the site build, each with a light: NatureServe is blue while a download is under way, and either step is amber when its last download is more than 30 days old. The site build's light counts the store as one of its inputs.
