# Status lists: conservation statuses from other systems

The status lists store holds conservation statuses from systems other than the IUCN Red List, for the public species site (`site build-db` reads it into `other_status`; see `docs/public-site.md`):

- NatureServe Explorer: the NatureServe global rank (G rank), the national and subnational (state, province and territory) ranks in the United States and Canada, and the US Endangered Species Act, COSEWIC and SARA statuses that NatureServe records (`statuses natureserve-fetch`).
- ECOS, the US Fish and Wildlife Service's Environmental Conservation Online System: the list of species, subspecies and populations listed under the US Endangered Species Act (`statuses ecos-import`).
- The New Zealand Threat Classification System database (nztcs.org.nz, Department of Conservation, CC BY 4.0): the current assessments (`statuses nztcs-import`).
- SALVE (salve.icmbio.gov.br), ICMBio's system for the national assessments of the extinction risk of Brazil's fauna: the current assessment of each species and subspecies (`statuses salve-import`).
- JNCC's Conservation Designations for UK Taxa (Joint Nature Conservation Committee, Open Government Licence v3.0): one row per taxon and designation, for the GB and England red lists, Birds of Conservation Concern, Nationally Rare and Scarce, the UK and country priority species lists, the Wildlife and Countryside Act and other UK legislation, and the international conventions and EU directives as they apply to UK taxa (`statuses jncc-import`).
- The Checklist of CITES Species (checklist.cites.org, compiled by UNEP-WCMC for the CITES Secretariat): the current CITES Appendix listings of every taxon in the Appendices, with the listings each taxon inherits from a higher taxon (`statuses cites-import`).
- PatriNat's BDC Statuts (base of species statuses in France, Licence Ouverte 2.0): the French national red list (Liste rouge nationale, by UICN France, MNHN and OFB) of metropolitan France and the overseas territories, the regional red lists, and the national, overseas, regional and departmental protection lists, with each listed taxon's TAXREF names and its IUCN, BirdLife, Catalogue of Life and GBIF ids from TAXREF (`statuses france-import`).

Code: `BeastieBot3/StatusLists/`. The schema is `StatusListStore.Ddl`, with a comment on every column.

The six imports (`statuses ecos-import`, `nztcs-import`, `salve-import`, `jncc-import`, `cites-import` and `france-import`) share one run, `StatusListImport.RunAsync`. It imports the file given with `--file`, or downloads the source into the status lists folder (written as a `.part` file and renamed when complete), as `<stem>-<yyyy-MM-dd>.<extension>` or, when the spec has `FindFileName`, under the name the source gives its file. It then reads the file, stops without changing the store when the file has no rows, replaces the source's rows and its `status_source` row in one transaction, and prints a table of counts. Each command passes a `StatusListImportSpec` with its download, reader, store method and messages, and `StatusListDownload` creates the HTTP client and writes the downloaded files. A spec's `SourceForFile` changes the `status_source` row for the file imported (JNCC uses it for the file's URL and the year in its attribution; the BDC for its citation, which has the version and date of the file). The store's methods for each source are in `StatusListStore.NatureServe.cs`, `StatusListStore.Ecos.cs`, `StatusListStore.Nztcs.cs`, `StatusListStore.Salve.cs`, `StatusListStore.Jncc.cs`, `StatusListStore.Cites.cs` and `StatusListStore.France.cs`.

## Files

| What | Where |
| --- | --- |
| Store | `Datastore:status_lists_sqlite` in paths.ini, else `status_lists.sqlite` in the datastore folder (`PathsService.GetStatusListsPath`, `ResolveStatusListsPath`) |
| Downloaded files | `Datasets:status_lists_dir`, else a `status-lists` folder in the datastore folder (`GetStatusListsDownloadDir`) |

## Tables

| Table | One row per |
| --- | --- |
| `status_source` | source (`natureserve`, `ecos`, `nztcs`, `salve`, `jncc`, `cites`, `france`, `taxref`): title, URL, licence, citation with the access date, version, when it was last fetched, row count. The site's credits can be built from it. |
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
