# Status lists: conservation statuses from other systems

The status lists store holds conservation statuses from systems other than the IUCN Red List, for the public species site (`site build-db` reads it into `other_status`; see `docs/public-site.md`):

- NatureServe Explorer: the NatureServe global rank (G rank), the national ranks in the United States and Canada, and the US Endangered Species Act, COSEWIC and SARA statuses that NatureServe records (`statuses natureserve-fetch`).
- ECOS, the US Fish and Wildlife Service's Environmental Conservation Online System: the list of species, subspecies and populations listed under the US Endangered Species Act (`statuses ecos-import`).
- The New Zealand Threat Classification System database (nztcs.org.nz, Department of Conservation, CC BY 4.0): the current assessments (`statuses nztcs-import`).
- SALVE (salve.icmbio.gov.br), ICMBio's system for the national assessments of the extinction risk of Brazil's fauna: the current assessment of each species and subspecies (`statuses salve-import`).

Code: `BeastieBot3/StatusLists/`. The schema is `StatusListStore.Ddl`, with a comment on every column.

The three imports (`statuses ecos-import`, `nztcs-import` and `salve-import`) share one run, `StatusListImport.RunAsync`. It imports the file given with `--file`, or downloads the source into the status lists folder (written as a `.part` file and renamed when complete). It then reads the file, stops without changing the store when the file has no rows, replaces the source's rows and its `status_source` row in one transaction, and prints a table of counts. Each command passes a `StatusListImportSpec` with its download, reader, store method and messages, and `StatusListDownload` creates the HTTP client and writes the downloaded files. The store's methods for each source are in `StatusListStore.NatureServe.cs`, `StatusListStore.Ecos.cs`, `StatusListStore.Nztcs.cs` and `StatusListStore.Salve.cs`.

## Files

| What | Where |
| --- | --- |
| Store | `Datastore:status_lists_sqlite` in paths.ini, else `status_lists.sqlite` in the datastore folder (`PathsService.GetStatusListsPath`, `ResolveStatusListsPath`) |
| Downloaded files | `Datasets:status_lists_dir`, else a `status-lists` folder in the datastore folder (`GetStatusListsDownloadDir`) |

## Tables

| Table | One row per |
| --- | --- |
| `status_source` | source (`natureserve`, `ecos`, `nztcs`, `salve`): title, URL, licence, citation with the access date, version, when it was last fetched, row count. The site's credits can be built from it. |
| `status_sync_state` | key of the NatureServe download's progress (`natureserve_pass_*`, `natureserve_completed*`) |
| `natureserve_species` | NatureServe record (`element_global_id`): a species, subspecies, variety or population |
| `natureserve_synonym` | synonym NatureServe lists for a record |
| `natureserve_partition` | name prefix of the NatureServe download under way, with the next page to ask for. Empty between downloads. |
| `ecos_listing` | ESA listing (`entity_id`, the ECOS Listed Species ID) |
| `ecos_name` | name an ECOS listing's scientific name gives, the main name included |
| `nztcs_assessment` | current NZTCS assessment (`assessment_id`), with the scientific name `NztcsApi.ChooseName` gives |
| `salve_assessment` | current SALVE assessment of a species or subspecies (`ficha_id`, SALVE's sheet id) |

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
| `--status` | Prints the progress and sends no requests. |

When a full download finishes, it deletes the stored records it did not see, but only when it stored at least as many records as NatureServe gave as its total. When it stored fewer, it keeps the old records and says so, because a record that paging missed is not a deleted record.

NatureServe last modified 113,413 of the 113,530 records on 2026-10-02 or 2026-10-03, so a refresh after a bulk update by NatureServe is close to a full download.

### What is stored

Per record: `element_global_id`, `unique_id`, `elcode`, `scientific_name`, the primary common name and its language, `g_rank` (as published, for example G3G4, G2T1, G3TNRQ) and `rounded_g_rank`, `classification_status`, kingdom to genus, `informal_taxonomy`, `infraspecies`, `usesa_code`, `cosewic_code`, `sara_code` (the English part of NatureServe's bilingual SARA status, "Endangered" from "Endangered/En voie de disparition") and `sara_code_raw`, `us_n_rank` and `ca_n_rank` (the rounded national ranks of the US and Canada), `nsx_url`, `last_modified`, `fetched_at`, and the synonyms.

Not stored: taxonomic comments and every other narrative text, other common names, subnational (state and province) ranks, and distribution.

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

## Web UI

The Data sources page has a card for the store, with the number of NatureServe records and ECOS listings. The "Update the public species site" workflow has a step for each command before the site build, each with a light: NatureServe is blue while a download is under way, and either step is amber when its last download is more than 30 days old. The site build's light counts the store as one of its inputs.
