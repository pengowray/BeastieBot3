# Status lists: conservation statuses from other systems

The status lists store holds conservation statuses from systems other than the IUCN Red List, for the public species site (`site build-db` reads it into `other_status`; see `docs/public-site.md`):

- NatureServe Explorer: the NatureServe global rank (G rank), the national ranks in the United States and Canada, and the US Endangered Species Act, COSEWIC and SARA statuses that NatureServe records (`statuses natureserve-fetch`).
- ECOS, the US Fish and Wildlife Service's Environmental Conservation Online System: the list of species, subspecies and populations listed under the US Endangered Species Act (`statuses ecos-import`).
- The New Zealand Threat Classification System database (nztcs.org.nz, Department of Conservation, CC BY 4.0): the current assessments (`statuses nztcs-import`).
- SALVE (salve.icmbio.gov.br), ICMBio's system for the national assessments of the extinction risk of Brazil's fauna: the current assessment of each species and subspecies (`statuses salve-import`).
- National and subnational red lists published on GBIF as Darwin Core checklists: 29 lists of 16 countries, chosen in `rules/status-lists/national-red-lists.yml` (`statuses red-lists-import`).

Code: `BeastieBot3/StatusLists/`. The schema is `StatusListStore.Ddl`, with a comment on every column.

The three imports (`statuses ecos-import`, `nztcs-import` and `salve-import`) share one run, `StatusListImport.RunAsync`. It imports the file given with `--file`, or downloads the source into the status lists folder (written as a `.part` file and renamed when complete). It then reads the file, stops without changing the store when the file has no rows, replaces the source's rows and its `status_source` row in one transaction, and prints a table of counts. Each command passes a `StatusListImportSpec` with its download, reader, store method and messages, and `StatusListDownload` creates the HTTP client and writes the downloaded files. The store's methods for each source are in `StatusListStore.NatureServe.cs`, `StatusListStore.Ecos.cs`, `StatusListStore.Nztcs.cs`, `StatusListStore.Salve.cs` and `StatusListStore.RedLists.cs`. `statuses red-lists-import` imports many datasets in one run, so it has its own run (see its section).

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
| `status_source` | source (`natureserve`, `ecos`, `nztcs`, `salve`, and `redlist:<key>` for each red list): title, URL, licence, citation with the access date, version, when it was last fetched, row count. The site's credits can be built from it. |
| `status_sync_state` | key of the NatureServe download's progress (`natureserve_pass_*`, `natureserve_completed*`) |
| `natureserve_species` | NatureServe record (`element_global_id`): a species, subspecies, variety or population |
| `natureserve_synonym` | synonym NatureServe lists for a record |
| `natureserve_partition` | name prefix of the NatureServe download under way, with the next page to ask for. Empty between downloads. |
| `ecos_listing` | ESA listing (`entity_id`, the ECOS Listed Species ID) |
| `ecos_name` | name an ECOS listing's scientific name gives, the main name included |
| `nztcs_assessment` | current NZTCS assessment (`assessment_id`), with the scientific name `NztcsApi.ChooseName` gives |
| `salve_assessment` | current SALVE assessment of a species or subspecies (`ficha_id`, SALVE's sheet id) |
| `red_list_dataset` | national or subnational red list from GBIF (`dataset_key`, the key in `national-red-lists.yml`) |
| `red_list_taxon` | status of a taxon in one of those lists (`dataset_key`, `taxon_id`, `seq`) |
| `red_list_synonym` | synonym a list gives for one of its taxa with a status (`dataset_key`, `taxon_id`) |

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

## Web UI

The Data sources page has a card for the store, with the number of NatureServe records and ECOS listings. `statuses red-lists-import` has no workflow step yet. The "Update the public species site" workflow has a step for each command before the site build, each with a light: NatureServe is blue while a download is under way, and either step is amber when its last download is more than 30 days old. The site build's light counts the store as one of its inputs.
