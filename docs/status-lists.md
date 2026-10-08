# Status lists: conservation statuses from other systems

The status lists store holds conservation statuses from systems other than the IUCN Red List, for the public species site (`site build-db` reads it into `other_status`; see `docs/public-site.md`):

- NatureServe Explorer: the NatureServe global rank (G rank), the national ranks in the United States and Canada, and the US Endangered Species Act, COSEWIC and SARA statuses that NatureServe records (`statuses natureserve-fetch`).
- ECOS, the US Fish and Wildlife Service's Environmental Conservation Online System: the list of species, subspecies and populations listed under the US Endangered Species Act (`statuses ecos-import`).
- The New Zealand Threat Classification System database (nztcs.org.nz, Department of Conservation, CC BY 4.0): the current assessments (`statuses nztcs-import`).
- SALVE (salve.icmbio.gov.br), ICMBio's system for the national assessments of the extinction risk of Brazil's fauna: the current assessment of each species and subspecies (`statuses salve-import`).
- JNCC's Conservation Designations for UK Taxa (Joint Nature Conservation Committee, Open Government Licence v3.0): one row per taxon and designation, for the GB and England red lists, Birds of Conservation Concern, Nationally Rare and Scarce, the UK and country priority species lists, the Wildlife and Countryside Act and other UK legislation, and the international conventions and EU directives as they apply to UK taxa (`statuses jncc-import`).

Code: `BeastieBot3/StatusLists/`. The schema is `StatusListStore.Ddl`, with a comment on every column.

The four imports (`statuses ecos-import`, `nztcs-import`, `salve-import` and `jncc-import`) share one run, `StatusListImport.RunAsync`. It imports the file given with `--file`, or downloads the source into the status lists folder (written as a `.part` file and renamed when complete), as `<stem>-<yyyy-MM-dd>.<extension>` or, when the spec has `FindFileName`, under the name the source gives its file. It then reads the file, stops without changing the store when the file has no rows, replaces the source's rows and its `status_source` row in one transaction, and prints a table of counts. Each command passes a `StatusListImportSpec` with its download, reader, store method and messages, and `StatusListDownload` creates the HTTP client and writes the downloaded files. A spec's `SourceForFile` changes the `status_source` row for the file imported (JNCC uses it for the file's URL and the year in its attribution). The store's methods for each source are in `StatusListStore.NatureServe.cs`, `StatusListStore.Ecos.cs`, `StatusListStore.Nztcs.cs`, `StatusListStore.Salve.cs` and `StatusListStore.Jncc.cs`.

## Files

| What | Where |
| --- | --- |
| Store | `Datastore:status_lists_sqlite` in paths.ini, else `status_lists.sqlite` in the datastore folder (`PathsService.GetStatusListsPath`, `ResolveStatusListsPath`) |
| Downloaded files | `Datasets:status_lists_dir`, else a `status-lists` folder in the datastore folder (`GetStatusListsDownloadDir`) |

## Tables

| Table | One row per |
| --- | --- |
| `status_source` | source (`natureserve`, `ecos`, `nztcs`, `salve`, `jncc`): title, URL, licence, citation with the access date, version, when it was last fetched, row count. The site's credits can be built from it. |
| `status_sync_state` | key of the NatureServe download's progress (`natureserve_pass_*`, `natureserve_completed*`) |
| `natureserve_species` | NatureServe record (`element_global_id`): a species, subspecies, variety or population |
| `natureserve_synonym` | synonym NatureServe lists for a record |
| `natureserve_partition` | name prefix of the NatureServe download under way, with the next page to ask for. Empty between downloads. |
| `ecos_listing` | ESA listing (`entity_id`, the ECOS Listed Species ID) |
| `ecos_name` | name an ECOS listing's scientific name gives, the main name included |
| `nztcs_assessment` | current NZTCS assessment (`assessment_id`), with the scientific name `NztcsApi.ChooseName` gives |
| `salve_assessment` | current SALVE assessment of a species or subspecies (`ficha_id`, SALVE's sheet id) |
| `jncc_designation` | row of the Master List sheet of JNCC's Conservation Designations for UK Taxa: one taxon and one of its designations (`row_number`, the row's number in the sheet) |

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

## Web UI

The Data sources page has a card for the store, with the number of NatureServe records and ECOS listings. The "Update the public species site" workflow has a step for each command before the site build, each with a light: NatureServe is blue while a download is under way, and either step is amber when its last download is more than 30 days old. The site build's light counts the store as one of its inputs.
