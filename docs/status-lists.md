# Status lists: conservation statuses from other systems

The status lists store holds conservation statuses from systems other than the IUCN Red List, for the public species site (`site build-db` reads it into `other_status`; see `docs/public-site.md`):

- NatureServe Explorer: the NatureServe global rank (G rank), the national ranks in the United States and Canada, and the US Endangered Species Act, COSEWIC and SARA statuses that NatureServe records (`statuses natureserve-fetch`).
- ECOS, the US Fish and Wildlife Service's Environmental Conservation Online System: the list of species, subspecies and populations listed under the US Endangered Species Act (`statuses ecos-import`).
- The New Zealand Threat Classification System database (nztcs.org.nz, Department of Conservation, CC BY 4.0): the current assessments (`statuses nztcs-import`).
- SALVE (salve.icmbio.gov.br), ICMBio's system for the national assessments of the extinction risk of Brazil's fauna: the current assessment of each species and subspecies (`statuses salve-import`).
- The Checklist of CITES Species (checklist.cites.org, compiled by UNEP-WCMC for the CITES Secretariat): the current CITES Appendix listings of every taxon in the Appendices, with the listings each taxon inherits from a higher taxon (`statuses cites-import`).

Code: `BeastieBot3/StatusLists/`. The schema is `StatusListStore.Ddl`, with a comment on every column.

The four imports (`statuses ecos-import`, `nztcs-import`, `salve-import` and `cites-import`) share one run, `StatusListImport.RunAsync`. It imports the file given with `--file`, or downloads the source into the status lists folder (written as a `.part` file and renamed when complete). It then reads the file, stops without changing the store when the file has no rows, replaces the source's rows and its `status_source` row in one transaction, and prints a table of counts. Each command passes a `StatusListImportSpec` with its download, reader, store method and messages, and `StatusListDownload` creates the HTTP client, sends GET requests that are tried again after a 429, 408, 5xx or network error (5 s, 15 s, 30 s, 1 min, 2 min; the same waits as `NatureServeClient`), and writes and reads the downloaded files. The store's methods for each source are in `StatusListStore.NatureServe.cs`, `StatusListStore.Ecos.cs`, `StatusListStore.Nztcs.cs`, `StatusListStore.Salve.cs` and `StatusListStore.Cites.cs`.

## Files

| What | Where |
| --- | --- |
| Store | `Datastore:status_lists_sqlite` in paths.ini, else `status_lists.sqlite` in the datastore folder (`PathsService.GetStatusListsPath`, `ResolveStatusListsPath`) |
| Downloaded files | `Datasets:status_lists_dir`, else a `status-lists` folder in the datastore folder (`GetStatusListsDownloadDir`) |

## Tables

| Table | One row per |
| --- | --- |
| `status_source` | source (`natureserve`, `ecos`, `nztcs`, `salve`, `cites`): title, URL, licence, citation with the access date, version, when it was last fetched, row count. The site's credits can be built from it. |
| `status_sync_state` | key of the NatureServe download's progress (`natureserve_pass_*`, `natureserve_completed*`) |
| `natureserve_species` | NatureServe record (`element_global_id`): a species, subspecies, variety or population |
| `natureserve_synonym` | synonym NatureServe lists for a record |
| `natureserve_partition` | name prefix of the NatureServe download under way, with the next page to ask for. Empty between downloads. |
| `ecos_listing` | ESA listing (`entity_id`, the ECOS Listed Species ID) |
| `ecos_name` | name an ECOS listing's scientific name gives, the main name included |
| `nztcs_assessment` | current NZTCS assessment (`assessment_id`), with the scientific name `NztcsApi.ChooseName` gives |
| `salve_assessment` | current SALVE assessment of a species or subspecies (`ficha_id`, SALVE's sheet id) |
| `cites_taxon` | taxon of the Checklist of CITES Species (`taxon_concept_id`, Species+'s id): a species, subspecies, variety or higher taxon, with Species+'s summary of its listing (`current_listing`) |
| `cites_listing` | current CITES listing of a taxon (`taxon_concept_id`, `listing_change_id`): its own, or inherited from a higher taxon |
| `cites_note` | long note of the CITES listings (a full note, the text of an annotation such as #4), stored once and referred to by id |
| `cites_synonym` | synonym the Checklist gives for a taxon, with and without its author |

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

## Web UI

The Data sources page has a card for the store, with the number of NatureServe records and ECOS listings. The "Update the public species site" workflow has a step for each command before the site build, each with a light: NatureServe is blue while a download is under way, and either step is amber when its last download is more than 30 days old. The site build's light counts the store as one of its inputs.
