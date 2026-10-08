using System.Globalization;
using BeastieBot3.Infrastructure;
using Microsoft.Data.Sqlite;

// Conservation statuses from systems other than the IUCN Red List (Datastore:status_lists_sqlite),
// for the public species site. This file has the schema, the source rows and the sync state; the
// methods that read and write each source's tables are in StatusListStore.NatureServe.cs,
// StatusListStore.Ecos.cs, StatusListStore.Nztcs.cs, StatusListStore.Salve.cs,
// StatusListStore.Jncc.cs, StatusListStore.Cites.cs, StatusListStore.RedLists.cs and StatusListStore.Japan.cs.
//   status_source         one row per source: title, licence, citation, when it was last fetched;
//   status_sync_state     key/value progress of `statuses natureserve-fetch` (the pass under way);
//   natureserve_species   one row per NatureServe Explorer species, subspecies, variety or
//                         population: G rank, US and Canadian N ranks, US ESA, COSEWIC and SARA codes;
//   natureserve_synonym   the synonyms NatureServe lists for each of them;
//   natureserve_nation    each record's rounded national ranks (United States, Canada);
//   natureserve_subnation each record's rounded state, province and territory ranks;
//   natureserve_partition the name prefixes of the pass under way and how far each has got;
//   ecos_listing          one row per US Endangered Species Act listing in ECOS (a species,
//                         subspecies or population);
//   ecos_name             every name the ECOS scientific name gives, brackets read (EcosScientificName);
//   nztcs_assessment      one row per current New Zealand Threat Classification System assessment;
//   salve_assessment      one row per current SALVE assessment of a species or subspecies of Brazil's
//                         fauna;
//   jncc_designation      one row per taxon and designation in JNCC's Conservation Designations for
//                         UK Taxa: UK, GB and UK country red lists, legislation and priority lists,
//                         and the international conventions and EU directives as they apply to UK taxa;
//   cites_taxon           one row per taxon of the Checklist of CITES Species (a species, subspecies,
//                         variety or higher taxon), with Species+'s summary of its listing;
//   cites_listing         one row per current CITES listing of a taxon, its own or inherited from a
//                         higher taxon;
//   cites_note            each long note of the CITES listings once (full notes, annotation texts);
//   cites_synonym         the synonyms the Checklist gives for each taxon;
//   red_list_dataset      one row per national or subnational red list imported from GBIF
//                         (rules/status-lists/national-red-lists.yml), with its own status_source row
//                         'redlist:<key>';
//   red_list_taxon        one row per status of a taxon in one of those lists;
//   red_list_synonym      the synonyms a list gives for its taxa with a status;
//   japan_listing         one row per taxon or threatened local population in the latest Red List of
//                         Japan's Ministry of the Environment for its group (5th Red List or Red
//                         List 2020).
//
// Nothing narrative is stored: no NatureServe taxonomic comments, ranking reasons or other text,
// none of JNCC's comments or descriptions, and none of the habitat, region or threat columns of
// Japan's Red List.

namespace BeastieBot3.StatusLists;

internal static class StatusSources {
    public const string NatureServe = "natureserve";
    public const string Ecos = "ecos";
    public const string Nztcs = "nztcs";
    public const string Salve = "salve";
    public const string Jncc = "jncc";
    public const string Cites = "cites";

    /// A red list's status_source row is 'redlist:<key>', key from national-red-lists.yml.
    public const string RedListPrefix = "redlist:";
    public const string Japan = "japan";
}

internal sealed record StatusSourceInfo(
    string Source,
    string Title,
    string Url,
    string Licence,
    string? Citation,
    string? Version,
    DateTime FetchedAtUtc,
    long RowCount);

internal sealed partial class StatusListStore : SqliteStore {
    private StatusListStore(SqliteConnection connection) : base(connection) { }

    public static StatusListStore Open(string path) {
        var store = new StatusListStore(OpenConnection(path));
        store.EnsureSchema();
        return store;
    }

    /// Null when the file is missing. No schema work: any write fails.
    public static StatusListStore? OpenReadOnly(string path) {
        if (!File.Exists(path)) {
            return null;
        }
        var connection = new SqliteConnection(new SqliteConnectionStringBuilder {
            DataSource = path, Mode = SqliteOpenMode.ReadOnly, Pooling = false,
        }.ConnectionString);
        connection.Open();
        return new StatusListStore(connection);
    }

    internal static StatusListStore OpenFromConnection(SqliteConnection connection) {
        EnableForeignKeys(connection);
        var store = new StatusListStore(connection);
        store.EnsureSchema();
        return store;
    }

    internal const string Ddl = """
        CREATE TABLE IF NOT EXISTS status_source (
            source      TEXT PRIMARY KEY,   -- 'natureserve' | 'ecos' | 'nztcs' | 'salve' | 'jncc' | 'cites' | 'redlist:<key>' (one per red list) | 'japan'
            title       TEXT NOT NULL,
            url         TEXT NOT NULL,      -- the source's site; for JNCC, the URL of the file imported
            licence     TEXT NOT NULL,
            citation    TEXT,               -- the source's citation form, with the access date filled in; for JNCC,
                                            -- its attribution statement with the year of the release
            version     TEXT,               -- the file imported (JNCC's name has the release date: taxon-designations-20260609.xlsx);
                                            -- for NatureServe, the date the download finished
            fetched_at  TEXT NOT NULL,      -- UTC "O": when the last full download or refresh finished
            row_count   INTEGER NOT NULL
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS status_sync_state (
            key   TEXT PRIMARY KEY,
            value TEXT
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS natureserve_species (
            element_global_id     INTEGER PRIMARY KEY,
            unique_id             TEXT NOT NULL,     -- ELEMENT_GLOBAL.2.<element_global_id>
            elcode                TEXT,
            scientific_name       TEXT NOT NULL,     -- as NatureServe writes it: "Atriplex cordulata var. cordulata", "Ambystoma californiense pop. 1"
            primary_common_name   TEXT,
            primary_common_name_language TEXT,       -- EN, HAW, ES, OTHER
            g_rank                TEXT,              -- global rank as published: G3G4, G2T1, G3TNRQ
            rounded_g_rank        TEXT,              -- G1..G5, GH, GX, GNR, GNA, GU, or a rounded T rank (T1 ...)
            classification_status TEXT,              -- Standard, Provisional, Nonstandard
            kingdom               TEXT,
            phylum                TEXT,
            taxclass              TEXT,
            taxorder              TEXT,
            family                TEXT,
            genus                 TEXT,
            informal_taxonomy     TEXT,              -- "Animals | Vertebrates | Amphibians"
            infraspecies          INTEGER NOT NULL,  -- 1 for a subspecies, variety or population
            usesa_code            TEXT,              -- US Endangered Species Act status codes as NatureServe writes them: E, T, PE, PT, C, SAT, PSAT, DL, PDL, UR, XN; several joined by ", " ("E, XN")
            cosewic_code          TEXT,              -- COSEWIC status code: E, T, SC, X, XT, NAR, DD, Non-active/Nonactive
            sara_code             TEXT,              -- SARA status, English part ("Endangered")
            sara_code_raw         TEXT,              -- SARA status as given ("Endangered/En voie de disparition")
            us_n_rank             TEXT,              -- rounded national rank in the United States
            ca_n_rank             TEXT,              -- rounded national rank in Canada
            nsx_url               TEXT NOT NULL,     -- the record's page on NatureServe Explorer
            last_modified         TEXT,              -- NatureServe's lastModified, as given
            fetched_at            TEXT NOT NULL      -- UTC "O": when a pass last stored the record
        );
        CREATE INDEX IF NOT EXISTS natureserve_species_name ON natureserve_species(scientific_name);
        CREATE INDEX IF NOT EXISTS natureserve_species_fetched ON natureserve_species(fetched_at);
        CREATE TABLE IF NOT EXISTS natureserve_synonym (
            element_global_id INTEGER NOT NULL REFERENCES natureserve_species(element_global_id) ON DELETE CASCADE,
            name              TEXT NOT NULL,
            PRIMARY KEY (element_global_id, name)
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS natureserve_synonym_name ON natureserve_synonym(name);
        CREATE TABLE IF NOT EXISTS natureserve_nation (
            element_global_id INTEGER NOT NULL REFERENCES natureserve_species(element_global_id) ON DELETE CASCADE,
            nation_code       TEXT NOT NULL,     -- US, CA
            rounded_n_rank    TEXT,              -- rounded national rank as given, with breeding (B), nonbreeding (N) and migrant (M) parts
                                                 -- joined by ",": N2, NNR, N5B,N5N, N3B,NUM, NNRB. The search gives no unrounded rank
            native            INTEGER,           -- 1 or 0 as NatureServe gives it; NULL when not given
            exotic            INTEGER,           -- 1 or 0 as NatureServe gives it; NULL when not given. native and exotic are both 1
                                                 -- for a taxon native in part of the nation and introduced in another
            PRIMARY KEY (element_global_id, nation_code)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS natureserve_subnation (
            element_global_id INTEGER NOT NULL REFERENCES natureserve_species(element_global_id) ON DELETE CASCADE,
            nation_code       TEXT NOT NULL,     -- the nation of the state or province: US, CA
            subnation_code    TEXT NOT NULL,     -- NatureServe's code of a US state or a Canadian province or territory: TX, ON, DC, NF (island
                                                 -- of Newfoundland), LB (Labrador), NN (Navajo Nation, under US). The search gives no names
            rounded_s_rank    TEXT,              -- rounded subnational rank as given: S1, SNR, S3B,S3N, S2,S4N. The search gives no unrounded rank
            native            INTEGER,           -- 1 or 0 as NatureServe gives it; NULL when not given
            exotic            INTEGER,           -- 1 or 0 as NatureServe gives it; NULL when not given. native and exotic can both be 1
            PRIMARY KEY (element_global_id, nation_code, subnation_code)
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS natureserve_subnation_place ON natureserve_subnation(nation_code, subnation_code);
        CREATE TABLE IF NOT EXISTS natureserve_partition (
            prefix    TEXT PRIMARY KEY,           -- scientific name prefix ('' = every record)
            expected  INTEGER,                    -- records NatureServe gave for the prefix
            next_page INTEGER NOT NULL DEFAULT 0,
            done      INTEGER NOT NULL DEFAULT 0
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS ecos_listing (
            entity_id           INTEGER PRIMARY KEY,  -- ECOS Listed Species ID: one listed entity (species, subspecies or population)
            species_id          INTEGER,              -- ECOS Species ID, the number in the species page URL; shared by the listings of one species
            scientific_name_raw TEXT NOT NULL,        -- as ECOS writes it: "Papasula (=Sula) abbotti"
            scientific_name     TEXT NOT NULL,        -- brackets removed: "Papasula abbotti"
            name_note           TEXT,                 -- bracketed notes that are not names: "entire genus", "incl. D. cascus"
            common_name         TEXT,                 -- NULL where ECOS gives "No common name"
            status              TEXT NOT NULL,        -- Endangered, Threatened, Experimental Population, Non-Essential, Similarity of Appearance (Threatened)
            entity_description  TEXT,                 -- where the listing applies: "Wherever found", "U.S.A. (FL)"
            listing_date        TEXT,                 -- yyyy-MM-dd
            is_dps              INTEGER,              -- 1 for a distinct population segment
            is_foreign          INTEGER,              -- 1 for a species found only outside the United States
            range_country       TEXT,                 -- Domestic, Foreign, Both Domestic and Foreign
            species_group       TEXT,                 -- FWS group: Birds, Flowering Plants, Clams ...
            itis_tsn            INTEGER,              -- ITIS Taxonomic Serial Number
            kingdom             TEXT,                 -- Animal, Plant, UNKNOWN
            family              TEXT,
            url                 TEXT NOT NULL,        -- the species page on ECOS
            imported_at         TEXT NOT NULL         -- UTC "O"
        );
        CREATE INDEX IF NOT EXISTS ecos_listing_species ON ecos_listing(species_id);
        CREATE INDEX IF NOT EXISTS ecos_listing_name ON ecos_listing(scientific_name);
        CREATE TABLE IF NOT EXISTS ecos_name (
            entity_id INTEGER NOT NULL REFERENCES ecos_listing(entity_id) ON DELETE CASCADE,
            name      TEXT NOT NULL,
            PRIMARY KEY (entity_id, name)
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS ecos_name_name ON ecos_name(name);
        CREATE TABLE IF NOT EXISTS nztcs_assessment (
            assessment_id   INTEGER PRIMARY KEY,  -- NZTCS assessment id; its page is https://nztcs.org.nz/assessments/<id>
            species_id      INTEGER NOT NULL,     -- NZTCS species id
            scientific_name TEXT,                 -- NztcsApi.ChooseName: the species record's name, or the name in the assessment's title
                                                  -- when the two differ; NULL for an informal name ("sp.", "aff.", quotes)
            assessment_name TEXT NOT NULL,        -- the assessment's name as plain text, with its authority: "Apteryx haastii Potts, 1872"
            common_name     TEXT,
            category        TEXT,                 -- Threatened, At Risk, Not Threatened, Data Deficient, Extinct, Introduced and Naturalised,
                                                  -- Non-resident Native, Taxonomically indistinct
            status          TEXT,                 -- the status within the category: Nationally Critical, Declining, Naturally Uncommon ...
            criteria        TEXT,                 -- NZTCS criteria code: NVu3p
            qualifiers      TEXT,                 -- qualifier codes: "CD, RF"
            report_id       INTEGER,
            report_name     TEXT,                 -- "Birds 2021 (Robertson et al. 2021)"
            report_year     INTEGER,
            imported_at     TEXT NOT NULL         -- UTC "O"
        );
        CREATE INDEX IF NOT EXISTS nztcs_assessment_name ON nztcs_assessment(scientific_name);
        CREATE TABLE IF NOT EXISTS salve_assessment (
            ficha_id        TEXT PRIMARY KEY,     -- SALVE's id of the species sheet (id_ficha)
            scientific_name TEXT NOT NULL,        -- the current name, without its authority: "Aaptos glutinans"
            authority       TEXT,
            common_name     TEXT,                 -- in Portuguese
            taxon_group     TEXT,                 -- SALVE's group: "Invertebrados Marinhos", "Aves"
            category        TEXT NOT NULL,        -- EX, EW, RE, CR, EN, VU, NT, LC, DD, NA
            possibly_extinct INTEGER NOT NULL,    -- 1 when SALVE flags a CR assessment possibly extinct
            criteria        TEXT,                 -- "B1ab(iii)"
            assessed_on     TEXT,                 -- yyyy-MM-dd, the end of the assessment
            doi             TEXT,                 -- the assessment's DOI: 10.37002/salve.ficha.32866.2
            taxon_level     TEXT,                 -- ESPECIE, SUBESPECIE
            published       INTEGER NOT NULL,     -- 1 when the sheet is published (PUBLICADA)
            imported_at     TEXT NOT NULL         -- UTC "O"
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS salve_assessment_name ON salve_assessment(scientific_name);
        CREATE TABLE IF NOT EXISTS jncc_designation (
            row_number        INTEGER PRIMARY KEY,  -- the row's number in the workbook's Master List sheet (the file has no row id; one taxon
                                                    -- can have the same designation twice, from two sources or two published names)
            taxon_version_key TEXT NOT NULL,        -- UK Species Inventory (NHM) recommended taxon version key: NBNSYS0000000131, NHMSYS0021054473
            scientific_name   TEXT NOT NULL,        -- the UKSI recommended name, as JNCC gives it: "Lithobius (Monotarsobius) crassipes",
                                                    -- "Alosa fallax subsp. fallax", "Anser fabalis/serrirostris"
            authority         TEXT,                 -- of the recommended name: "Koehler, 1886"
            qualifier         TEXT,                 -- of the recommended name: "s.l.", "agg.", "sensu stricto"
            rank              TEXT,                 -- read from the form of the name and the kingdom (JNCC gives no rank): species, subspecies,
                                                    -- variety, form, aggregate, section, hybrid, above species; NULL for other forms
            designated_name   TEXT,                 -- the name as the source published it ("Designated name")
            common_name       TEXT,                 -- as JNCC gives it, from the source or UKSI
            category          TEXT,                 -- JNCC's group: Bird, Mammal, Fish, Reptile, Amphibian, Invertebrate, Vascular plant,
                                                    -- Non-vascular plant, Fungi, Algae, Slime mould
            taxon_group       TEXT,                 -- UKSI's informal group: "insect - beetle (Coleoptera)", "lichen", "flowering plant"
            kingdom           TEXT,                 -- in IUCN's spelling, read from category and taxon_group (JnccClassification.KingdomOf):
                                                    -- ANIMALIA, PLANTAE, FUNGI (lichens too), CHROMISTA; NULL for algae and slime moulds
            reporting_category TEXT NOT NULL,       -- the list as JNCC names it: "Wildlife and Countryside Act 1981",
                                                    -- "Red listing based on 2001 IUCN guidelines", "Biodiversity Lists - England"
            sort_code         TEXT,                 -- JNCC's sort order of the reporting category: A Bern Convention, Fc red lists (2001
                                                    -- guidelines), Ga rare and scarce species, Hb England, I Wildlife and Countryside Act
            designation       TEXT NOT NULL,        -- the designation as JNCC writes it: "Schedule 5 Section 9.4b", "Vulnerable",
                                                    -- "Bird Population Status - red", "England NERC S.41"
            designation_code  TEXT NOT NULL,        -- JNCC's code ("Designation abbreviation"): WACA-Sch5_sect9.4b, RedList_GB_post2001-VU,
                                                    -- Bird-Red, England_NERC_S.41
            status_code       TEXT,                 -- the category in a red list's code: EX, EW, RE, CR, CR(PE), EN, VU, NT, LC, DD, NE, NA,
                                                    -- LR(cd); pre-1994 R, Insu, Inde; WL (Waiting List); Red or Amber for Birds of
                                                    -- Conservation Concern and the spider list; NULL for other designations
            population        TEXT,                 -- breeding, non-breeding: the population the bird red list assessed
            iucn_version      TEXT,                 -- the IUCN criteria a red list used: 2001, 1994, pre 1994
            scope             TEXT,                 -- uk (the UK or Great Britain), country (part of the UK), international (a convention, an
                                                    -- EU directive or regulation, IUCN's global or European red list); NULL for a code that
                                                    -- JnccClassification does not know
            area              TEXT,                 -- where it applies: United Kingdom, Great Britain, England, Scotland, Wales, Northern
                                                    -- Ireland, England and Wales; World, Europe, European Union, Africa-Eurasia,
                                                    -- North-East Atlantic, North-East Atlantic and Baltic
            source            TEXT,                 -- the document listing it: "Birds of Conservation Concern 5: the red list for birds ...",
                                                    -- "A new vascular plant Red List for Great Britain, 2025"
            source_url        TEXT,
            designated_on     TEXT,                 -- yyyy-MM-dd as JNCC gives it; often 1 January of the year of the source
            imported_at       TEXT NOT NULL         -- UTC "O"
        );
        CREATE INDEX IF NOT EXISTS jncc_designation_key ON jncc_designation(taxon_version_key);
        CREATE INDEX IF NOT EXISTS jncc_designation_name ON jncc_designation(scientific_name);
        CREATE INDEX IF NOT EXISTS jncc_designation_code ON jncc_designation(designation_code);
        CREATE TABLE IF NOT EXISTS cites_taxon (
            taxon_concept_id INTEGER PRIMARY KEY,  -- Species+ taxon concept id, shared by the Checklist and Species+
            full_name        TEXT NOT NULL,        -- as the Checklist writes it: "Loxodonta africana", "Achillides chikae chikae" (a subspecies
                                                   -- has no rank word), "Euphorbia decaryi var. robinsonii", "Woodworthia "Pygmy"" (informal)
            author_year      TEXT,                 -- "(Blumenbach, 1797)"
            taxon_rank       TEXT NOT NULL,        -- SPECIES, SUBSPECIES, VARIETY, GENUS, SUBFAMILY, FAMILY, ORDER
            cites_accepted   INTEGER NOT NULL,     -- 1 when Species+ marks the name CITES accepted (the Checklist shows it in bold), else 0
            kingdom          TEXT,                 -- Animalia, Plantae
            phylum           TEXT,
            taxclass         TEXT,
            taxorder         TEXT,
            family           TEXT,
            genus            TEXT,
            current_listing  TEXT,                 -- Species+'s summary of the appendices of the taxon and its descendants: I, II, III, I/II,
                                                   -- II/NC ... ("NC": some descendants or populations are in no appendix; "NC" alone: the
                                                   -- taxon is not listed, such as a species excluded from its family's listing); NULL when empty
            url              TEXT NOT NULL,        -- the taxon's page on Species+, with its listings
            imported_at      TEXT NOT NULL         -- UTC "O"
        );
        CREATE INDEX IF NOT EXISTS cites_taxon_name ON cites_taxon(full_name);
        CREATE TABLE IF NOT EXISTS cites_note (
            note_id INTEGER PRIMARY KEY,
            html    TEXT NOT NULL UNIQUE            -- a note as Species+ gives it: HTML with <i>, <p>, entities and \r\n
        );
        CREATE TABLE IF NOT EXISTS cites_listing (
            taxon_concept_id       INTEGER NOT NULL REFERENCES cites_taxon(taxon_concept_id) ON DELETE CASCADE,
            listing_change_id      INTEGER NOT NULL,  -- Species+ id of the listing; an inherited listing often has the higher taxon's id
            appendix               TEXT NOT NULL,     -- I, II or III
            party_iso_code         TEXT,              -- Appendix III: the Party that listed the taxon, ISO 3166 alpha-2 (EU for the European Union)
            party_name             TEXT,              -- "Mauritius", "Bolivia (Plurinational State of)"
            effective_on           TEXT,              -- yyyy-MM-dd, when the listing took effect
            short_note             TEXT,              -- HTML: which populations or parts the listing covers, quotas and exclusions
                                                      -- ("Populations of AR and BR."); with inherited_short_note, the only place that says
                                                      -- which populations a split listing covers
            full_note_id           INTEGER REFERENCES cites_note(note_id),  -- the full text of the note
            annotation_symbol      TEXT,              -- the annotation that says which parts and derivatives are covered: #1 to #19
            annotation_note_id     INTEGER REFERENCES cites_note(note_id),  -- the text of that annotation
            inherited_rank         TEXT,              -- for a listing inherited from a higher taxon: its rank (FAMILY, GENUS, ORDER, SUBFAMILY, SPECIES)
            inherited_name         TEXT,              -- and its name ("Trochilidae"); NOT NULL marks an inherited listing
            inherited_from_id      INTEGER,           -- and its taxon_concept_id, when the download has one taxon of that name and rank
            inherited_short_note   TEXT,              -- HTML: the higher taxon's note that applies to this taxon ("Excludes fossils.")
            inherited_full_note_id INTEGER REFERENCES cites_note(note_id),
            nomenclature_note      TEXT,              -- HTML: a note on the name the taxon was listed under
            PRIMARY KEY (taxon_concept_id, listing_change_id)
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS cites_listing_inherited ON cites_listing(inherited_from_id);
        CREATE TABLE IF NOT EXISTS cites_synonym (
            taxon_concept_id INTEGER NOT NULL REFERENCES cites_taxon(taxon_concept_id) ON DELETE CASCADE,
            name_with_author TEXT NOT NULL,         -- as the Checklist gives it: "Ornismya abeillei Lesson & DeLattre, 1839"
            name             TEXT NOT NULL,         -- without the author (CitesChecklist.SplitSynonym): "Ornismya abeillei"; a subgenus in
                                                    -- brackets is kept: "Phyllomedusa (agalychnis) callidryas"
            author           TEXT,                  -- "Lesson & DeLattre, 1839"
            PRIMARY KEY (taxon_concept_id, name_with_author)
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS cites_synonym_name ON cites_synonym(name);
        CREATE TABLE IF NOT EXISTS red_list_dataset (
            dataset_key      TEXT PRIMARY KEY,    -- the dataset's key in rules/status-lists/national-red-lists.yml: 'se-redlist-2025'
            gbif_dataset_key TEXT NOT NULL,       -- GBIF dataset key; its page is https://www.gbif.org/dataset/<key>
            title            TEXT NOT NULL,       -- the dataset's title in the GBIF registry
            list_name        TEXT NOT NULL,       -- the list's name as its publisher gives it, in the publisher's language
            list_name_en     TEXT,                -- an English name, when list_name is not in English
            list_year        INTEGER,             -- the edition's year as the publisher gives it; NULL when the dataset holds lists of several years
            publisher        TEXT NOT NULL,       -- who published the list (not who put it on GBIF, when they differ)
            country_code     TEXT NOT NULL,       -- ISO 3166-1 alpha-2 code of the country the list covers
            region           TEXT,                -- the part of the country a subnational list covers ('Flanders'); NULL for a national list
            region_code      TEXT,                -- ISO 3166-2 code of the region ('BE-VLG')
            licence          TEXT NOT NULL,       -- from the GBIF registry: 'CC0 1.0', 'CC BY 4.0' or 'CC BY-NC 4.0'
            citation         TEXT NOT NULL,       -- the original list, from national-red-lists.yml
            gbif_citation    TEXT,                -- GBIF's citation of the dataset, with its DOI and the access date
            doi              TEXT,                -- the dataset's DOI in the GBIF registry
            pub_date         TEXT,                -- the dataset's pubDate in the GBIF registry (yyyy-MM-dd) when it was last checked
            archive_url      TEXT NOT NULL,       -- where the archive was downloaded from
            archive_file     TEXT NOT NULL,       -- the archive's file name in the red-lists folder of the status lists folder
            archive_sha256   TEXT NOT NULL,       -- SHA-256 of the archive imported, in lower-case hex
            archive_size     INTEGER NOT NULL,    -- bytes
            notes            TEXT,                -- from national-red-lists.yml
            fetched_at       TEXT NOT NULL,       -- UTC "O": when the import last checked the dataset (downloaded it, or found it unchanged)
            imported_at      TEXT NOT NULL,       -- UTC "O": when its rows were last replaced
            row_count        INTEGER NOT NULL,    -- rows in red_list_taxon
            taxon_count      INTEGER NOT NULL,    -- distinct taxa in red_list_taxon
            synonym_count    INTEGER NOT NULL,    -- rows in red_list_synonym
            reader_version   INTEGER NOT NULL     -- RedListArchiveReader.Version that read the rows; a newer one reads the archive again
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS red_list_taxon (
            dataset_key         TEXT NOT NULL REFERENCES red_list_dataset(dataset_key) ON DELETE CASCADE,
            taxon_id            TEXT NOT NULL,    -- the archive's id of the core taxon (its taxonID)
            seq                 INTEGER NOT NULL, -- 0, 1, ...: a taxon can have several statuses (Ecuador's birds: mainland and Galápagos)
            scientific_name     TEXT NOT NULL,    -- as given, with the authority when the archive includes it
            canonical_name      TEXT,             -- the name without its authority: the archive's canonicalName, else computed by
                                                  -- RedListArchiveReader.CanonicalName; NULL for hybrids, populations and group ranks
            authorship          TEXT,             -- scientificNameAuthorship as given
            taxon_rank          TEXT,             -- as given: 'species', 'Especie', 'Art', 'SPECIES'
            taxonomic_status    TEXT,             -- as given: 'accepted', 'Aceptado', 'Sinónimo'
            accepted_taxon_id   TEXT,             -- the taxon_id of the accepted taxon when this taxon is a synonym with its own status
            accepted_name       TEXT,             -- acceptedNameUsage as given
            kingdom             TEXT,             -- as given, else the dataset's kingdom in national-red-lists.yml
            phylum              TEXT,             -- as given
            taxclass            TEXT,             -- class as given
            taxorder            TEXT,             -- order as given
            family              TEXT,             -- as given
            genus               TEXT,             -- as given
            threat_status       TEXT NOT NULL,    -- threatStatus as given (trimmed): 'VU', 'Least Concern', 'CR(PE)', '3', 'вразливий'
            iucn_code           TEXT,             -- EX, EW, RE, CR, EN, VU, NT, LC, DD or NA when threat_status is an IUCN category
                                                  -- (RedListCategories.ToIucnCode); NULL for every other status
            status_label        TEXT,             -- the English label of threat_status from the dataset's categories, if it has one
            country_code        TEXT,             -- the status row's countryCode as given ('EC', 'Ecuador', 'fr')
            locality            TEXT,             -- as given: 'Flanders', 'Ecuador insular | Islas Galápagos'
            location_id         TEXT,             -- as given: 'ISO_3166:BE-VLG'
            establishment_means TEXT,             -- as given: 'native', 'introduced'
            occurrence_status   TEXT,             -- as given: 'present', 'absent'
            event_date          TEXT,             -- eventDate (or temporal) as given: the year or years the assessment is of ('2013', '2005-2022')
            source              TEXT,             -- the status row's source as given: the group's list ('Lock et al. (2013)') or a URL
            url                 TEXT,             -- the taxon's page at the publisher, when the archive gives one as a URL
            PRIMARY KEY (dataset_key, taxon_id, seq)
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS red_list_taxon_canonical ON red_list_taxon(canonical_name);
        CREATE INDEX IF NOT EXISTS red_list_taxon_name ON red_list_taxon(scientific_name);
        CREATE TABLE IF NOT EXISTS red_list_synonym (
            dataset_key       TEXT NOT NULL REFERENCES red_list_dataset(dataset_key) ON DELETE CASCADE,
            taxon_id          TEXT NOT NULL,      -- the synonym's own id in the archive
            scientific_name   TEXT NOT NULL,      -- as given
            canonical_name    TEXT,               -- as in red_list_taxon
            authorship        TEXT,               -- as given
            taxonomic_status  TEXT,               -- as given: 'synonym', 'homotypicSynonym', 'heterotypicSynonym', 'proParteSynonym'
            accepted_taxon_id TEXT NOT NULL,      -- red_list_taxon.taxon_id of the accepted taxon, which has a status
            PRIMARY KEY (dataset_key, taxon_id)
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS red_list_synonym_canonical ON red_list_synonym(canonical_name);
        CREATE TABLE IF NOT EXISTS japan_listing (
            row_id          INTEGER PRIMARY KEY,  -- the row's place in the import: groups in the Ministry's order (mammals first, fungi last), then
                                                  -- the rows in their file's order. Not an id: it changes when a list changes. An LP row's
                                                  -- scientific name is shared by the other populations and the taxon itself, so no column is unique
            group_key       TEXT NOT NULL,        -- mammals, birds, reptiles, amphibians, fishes, insects, molluscs, other-invertebrates,
                                                  -- vascular-plants, bryophytes, algae, lichens, fungi
            group_en        TEXT NOT NULL,        -- Mammals, Brackish and freshwater fishes, Other invertebrates, Vascular plants ...
            group_ja        TEXT NOT NULL,        -- the group as the list names it: 哺乳類, 汽水・淡水魚類, その他無脊椎動物, 維管束植物
            kingdom         TEXT,                 -- in IUCN's spelling, from the group: ANIMALIA; PLANTAE (vascular plants, bryophytes); FUNGI
                                                  -- (lichens, fungi); NULL for algae
            list_version    TEXT NOT NULL,        -- the edition of the group's latest list: 'Red List 2020' (環境省レッドリスト2020) or
                                                  -- '5th Red List' (環境省第５次レッドリスト)
            list_year       INTEGER NOT NULL,     -- the year that list was published: 2020; 2025 (plants, algae, lichens, fungi); 2026 (birds,
                                                  -- reptiles, amphibians)
            category        TEXT NOT NULL,        -- EX, EW, CR (IA), EN (IB), CR+EN (I: Red List 2020 groups that do not split I into IA and
                                                  -- IB for every taxon), VU (II), NT, DD, LP (threatened local population)
            category_ja     TEXT NOT NULL,        -- the category as the list writes it: 絶滅危惧IA類 (animal CSVs), 絶滅危惧ⅠＡ類（CR） (plant
                                                  -- CSVs), 絶滅危惧I類（CR+EN） (the Red List 2020 PDF's heading)
            japanese_name   TEXT,                 -- 和名 as written; for an LP row, the place and the name: 九州地方のカワネズミ
            scientific_name TEXT NOT NULL,        -- 学名 as written, without authors; full-width letters and punctuation made ASCII (NFKC) and
                                                  -- spaces collapsed: "Lutra lutra nippon", "Assiminea sp. D", "Heptathela kimurai sensu lato"
            population      TEXT,                 -- LP rows: the place, japanese_name before its last の: 九州地方, 本州の太平洋側湖沼系群
            higher_taxa     TEXT,                 -- the groups the list gives before the name, as written: 'カモ目 カモ科' (order and family,
                                                  -- animal CSVs), 'コウチュウ目' (order, Red List 2020 insects), '節足動物門 甲殻綱 エビ目'
                                                  -- (phylum, class and order, Red List 2020 other invertebrates); NULL for the other groups
            criteria        TEXT,                 -- 判定基準 of the animal CSVs as written: 'B2ab', 'A2 C1', '①②' (the Ministry's qualitative
                                                  -- criteria, numbered); NULL for the other groups
            list_number     TEXT,                 -- 掲載No. of the animal CSVs: BI0001, RE0044, AM0001; NULL for the other groups
            source_file     TEXT NOT NULL,        -- the file the row was read from, named as in its URL: redlist2026_birds.csv, 900515981.pdf
            source_url      TEXT NOT NULL,        -- that file's URL
            source_page     INTEGER,              -- the PDF's page (1 to 131); NULL for CSV rows
            source_line     INTEGER NOT NULL,     -- CSV: the row's number in the file, heading rows included; PDF: the line's number on its page,
                                                  -- top to bottom, header included
            imported_at     TEXT NOT NULL         -- UTC "O"
        );
        CREATE INDEX IF NOT EXISTS japan_listing_name ON japan_listing(scientific_name);
        """;

    protected override void EnsureSchema() {
        using var command = _connection.CreateCommand();
        command.CommandText = Ddl;
        command.ExecuteNonQuery();
    }

    internal static string Stamp(DateTime utc) => utc.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture);

    // ---- status_sync_state ----

    public string? GetState(string key) {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT value FROM status_sync_state WHERE key = @key";
        command.Parameters.AddWithValue("@key", key);
        return command.ExecuteScalar() as string;
    }

    public void SetState(string key, string? value, SqliteTransaction? tx = null) {
        using var command = _connection.CreateCommand();
        command.Transaction = tx;
        if (value is null) {
            command.CommandText = "DELETE FROM status_sync_state WHERE key = @key";
        } else {
            command.CommandText = "INSERT INTO status_sync_state(key, value) VALUES (@key, @value) ON CONFLICT(key) DO UPDATE SET value = excluded.value";
            command.Parameters.AddWithValue("@value", value);
        }
        command.Parameters.AddWithValue("@key", key);
        command.ExecuteNonQuery();
    }

    public SqliteTransaction BeginTransaction() => _connection.BeginTransaction();

    // ---- status_source ----

    public void UpsertSource(StatusSourceInfo info, SqliteTransaction? tx = null) {
        using var command = _connection.CreateCommand();
        command.Transaction = tx;
        command.CommandText = """
            INSERT INTO status_source(source, title, url, licence, citation, version, fetched_at, row_count)
            VALUES (@source, @title, @url, @licence, @citation, @version, @fetched, @rows)
            ON CONFLICT(source) DO UPDATE SET title = excluded.title, url = excluded.url, licence = excluded.licence,
                citation = excluded.citation, version = excluded.version, fetched_at = excluded.fetched_at, row_count = excluded.row_count
            """;
        command.Parameters.AddWithValue("@source", info.Source);
        command.Parameters.AddWithValue("@title", info.Title);
        command.Parameters.AddWithValue("@url", info.Url);
        command.Parameters.AddWithValue("@licence", info.Licence);
        command.Parameters.AddWithValue("@citation", (object?)info.Citation ?? DBNull.Value);
        command.Parameters.AddWithValue("@version", (object?)info.Version ?? DBNull.Value);
        command.Parameters.AddWithValue("@fetched", Stamp(info.FetchedAtUtc));
        command.Parameters.AddWithValue("@rows", info.RowCount);
        command.ExecuteNonQuery();
    }

    public IReadOnlyList<StatusSourceInfo> Sources() {
        var list = new List<StatusSourceInfo>();
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT source, title, url, licence, citation, version, fetched_at, row_count FROM status_source ORDER BY source";
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            list.Add(new StatusSourceInfo(reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetString(4), reader.IsDBNull(5) ? null : reader.GetString(5),
                StoredUtc.Parse(reader.GetString(6)) ?? DateTime.MinValue, reader.GetInt64(7)));
        }
        return list;
    }

    // ---- helpers ----

    // A parameter whose SQLite type follows the value it is given, so a number is stored as an
    // INTEGER and a string as TEXT.
    private static SqliteParameter AddParameter(SqliteCommand command, string name) {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = DBNull.Value;
        command.Parameters.Add(parameter);
        return parameter;
    }

    private long Scalar(string sql, params (string Name, object Value)[] parameters) {
        using var command = _connection.CreateCommand();
        command.CommandText = sql;
        foreach (var (name, value) in parameters) {
            command.Parameters.AddWithValue(name, value);
        }
        return Convert.ToInt64(command.ExecuteScalar(), CultureInfo.InvariantCulture);
    }

    private int Execute(SqliteTransaction tx, string sql, params (string Name, object Value)[] parameters) {
        using var command = _connection.CreateCommand();
        command.Transaction = tx;
        command.CommandText = sql;
        foreach (var (name, value) in parameters) {
            command.Parameters.AddWithValue(name, value);
        }
        return command.ExecuteNonQuery();
    }
}
