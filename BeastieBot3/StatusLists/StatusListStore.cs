using System.Globalization;
using BeastieBot3.Infrastructure;
using Microsoft.Data.Sqlite;

// Conservation statuses from systems other than the IUCN Red List (Datastore:status_lists_sqlite),
// for the public species site. This file has the schema, the source rows and the sync state; the
// methods that read and write each source's tables are in StatusListStore.NatureServe.cs,
// StatusListStore.Ecos.cs, StatusListStore.Nztcs.cs, StatusListStore.Salve.cs,
// StatusListStore.Jncc.cs, StatusListStore.Cites.cs and StatusListStore.France.cs.
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
//   france_status         one row per taxon, status and place of PatriNat's BDC Statuts, for the French
//                         national and regional red lists and protection lists;
//   france_status_type    the BDC's status types (LRN national red list, PN national protection ...);
//   france_territory      the places of the stored statuses (metropolitan France, an overseas
//                         territory, a region, a département);
//   france_document       the documents that give the stored statuses (a red list, a decree), with
//                         their citations;
//   france_taxref_name    every TAXREF name of the accepted names of the stored statuses, synonyms
//                         included;
//   france_taxref_link    those names' ids in the IUCN Red List, BirdLife, the Catalogue of Life and
//                         GBIF, from TAXREF.
//
// Nothing narrative is stored: no NatureServe taxonomic comments, ranking reasons or other text, none
// of JNCC's comments or descriptions, and none of the BDC's remarks except the red lists' codes.

namespace BeastieBot3.StatusLists;

internal static class StatusSources {
    public const string NatureServe = "natureserve";
    public const string Ecos = "ecos";
    public const string Nztcs = "nztcs";
    public const string Salve = "salve";
    public const string Jncc = "jncc";
    public const string Cites = "cites";
    public const string France = "france";
    public const string Taxref = "taxref";
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
            source      TEXT PRIMARY KEY,   -- 'natureserve' | 'ecos' | 'nztcs' | 'salve' | 'jncc' | 'cites' | 'france' | 'taxref'
            title       TEXT NOT NULL,
            url         TEXT NOT NULL,      -- the source's site; for JNCC, the URL of the file imported; for the BDC (france)
                                            -- and TAXREF, the dataset's page on data.gouv.fr
            licence     TEXT NOT NULL,
            citation    TEXT,               -- the source's citation form, with the access date filled in; for JNCC,
                                            -- its attribution statement with the year of the release; for the BDC and
                                            -- TAXREF, their forms with the version and date of the files imported
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
        CREATE TABLE IF NOT EXISTS france_status_type (
            type_code  TEXT PRIMARY KEY,   -- CD_TYPE_STATUT: LRN, LRR, PN, POM, PR, PD, and the types not stored (LRM, ZDET, BERN ...)
            label      TEXT NOT NULL,      -- LB_TYPE_STATUT, in French: "Liste rouge nationale", "Protection nationale"
            type_group TEXT,               -- REGROUPEMENT_TYPE: "Liste rouge", "Protection", "Conventions internationales" ...
            stored     INTEGER NOT NULL    -- 1 when france_status has the rows of this type (FranceBdc.StoredTypes), else 0
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS france_territory (
            territory_code TEXT PRIMARY KEY,  -- CD_SIG: TERFXFR (metropolitan France), TER971 (Guadeloupe), INSEER84 (a region),
                                              -- INSEED01 (a département)
            name           TEXT NOT NULL,     -- LB_ADM_TR, in French: "France métropolitaine", "TAAF : Îles éparses"
            name_en        TEXT,              -- FranceTerritories.EnglishName: "Metropolitan France", "French Guiana"; NULL for a
                                              -- département of metropolitan France
            admin_level    TEXT,              -- NIVEAU_ADMIN: Territoire, État, Région, Ancienne région (a region before 2016),
                                              -- Département, Collectivité d'outre-mer, Subdivision administrative
            iso3166_1      TEXT,              -- CD_ISO3166_1 as given: FXX, GLP, GUF, FRA; often NULL
            iso3166_2      TEXT               -- CD_ISO3166_2 as given: FR-GP, FR-ARA; often NULL
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS france_document (
            cd_doc        INTEGER PRIMARY KEY,  -- PatriNat's DOCS-Web id of the document (CD_DOC)
            year          INTEGER,              -- the first year in the citation (FranceBdc.DocumentYear): the year of a red list, of a decree
            title         TEXT,                 -- the longest italic part of the citation: "La Liste rouge des espèces menacées en France -
                                                -- Chapitre Oiseaux de France métropolitaine"; NULL when it has none (decrees)
            citation      TEXT,                 -- FULL_CITATION as plain text (tags removed, entities decoded)
            citation_html TEXT,                 -- FULL_CITATION as the BDC gives it: HTML with <em> and &amp;
            url           TEXT,                 -- DOC_URL: the document on inpn.mnhn.fr (down since July 2025); NULL when not given
            bird_population TEXT                -- breeding, wintering or visiting when the title names one population of birds
                                                -- (FranceBdc.TitlePopulation: "oiseaux nicheurs"); NULL when it names none or several
        );
        CREATE TABLE IF NOT EXISTS france_status (
            row_number     INTEGER PRIMARY KEY,  -- the row's number in the BDC's CSV, counting every row from 1 (the file has no row id,
                                                 -- and one list can give one name two rows)
            cd_nom         INTEGER NOT NULL,     -- TAXREF id of the name the document used
            cd_ref         INTEGER NOT NULL,     -- TAXREF id of the accepted name of cd_nom, in the TAXREF version of the BDC
            type_code      TEXT NOT NULL REFERENCES france_status_type(type_code),  -- LRN, LRR, PN, POM, PR, PD
            code           TEXT NOT NULL,        -- CODE_STATUT. Red lists: EX, EW, RE, CR, CR* (CR, possibly extinct or regionally
                                                 -- extinct), EN, VU, NT, LC, DD, NA, NE, and RE? in regional lists. Protection lists: the
                                                 -- list and article as PatriNat codes them (NV1, NO3, GO4)
            label          TEXT,                 -- LABEL_STATUT, in French: "En danger critique", "Liste des oiseaux protégés ... : Article 3"
            remark         TEXT,                 -- red lists only: RQ_STATUT as given ("pr. D2", "VU D1 (-1) - Nicheur", "b - Visiteur"),
                                                 -- read by FranceRedListRemark; NULL when empty or a sentence, and for other types
            criteria       TEXT,                 -- from remark: the IUCN criteria as written ("B2ab(iii)", "D1"); "pr. D2" for an NT taxon
                                                 -- that came close to meeting D2; NULL when the remark has none or another form
            adjusted_from  TEXT,                 -- from remark: the category before the regional adjustment ("VU" in "VU D1 (-1)")
            adjustment     INTEGER,              -- from remark: that adjustment, in categories: -1, -2, +1
            na_reason      TEXT,                 -- from remark, for NA: a (introduced in recent times), b (occasional or marginal), c or d
                                                 -- (birds that winter or pass through regularly); see docs/status-lists.md
            population_fr  TEXT,                 -- from remark: the population or presence the row assesses, as written: Nicheur,
                                                 -- Hivernant, Visiteur, Visiteur régulier, Reproducteur certain ...
            population     TEXT,                 -- population_fr as breeding, wintering or visiting; for a bird with no population_fr, the
                                                 -- bird_population of its document; NULL for a row about the whole taxon
            is_current     INTEGER,              -- red lists: 1 for the row in force for its type, accepted name (cd_ref), place and
                                                 -- population (FranceBdc.MarkCurrent); 0 for a row of an older list, or a row of another
                                                 -- name when the same list has a row of the accepted name; NULL for other types
            territory_code TEXT NOT NULL REFERENCES france_territory(territory_code),
            name           TEXT NOT NULL,        -- LB_NOM: the name of cd_nom, without author
            author         TEXT,                 -- LB_AUTEUR
            kingdom        TEXT,                 -- REGNE: Animalia, Plantae, Fungi, Chromista ...
            phylum         TEXT,
            taxclass       TEXT,
            taxorder       TEXT,
            family         TEXT,
            cd_doc         INTEGER REFERENCES france_document(cd_doc),
            imported_at    TEXT NOT NULL         -- UTC "O"
        );
        CREATE INDEX IF NOT EXISTS france_status_ref ON france_status(cd_ref);
        CREATE INDEX IF NOT EXISTS france_status_nom ON france_status(cd_nom);
        CREATE INDEX IF NOT EXISTS france_status_place ON france_status(type_code, territory_code);
        CREATE TABLE IF NOT EXISTS france_taxref_name (
            cd_nom INTEGER PRIMARY KEY,  -- TAXREF name id
            cd_ref INTEGER NOT NULL,     -- TAXREF id of its accepted name (cd_nom itself for an accepted name); only accepted names of
                                         -- france_status rows
            rank   TEXT,                 -- RANG: ES species, SSES subspecies, VAR variety, FO form, GN genus ...
            name   TEXT NOT NULL,        -- LB_NOM, without author
            author TEXT                  -- LB_AUTEUR
        );
        CREATE INDEX IF NOT EXISTS france_taxref_name_ref ON france_taxref_name(cd_ref);
        CREATE INDEX IF NOT EXISTS france_taxref_name_name ON france_taxref_name(name);
        CREATE TABLE IF NOT EXISTS france_taxref_link (
            cd_nom      INTEGER NOT NULL,  -- the TAXREF name that TAXREF_LIENS gives the link on
            cd_ref      INTEGER NOT NULL,  -- the id of its accepted name
            source      TEXT NOT NULL,     -- CT_NAME: "IUCN Red List" (an IUCN taxon id), "IUCN Red List > BirdLife" (BirdLife's id,
                                           -- which is the IUCN taxon id of a bird), "Catalogue of Life" (a CoL id), "GBIF" (a GBIF key)
            external_id TEXT NOT NULL,     -- CT_SP_ID: 22688522, 4QHKG
            PRIMARY KEY (cd_nom, source, external_id)
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS france_taxref_link_ref ON france_taxref_link(cd_ref);
        CREATE INDEX IF NOT EXISTS france_taxref_link_id ON france_taxref_link(source, external_id);
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
