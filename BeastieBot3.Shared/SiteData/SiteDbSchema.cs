namespace BeastieBot3.Shared.SiteData;

// Schema of the public site's database (Datastore:site_sqlite). `site build-db` writes it from the
// local caches; BeastieBot3.Site only reads it. The site refuses a database whose schema_version
// differs from Version, so bump Version whenever a table or column changes meaning.
//
// What the site may show from IUCN is limited by the IUCN Red List Terms of Use: no assessment
// narrative text (rationale, range, threats ...), no coded threats/habitats/countries, no downloads.
// Keep such fields out of this database rather than hiding them in the site.
public static class SiteDbSchema {
    public const int Version = 4;

    public const string Ddl = """
        CREATE TABLE meta (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        ) WITHOUT ROWID;

        CREATE TABLE taxon (
            taxon_id                    INTEGER PRIMARY KEY,  -- IUCN SIS taxon id
            scientific_name             TEXT NOT NULL,        -- as IUCN writes it ("Panthera pardus ssp. orientalis")
            kind                        TEXT NOT NULL,        -- 'species' | 'subspecies' | 'variety' | 'subpopulation'
            kingdom                     TEXT,                 -- IUCN's spelling (upper case down to family)
            phylum                      TEXT,
            class_name                  TEXT,
            order_name                  TEXT,
            family                      TEXT,
            genus                       TEXT,
            species_epithet             TEXT,
            infra_rank                  TEXT,                 -- rank marker as written in scientific_name: 'ssp.' | 'subsp.' | 'var.'
            infra_name                  TEXT,
            subpopulation_name          TEXT,
            authority                   TEXT,                 -- HTML entities decoded
            parent_taxon_id             INTEGER,              -- the species of an infraspecific taxon or subpopulation
            common_name_en              TEXT,                 -- best English common name, capitalised; NULL when none or ambiguous
            enwiki_title                TEXT,                 -- English Wikipedia article (final title after redirects)
            wikidata_qid                TEXT,                 -- 'Q19939'
            wikidata_qid_source         TEXT,                 -- 'p627': the item states this IUCN taxon id; 'name-match': matched by name
            col_id                      TEXT,                 -- Catalogue of Life accepted name usage id
            latest_global_assessment_id INTEGER,              -- NULL when the taxon has regional assessments only, or is not in the release
            in_release                  INTEGER NOT NULL,     -- 1: in the Red List version's CSV export. 0: only in the IUCN API cache
                                                              -- (an old or merged id, or a taxon IUCN no longer assesses); every one
                                                              -- of its assessments has is_latest = 0
            current_taxon_id            INTEGER               -- in_release = 0 only: the taxon in the release with the same scientific
                                                              -- name (same kingdom first); NULL when there is none
        );
        CREATE INDEX taxon_parent ON taxon(parent_taxon_id);
        CREATE INDEX taxon_current ON taxon(current_taxon_id);

        -- SPRAT profiles of the taxon (Australia's Species Profile and Threats Database) and their
        -- EPBC Act listings. One row per SPRAT profile: the profile whose name is the taxon's name (or
        -- lists it among its IUCN names), and every profile named after the taxon with a population in
        -- brackets: "Phascolarctos cinereus (combined populations of Qld, NSW and the ACT)".
        CREATE TABLE epbc_listing (
            taxon_id       INTEGER NOT NULL,
            sprat_taxon_id INTEGER NOT NULL,
            listed_name    TEXT NOT NULL,                     -- the name the EPBC Act lists it under, else SPRAT's scientific name
            status         TEXT,                              -- EPBC Act category: 'EX' 'EW' 'CR' 'EN' 'VU' 'CD'; NULL when not listed
            applies_to     TEXT NOT NULL,                     -- 'taxon' | 'population'
            population     TEXT,                              -- applies_to = 'population': the text in brackets after the taxon's name
            PRIMARY KEY (taxon_id, sprat_taxon_id)
        ) WITHOUT ROWID;

        CREATE TABLE assessment (
            assessment_id                INTEGER PRIMARY KEY,
            taxon_id                     INTEGER NOT NULL,
            scope                        TEXT NOT NULL,       -- 'Global', or the region as IUCN names it ('Europe')
            is_latest                    INTEGER NOT NULL,    -- 1 = latest assessment for its scope
            category                     TEXT NOT NULL,       -- IUCN code as published: 'LC', 'LR/nt', and pre-1994 codes such as 'V', 'Ex'
            possibly_extinct             INTEGER NOT NULL DEFAULT 0,
            possibly_extinct_in_the_wild INTEGER NOT NULL DEFAULT 0,
            criteria                     TEXT,                -- 'A2cd+4cd'
            criteria_version             TEXT,                -- '3.1', '2.3'; NULL for older assessments
            year_published               INTEGER,
            assessment_date              TEXT,                -- 'yyyy-MM-dd'
            population_trend             TEXT,                -- 'Increasing' | 'Decreasing' | 'Stable' | 'Unknown'; NULL when not given
            citation_json                TEXT,                -- IucnCitationParts as JSON; NULL when the API payload is not cached
            replaced_by_assessment_id    INTEGER,             -- the errata or amended version that replaced this assessment; NULL otherwise
            wikidata_item_qid            TEXT,                -- Wikidata item for this assessment as a publication ('Q123'); NULL when none is known
            wikidata_item_properties     TEXT                 -- space-separated properties that item already has ('P31 P356 P2093'); NULL when no item
        );
        CREATE INDEX assessment_taxon ON assessment(taxon_id, year_published);

        CREATE TABLE name (
            name_id      INTEGER PRIMARY KEY,
            taxon_id     INTEGER NOT NULL,
            name         TEXT NOT NULL,
            name_type    TEXT NOT NULL,                       -- 'scientific' | 'common' | 'synonym'
            language     TEXT,                                -- ISO 639-1 code where one exists ('en', 'fr'), else IUCN's ISO 639-2 code; NULL when not given
            source       TEXT NOT NULL,                       -- 'iucn' | 'col' | 'wikidata' | 'wikipedia'
            is_preferred INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX name_taxon ON name(taxon_id);

        -- Exact lookups by a folded key: lower case, diacritics removed, whitespace collapsed
        -- (SiteNameKey.Fold). Used for /name/{name} and to rank exact matches first in search.
        CREATE TABLE name_key (
            key      TEXT NOT NULL,
            taxon_id INTEGER NOT NULL,
            name_id  INTEGER NOT NULL,
            PRIMARY KEY (key, name_id)
        ) WITHOUT ROWID;

        CREATE VIRTUAL TABLE name_fts USING fts5(
            name,
            content = 'name',
            content_rowid = 'name_id',
            tokenize = 'unicode61 remove_diacritics 2',
            prefix = '2 3'
        );
        """;

    /// Keys of the meta table. Every value is text.
    public static class MetaKeys {
        public const string SchemaVersion = "schema_version";
        /// UTC, ISO 8601.
        public const string BuiltAtUtc = "built_at_utc";
        /// Red List version of the CSV export the site was built from ("2026-1").
        public const string IucnRelease = "iucn_release";
        /// Range of API payload download dates ('yyyy-MM-dd').
        public const string IucnApiDownloadedFrom = "iucn_api_downloaded_from";
        public const string IucnApiDownloadedTo = "iucn_api_downloaded_to";
        /// GBIF's copy of the IUCN checklist: version text from its metadata, and its publication date.
        public const string GbifChecklistVersion = "gbif_checklist_version";
        public const string GbifChecklistPublished = "gbif_checklist_published";
        /// GBIF's recommended citation of the checklist (from its eml.xml) and the dataset DOI
        /// without a resolver prefix ("10.15468/0qnb58").
        public const string GbifChecklistCitation = "gbif_checklist_citation";
        public const string GbifChecklistDoi = "gbif_checklist_doi";
        /// Catalogue of Life release used for CoL ids ("COL26.7 XR").
        public const string ColRelease = "col_release";
        /// The release's recommended citation and DOI from the ColDP metadata ("10.48580/dgykv").
        public const string ColCitation = "col_citation";
        public const string ColDoi = "col_doi";
        /// SPRAT report file name the EPBC statuses came from.
        public const string SpratReport = "sprat_report";
        /// The newest checked_at date ('yyyy-MM-dd') of any row of `iucn resolve-dois`'s doi_check
        /// table, whether the DOI was found in Crossref's list, found at doi.org or not found, when the
        /// build read that cache.
        public const string IucnDoiCheckedTo = "iucn_doi_checked_to";
        /// JSON of the Wikidata assessment item model (rules/wikidata/iucn-status.yml assessment_item)
        /// that QuickStatements batches on the site follow: WikidataItemModel.ToJson().
        public const string WikidataItemModel = "wikidata_item_model";
        public const string TaxonCount = "taxon_count";
        public const string AssessmentCount = "assessment_count";
    }
}
