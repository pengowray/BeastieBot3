namespace BeastieBot3.Shared.SiteData;

// Schema of the public site's database (Datastore:site_sqlite). `site build-db` writes it from the
// local caches; BeastieBot3.Site only reads it. The site refuses a database whose schema_version
// differs from Version, so bump Version whenever a table or column changes meaning.
//
// What the site may show from IUCN is limited by the IUCN Red List Terms of Use: no assessment
// narrative text (rationale, range, threats ...), no coded threats/habitats/countries, no downloads.
// Keep such fields out of this database rather than hiding them in the site.
public static class SiteDbSchema {
    public const int Version = 1;

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
            sprat_taxon_id              INTEGER,              -- Australian SPRAT taxon id
            epbc_status                 TEXT,                 -- EPBC Act category: 'EX' 'EW' 'CR' 'EN' 'VU' 'CD'; NULL when not listed
            latest_global_assessment_id INTEGER               -- NULL when the taxon has regional assessments only
        );
        CREATE INDEX taxon_parent ON taxon(parent_taxon_id);

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
            citation_json                TEXT                 -- IucnCitationParts as JSON; NULL when the API payload is not cached
        );
        CREATE INDEX assessment_taxon ON assessment(taxon_id, year_published);

        CREATE TABLE name (
            name_id      INTEGER PRIMARY KEY,
            taxon_id     INTEGER NOT NULL,
            name         TEXT NOT NULL,
            name_type    TEXT NOT NULL,                       -- 'scientific' | 'common' | 'synonym'
            language     TEXT,                                -- ISO 639 code of a common name ('en', 'fr', ...)
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
        /// Catalogue of Life release used for CoL ids ("COL26.7 XR").
        public const string ColRelease = "col_release";
        /// SPRAT report file name the EPBC statuses came from.
        public const string SpratReport = "sprat_report";
        public const string TaxonCount = "taxon_count";
        public const string AssessmentCount = "assessment_count";
    }
}
