using System.Globalization;
using BeastieBot3.Infrastructure;
using Microsoft.Data.Sqlite;

// Conservation statuses from systems other than the IUCN Red List (Datastore:status_lists_sqlite),
// for the public species site. This file has the schema, the source rows and the sync state; the
// methods that read and write each source's tables are in StatusListStore.NatureServe.cs,
// StatusListStore.Ecos.cs, StatusListStore.Nztcs.cs, StatusListStore.Salve.cs and
// StatusListStore.Cites.cs.
//   status_source         one row per source: title, licence, citation, when it was last fetched;
//   status_sync_state     key/value progress of `statuses natureserve-fetch` (the pass under way);
//   natureserve_species   one row per NatureServe Explorer species, subspecies, variety or
//                         population: G rank, US and Canadian N ranks, US ESA, COSEWIC and SARA codes;
//   natureserve_synonym   the synonyms NatureServe lists for each of them;
//   natureserve_partition the name prefixes of the pass under way and how far each has got;
//   ecos_listing          one row per US Endangered Species Act listing in ECOS (a species,
//                         subspecies or population);
//   ecos_name             every name the ECOS scientific name gives, brackets read (EcosScientificName);
//   nztcs_assessment      one row per current New Zealand Threat Classification System assessment;
//   salve_assessment      one row per current SALVE assessment of a species or subspecies of Brazil's
//                         fauna;
//   cites_taxon           one row per taxon of the Checklist of CITES Species (a species, subspecies,
//                         variety or higher taxon), with Species+'s summary of its listing;
//   cites_listing         one row per current CITES listing of a taxon, its own or inherited from a
//                         higher taxon;
//   cites_note            each long note of the CITES listings once (full notes, annotation texts);
//   cites_synonym         the synonyms the Checklist gives for each taxon.
//
// Nothing narrative is stored: no NatureServe taxonomic comments, ranking reasons or other text.

namespace BeastieBot3.StatusLists;

internal static class StatusSources {
    public const string NatureServe = "natureserve";
    public const string Ecos = "ecos";
    public const string Nztcs = "nztcs";
    public const string Salve = "salve";
    public const string Cites = "cites";
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
            source      TEXT PRIMARY KEY,   -- 'natureserve' | 'ecos' | 'nztcs' | 'salve' | 'cites'
            title       TEXT NOT NULL,
            url         TEXT NOT NULL,
            licence     TEXT NOT NULL,
            citation    TEXT,               -- the source's citation form, with the access date filled in
            version     TEXT,
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
