using System.Text.RegularExpressions;
using BeastieBot3.Shared.SiteData;
using Microsoft.Data.Sqlite;

// Writes the site database (SiteDbSchema.Ddl) into a new file. Fast while building: no journal, no
// fsync, one transaction, and the secondary indexes in the DDL dropped until the rows are in.
// Finish() recreates them from the DDL's own statements, so the finished schema is the DDL exactly,
// rebuilds the full-text index, runs ANALYZE, switches the file to rollback-journal mode (the site
// opens it read-only, and a WAL-mode file cannot be opened read-only in a folder the service
// cannot write) and compacts it with VACUUM.

namespace BeastieBot3.SiteBuild;

internal sealed class SiteDbWriter : IDisposable {
    private static readonly Regex IndexStatement = new(
        @"CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?<name>\w+)\s+ON\s+[^;]+;",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    private readonly SqliteConnection _connection;
    private SqliteTransaction? _transaction;
    private readonly List<string> _deferredIndexes = new();
    private readonly List<(string Key, long TaxonId, long NameId)> _nameKeys = new();
    // The keys whose words go into name_word: scientific names, synonyms, English names and the
    // names of groups (names in other languages would crowd the suggestions: "kaola" -> "Kola").
    private readonly HashSet<string> _wordKeys = new(StringComparer.Ordinal);
    private readonly SqliteCommand _taxon;
    private readonly SqliteCommand _assessment;
    private readonly SqliteCommand _name;
    private readonly SqliteCommand _replacedBy;
    private readonly SqliteCommand _epbcListing;
    private readonly SqliteCommand _otherStatus;
    private readonly SqliteCommand _taxonLink;
    private long _nextNameId = 1;
    private bool _finished;

    public long NameCount => _nextNameId - 1;

    private SiteDbWriter(SqliteConnection connection) {
        _connection = connection;
        Execute("PRAGMA journal_mode = OFF; PRAGMA synchronous = OFF; PRAGMA temp_store = MEMORY; PRAGMA cache_size = -262144;");
        Execute(SiteDbSchema.Ddl);
        foreach (Match match in IndexStatement.Matches(SiteDbSchema.Ddl)) {
            _deferredIndexes.Add(match.Value);
            Execute($"DROP INDEX {match.Groups["name"].Value};");
        }
        _transaction = _connection.BeginTransaction();

        _taxon = Prepare("""
            INSERT INTO taxon (taxon_id, scientific_name, kind, kingdom, phylum, class_name, order_name, family, genus,
                species_epithet, infra_rank, infra_name, subpopulation_name, authority, parent_taxon_id, common_name_en,
                enwiki_title, wikidata_qid, wikidata_qid_source, wikidata_p141, wikidata_item_downloaded, wikidata_p627_deprecated,
                wikidata_other_items, col_id, latest_global_assessment_id, in_release, current_taxon_id,
                node_id, tree_pos, list_article_title, list_parent_article_title)
            VALUES (@taxon_id, @scientific_name, @kind, @kingdom, @phylum, @class_name, @order_name, @family, @genus,
                @species_epithet, @infra_rank, @infra_name, @subpopulation_name, @authority, @parent_taxon_id, @common_name_en,
                @enwiki_title, @wikidata_qid, @wikidata_qid_source, @wikidata_p141, @wikidata_item_downloaded, @wikidata_p627_deprecated,
                @wikidata_other_items, @col_id, @latest_global_assessment_id, @in_release, @current_taxon_id,
                @node_id, @tree_pos, @list_article_title, @list_parent_article_title)
            """,
            "@taxon_id", "@scientific_name", "@kind", "@kingdom", "@phylum", "@class_name", "@order_name", "@family", "@genus",
            "@species_epithet", "@infra_rank", "@infra_name", "@subpopulation_name", "@authority", "@parent_taxon_id", "@common_name_en",
            "@enwiki_title", "@wikidata_qid", "@wikidata_qid_source", "@wikidata_p141", "@wikidata_item_downloaded", "@wikidata_p627_deprecated",
            "@wikidata_other_items", "@col_id", "@latest_global_assessment_id", "@in_release", "@current_taxon_id",
            "@node_id", "@tree_pos", "@list_article_title", "@list_parent_article_title");
        _assessment = Prepare("""
            INSERT INTO assessment (assessment_id, taxon_id, scope, is_latest, category, possibly_extinct,
                possibly_extinct_in_the_wild, criteria, criteria_version, year_published, assessment_date, population_trend, population_size, citation_json,
                has_taxonomic_notes, wikidata_item_qid, wikidata_item_properties, wikidata_item_titles, wikidata_item_label_en, wikidata_item_assessment_id,
                api_not_found, credits)
            VALUES (@assessment_id, @taxon_id, @scope, @is_latest, @category, @possibly_extinct,
                @possibly_extinct_in_the_wild, @criteria, @criteria_version, @year_published, @assessment_date, @population_trend, @population_size, @citation_json,
                @has_taxonomic_notes, @wikidata_item_qid, @wikidata_item_properties, @wikidata_item_titles, @wikidata_item_label_en, @wikidata_item_assessment_id,
                @api_not_found, @credits)
            """,
            "@assessment_id", "@taxon_id", "@scope", "@is_latest", "@category", "@possibly_extinct",
            "@possibly_extinct_in_the_wild", "@criteria", "@criteria_version", "@year_published", "@assessment_date", "@population_trend", "@population_size", "@citation_json",
            "@has_taxonomic_notes", "@wikidata_item_qid", "@wikidata_item_properties", "@wikidata_item_titles", "@wikidata_item_label_en", "@wikidata_item_assessment_id",
            "@api_not_found", "@credits");
        _name = Prepare("""
            INSERT INTO name (name_id, taxon_id, name, name_type, language, source, is_preferred, authority)
            VALUES (@name_id, @taxon_id, @name, @name_type, @language, @source, @is_preferred, @authority)
            """,
            "@name_id", "@taxon_id", "@name", "@name_type", "@language", "@source", "@is_preferred", "@authority");
        _replacedBy = Prepare("UPDATE assessment SET replaced_by_assessment_id = @replaced_by WHERE assessment_id = @assessment_id",
            "@replaced_by", "@assessment_id");
        _epbcListing = Prepare("""
            INSERT INTO epbc_listing (taxon_id, sprat_taxon_id, listed_name, status, applies_to, population)
            VALUES (@taxon_id, @sprat_taxon_id, @listed_name, @status, @applies_to, @population)
            """,
            "@taxon_id", "@sprat_taxon_id", "@listed_name", "@status", "@applies_to", "@population");
        _otherStatus = Prepare("""
            INSERT INTO other_status (taxon_id, system, status, listed_name, population, source, source_id, listed_on)
            VALUES (@taxon_id, @system, @status, @listed_name, @population, @source, @source_id, @listed_on)
            """,
            "@taxon_id", "@system", "@status", "@listed_name", "@population", "@source", "@source_id", "@listed_on");
        _taxonLink = Prepare("INSERT INTO taxon_link (taxon_id, current_taxon_id, link_kind) VALUES (@taxon_id, @current_taxon_id, @link_kind)",
            "@taxon_id", "@current_taxon_id", "@link_kind");
    }

    /// Creates the file, replacing any file already at the path.
    public static SiteDbWriter Create(string path) {
        DeleteDatabaseFiles(path);
        var directory = Path.GetDirectoryName(Path.GetFullPath(path));
        if (!string.IsNullOrEmpty(directory)) {
            Directory.CreateDirectory(directory);
        }
        var connection = new SqliteConnection(new SqliteConnectionStringBuilder {
            DataSource = path,
            Mode = SqliteOpenMode.ReadWriteCreate,
            Pooling = false,
        }.ConnectionString);
        connection.Open();
        return new SiteDbWriter(connection);
    }

    /// Deletes a database file and any journal files beside it.
    public static void DeleteDatabaseFiles(string path) {
        foreach (var file in new[] { path, path + "-journal", path + "-wal", path + "-shm" }) {
            if (File.Exists(file)) {
                File.Delete(file);
            }
        }
    }

    public void AddTaxon(SiteTaxon t) {
        Bind(_taxon, t.TaxonId, t.ScientificName, t.Kind, t.Kingdom, t.Phylum, t.ClassName, t.OrderName, t.Family, t.Genus,
            t.SpeciesEpithet, t.InfraRank, t.InfraName, t.SubpopulationName, t.Authority, t.ParentTaxonId, t.CommonNameEn,
            t.EnwikiTitle, t.WikidataQid, t.WikidataQidSource, t.WikidataP141, t.WikidataItemDownloaded, t.WikidataP627Deprecated ? 1 : 0,
            t.WikidataOtherItems, t.ColId, t.LatestGlobalAssessmentId, t.InRelease ? 1 : 0, t.CurrentTaxonId,
            t.NodeId, t.TreePos, t.ListArticleTitle, t.ListParentArticleTitle);
        _taxon.ExecuteNonQuery();
    }

    public void AddEpbcListing(long taxonId, EpbcListing listing) {
        Bind(_epbcListing, taxonId, listing.SpratTaxonId, listing.ListedName, listing.Status, listing.AppliesTo, listing.Population);
        _epbcListing.ExecuteNonQuery();
    }

    public void AddOtherStatus(long taxonId, OtherStatus status) {
        Bind(_otherStatus, taxonId, status.System, status.Status, status.ListedName, status.Population, status.Source,
            status.SourceId, status.ListedOn);
        _otherStatus.ExecuteNonQuery();
    }

    public void AddAssessment(SiteAssessment a) {
        Bind(_assessment, a.AssessmentId, a.TaxonId, a.Scope, a.IsLatest ? 1 : 0, a.Category, a.PossiblyExtinct ? 1 : 0,
            a.PossiblyExtinctInTheWild ? 1 : 0, a.Criteria, a.CriteriaVersion, a.YearPublished, a.AssessmentDate,
            a.PopulationTrend, a.PopulationSize, a.CitationJson, a.HasTaxonomicNotes is { } notes ? (notes ? 1 : 0) : null, a.WikidataItemQid, a.WikidataItemProperties,
            a.WikidataItemTitles, a.WikidataItemLabelEn, a.WikidataItemAssessmentId, a.ApiNotFound ? 1 : 0, a.CreditsJson);
        _assessment.ExecuteNonQuery();
    }

    /// Writes credit_name: each credit entry or "full" string with its id.
    public void AddCreditNames(IReadOnlyDictionary<string, long> names) {
        using var command = Prepare("INSERT INTO credit_name (credit_name_id, text) VALUES (@id, @text)", "@id", "@text");
        foreach (var (text, id) in names.OrderBy(n => n.Value)) {
            Bind(command, id, text);
            command.ExecuteNonQuery();
        }
    }

    public void AddTaxonLink(SiteTaxonLink link) {
        Bind(_taxonLink, link.TaxonId, link.CurrentTaxonId, link.Kind);
        _taxonLink.ExecuteNonQuery();
    }

    /// Writes the tree of groups: higher_taxon, its counts by category and CoL's English names.
    public void AddHigherTaxa(IReadOnlyList<SiteTreeNode> nodes) {
        using var node = Prepare("""
            INSERT INTO higher_taxon (node_id, parent_node_id, depth, rank, name, name_key, link_query, source, show_rank, kingdom, col_id,
                common_name_en, common_name_source, enwiki_title, first_pos, last_pos, species_count, infra_count, subpopulation_count)
            VALUES (@node_id, @parent_node_id, @depth, @rank, @name, @name_key, @link_query, @source, @show_rank, @kingdom, @col_id,
                @common_name_en, @common_name_source, @enwiki_title, @first_pos, @last_pos, @species_count, @infra_count, @subpopulation_count)
            """,
            "@node_id", "@parent_node_id", "@depth", "@rank", "@name", "@name_key", "@link_query", "@source", "@show_rank", "@kingdom", "@col_id",
            "@common_name_en", "@common_name_source", "@enwiki_title", "@first_pos", "@last_pos", "@species_count", "@infra_count", "@subpopulation_count");
        using var count = Prepare("""
            INSERT INTO higher_taxon_count (node_id, category, species_count, infra_count, subpopulation_count)
            VALUES (@node_id, @category, @species_count, @infra_count, @subpopulation_count)
            """, "@node_id", "@category", "@species_count", "@infra_count", "@subpopulation_count");
        using var name = Prepare("INSERT OR IGNORE INTO higher_taxon_name (node_id, name, source, name_key) VALUES (@node_id, @name, @source, @name_key)",
            "@node_id", "@name", "@source", "@name_key");
        foreach (var n in nodes) {
            _wordKeys.Add(SiteNameKey.Fold(n.Name));
            if (n.CommonNameEn is { } groupName) {
                _wordKeys.Add(SiteNameKey.Fold(groupName));
            }
            Bind(node, n.NodeId, n.Parent?.NodeId, n.Depth, n.Rank, n.Name, SiteNameKey.Fold(n.Name), n.LinkQuery, n.Source, n.ShowRank ? 1 : 0,
                n.Kingdom, n.ColId, n.CommonNameEn, n.CommonNameSource, n.EnwikiTitle, n.FirstPos, n.LastPos, n.SpeciesCount,
                n.InfraCount, n.SubpopulationCount);
            node.ExecuteNonQuery();
            foreach (var (category, counts) in n.CategoryCounts) {
                Bind(count, n.NodeId, category, counts[0], counts[1], counts[2]);
                count.ExecuteNonQuery();
            }
            foreach (var colName in n.ColNames) {
                Bind(name, n.NodeId, colName, SiteTreeSource.Col, SiteNameKey.Fold(colName));
                name.ExecuteNonQuery();
            }
            foreach (var wikipediaName in n.WikipediaNames) {
                Bind(name, n.NodeId, wikipediaName, SiteNameSource.Wikipedia, SiteNameKey.Fold(wikipediaName));
                name.ExecuteNonQuery();
            }
        }
    }

    /// Inserts rows with one prepared statement; each row's values are in the order of parameters.
    public void InsertRows(string sql, string[] parameters, IEnumerable<object?[]> rows) {
        using var command = Prepare(sql, parameters);
        foreach (var row in rows) {
            Bind(command, row);
            command.ExecuteNonQuery();
        }
    }

    /// Sets replaced_by_assessment_id on an assessment already written.
    public void SetReplacedBy(long assessmentId, long replacedBy) {
        Bind(_replacedBy, replacedBy, assessmentId);
        _replacedBy.ExecuteNonQuery();
    }

    /// Writes one name and remembers its folded key for name_key.
    public void AddName(long taxonId, SiteName name) {
        var nameId = _nextNameId++;
        Bind(_name, nameId, taxonId, name.Name, name.NameType, name.Language, name.Source, name.IsPreferred ? 1 : 0, name.Authority);
        _name.ExecuteNonQuery();
        var key = SiteNameKey.Fold(name.Name);
        if (key.Length > 0) {
            _nameKeys.Add((key, taxonId, nameId));
            if (name.NameType != "common" || name.Language == "en") {
                _wordKeys.Add(key);
            }
        }
    }

    public void SetMeta(string key, string? value) {
        if (value is null) {
            return;
        }
        using var command = _connection.CreateCommand();
        command.Transaction = _transaction;
        command.CommandText = "INSERT OR REPLACE INTO meta (key, value) VALUES (@key, @value)";
        command.Parameters.AddWithValue("@key", key);
        command.Parameters.AddWithValue("@value", value);
        command.ExecuteNonQuery();
    }

    /// Writes name_key, commits, builds the indexes and the full-text index, and compacts the file.
    /// Each step is reported to onStep so the command can time it.
    public void Finish(Action<string, Action> step) {
        step("Writing the exact-name lookup table", () => {
            _nameKeys.Sort((a, b) => {
                var byKey = string.CompareOrdinal(a.Key, b.Key);
                return byKey != 0 ? byKey : a.NameId.CompareTo(b.NameId);
            });
            using var command = Prepare("INSERT INTO name_key (key, taxon_id, name_id) VALUES (@key, @taxon_id, @name_id)",
                "@key", "@taxon_id", "@name_id");
            foreach (var (key, taxonId, nameId) in _nameKeys) {
                Bind(command, key, taxonId, nameId);
                command.ExecuteNonQuery();
            }
            WriteNameWords();
            _nameKeys.Clear();
            _transaction!.Commit();
            _transaction.Dispose();
            _transaction = null;
        });
        step("Creating indexes", () => {
            foreach (var statement in _deferredIndexes) {
                Execute(statement);
            }
        });
        step("Building the search index", () => Execute("INSERT INTO name_fts(name_fts) VALUES('rebuild');"));
        step("Building the search index of extra species", () => Execute("INSERT INTO extra_name_fts(extra_name_fts) VALUES('rebuild');"));
        step("Running ANALYZE", () => Execute("ANALYZE;"));
        step("Compacting the file (VACUUM)", () => {
            Execute("PRAGMA journal_mode = DELETE;");
            Execute("VACUUM;");
        });
        var mode = Scalar("PRAGMA journal_mode;");
        if (!string.Equals(mode, "delete", StringComparison.OrdinalIgnoreCase)) {
            throw new InvalidOperationException($"The site database is in journal mode '{mode}', not 'delete'.");
        }
        _finished = true;
    }

    // name_word: each word of the keys in _wordKeys, with how many of them have it.
    private void WriteNameWords() {
        var uses = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (var key in _wordKeys) {
            foreach (var (start, length) in BeastieBot3.Shared.SiteData.NameWords.Find(key)) {
                var word = key.Substring(start, length);
                uses[word] = uses.GetValueOrDefault(word) + 1;
            }
        }
        using var command = Prepare("INSERT INTO name_word (word, uses) VALUES (@word, @uses)", "@word", "@uses");
        foreach (var (word, n) in uses.OrderBy(p => p.Key, StringComparer.Ordinal)) {
            Bind(command, word, n);
            command.ExecuteNonQuery();
        }
    }

    public string? Scalar(string sql) {
        using var command = _connection.CreateCommand();
        command.Transaction = _transaction;
        command.CommandText = sql;
        command.CommandTimeout = 0;
        return command.ExecuteScalar()?.ToString();
    }

    private void Execute(string sql) {
        using var command = _connection.CreateCommand();
        command.Transaction = _transaction;
        command.CommandText = sql;
        command.CommandTimeout = 0;
        command.ExecuteNonQuery();
    }

    private SqliteCommand Prepare(string sql, params string[] parameters) {
        var command = _connection.CreateCommand();
        command.Transaction = _transaction;
        command.CommandText = sql;
        foreach (var parameter in parameters) {
            command.Parameters.Add(new SqliteParameter { ParameterName = parameter });
        }
        command.Prepare();
        return command;
    }

    private static void Bind(SqliteCommand command, params object?[] values) {
        for (var i = 0; i < values.Length; i++) {
            command.Parameters[i].Value = values[i] ?? DBNull.Value;
        }
    }

    public void Dispose() {
        _taxon.Dispose();
        _assessment.Dispose();
        _name.Dispose();
        _replacedBy.Dispose();
        _epbcListing.Dispose();
        _otherStatus.Dispose();
        _taxonLink.Dispose();
        if (!_finished && _transaction is not null) {
            try {
                _transaction.Rollback();
            } catch (SqliteException) {
                // With journal_mode=OFF a rollback may not be possible; the file is deleted anyway.
            }
        }
        _transaction?.Dispose();
        _connection.Dispose();
    }
}
