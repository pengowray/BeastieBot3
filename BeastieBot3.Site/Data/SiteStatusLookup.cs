using BeastieBot3.Shared.SiteData;
using BeastieBot3.Site.Update;
using Microsoft.Data.Sqlite;
using static BeastieBot3.Site.Data.ReaderValues;

namespace BeastieBot3.Site.Data;

/// The status updater's reads (IStatusLookup) over one connection, which the page holds for one
/// request. Answers are kept, so a taxon or name that appears many times is read once.
public sealed class SiteStatusLookup : IStatusLookup, IDisposable {
    private readonly SqliteConnection _connection;
    private readonly Dictionary<long, StatusTaxon?> _taxa = [];
    private readonly Dictionary<(string, StatusNameKind), IReadOnlyCollection<long>> _names = [];
    private readonly Dictionary<long, string?> _scopes = [];

    private readonly Dictionary<long, StatusTaxon?> _globalTaxa = [];

    internal SiteStatusLookup(SqliteConnection connection, string? region = null) {
        _connection = connection;
        Region = region;
    }

    public string? Region { get; }

    public StatusTaxon? GetTaxon(long taxonId) => Read(taxonId, Region, _taxa);

    public StatusTaxon? GetGlobalTaxon(long taxonId) => Region is null ? GetTaxon(taxonId) : Read(taxonId, null, _globalTaxa);

    // The taxon with its latest global assessment, or with its latest assessment in the region: the
    // one IUCN flags latest, else the newest.
    private StatusTaxon? Read(long taxonId, string? region, Dictionary<long, StatusTaxon?> cache) {
        if (cache.TryGetValue(taxonId, out var known)) {
            return known;
        }
        using var command = _connection.CreateCommand();
        command.CommandText = """
            SELECT t.taxon_id, t.scientific_name, t.in_release, t.current_taxon_id,
                   a.assessment_id, a.taxon_id, a.scope, a.is_latest, a.category, a.possibly_extinct,
                   a.possibly_extinct_in_the_wild, a.criteria, a.criteria_version, a.year_published,
                   a.assessment_date, a.population_trend, a.citation_json, a.population_size,
                   a.wikidata_item_qid, a.wikidata_item_properties, t.kind, t.node_id
            FROM taxon t
            LEFT JOIN assessment a ON a.assessment_id = CASE WHEN @region IS NULL THEN t.latest_global_assessment_id ELSE (
                SELECT r.assessment_id FROM assessment r WHERE r.taxon_id = t.taxon_id AND r.scope = @region
                ORDER BY r.is_latest DESC, r.year_published DESC, r.assessment_id DESC LIMIT 1) END
            WHERE t.taxon_id = @id
            """;
        command.Parameters.AddWithValue("@id", taxonId);
        command.Parameters.AddWithValue("@region", (object?)region ?? DBNull.Value);
        using var reader = command.ExecuteReader();
        StatusTaxon? taxon = null;
        if (reader.Read()) {
            var inRelease = reader.GetInt64(2) != 0;
            AssessmentRow? latest = !inRelease || reader.IsDBNull(4) ? null : new AssessmentRow(
                reader.GetInt64(4),
                reader.GetInt64(5),
                reader.GetString(6),
                reader.GetInt64(7) != 0,
                reader.GetString(8),
                reader.GetInt64(9) != 0,
                reader.GetInt64(10) != 0,
                Text(reader, 11),
                Text(reader, 12),
                reader.IsDBNull(13) ? null : reader.GetInt32(13),
                Text(reader, 14),
                Text(reader, 15),
                Text(reader, 16),
                WikidataItemQid: Text(reader, 18),
                WikidataItemProperties: Text(reader, 19),
                PopulationSize: Text(reader, 17));
            taxon = new StatusTaxon(reader.GetInt64(0), reader.GetString(1), inRelease,
                reader.IsDBNull(3) ? null : reader.GetInt64(3), latest, reader.GetString(20), reader.IsDBNull(21) ? null : reader.GetInt32(21));
        }
        cache[taxonId] = taxon;
        return taxon;
    }

    public IReadOnlyCollection<long> InReleaseTaxaWithName(string name, StatusNameKind kind) {
        var key = SiteNameKey.Fold(name);
        if (key.Length == 0) {
            return [];
        }
        if (_names.TryGetValue((key, kind), out var known)) {
            return known;
        }
        using var command = _connection.CreateCommand();
        command.CommandText = """
            SELECT DISTINCT k.taxon_id
            FROM name_key k
            JOIN name n ON n.name_id = k.name_id
            JOIN taxon t ON t.taxon_id = k.taxon_id
            WHERE k.key = @key AND n.name_type = @type AND t.in_release = 1
              AND (@type <> 'common' OR n.language = 'en')
              AND (@source IS NULL OR n.source = @source)
            """;
        command.Parameters.AddWithValue("@key", key);
        command.Parameters.AddWithValue("@type", kind switch {
            StatusNameKind.Synonym => NameTypes.Synonym,
            StatusNameKind.EnglishCommonName or StatusNameKind.ArticleTitle => NameTypes.Common,
            _ => NameTypes.Scientific,
        });
        // An article title is stored as an English common name from the source 'wikipedia'.
        command.Parameters.AddWithValue("@source", kind == StatusNameKind.ArticleTitle ? "wikipedia" : DBNull.Value);
        using var reader = command.ExecuteReader();
        var ids = new List<long>();
        while (reader.Read()) {
            ids.Add(reader.GetInt64(0));
        }
        _names[(key, kind)] = ids;
        return ids;
    }

    public string? AssessmentScope(long assessmentId) {
        if (_scopes.TryGetValue(assessmentId, out var known)) {
            return known;
        }
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT scope FROM assessment WHERE assessment_id = @id";
        command.Parameters.AddWithValue("@id", assessmentId);
        var scope = command.ExecuteScalar() as string;
        _scopes[assessmentId] = scope;
        return scope;
    }

    public void Dispose() => _connection.Dispose();
}
