using System.Runtime.CompilerServices;
using BeastieBot3.Site.Display;

namespace BeastieBot3.Site.Data;

public sealed partial class SiteQueries {
    // One lookup per database file: a new file has a new snapshot.
    private static readonly ConditionalWeakTable<SiteSnapshot, Lazy<IReadOnlyDictionary<string, ExampleTaxon>>> ExampleTaxa = new();

    /// The species of HomeExamples' pools that are in the release, by scientific name, with the
    /// category of their latest global assessment; empty when the database is not ready.
    public IReadOnlyDictionary<string, ExampleTaxon> GetExampleTaxa() {
        if (_db.Snapshot is not { } snapshot) {
            return new Dictionary<string, ExampleTaxon>();
        }
        return ExampleTaxa.GetValue(snapshot, _ => new Lazy<IReadOnlyDictionary<string, ExampleTaxon>>(LoadExampleTaxa)).Value;
    }

    private IReadOnlyDictionary<string, ExampleTaxon> LoadExampleTaxa() {
        var names = HomeExamples.AllNames.Distinct().ToList();
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        var parameters = names.Select((name, i) => {
            command.Parameters.AddWithValue($"@n{i}", name);
            return $"@n{i}";
        }).ToList();
        command.CommandText = $"""
            SELECT t.taxon_id, t.scientific_name, t.common_name_en, a.category, a.possibly_extinct, a.possibly_extinct_in_the_wild
            FROM taxon t LEFT JOIN assessment a ON a.assessment_id = t.latest_global_assessment_id
            WHERE t.scientific_name IN ({string.Join(", ", parameters)}) AND t.kind = 'species' AND t.in_release = 1
            """;
        var taxa = new Dictionary<string, ExampleTaxon>(StringComparer.Ordinal);
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            var name = reader.GetString(1);
            var category = reader.IsDBNull(3) ? null : reader.GetString(3);
            if (category == "CR" && !reader.IsDBNull(4) && reader.GetInt64(4) != 0) {
                category = "CR(PE)";
            } else if (category == "CR" && !reader.IsDBNull(5) && reader.GetInt64(5) != 0) {
                category = "CR(PEW)";
            }
            taxa.TryAdd(name, new ExampleTaxon(reader.GetInt64(0), name, reader.IsDBNull(2) ? null : reader.GetString(2), category));
        }
        return taxa;
    }
}
