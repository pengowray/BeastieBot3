using BeastieBot3.Site.Lists;
using Microsoft.Data.Sqlite;
using static BeastieBot3.Site.Data.ReaderValues;

namespace BeastieBot3.Site.Data;

/// The reads of the group page's species tables (SpeciesTable): what a {{Species table/row}} needs
/// beyond a list line. Kept apart from SiteQueries so the table list type stays in its own files.
public sealed class SpeciesTableQueries(SiteDatabase db) {
    /// The authority, population and citation of every species in the group, by taxon id. The
    /// assessment columns are null for a species with no global assessment.
    public IReadOnlyDictionary<long, TableTaxonExtra> GetExtras(GroupRow group) {
        using var connection = db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT t.taxon_id, t.authority, a.population_trend, a.population_size, a.citation_json,
                   a.wikidata_item_qid, a.wikidata_item_properties
            FROM taxon t LEFT JOIN assessment a ON a.assessment_id = t.latest_global_assessment_id
            WHERE t.tree_pos BETWEEN @first AND @last AND t.kind = 'species'
            """;
        command.Parameters.AddWithValue("@first", group.FirstPos);
        command.Parameters.AddWithValue("@last", group.LastPos);
        using var reader = command.ExecuteReader();
        var rows = new Dictionary<long, TableTaxonExtra>();
        while (reader.Read()) {
            var id = reader.GetInt64(0);
            rows[id] = new TableTaxonExtra(id, Text(reader, 1), Text(reader, 2), Text(reader, 3), Text(reader, 4),
                Text(reader, 5), Text(reader, 6));
        }
        return rows;
    }
}
