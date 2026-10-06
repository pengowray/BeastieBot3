using BeastieBot3.Site.Lists;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Site.Data;

/// The citation of each assessed taxon's latest global assessment in a group, of every kind (species,
/// subspecies, varieties, subpopulations), for the references after the lines of a bullet list
/// (ListReferences). Kept apart from SiteQueries so the reference option stays in its own files.
public sealed class ListReferenceQueries(SiteDatabase db) {
    public IReadOnlyDictionary<long, AssessmentCitation> GetCitations(GroupRow group) {
        using var connection = db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT t.taxon_id, a.citation_json, a.wikidata_item_qid, a.wikidata_item_properties
            FROM taxon t JOIN assessment a ON a.assessment_id = t.latest_global_assessment_id
            WHERE t.tree_pos BETWEEN @first AND @last
            """;
        command.Parameters.AddWithValue("@first", group.FirstPos);
        command.Parameters.AddWithValue("@last", group.LastPos);
        using var reader = command.ExecuteReader();
        var rows = new Dictionary<long, AssessmentCitation>();
        while (reader.Read()) {
            var id = reader.GetInt64(0);
            rows[id] = new AssessmentCitation(Text(reader, 1), Text(reader, 2), Text(reader, 3));
        }
        return rows;
    }

    private static string? Text(SqliteDataReader reader, int i) => reader.IsDBNull(i) ? null : reader.GetString(i);
}
