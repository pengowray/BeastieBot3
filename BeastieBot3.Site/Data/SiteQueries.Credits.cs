using BeastieBot3.Shared.SiteData;

namespace BeastieBot3.Site.Data;

public sealed partial class SiteQueries {
    /// The credits of one assessment, as stored (StoredCredits), with the text of every credit_name
    /// id they use. Empty when the assessment has none. Two primary-key reads: the row, then the
    /// names, whose ids go in as one JSON array so a long contributor list needs one parameter.
    public AssessmentCredits GetCredits(long assessmentId) {
        using var connection = _db.OpenConnection();
        string? json;
        using (var command = connection.CreateCommand()) {
            command.CommandText = "SELECT credits FROM assessment WHERE assessment_id = @id";
            command.Parameters.AddWithValue("@id", assessmentId);
            json = command.ExecuteScalar() as string;
        }
        var groups = StoredCredits.FromJson(json);
        if (groups.Count == 0) {
            return AssessmentCredits.None;
        }
        var names = new Dictionary<long, string>();
        using (var command = connection.CreateCommand()) {
            command.CommandText = "SELECT credit_name_id, text FROM credit_name WHERE credit_name_id IN (SELECT value FROM json_each(@ids))";
            command.Parameters.AddWithValue("@ids", "[" + string.Join(',', StoredCredits.NameIds(groups)) + "]");
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                names[reader.GetInt64(0)] = reader.GetString(1);
            }
        }
        return new AssessmentCredits(groups, names);
    }
}

/// An assessment's stored credit groups and the text of each credit_name id they use.
public sealed record AssessmentCredits(IReadOnlyList<StoredCreditGroup> Groups, IReadOnlyDictionary<long, string> Names) {
    public static readonly AssessmentCredits None = new([], new Dictionary<long, string>());
}
