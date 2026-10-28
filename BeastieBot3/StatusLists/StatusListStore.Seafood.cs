// The status lists store's seafood guide tables (`seafood goodfish-import`, `seafood seafoodwatch-import`).

namespace BeastieBot3.StatusLists;

internal sealed partial class StatusListStore {
    public long CountSeafoodRatings(string guide) =>
        Scalar("SELECT COUNT(*) FROM seafood_rating WHERE guide = @guide", ("@guide", guide));

    /// Replaces every rating of one guide, with their names, and the guide's source row, in one
    /// transaction.
    public void ReplaceSeafoodRatings(string guide, IReadOnlyList<SeafoodRating> rows, DateTime importedAtUtc, StatusSourceInfo source) {
        using var tx = _connection.BeginTransaction();
        Execute(tx, "DELETE FROM seafood_rating WHERE guide = @guide", ("@guide", guide));
        using var insert = _connection.CreateCommand();
        insert.Transaction = tx;
        insert.CommandText = """
            INSERT INTO seafood_rating(guide, rating_id, common_name, group_name, ratings, color, score, region, water_body, method,
                production, fishery, note, url, published, modified, imported_at)
            VALUES (@guide, @id, @common, @group, @ratings, @color, @score, @region, @water, @method, @production, @fishery, @note, @url,
                @published, @modified, @imported)
            ON CONFLICT(guide, rating_id) DO NOTHING
            """;
        var names = new[] { "@guide", "@id", "@common", "@group", "@ratings", "@color", "@score", "@region", "@water", "@method",
            "@production", "@fishery", "@note", "@url", "@published", "@modified" };
        var p = names.ToDictionary(n => n, n => AddParameter(insert, n), StringComparer.Ordinal);
        insert.Parameters.AddWithValue("@imported", Stamp(importedAtUtc));
        using var insertName = _connection.CreateCommand();
        insertName.Transaction = tx;
        insertName.CommandText = """
            INSERT INTO seafood_rating_name(guide, rating_id, position, name, scientific_name)
            VALUES (@guide, @id, @position, @name, @scientific)
            """;
        var n = new[] { "@guide", "@id", "@position", "@name", "@scientific" }.ToDictionary(x => x, x => AddParameter(insertName, x), StringComparer.Ordinal);
        static object Value(object? v) => v ?? DBNull.Value;
        static object Joined(IReadOnlyList<string> parts) => parts.Count == 0 ? DBNull.Value : string.Join('|', parts);
        foreach (var row in rows) {
            p["@guide"].Value = guide;
            p["@id"].Value = row.RatingId;
            p["@common"].Value = row.CommonName;
            p["@group"].Value = Value(row.GroupName);
            p["@ratings"].Value = string.Join('|', row.Ratings);
            p["@color"].Value = Value(row.Color);
            p["@score"].Value = Value(row.Score);
            p["@region"].Value = Value(row.Region);
            p["@water"].Value = Value(row.WaterBody);
            p["@method"].Value = Joined(row.Methods);
            p["@production"].Value = Value(row.Production);
            p["@fishery"].Value = Joined(row.Fisheries);
            p["@note"].Value = Value(row.Note);
            p["@url"].Value = row.Url;
            p["@published"].Value = Value(row.Published);
            p["@modified"].Value = Value(row.Modified);
            if (insert.ExecuteNonQuery() == 0) {
                continue;
            }
            for (var i = 0; i < row.Names.Count; i++) {
                n["@guide"].Value = guide;
                n["@id"].Value = row.RatingId;
                n["@position"].Value = i;
                n["@name"].Value = row.Names[i].Written;
                n["@scientific"].Value = Value(row.Names[i].ScientificName);
                insertName.ExecuteNonQuery();
            }
        }
        UpsertSource(source, tx);
        tx.Commit();
    }
}
