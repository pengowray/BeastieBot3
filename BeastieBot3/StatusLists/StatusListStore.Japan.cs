// The status lists store's table of Japan's Red List (`statuses japan-import`): japan_listing.

namespace BeastieBot3.StatusLists;

internal sealed partial class StatusListStore {
    public long CountJapan() => Scalar("SELECT COUNT(*) FROM japan_listing");

    /// Replaces every row of Japan's Red List, and its source row, in one transaction. Rows are
    /// numbered in the order given.
    public void ReplaceJapan(IReadOnlyList<JapanRedListEntry> rows, DateTime importedAtUtc, StatusSourceInfo source) {
        using var tx = _connection.BeginTransaction();
        Execute(tx, "DELETE FROM japan_listing");
        using var insert = _connection.CreateCommand();
        insert.Transaction = tx;
        insert.CommandText = """
            INSERT INTO japan_listing(row_id, group_key, group_en, group_ja, kingdom, list_version, list_year, category, category_ja,
                japanese_name, scientific_name, population, higher_taxa, criteria, list_number, source_file, source_url, source_page,
                source_line, imported_at)
            VALUES (@row, @key, @en, @ja, @kingdom, @version, @year, @category, @categoryJa, @japaneseName, @name, @population,
                @taxa, @criteria, @number, @file, @url, @page, @line, @imported)
            """;
        var names = new[] { "@row", "@key", "@en", "@ja", "@kingdom", "@version", "@year", "@category", "@categoryJa", "@japaneseName",
            "@name", "@population", "@taxa", "@criteria", "@number", "@file", "@url", "@page", "@line" };
        var p = names.ToDictionary(n => n, n => AddParameter(insert, n), StringComparer.Ordinal);
        insert.Parameters.AddWithValue("@imported", Stamp(importedAtUtc));
        static object Value(object? v) => v ?? DBNull.Value;
        var rowId = 0;
        foreach (var row in rows) {
            var group = row.Group;
            p["@row"].Value = ++rowId;
            p["@key"].Value = group.Key;
            p["@en"].Value = group.English;
            p["@ja"].Value = group.Japanese;
            p["@kingdom"].Value = Value(group.Kingdom);
            p["@version"].Value = group.ListVersion;
            p["@year"].Value = group.ListYear;
            p["@category"].Value = row.Category;
            p["@categoryJa"].Value = row.CategoryJapanese;
            p["@japaneseName"].Value = Value(row.JapaneseName);
            p["@name"].Value = row.ScientificName;
            p["@population"].Value = Value(row.Population);
            p["@taxa"].Value = Value(row.HigherTaxa);
            p["@criteria"].Value = Value(row.Criteria);
            p["@number"].Value = Value(row.ListNumber);
            p["@file"].Value = group.FileName;
            p["@url"].Value = group.Url;
            p["@page"].Value = Value(row.SourcePage);
            p["@line"].Value = row.SourceLine;
            insert.ExecuteNonQuery();
        }
        UpsertSource(source, tx);
        tx.Commit();
    }
}
