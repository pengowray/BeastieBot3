using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Microsoft.Data.Sqlite;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.WikidataEdits;

// Wikidata items that are IUCN Red List assessment publications (most are scholarly articles a
// 2018 SourceMD batch created from DOIs; a few newer ones are data sets), as stored in the
// wikidata_iucn_assessment_items table of the Wikidata cache. Discovered and written by
// `wikidata iucn-assessment-items`; read by the status dry run so it cites these items rather
// than proposing duplicates.
//
// List columns (all_dois, instance_of, main_subjects, published_in, urls, found_by) hold values
// separated by single spaces: none of the values can contain one.
//
// title_statements holds every title (P1476) statement of the item, of any rank, as JSON
// [{"text":..., "lang":..., "rank":...}], so a QuickStatements batch can remove an old title by its
// exact text and language. NULL means the row was written before the column existed (or by a run
// that did not read the statements); [] means the item has no title statement. The title column
// keeps the best-ranked English title (wdt:P1476), which the status dry run reads.

namespace BeastieBot3.Wikidata;

internal sealed record WikidataAssessmentItemRow {
    public required string Qid { get; init; }
    /// The DOI the ids came from, else the first IUCN assessment DOI, else the first DOI.
    public string? Doi { get; init; }
    public IReadOnlyList<string> AllDois { get; init; } = Array.Empty<string>();
    public long? TaxonId { get; init; }
    public long? AssessmentId { get; init; }
    /// The Red List version token inside the DOI ("2015-4", "2008"). This, not P577, says which
    /// release the item is: on the 2018 batch P577 holds the assessment date.
    public string? DoiRelease { get; init; }
    public string? DoiLanguage { get; init; }
    /// "doi" or "url": where TaxonId/AssessmentId were read from. Null when nothing parsed.
    public string? IdSource { get; init; }
    /// P1476 title, English when there is one.
    public string? Title { get; init; }
    /// Every P1476 statement, any rank, duplicates kept. Null when not recorded (a row written
    /// before title_statements existed).
    public IReadOnlyList<WikidataTitle>? TitleStatements { get; init; }
    public string? LabelEn { get; init; }
    public IReadOnlyList<string> InstanceOf { get; init; } = Array.Empty<string>();
    /// P921 main subject.
    public IReadOnlyList<string> MainSubjects { get; init; } = Array.Empty<string>();
    /// P1433 published in.
    public IReadOnlyList<string> PublishedIn { get; init; } = Array.Empty<string>();
    /// Earliest P577 value in Wikibase form ("2014-06-17T00:00:00Z").
    public string? PublicationDate { get; init; }
    public int? PublicationYear { get; init; }
    /// Distinct P50 author items.
    public int AuthorItemCount { get; init; }
    /// Distinct P2093 author name strings.
    public int AuthorStringCount { get; init; }
    /// P953 full work URL, P854 reference URL, P856 official website.
    public IReadOnlyList<string> Urls { get; init; } = Array.Empty<string>();
    /// Discovery routes that returned the item, e.g. "published-in:scholarly doi-search".
    public IReadOnlyList<string> FoundBy { get; init; } = Array.Empty<string>();
    /// "scholarly" or "main": the query service graph the item's statements were read from.
    public string? SourceEndpoint { get; init; }
    /// schema:dateModified of the item when fetched.
    public string? ModifiedAt { get; init; }
    public required DateTime FetchedAtUtc { get; init; }
    public DateTime? FirstSeenAtUtc { get; init; }

    public bool IsTaxonItem => InstanceOf.Contains(WikidataAssessmentItemTable.TaxonQid, StringComparer.Ordinal);

    public ExistingAssessmentItem ToExistingAssessmentItem() => new() {
        Qid = Qid,
        TaxonId = TaxonId,
        AssessmentId = AssessmentId,
        Doi = Doi,
        Title = Title ?? LabelEn,
        InstanceOf = InstanceOf,
        MainSubjects = MainSubjects,
        PublicationYear = PublicationYear,
    };
}

internal static class WikidataAssessmentItemTable {
    public const string TableName = "wikidata_iucn_assessment_items";
    public const string RedListQid = "Q32059";
    public const string TaxonQid = "Q16521";

    public const string Ddl =
        """
CREATE TABLE IF NOT EXISTS wikidata_iucn_assessment_items (
    qid TEXT PRIMARY KEY,
    qid_numeric INTEGER NOT NULL,
    doi TEXT,
    all_dois TEXT,
    taxon_id INTEGER,
    assessment_id INTEGER,
    doi_release TEXT,
    doi_language TEXT,
    id_source TEXT,
    title TEXT,
    title_statements TEXT,
    label_en TEXT,
    instance_of TEXT,
    main_subjects TEXT,
    published_in TEXT,
    publication_date TEXT,
    publication_year INTEGER,
    author_item_count INTEGER NOT NULL DEFAULT 0,
    author_string_count INTEGER NOT NULL DEFAULT 0,
    urls TEXT,
    found_by TEXT,
    source_endpoint TEXT,
    modified_at TEXT,
    fetched_at TEXT NOT NULL,
    first_seen_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_wikidata_iucn_assessment_items_taxon ON wikidata_iucn_assessment_items(taxon_id);
CREATE INDEX IF NOT EXISTS idx_wikidata_iucn_assessment_items_assessment ON wikidata_iucn_assessment_items(assessment_id);
""";

    private const string SelectColumns =
        "qid, doi, all_dois, taxon_id, assessment_id, doi_release, doi_language, id_source, title, label_en, " +
        "instance_of, main_subjects, published_in, publication_date, publication_year, author_item_count, " +
        "author_string_count, urls, found_by, source_endpoint, modified_at, fetched_at, first_seen_at";

    public const string TitleStatementsColumn = "title_statements";

    /// True when the table has the title_statements column. A cache written before it existed and
    /// opened read-only (site build-db, the dry run) has not been migrated.
    public static bool HasTitleStatements(SqliteConnection connection) {
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT 1 FROM pragma_table_info('{TableName}') WHERE name = '{TitleStatementsColumn}'";
        return command.ExecuteScalar() is not null;
    }

    public static bool Exists(SqliteConnection connection) {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1 FROM sqlite_master WHERE type='table' AND name=@name";
        command.Parameters.AddWithValue("@name", TableName);
        return command.ExecuteScalar() is not null;
    }

    public static IReadOnlyList<WikidataAssessmentItemRow> ReadAll(SqliteConnection connection) {
        var list = new List<WikidataAssessmentItemRow>();
        var titles = HasTitleStatements(connection) ? TitleStatementsColumn : "NULL";
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT {SelectColumns}, {titles} FROM {TableName} ORDER BY qid_numeric";
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            list.Add(new WikidataAssessmentItemRow {
                Qid = reader.GetString(0),
                Doi = NullableString(reader, 1),
                AllDois = SplitList(NullableString(reader, 2)),
                TaxonId = reader.IsDBNull(3) ? null : reader.GetInt64(3),
                AssessmentId = reader.IsDBNull(4) ? null : reader.GetInt64(4),
                DoiRelease = NullableString(reader, 5),
                DoiLanguage = NullableString(reader, 6),
                IdSource = NullableString(reader, 7),
                Title = NullableString(reader, 8),
                LabelEn = NullableString(reader, 9),
                InstanceOf = SplitList(NullableString(reader, 10)),
                MainSubjects = SplitList(NullableString(reader, 11)),
                PublishedIn = SplitList(NullableString(reader, 12)),
                PublicationDate = NullableString(reader, 13),
                PublicationYear = reader.IsDBNull(14) ? null : reader.GetInt32(14),
                AuthorItemCount = reader.GetInt32(15),
                AuthorStringCount = reader.GetInt32(16),
                Urls = SplitList(NullableString(reader, 17)),
                FoundBy = SplitList(NullableString(reader, 18)),
                SourceEndpoint = NullableString(reader, 19),
                ModifiedAt = NullableString(reader, 20),
                FetchedAtUtc = ParseUtc(reader.GetString(21)) ?? DateTime.MinValue,
                FirstSeenAtUtc = ParseUtc(NullableString(reader, 22)),
                TitleStatements = WikidataTitle.ListFromJson(NullableString(reader, 23)),
            });
        }

        return list;
    }

    public static string? JoinList(IReadOnlyList<string> values) => values.Count == 0 ? null : string.Join(' ', values);

    public static IReadOnlyList<string> SplitList(string? value) =>
        string.IsNullOrWhiteSpace(value)
            ? Array.Empty<string>()
            : value.Split(' ', StringSplitOptions.RemoveEmptyEntries);

    // Stored as a UTC "O" string; RoundtripKind keeps the Z from being read as local time.
    public static DateTime? ParseUtc(string? value) =>
        DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var parsed)
            ? parsed.ToUniversalTime()
            : null;

    private static string? NullableString(SqliteDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
}

/// One statement value read from the query service: Property is "P31", "label" or "modified", or
/// TitleStatementProperty for a title statement of any rank; Value is an item id ("Q13442814") or
/// the literal; Language is set for monolingual text. Rank and Statement (the statement node) are
/// set for title statements only.
internal readonly record struct WikidataTriple(string Property, string Value, string? Language, string? Rank = null, string? Statement = null);

internal static class WikidataAssessmentItemBuilder {
    public static WikidataAssessmentItemRow Build(
        string qid,
        IEnumerable<WikidataTriple> triples,
        IReadOnlyList<string> foundBy,
        string sourceEndpoint,
        DateTime fetchedAtUtc) {
        var byProperty = triples
            .GroupBy(t => t.Property, StringComparer.Ordinal)
            .ToDictionary(g => g.Key, g => g.ToList(), StringComparer.Ordinal);

        IReadOnlyList<string> Values(string property) =>
            byProperty.TryGetValue(property, out var list)
                ? list.Select(t => t.Value).Distinct(StringComparer.Ordinal).OrderBy(v => v, StringComparer.Ordinal).ToList()
                : Array.Empty<string>();

        var dois = Values("P356");
        var urls = Values("P953").Concat(Values("P854")).Concat(Values("P856"))
            .Distinct(StringComparer.Ordinal).OrderBy(v => v, StringComparer.Ordinal).ToList();
        var parsed = IucnAssessmentRefParser.FromItem(dois, urls);

        var primaryDoi = dois.FirstOrDefault(d => IucnAssessmentRefParser.TryParseDoi(d) is not null)
            ?? dois.FirstOrDefault(IucnAssessmentRefParser.IsIucnAssessmentDoi)
            ?? dois.FirstOrDefault();

        var dates = Values("P577").Where(v => !v.Contains("/genid/", StringComparison.Ordinal)).ToList();
        var date = dates.FirstOrDefault();

        return new WikidataAssessmentItemRow {
            Qid = qid,
            Doi = primaryDoi,
            AllDois = dois,
            TaxonId = parsed?.TaxonId,
            AssessmentId = parsed?.AssessmentId,
            DoiRelease = parsed?.Release,
            DoiLanguage = parsed?.Language,
            IdSource = parsed is null ? null : parsed.Source == IucnAssessmentRefSource.Doi ? "doi" : "url",
            Title = PickText(byProperty, "P1476"),
            TitleStatements = TitleStatements(byProperty),
            LabelEn = PickText(byProperty, "label", englishOnly: true),
            InstanceOf = ItemIds(Values("P31")),
            MainSubjects = ItemIds(Values("P921")),
            PublishedIn = ItemIds(Values("P1433")),
            PublicationDate = date,
            PublicationYear = ParseYear(date),
            AuthorItemCount = ItemIds(Values("P50")).Count,
            AuthorStringCount = Values("P2093").Count,
            Urls = urls,
            FoundBy = foundBy,
            SourceEndpoint = sourceEndpoint,
            ModifiedAt = Values("modified").FirstOrDefault(),
            FetchedAtUtc = fetchedAtUtc,
        };
    }

    // "+2014-06-17T00:00:00Z" or "2014-06-17T00:00:00Z"; the query service drops the plus sign.
    internal static int? ParseYear(string? value) {
        if (string.IsNullOrWhiteSpace(value)) {
            return null;
        }

        var text = value.TrimStart('+');
        var dash = text.IndexOf('-', 1);
        return dash > 0 && int.TryParse(text[..dash], NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var year)
            ? year
            : null;
    }

    // One entry per statement node, sorted so a re-run writes the same JSON for the same item.
    private static IReadOnlyList<WikidataTitle> TitleStatements(Dictionary<string, List<WikidataTriple>> byProperty) {
        if (!byProperty.TryGetValue(WikidataAssessmentItemQueries.TitleStatementProperty, out var list)) {
            return Array.Empty<WikidataTitle>();
        }

        return list
            .GroupBy(t => t.Statement ?? $"{t.Value}\u0000{t.Language}\u0000{t.Rank}", StringComparer.Ordinal)
            .Select(g => g.First())
            .Select(t => new WikidataTitle(t.Value, t.Language, t.Rank ?? "normal"))
            .OrderBy(t => t.Text, StringComparer.Ordinal)
            .ThenBy(t => t.Language, StringComparer.Ordinal)
            .ThenBy(t => t.Rank, StringComparer.Ordinal)
            .ToList();
    }

    private static IReadOnlyList<string> ItemIds(IReadOnlyList<string> values) =>
        values.Where(v => v.Length > 1 && v[0] == 'Q' && v.Skip(1).All(char.IsAsciiDigit)).ToList();

    private static string? PickText(Dictionary<string, List<WikidataTriple>> byProperty, string property, bool englishOnly = false) {
        if (!byProperty.TryGetValue(property, out var list) || list.Count == 0) {
            return null;
        }

        var english = list.Where(t => string.Equals(t.Language, "en", StringComparison.OrdinalIgnoreCase))
            .Select(t => t.Value).OrderBy(v => v, StringComparer.Ordinal).FirstOrDefault();
        if (english is not null || englishOnly) {
            return english;
        }

        return list.Select(t => t.Value).OrderBy(v => v, StringComparer.Ordinal).FirstOrDefault();
    }
}
