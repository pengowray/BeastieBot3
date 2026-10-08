using System.Globalization;

namespace BeastieBot3.Site.Display;

// The assessment history tables' "Reason for change" column and their footnotes, from IUCN's summary
// statistics Table 7 ("Species changing IUCN Red List Status") and Table 9 ("Possibly Extinct and
// Possibly Extinct in the Wild Species").
public static partial class SiteText {
    public const string ColReasonForChange = "Reason for change";

    /// "2007 to 2026-1", or the one version; null when either is missing.
    public static string? VersionRange(string? first, string? last) =>
        first is null || last is null ? null : first == last ? first : $"{first} to {last}";

    public const string SummaryStatisticsUrl = "https://www.iucnredlist.org/resources/summary-statistics";

    // About page: the data sources table's row for IUCN's summary tables.
    public const string AboutSummaryTablesSource = "IUCN Red List summary statistics, Tables 7 and 9";
    public const string AboutSummaryTablesUse =
        "The reason for each change of Red List category (Table 7), and species listed as Possibly Extinct in the tables whose assessment is not flagged as Possibly Extinct (Tables 7 and 9)";
    public static string AboutSummaryTablesVersions(string? table7, string? table9) {
        static string? Part(int table, string? range) =>
            range is null ? null : $"Table {table}: {(range.Contains(" to ", StringComparison.Ordinal) ? "versions" : "version")} {range}";
        return string.Join("; ", new[] { Part(7, table7), Part(9, table9) }.OfType<string>());
    }

    /// The cell of a reason code: IUCN's name for it in Table 7, then the letter the table prints.
    public static string ReasonLabel(string code) => code switch {
        "G" => "Genuine status change (G)",
        "N" => "Non-genuine status change (N)",
        "E" => "Previous listing was an error (E)",
        _ => code,
    };

    // A "Reason for change" cell with no reason, in light italic: the first global assessment, the
    // same category as the assessment before it, a change published in 2008 (whose table lists
    // genuine changes only), or a change that no Table 7 row was matched to.
    public const string ReasonFirstAssessment = "first assessment";
    public const string ReasonNoChange = "no change";
    public const string ReasonNoneGiven = "no reason given";
    public const string ReasonNotFound = "not found";
    /// The cell of an assessment published before the first Table 7, and the line under the table
    /// that explains it.
    public const string ReasonBeforeTables = "—";
    public const string ReasonBeforeTablesLegend = "— Published before 2007, the year of IUCN's first Table 7.";

    /// The accessible name of a footnote reference.
    public static string FootnoteLabel(int number) => $"Footnote {number.ToString(CultureInfo.InvariantCulture)}";

    // A footnote with the Table 7 a reason is from: TableFootnoteBefore + link(TableFootnoteLink) + TableFootnoteAfter,
    // and for the 2008 table, TableFootnote2008 after it.
    public const string TableFootnoteBefore = "From ";
    public static string TableFootnoteLink(string release) =>
        $"Table 7 (“Species changing IUCN Red List Status”) of IUCN Red List version {release}";
    public const string TableFootnoteAfter = " (PDF).";
    public const string TableFootnote2008 = " The 2008 table lists genuine changes only.";

    public static string TagName(string tag) => tag == "PEW" ? "Possibly Extinct in the Wild" : "Possibly Extinct";

    /// The footnote of a CR (PE) or CR (PEW) badge that comes from IUCN's tables, not the assessment:
    /// "Table 9 of IUCN Red List versions 2014-1 to 2016-3 lists this species as Possibly Extinct. The
    /// assessment itself is not flagged as Possibly Extinct." IUCN calls PE and PEW tags, and says an
    /// assessment is "flagged as" one. otherTag: the tag the assessment has instead ("PEW"), or null.
    public static string ListedTagFootnote(string tag, string tables, string firstRelease, string lastRelease, string kind, string? otherTag) {
        var both = tables.Contains('7') && tables.Contains('9');
        var tableName = both ? "Tables 7 and 9" : $"Table {tables.Trim()}";
        var versions = firstRelease == lastRelease ? $"version {firstRelease}" : $"versions {firstRelease} to {lastRelease}";
        var verb = both ? "list" : "lists";
        var itself = otherTag is null
            ? $"The assessment itself is not flagged as {TagName(tag)}."
            : $"The assessment itself is flagged as {TagName(otherTag)}.";
        return $"{tableName} of IUCN Red List {versions} {verb} this {kind} as {TagName(tag)}. {itself}";
    }
}
