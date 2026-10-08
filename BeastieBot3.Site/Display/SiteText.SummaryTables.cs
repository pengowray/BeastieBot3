using System.Globalization;

namespace BeastieBot3.Site.Display;

// The assessment history tables' "Reason for change" column and the [PE] markers, from IUCN's summary
// statistics Table 7 ("Species changing IUCN Red List Status") and Table 9 ("Possibly Extinct and
// Possibly Extinct in the Wild Species"). The tooltips quote IUCN's definitions of the codes.
public static partial class SiteText {
    public const string ColReasonForChange = "Reason for change";

    /// "2007 to 2026-1", or the one version; null when either is missing.
    public static string? VersionRange(string? first, string? last) =>
        first is null || last is null ? null : first == last ? first : $"{first} to {last}";

    public const string SummaryStatisticsUrl = "https://www.iucnredlist.org/resources/summary-statistics";

    // About page: the data sources table's row for IUCN's summary tables.
    public const string AboutSummaryTablesSource = "IUCN Red List summary statistics, Tables 7 and 9";
    public const string AboutSummaryTablesUse =
        "The reason for each change of Red List category (Table 7), and species listed as Possibly Extinct in the tables but not tagged in their assessment (Tables 7 and 9)";
    public static string AboutSummaryTablesVersions(string? table7, string? table9) {
        static string? Part(int table, string? range) =>
            range is null ? null : $"Table {table}: {(range.Contains(" to ", StringComparison.Ordinal) ? "versions" : "version")} {range}";
        return string.Join("; ", new[] { Part(7, table7), Part(9, table9) }.OfType<string>());
    }

    /// The cell of a reason code: IUCN's name for it, then the letter IUCN's table prints.
    public static string ReasonLabel(string code) => code switch {
        "G" => "Genuine change (G)",
        "N" => "Non-genuine change (N)",
        "E" => "Previous listing was an error (E)",
        _ => code,
    };

    /// The tooltip of a reason cell: IUCN's definition of the code and the table it is from.
    public static string ReasonTooltip(string code, string release) {
        var source = $"Source: Table 7 of IUCN Red List version {release}.";
        return code switch {
            "G" => $"Genuine status change: a genuine improvement or deterioration in the species' status. {source}",
            "N" => "Non-genuine status change: the category changed because of new information, improved knowledge of the criteria, "
                + $"incorrect data used previously, taxonomic revision or similar. {source}",
            "E" => $"Previous listing was an error. {source}",
            _ => source,
        };
    }

    // The note under a history table with a "Reason for change" column:
    // ReasonNoteBefore + ReasonNoteVersions(n) + link(version) [+ separator + link ...] + ReasonNoteAfter.
    public const string ReasonNoteBefore =
        "The “Reason for change” entries on this page are from Table 7 (“Species changing IUCN Red List Status”) of IUCN Red List ";
    public static string ReasonNoteVersions(int count) => count == 1 ? "version " : "versions ";
    public static string VersionSeparator(int index, int count) => index == 0 ? string.Empty : index == count - 1 ? " and " : ", ";
    public const string ReasonNoteAfter = " (PDF).";
    public static string ReasonNoteLinkLabel(string release) => $"Table 7 of IUCN Red List version {release} (PDF)";
    public const string ReasonNoteBlank =
        "A blank cell means that Table 7 lists no reason for the assessment in that row: the category did not change, "
        + "the change was published before 2007, or this site could not match the assessment to a row in Table 7.";
    public const string ReasonNote2008 =
        "Table 7 of version 2008 lists genuine changes only, so a blank cell in a row published in 2008 can also mean a non-genuine change.";

    /// The marker after the category of an assessment that IUCN's tables list as Possibly Extinct
    /// (tag "PE") or Possibly Extinct in the Wild ("PEW") when the assessment itself has no such tag.
    public static string ListedTagMarker(string tag) => $"[{tag}]";

    public static string TagName(string tag) => tag == "PEW" ? "Possibly Extinct in the Wild" : "Possibly Extinct";

    /// The accessible name of the marker's link to its footnote. kind: the taxon's kind ("species").
    public static string ListedTagMarkerLabel(string tag, string kind) =>
        $"Footnote: IUCN's summary tables list this {kind} as {TagName(tag)}. The assessment has no {TagName(tag)} tag.";

    /// The footnote of a marker: "[PE] Assessment published in 2008: Table 9 of IUCN Red List versions
    /// 2014-1 to 2016-3 lists this species as Possibly Extinct. The assessment itself has no Possibly
    /// Extinct tag." otherId: the IUCN id of a row of a combined history that is not this page's.
    /// otherTag: the tag the assessment has instead ("PEW"), or null.
    public static string ListedTagFootnote(string tag, int? year, long? otherId, string tables, string firstRelease,
        string lastRelease, string kind, string? otherTag) {
        var row = year is null ? "Assessment" : $"Assessment published in {year}";
        if (otherId is { } id) row += $" (IUCN ID {id.ToString(CultureInfo.InvariantCulture)})";
        var both = tables.Contains('7') && tables.Contains('9');
        var tableName = both ? "Tables 7 and 9" : $"Table {tables.Trim()}";
        var versions = firstRelease == lastRelease ? $"version {firstRelease}" : $"versions {firstRelease} to {lastRelease}";
        var verb = both ? "list" : "lists";
        var itself = otherTag is null
            ? $"The assessment itself has no {TagName(tag)} tag."
            : $"The assessment itself is tagged {TagName(otherTag)}.";
        return $"{ListedTagMarker(tag)} {row}: {tableName} of IUCN Red List {versions} {verb} this {kind} as {TagName(tag)}. {itself}";
    }
}
