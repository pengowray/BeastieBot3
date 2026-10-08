using BeastieBot3.Shared.SiteData;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;

namespace BeastieBot3.Site.Display;

/// A part of a note under the tables: plain text, or a link when Href is set.
public sealed record NoteSegment(string Text, string? Href = null);

/// The heading of a table's date column, with its hover title or null.
public sealed record OtherStatusDateHeading(string Text, string? Title = null) {
    /// For a table whose dates are not all from one source with a heading of its own.
    public static readonly OtherStatusDateHeading InEffectFrom = new(SiteText.ColOtherListedOn);
}

/// The text of a cell. NoValue: the source gives no value, and the cell says so ("not given").
public sealed record OtherStatusCell(string Text, bool NoValue = false);

/// One row of a table, ready to show. ListTitle: the hover title of the list's label, shown with
/// abbr when ListIsAbbreviation, else with span. StatusTitle: the hover title of the status (a
/// NatureServe rank with "?" or "Q"). RankMeaning: the line under a NatureServe rank. ListedNameHtml:
/// the name the listing uses, as HTML, when it is not the taxon's own. SourceUrl: the record's page,
/// or null when the source gives none (the link text is then plain text).
public sealed record OtherStatusLine(
    string ListLabel,
    string? ListTitle,
    bool ListIsAbbreviation,
    string Status,
    string? StatusTitle,
    string? RankMeaning,
    OtherStatusCell AppliesTo,
    string? ListedNameHtml,
    string? Report,
    OtherStatusCell Date,
    string SourceLinkText,
    string? SourceUrl);

/// The table of one group (a country, or NatureServe's global ranks). A column after List and Status
/// shows only when one of the rows has a value for it; DateHeading is null when no row has a date.
public sealed record OtherStatusTable(
    string Heading,
    bool ShowAppliesTo,
    bool ShowListedName,
    bool ShowReport,
    OtherStatusDateHeading? DateHeading,
    IReadOnlyList<OtherStatusLine> Rows);

/// The species page's "Other conservation statuses" section: one table per group, in the order of the
/// rows (OtherStatusSystems.All), then one note for each source, in the order the sources first
/// appear in the rows.
public sealed record OtherStatusSection(IReadOnlyList<OtherStatusTable> Tables, IReadOnlyList<IReadOnlyList<NoteSegment>> Notes) {
    /// rows: the taxon's other_status rows, as SiteQueries.GetOtherStatuses orders them. taxonKind:
    /// the page's taxon's TaxonKinds value. sourceDate: the date the site database's copy of a source
    /// was downloaded ("25 June 2026"), or null when unknown.
    public static OtherStatusSection Build(IReadOnlyList<OtherStatusRow> rows, string taxonKind, Func<string, string?> sourceDate) {
        var tables = rows
            .GroupBy(r => OtherStatusSystems.Find(r.System)?.Group ?? "")
            .Select(group => BuildTable(group.Key, group.ToList(), taxonKind))
            .ToList();
        var notes = rows.Select(r => r.Source).Distinct()
            .Select(OtherStatusSourceInfo.Find)
            .OfType<OtherStatusSourceInfo>()
            .Select(source => source.Note(sourceDate(source.Key), rows))
            .ToList();
        return new OtherStatusSection(tables, notes);
    }

    private static OtherStatusTable BuildTable(string group, IReadOnlyList<OtherStatusRow> rows, string taxonKind) {
        var datedSources = rows.Where(r => r.ListedOn is not null).Select(r => r.Source).Distinct().ToList();
        return new OtherStatusTable(
            SiteText.OtherStatusGroup(group),
            ShowAppliesTo: rows.Any(r => r.Population is not null),
            ShowListedName: rows.Any(r => r.ListedName is not null),
            ShowReport: rows.Any(r => r.Report is not null),
            DateHeading: datedSources.Count == 0 ? null : DateHeadingFor(datedSources),
            rows.Select(r => BuildLine(r, taxonKind)).ToList());
    }

    /// The heading of a date column whose dates come from these sources: the heading of a source with
    /// DateHeadingRule.AnyDatedRow (ECOS's "First listed") when it is one of them, else the heading
    /// of the only source when there is one (SALVE's "Assessed"), else "In effect from".
    public static OtherStatusDateHeading DateHeadingFor(IReadOnlyList<string> datedSources) {
        var sources = datedSources.Select(OtherStatusSourceInfo.Find).ToList();
        if (sources.FirstOrDefault(s => s?.DateHeadingRule == DateHeadingRule.AnyDatedRow) is { } any) {
            return any.DateHeading;
        }
        return sources is [{ } only] ? only.DateHeading : OtherStatusDateHeading.InEffectFrom;
    }

    private static OtherStatusLine BuildLine(OtherStatusRow row, string taxonKind) {
        var (label, title, isAbbreviation) = SiteText.OtherStatusList(row.System);
        return new OtherStatusLine(
            label,
            title,
            isAbbreviation,
            row.Status,
            StatusTitle: SiteText.NatureServeRankTitle(row),
            RankMeaning: SiteText.NatureServeRankMeaning(row, taxonKind),
            AppliesTo: AppliesTo(row, taxonKind),
            ListedNameHtml: row.ListedName is { } listedName ? ListedNameHtml(listedName) : null,
            row.Report,
            Date: row.ListedOn is { } listedOn ? new OtherStatusCell(SiteFormat.Date(listedOn)) : new OtherStatusCell(SiteText.OtherStatusNoDate, NoValue: true),
            SourceLinkText: OtherStatusSourceInfo.Find(row.Source)?.LinkText ?? row.Source,
            row.Url);
    }

    /// The population or area a status applies to; "not given" when the source does not say (the
    /// COSEWIC and SARA statuses that NatureServe records: COSEWIC can assess populations
    /// separately); else the whole taxon.
    private static OtherStatusCell AppliesTo(OtherStatusRow row, string taxonKind) {
        if (row.Population is { } population) {
            return new OtherStatusCell(population);
        }
        return row.System is OtherStatusSystems.Cosewic or OtherStatusSystems.Sara
            ? new OtherStatusCell(SiteText.OtherStatusNoDate, NoValue: true)
            : new OtherStatusCell(SiteText.OtherStatusWholeTaxon(taxonKind));
    }

    /// A name a list uses for a taxon, as HTML: wholly italic when it is a plain binomial or trinomial
    /// ("Casuarius casuarius johnsonii", which SPRAT writes without a rank marker), else marked up as
    /// IUCN names are.
    public static string ListedNameHtml(string name) {
        var words = name.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        var plain = words.Length is 2 or 3 && words.Skip(1).All(w => w.All(c => char.IsLower(c) || c == '-'));
        return plain ? "<i>" + SiteHtml.Encode(string.Join(' ', words)) + "</i>" : ScientificNameMarkup.ToHtml(name);
    }
}
