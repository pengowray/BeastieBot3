using BeastieBot3.Shared.SiteData;
using BeastieBot3.Site.Data;

namespace BeastieBot3.Site.Display;

/// When a source's date heading heads a table's date column (OtherStatusSection.DateHeadingFor).
public enum DateHeadingRule {
    /// When every row of the table that has a date is from this source.
    AllDatedRows,
    /// When any row of the table that has a date is from this source. For ECOS: its date is the first
    /// listing, and the heading's title says the status may have changed since then, so a table with
    /// an ECOS date keeps that heading whatever its other dates are.
    AnyDatedRow,
}

/// What the species page shows for one source of other statuses (other_status.source, keys in
/// OtherStatusSources in BeastieBot3.Shared): the text of a row's link to its record, the heading of
/// the date column for the source's dates, and the note under the tables.
/// Note: the note's parts, given the date the site database's copy of the source was downloaded
/// ("25 June 2026", or null) and all the rows of the page.
public sealed record OtherStatusSourceInfo(
    string Key,
    string LinkText,
    OtherStatusDateHeading DateHeading,
    DateHeadingRule DateHeadingRule,
    Func<string?, IReadOnlyList<OtherStatusRow>, IReadOnlyList<NoteSegment>> Note) {

    /// Australia's Species Profile and Threats Database: the EPBC Act and the states and territories.
    public static readonly OtherStatusSourceInfo Sprat = new(OtherStatusSources.Sprat, SiteText.LinkSprat,
        OtherStatusDateHeading.InEffectFrom, DateHeadingRule.AllDatedRows,
        (date, _) => [new(SiteText.OtherStatusSpratNote(date))]);

    /// The US Fish and Wildlife Service's ECOS, whose date is the first listing.
    public static readonly OtherStatusSourceInfo Ecos = new(OtherStatusSources.Ecos, SiteText.OtherStatusEcosRecordLink,
        new(SiteText.ColOtherFirstListed, SiteText.ColOtherFirstListedTitle), DateHeadingRule.AnyDatedRow,
        (date, _) => [new(SiteText.OtherStatusEcosNote(date))]);

    /// NatureServe Explorer: global ranks, and the COSEWIC and SARA statuses it records. The note
    /// says which of them the page has, and adds a sentence when it has Canadian statuses.
    public static readonly OtherStatusSourceInfo NatureServe = new(OtherStatusSources.NatureServe, SiteText.NatureServeExplorer,
        OtherStatusDateHeading.InEffectFrom, DateHeadingRule.AllDatedRows,
        (date, rows) => {
            var canadian = rows.Any(r => r.System is OtherStatusSystems.Cosewic or OtherStatusSystems.Sara);
            var global = rows.Any(r => r.System == OtherStatusSystems.NatureServeGlobal);
            var local = rows.Any(r => r.System is OtherStatusSystems.NatureServeNational or OtherStatusSystems.NatureServeSubnational);
            return [
                new(SiteText.OtherStatusNatureServeSubject(canadian, global, local)),
                new(SiteText.NatureServeExplorer, SiteText.NatureServeExplorerUrl),
                new(SiteText.OtherStatusNatureServeCopyright),
                new(SiteText.LicenceCcByName, SiteText.LicenceCcBy),
                new(")" + SiteText.OtherStatusNatureServeDate(date) + (canadian ? " " + SiteText.OtherStatusNatureServeCanadaCopy : "")),
            ];
        });

    /// The New Zealand Threat Classification System database.
    public static readonly OtherStatusSourceInfo Nztcs = new(OtherStatusSources.Nztcs, SiteText.OtherStatusNztcsRecordLink,
        OtherStatusDateHeading.InEffectFrom, DateHeadingRule.AllDatedRows,
        (date, _) => [
            new(SiteText.OtherStatusNztcsSubject),
            new(SiteText.OtherStatusNztcsLink, SiteText.NztcsUrl),
            new(SiteText.OtherStatusNztcsPublisher),
            new(SiteText.LicenceCcByName, SiteText.LicenceCcBy),
            new(")" + SiteText.OtherStatusNztcsDate(date)),
        ]);

    /// SALVE, ICMBio's assessments of Brazil's fauna, whose date is the end of the assessment.
    public static readonly OtherStatusSourceInfo Salve = new(OtherStatusSources.Salve, SiteText.OtherStatusSalveRecordLink,
        new(SiteText.ColOtherAssessed), DateHeadingRule.AllDatedRows,
        (date, _) => [
            new(SiteText.OtherStatusSalveSubject),
            new(SiteText.OtherStatusSalveLink, SiteText.SalveUrl),
            new(SiteText.OtherStatusSalveName + SiteText.OtherStatusSalveRest(date)),
        ]);

    /// JNCC's Conservation Designations for UK Taxa. The dates and the attribution are those of the
    /// rows' lists (other_status_list), which all come from one workbook.
    public static readonly OtherStatusSourceInfo Jncc = new(OtherStatusSources.Jncc, SiteText.OtherStatusJnccRecordLink,
        OtherStatusDateHeading.InEffectFrom, DateHeadingRule.AllDatedRows,
        (_, rows) => {
            var list = rows.FirstOrDefault(r => r.Source == OtherStatusSources.Jncc)?.List;
            return [
                new(SiteText.OtherStatusJnccSubject),
                new(SiteText.OtherStatusJnccLink, list?.Url),
                new(SiteText.OtherStatusJnccSpreadsheet(DateText(list?.Version))),
                new(SiteText.OtherStatusJnccLicence, list?.LicenceUrl),
                new(SiteText.OtherStatusJnccRest(DateText(list?.Fetched), list?.Citation)),
            ];
        });

    /// National and subnational red lists from GBIF: one part per list on the page, with its publisher,
    /// licence and download date.
    public static readonly OtherStatusSourceInfo RedLists = new(OtherStatusSources.RedLists, SiteText.RedListRecordLink(null),
        OtherStatusDateHeading.InEffectFrom, DateHeadingRule.AllDatedRows,
        (_, rows) => rows.Where(r => r.Source == OtherStatusSources.RedLists).Select(r => r.List).OfType<OtherStatusListRow>()
            .DistinctBy(l => l.ListKey)
            .SelectMany(l => new NoteSegment[] {
                new(l.Name, l.Url),
                new(SiteText.RedListNotePublisher(l.Publisher)),
                new(l.Licence ?? "", l.LicenceUrl),
                new(SiteText.RedListNoteRest(DateText(l.Fetched))),
            })
            .ToList());

    /// PatriNat's BDC Statuts: France's national red list and protection lists.
    public static readonly OtherStatusSourceInfo France = new(OtherStatusSources.France, SiteText.OtherStatusFranceRecordLink,
        OtherStatusDateHeading.InEffectFrom, DateHeadingRule.AllDatedRows,
        (date, _) => [
            new(SiteText.OtherStatusFranceSubject),
            new(SiteText.OtherStatusFranceLink, SiteText.FranceBdcUrl),
            new(SiteText.OtherStatusFranceMiddle),
            new(SiteText.LicenceOuverteName, SiteText.LicenceOuverteUrl),
            new(SiteText.OtherStatusFranceRest(date)),
        ]);

    /// The Red List of Japan's Ministry of the Environment.
    public static readonly OtherStatusSourceInfo Japan = new(OtherStatusSources.Japan, SiteText.OtherStatusJapanRecordLink,
        OtherStatusDateHeading.InEffectFrom, DateHeadingRule.AllDatedRows,
        (date, _) => [new(SiteText.OtherStatusJapanNote(date))]);

    // "2026-06-09" as "9 June 2026"; null when not a date.
    private static string? DateText(string? isoDate) => isoDate is null ? null : SiteFormat.Date(isoDate);

    /// The Checklist of CITES Species, with the citation it asks for. The note's date is the download
    /// date, as "8 October 2026".
    public static readonly OtherStatusSourceInfo Cites = new(OtherStatusSources.Cites, SiteText.OtherStatusCitesRecordLink,
        OtherStatusDateHeading.InEffectFrom, DateHeadingRule.AllDatedRows,
        (date, _) => [
            new(SiteText.OtherStatusCitesSubject),
            new(SiteText.OtherStatusCitesLink, SiteText.CitesChecklistUrl),
            new(SiteText.OtherStatusCitesRest(DateOnly.TryParseExact(date, "d MMMM yyyy", System.Globalization.CultureInfo.InvariantCulture,
                System.Globalization.DateTimeStyles.None, out var accessed) ? accessed : null)),
        ]);

    public static readonly IReadOnlyList<OtherStatusSourceInfo> All = [Sprat, Ecos, NatureServe, Nztcs, Salve, Jncc, Cites, RedLists, Japan, France];

    /// The source with this key, or null for a source the site does not know.
    public static OtherStatusSourceInfo? Find(string source) => All.FirstOrDefault(s => s.Key == source);
}
