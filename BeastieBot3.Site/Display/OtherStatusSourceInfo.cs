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

    public static readonly IReadOnlyList<OtherStatusSourceInfo> All = [Sprat, Ecos, NatureServe, Nztcs, Salve];

    /// The source with this key, or null for a source the site does not know.
    public static OtherStatusSourceInfo? Find(string source) => All.FirstOrDefault(s => s.Key == source);
}
