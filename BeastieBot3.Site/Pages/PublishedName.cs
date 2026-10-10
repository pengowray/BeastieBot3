using BeastieBot3.Shared.SiteData;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;

namespace BeastieBot3.Site.Pages;

/// Where the site read the name an assessment was published under.
public enum PublishedNameSource {
    /// IUCN's Table 7 of the version's year or the next printed it (category_change.printed_name).
    SummaryTable,
    /// The title of the assessment's DOI, which IUCN created within a year of the release
    /// (IucnCitationParts.RegisteredNameIsFromPublication).
    DoiAtPublication,
    /// The title of a DOI that IUCN created later (the DOIs of the releases before 2015 were created
    /// in 2015 and 2016), so the name may be the one IUCN used then.
    DoiLater,
}

/// How sure the site is that an assessment was published under a name: the citations page labels
/// its option by it.
public enum PublishedNameKind {
    Published,
    DoiTitle,
}

/// The name an assessment was published under, or the name in its DOI's title, when it differs from
/// the taxon's current name (PublishedName.For). Change: the Table 7 row it was read from.
public sealed record PublishedName(string Name, PublishedNameSource Source, CategoryChangeRow? Change = null) {
    public PublishedNameKind Kind => Source == PublishedNameSource.DoiLater ? PublishedNameKind.DoiTitle : PublishedNameKind.Published;

    /// The name for an assessment of a taxon whose current name is currentName: the name Table 7
    /// prints (change.PrintedName), else the name in the DOI's title (parts.RegisteredName); null when
    /// that name is the current one (rank markers, brackets and spacing aside), one of IUCN's internal
    /// names ("_new"), or not known. A Table 7 name that is the current name settles it: the DOI's
    /// title then has a later name and is not shown.
    public static PublishedName? For(string? currentName, CategoryChangeRow? change, IucnCitationParts? parts) {
        if (change?.PrintedName is { } printed) {
            return Differs(printed, currentName) ? new PublishedName(printed.Trim(), PublishedNameSource.SummaryTable, change) : null;
        }
        if (parts?.RegisteredNameDifferentFrom(currentName) is { } registered && Differs(registered, currentName)) {
            return new PublishedName(registered, parts.RegisteredNameIsFromPublication ? PublishedNameSource.DoiAtPublication : PublishedNameSource.DoiLater);
        }
        return null;
    }

    private static bool Differs(string name, string? currentName) =>
        !WikidataCitation.IsIucnInternalName(name) && !WikidataCitation.SameName(name, currentName)
        && (currentName is null || !RankMarkerKeys.For(currentName).Contains(SiteNameKey.Fold(name)));
}

/// What an assessment table row shows under its year: "as" + the name, and the footnote with its source.
public sealed record PublishedNameCell(PublishedName Name, FootnoteRef Footnote);

/// A numbered reference to a footnote under an assessment table; Id is the footnote's element id.
public sealed record FootnoteRef(int Number, string Id) {
    /// A footnote of the history table (HistoryTableNotes.HistoryPrefix).
    public static FootnoteRef History(int number) => new(number, HistoryTableNotes.HistoryPrefix + number.ToString(System.Globalization.CultureInfo.InvariantCulture));
}
