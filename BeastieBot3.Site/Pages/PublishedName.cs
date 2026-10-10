using BeastieBot3.Shared.SiteData;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;

namespace BeastieBot3.Site.Pages;

/// How sure the site is that an assessment was published under a name.
public enum PublishedNameKind {
    /// IUCN's Table 7 of the release prints it, or the assessment's DOI was created within a year of
    /// its release with it in the title (IucnCitationParts.RegisteredNameIsFromPublication).
    Published,
    /// Only the title of a DOI created later has it (IUCN created the DOIs of the releases before 2015
    /// in 2015 and 2016), so it is the name IUCN used then.
    DoiTitle,
}

/// The name an assessment was published under, or the name in its DOI's title, when it differs from
/// the taxon's current name (PublishedName.For).
public sealed record PublishedName(string Name, PublishedNameKind Kind) {
    /// The name for an assessment of a taxon whose current name is currentName: the name Table 7
    /// prints (change.PrintedName), else the name in the DOI's title (parts.RegisteredName); null when
    /// that name is the current one (rank markers, brackets and spacing aside), one of IUCN's internal
    /// names ("_new"), or not known. A Table 7 name that is the current name settles it: the DOI's
    /// title then has a later name and is not shown.
    public static PublishedName? For(string? currentName, CategoryChangeRow? change, IucnCitationParts? parts) {
        if (change?.PrintedName is { } printed) {
            return Differs(printed, currentName) ? new PublishedName(printed.Trim(), PublishedNameKind.Published) : null;
        }
        if (parts?.RegisteredNameDifferentFrom(currentName) is { } registered && Differs(registered, currentName)) {
            return new PublishedName(registered, parts.RegisteredNameIsFromPublication ? PublishedNameKind.Published : PublishedNameKind.DoiTitle);
        }
        return null;
    }

    private static bool Differs(string name, string? currentName) =>
        !WikidataCitation.IsIucnInternalName(name) && !WikidataCitation.SameName(name, currentName)
        && (currentName is null || !RankMarkerKeys.For(currentName).Contains(SiteNameKey.Fold(name)));
}

/// Which kinds of PublishedName an assessment table shows, for the note under it.
public sealed class PublishedNameTally {
    public bool AnyPublished { get; private set; }
    public bool AnyDoiTitle { get; private set; }
    public bool Any => AnyPublished || AnyDoiTitle;

    public PublishedName? Add(PublishedName? name) {
        if (name?.Kind == PublishedNameKind.Published) AnyPublished = true;
        if (name?.Kind == PublishedNameKind.DoiTitle) AnyDoiTitle = true;
        return name;
    }
}
