using System;

// Which taxa that `wikipedia match-taxa` matched to a Wikipedia page get common names from it (its
// title and taxobox name) in `common-names aggregate --source wikipedia`.
//
// The matcher can match one page to several IUCN taxa: one by its own name, and others through a
// synonym or a Catalogue of Life name. The page "Hector's dolphin" is matched to Cephalorhynchus
// hectori by name, and to the subspecies C. hectori maui because IUCN lists "Cephalorhynchus
// hectori" as a synonym of it; "European green toad" is matched to Bufotes viridis, and to
// B. balearicus through a synonym. Giving the page's names to both taxa made the subspecies show
// "Hector's dolphin" instead of "Maui dolphin", and two unrelated taxa sharing a title both skip
// it as ambiguous, so B. viridis lost "European green toad".

namespace BeastieBot3.CommonNames;

internal static class WikipediaPageMatch {
    /// <summary>
    /// True when the matcher found the page through a name that is not the taxon's own: an IUCN
    /// synonym ("iucn-synonym") or a Catalogue of Life name ("col-accepted", "col-synonym" and the
    /// other "col-" methods). The taxon's own names, its Wikidata item and unknown methods are not.
    /// </summary>
    public static bool IsThroughAnotherName(string? matchMethod) =>
        matchMethod is not null
        && (matchMethod.Equals("iucn-synonym", StringComparison.Ordinal) || matchMethod.StartsWith("col-", StringComparison.Ordinal));

    /// <summary>
    /// True when a taxon matched to a page by <paramref name="matchMethod"/> gets the page's names:
    /// always, unless it was matched through another name and <paramref name="pageMatchedByOwnName"/>
    /// (some taxon is matched to the same page by its own name), because the page is then about
    /// that taxon.
    /// </summary>
    public static bool GivesNames(string? matchMethod, bool pageMatchedByOwnName) =>
        !(pageMatchedByOwnName && IsThroughAnotherName(matchMethod));
}
