namespace BeastieBot3.Site.Display;

// The species page's list of subspecies and varieties from the IUCN Red List, the Catalogue of Life
// and Wikidata (Pages/Shared/_Subspecies.cshtml).
public static partial class SiteText {
    /// "Subspecies", "Subspecies and varieties" or "Varieties", by what the list has.
    public static string HeadingSubspeciesList(bool subspecies, bool varieties) => (subspecies, varieties) switch {
        (true, true) => "Subspecies and varieties",
        (false, true) => "Varieties",
        _ => "Subspecies",
    };

    /// The line under the heading: "Sources often disagree about which subspecies this species has."
    public static string SubspeciesListIntro(bool subspecies, bool varieties) =>
        $"Sources often disagree about which {HeadingSubspeciesList(subspecies, varieties).ToLowerInvariant()} this species has.";

    /// Added to the line under the heading when a row's only source is Wikidata.
    public const string SubspeciesListWikidataOnly =
        "A name listed only by Wikidata may be an older name that current classifications treat as a synonym.";

    /// After a source whose record spells the name another way: " (as " + the record's name + ")".
    public const string SourceNameBefore = " (as ";
    public const string SourceNameAfter = ")";
    /// After the id of one of a source's records that spells the name another way: " as " + its name.
    public const string RecordNameBefore = " as ";

    public static string ShowMoreSubspecies(int n, bool subspecies, bool varieties) =>
        $"Show all {HeadingSubspeciesList(subspecies, varieties).ToLowerInvariant()} ({SiteFormat.Number(n)} more)";
}
