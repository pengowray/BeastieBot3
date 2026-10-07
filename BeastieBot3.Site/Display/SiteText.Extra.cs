namespace BeastieBot3.Site.Display;

// The words of the pages of species from the Catalogue of Life and Wikidata that are not on the IUCN
// Red List (/col/{id}, /wikidata/{qid}), and of those species in search results and on IUCN taxon pages.

public static partial class SiteText {
    /// The status line: "Not on the IUCN Red List. Listed in the Catalogue of Life and on Wikidata."
    public static string ExtraStatusLine(bool inCol, bool inWikidata) => (inCol, inWikidata) switch {
        (true, true) => "Not on the IUCN Red List. Listed in the Catalogue of Life and on Wikidata.",
        (true, false) => "Not on the IUCN Red List. Listed in the Catalogue of Life.",
        _ => "Not on the IUCN Red List. Listed on Wikidata.",
    };

    /// "Shown under the family Ursidae on this site, because the IUCN Red List has no genus " + genus + "."
    public static string ExtraUnderFamilyBefore(string family) => $"Shown under the family {family} on this site, because the IUCN Red List has no genus ";
    public const string ExtraUnderFamilyAfter = ".";

    public const string LabelWikidataName = "Scientific name on Wikidata";
    public const string LabelSameColId = "IUCN Red List taxon with the same Catalogue of Life ID:";

    // Possible duplicates
    public const string ExtraPairsHeading = "Possible duplicates";
    public const string ExtraPairsIntro = "Found by comparing names and synonyms. No ID links these taxa to this species.";
    public const string TaxonExtraPairsHeading = "Possible duplicates in the Catalogue of Life and Wikidata";
    public const string TaxonExtraPairsIntro = "Found by comparing names. No ID links these species to this taxon.";
    /// After the other taxon's name: "(IUCN Red List)", "(CoL and Wikidata)".
    public const string PairIucn = "IUCN Red List";

    /// The sources of an extra species: "CoL", "Wikidata", "CoL and Wikidata".
    public static string ExtraSourcesTag(bool inCol, bool inWikidata) => (inCol, inWikidata) switch {
        (true, true) => "CoL and Wikidata",
        (true, false) => "CoL",
        _ => "Wikidata",
    };

    // Search
    public const string SearchExtraHeading = "Species not on the IUCN Red List";
    public const string SearchExtraNote = "From the Catalogue of Life and Wikidata.";
    public static string SearchExtraTruncated(int shown, long total) =>
        $"Showing the first {SiteFormat.Number(shown)} of {SiteFormat.Number(total)} species.";

    // Not found
    public static string ColIdNotFoundHeading(string colId) => $"No taxon with Catalogue of Life ID {colId}";
    public const string ColIdNotFoundLine = "This site has no taxon with this Catalogue of Life ID.";
    public static string WikidataNotFoundHeading(string qid) => $"No taxon with Wikidata item {qid}";
    public const string WikidataNotFoundLine = "This site has no taxon with this Wikidata item.";
}
