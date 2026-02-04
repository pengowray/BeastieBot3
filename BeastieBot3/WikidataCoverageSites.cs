// Defines Wikimedia project descriptors for sitelink coverage analysis.
// Keys match Wikidata sitelinks property names: "enwiki" (English Wikipedia),
// "commonswiki" (Wikimedia Commons), "specieswiki" (Wikispecies).
// Used to check which projects have articles for IUCN taxa.

namespace BeastieBot3;

internal static class WikidataCoverageSites {
    public static readonly WikiSiteDescriptor[] All = new[] {
        new WikiSiteDescriptor("enwiki", "English Wikipedia"),
        new WikiSiteDescriptor("commonswiki", "Wikimedia Commons"),
        new WikiSiteDescriptor("specieswiki", "Wikispecies")
    };
}
