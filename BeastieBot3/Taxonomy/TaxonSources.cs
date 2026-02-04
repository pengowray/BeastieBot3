// Constants for tagging records with their origin database. Currently only
// "iucn" is defined; other sources (col, wikidata, wikipedia) use string
// literals directly. Could be expanded for consistency.

namespace BeastieBot3.Taxonomy;

internal static class TaxonSources {
    public const string Iucn = "iucn";
}
