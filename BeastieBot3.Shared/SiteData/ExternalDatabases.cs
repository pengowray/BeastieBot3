namespace BeastieBot3.Shared.SiteData;

/// The other databases a species page links, by the Wikidata property that holds a taxon's id in
/// each (an external identifier on the taxon's Wikidata item), in the order the page lists them.
/// Url: the address of a taxon's page, with $1 for the id (each property's formatter URL, P1630,
/// on Wikidata in October 2026). site build-db stores the ids in taxon_external_id.
public static class ExternalDatabases {
    public sealed record Database(string Property, string Name, string Url) {
        public string UrlFor(string id) => Url.Replace("$1", id.Trim().Replace(" ", "%20"), StringComparison.Ordinal);
    }

    public static readonly IReadOnlyList<Database> All = [
        new("P846", "GBIF", "https://www.gbif.org/species/$1"),
        new("P3151", "iNaturalist", "https://www.inaturalist.org/taxa/$1"),
        new("P830", "Encyclopedia of Life", "https://eol.org/pages/$1"),
        new("P685", "NCBI Taxonomy", "https://www.ncbi.nlm.nih.gov/datasets/taxonomy/$1/"),
        new("P815", "ITIS", "https://www.itis.gov/servlet/SingleRpt/SingleRpt?search_topic=TSN&search_value=$1"),
        new("P5037", "Plants of the World Online", "https://powo.science.kew.org/taxon/$1"),
        new("P960", "Tropicos", "https://www.tropicos.org/name/$1"),
        new("P850", "WoRMS", "https://www.marinespecies.org/aphia.php?p=taxdetails&id=$1"),
        new("P938", "FishBase", "https://www.fishbase.se/summary/$1"),
        new("P5036", "AmphibiaWeb", "https://amphibiaweb.org/species/$1"),
        new("P5473", "The Reptile Database", "https://reptile-database.reptarium.cz/species?$1"),
        new("P5257", "BirdLife DataZone", "https://datazone.birdlife.org/species/factsheet/$1"),
        new("P2026", "Avibase", "https://avibase.bsc-eoc.org/species.jsp?avibaseid=$1"),
        new("P3444", "eBird", "https://ebird.org/species/$1"),
        new("P2426", "xeno-canto", "https://xeno-canto.org/species/$1"),
        new("P4024", "Animal Diversity Web", "https://animaldiversity.org/accounts/$1/"),
        new("P3606", "BOLD Systems", "https://bench.boldsystems.org/index.php/TaxBrowser_TaxonPage?taxid=$1"),
    ];

    public static Database? ByProperty(string property) => All.FirstOrDefault(d => d.Property == property);
}
