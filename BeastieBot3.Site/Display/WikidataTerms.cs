using BeastieBot3.Shared.SiteData;
using BeastieBot3.Shared.Wikitext;

namespace BeastieBot3.Site.Display;

/// The Wikidata properties and items this site shows (in QuickStatements commands, the Wikidata
/// status and item checks, and the links to other databases), with their English labels, so a
/// search for "P31" or "Q32059" can say what it is. The labels are Wikidata's English labels as of
/// LabelsCheckedOn; the status values and database properties come from the lists the site uses.
public static class WikidataTerms {
    public const string LabelsCheckedOn = "2026-10-08";

    private static readonly Dictionary<string, string> Labels = Build();

    /// The English label of a property ("P31") or item ("Q32059") this site uses; null otherwise.
    public static string? Label(string id) => Labels.GetValueOrDefault(id.Trim().ToUpperInvariant());

    private static Dictionary<string, string> Build() {
        var labels = new Dictionary<string, string>(StringComparer.Ordinal) {
            // Properties of assessment items and of the status statements and their references.
            ["P31"] = "instance of",
            ["P50"] = "author",
            ["P123"] = "publisher",
            ["P141"] = "IUCN conservation status",
            ["P248"] = "stated in",
            ["P356"] = "DOI",
            ["P373"] = "Commons category",
            ["P407"] = "language of work or name",
            ["P478"] = "volume",
            ["P577"] = "publication date",
            ["P627"] = "IUCN taxon ID",
            ["P813"] = "retrieved",
            ["P854"] = "reference URL",
            ["P921"] = "main subject",
            ["P935"] = "Commons gallery",
            ["P953"] = "work available at URL",
            ["P1433"] = "published in",
            ["P1476"] = "title",
            ["P1545"] = "series ordinal",
            ["P1932"] = "object named as",
            ["P2093"] = "author name string",
            ["P2322"] = "article ID",
            ["P9687"] = "author given names",
            ["P9688"] = "author last names",
            // Ids in other databases (ExternalDatabases).
            ["P685"] = "NCBI taxonomy ID",
            ["P815"] = "ITIS TSN",
            ["P830"] = "Encyclopedia of Life ID",
            ["P846"] = "GBIF-species-ID (before 2026 update)",
            ["P850"] = "WoRMS-ID for taxa",
            ["P938"] = "FishBase species ID",
            ["P960"] = "Tropicos ID",
            ["P2026"] = "Avibase taxon ID",
            ["P2426"] = "Xeno-canto species ID",
            ["P3151"] = "iNaturalist taxon ID",
            ["P3444"] = "eBird taxon ID",
            ["P3606"] = "BOLD Systems taxon ID",
            ["P4024"] = "ADW taxon ID",
            ["P5036"] = "AmphibiaWeb Species ID",
            ["P5037"] = "Plants of the World Online ID",
            ["P5257"] = "BirdLife taxon ID",
            ["P5473"] = "The Reptile Database ID",
            // Items of the assessment item model and the references.
            ["Q13442814"] = "scholarly article",
            ["Q32059"] = "IUCN Red List",
            ["Q48268"] = "International Union for Conservation of Nature",
            ["Q1860"] = "English",
            ["Q1321"] = "Spanish",
            ["Q150"] = "French",
            ["Q5146"] = "Portuguese",
            ["Q115962546"] = "The IUCN Red List of Threatened Species 2022.2",
            ["Q136547248"] = "The IUCN Red List of Threatened Species 2025.2",
            // Organisations cited as authors (IucnAuthorItems).
            ["Q210108"] = "BirdLife International",
            ["Q1339511"] = "Botanic Gardens Conservation International",
            ["Q590861"] = "World Conservation Monitoring Centre",
            ["Q3151725"] = "Chico Mendes Institute for Biodiversity Conservation",
            ["Q5376915"] = "Energy Development Corporation",
            ["Q3337099"] = "NatureServe",
            ["Q3532531"] = "Tortoise and Freshwater Turtle Specialist Group",
            ["Q1852803"] = "Missouri Botanical Garden",
            ["Q137999345"] = "IUCN SSC Bryophyte Specialist Group",
            ["Q50315638"] = "IUCN/SSC Cat Specialist Group",
            ["Q1125558"] = "Ministry of the Environment of Japan",
            ["Q67405025"] = "Bison Specialist Group",
        };
        foreach (var value in WikidataStatusValues.AllowedValues.Concat(WikidataStatusValues.DisallowedValues)) {
            labels.TryAdd(value.Qid, value.LabelEn);
        }
        return labels;
    }

    /// The properties of ExternalDatabases that have no label above; empty when the list is complete.
    internal static IEnumerable<string> MissingDatabaseProperties() =>
        ExternalDatabases.All.Select(d => d.Property).Where(p => p.StartsWith('P') && !Labels.ContainsKey(p));
}
