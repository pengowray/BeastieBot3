namespace BeastieBot3.Shared.Wikitext;

// The Wikidata items of organisations that IUCN's citations name as authors, keyed by the name
// exactly as the citation prints it. An assessment item cites such an author as author (P50) with
// "object named as" (P1932) holding the printed name, instead of an author name string (P2093).
// {{cite Q}} shows the P1932 name and links the organisation's English Wikipedia article.
//
// Only organisations with an item that is clearly the one IUCN means. Each was checked on Wikidata
// on 2026-10-08; counts are the assessments in the 2026-1 site database that name it. Most IUCN SSC
// specialist groups (Global Tree, Amphibian, Primate, Chiroptera, Mollusc, Conifer...) had no item
// then. Left out: "FishBase team RMCA" (neither FishBase nor the museum), "NatureServe (Hammerson,
// G.)" and the like (people as well as the organisation), "American Fisheries Society Endangered
// Species Committee" (a committee of the society).
public static class IucnAuthorItems {
    private static readonly Dictionary<string, string> Items = new(StringComparer.Ordinal) {
        ["BirdLife International"] = "Q210108",                                            // 93,540
        ["Botanic Gardens Conservation International (BGCI)"] = "Q1339511",               // 9,190
        ["World Conservation Monitoring Centre"] = "Q590861",                              // 4,655
        ["Instituto Chico Mendes de Conservação da Biodiversidade (ICMBio)"] = "Q3151725", // 1,859
        ["Energy Development Corporation (EDC)"] = "Q5376915",                             // 1,598
        ["NatureServe"] = "Q3337099",                                                      // 933
        ["Tortoise & Freshwater Turtle Specialist Group"] = "Q3532531",                    // 222
        ["Missouri Botanical Garden"] = "Q1852803",                                        // 157
        ["Bryophyte Specialist Group"] = "Q137999345",                                     // 108
        ["IUCN SSC Bryophyte Specialist Group"] = "Q137999345",                            // 4
        ["Cat Specialist Group"] = "Q50315638",                                            // 78
        ["Ministry of the Environment, Japan"] = "Q1125558",                               // 52
        ["Bison Specialist Group"] = "Q67405025",                                          // 2
    };

    /// The item of the organisation a citation prints this way, or null.
    public static string? ItemFor(string? printedName) =>
        printedName is not null && Items.TryGetValue(printedName.Trim(), out var qid) ? qid : null;
}
