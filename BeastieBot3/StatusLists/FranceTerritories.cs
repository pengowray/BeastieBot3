// English names of the places (CD_SIG codes) where the stored BDC statuses apply: metropolitan
// France and the overseas territories (TER...), the State (ETATFRA), the overseas collectivities and
// départements as INSEE codes them (INSEET..., INSEED...), the provinces of New Caledonia, and the
// regions of France before and after the merger of 2016 (INSEER...). The départements of metropolitan
// France (INSEED01 ...), which only the départemental protection lists use, have no English name;
// their French names are the ones used in English.

namespace BeastieBot3.StatusLists;

internal static class FranceTerritories {
    private static readonly Dictionary<string, string> English = new(StringComparer.Ordinal) {
        ["ETATFRA"] = "France",
        ["TERFXFR"] = "Metropolitan France",
        ["TER971"] = "Guadeloupe",
        ["TER972"] = "Martinique",
        ["TER973"] = "French Guiana",
        ["TER974"] = "Réunion",
        ["TER975"] = "Saint Pierre and Miquelon",
        ["TER976"] = "Mayotte",
        ["TER977"] = "Saint Barthélemy",
        ["TER978"] = "Saint Martin",
        ["TER984"] = "French Southern and Antarctic Lands",
        ["TER984A"] = "French Southern and Antarctic Lands: sub-Antarctic islands",
        ["TER984B"] = "French Southern and Antarctic Lands: Scattered Islands",
        ["TER984C"] = "French Southern and Antarctic Lands: Adélie Land",
        ["TER986"] = "Wallis and Futuna",
        ["TER987"] = "French Polynesia",
        ["TER988"] = "New Caledonia",
        ["TER989"] = "Clipperton Island",
        ["INSEED971"] = "Guadeloupe",
        ["INSEED972"] = "Martinique",
        ["INSEED973"] = "French Guiana",
        ["INSEED974"] = "Réunion",
        ["INSEED976"] = "Mayotte",
        ["INSEET975"] = "Saint Pierre and Miquelon",
        ["INSEET977"] = "Saint Barthélemy",
        ["INSEET978"] = "Saint Martin",
        ["INSEET984"] = "French Southern and Antarctic Lands",
        ["INSEET986"] = "Wallis and Futuna",
        ["INSEET987"] = "French Polynesia",
        ["INSEED9881"] = "North Province (New Caledonia)",
        ["INSEED9882"] = "South Province (New Caledonia)",
        ["INSEED9883"] = "Loyalty Islands Province (New Caledonia)",
        // Regions since 2016.
        ["INSEER01"] = "Guadeloupe",
        ["INSEER02"] = "Martinique",
        ["INSEER03"] = "French Guiana",
        ["INSEER04"] = "Réunion",
        ["INSEER06"] = "Mayotte",
        ["INSEER11"] = "Île-de-France",
        ["INSEER24"] = "Centre-Val de Loire",
        ["INSEER27"] = "Bourgogne-Franche-Comté",
        ["INSEER28"] = "Normandy",
        ["INSEER32"] = "Hauts-de-France",
        ["INSEER44"] = "Grand Est",
        ["INSEER52"] = "Pays de la Loire",
        ["INSEER53"] = "Brittany",
        ["INSEER75"] = "Nouvelle-Aquitaine",
        ["INSEER76"] = "Occitania",
        ["INSEER84"] = "Auvergne-Rhône-Alpes",
        ["INSEER93"] = "Provence-Alpes-Côte d'Azur",
        ["INSEER94"] = "Corsica",
        // Regions before 2016 (NIVEAU_ADMIN "Ancienne région").
        ["INSEER21"] = "Champagne-Ardenne",
        ["INSEER22"] = "Picardy",
        ["INSEER23"] = "Upper Normandy",
        ["INSEER25"] = "Lower Normandy",
        ["INSEER26"] = "Burgundy",
        ["INSEER31"] = "Nord-Pas-de-Calais",
        ["INSEER41"] = "Lorraine",
        ["INSEER42"] = "Alsace",
        ["INSEER43"] = "Franche-Comté",
        ["INSEER54"] = "Poitou-Charentes",
        ["INSEER72"] = "Aquitaine",
        ["INSEER73"] = "Midi-Pyrénées",
        ["INSEER74"] = "Limousin",
        ["INSEER82"] = "Rhône-Alpes",
        ["INSEER83"] = "Auvergne",
        ["INSEER91"] = "Languedoc-Roussillon",
    };

    /// The English name of a place code; null for a code not in the list (the départements).
    public static string? EnglishName(string code) => English.TryGetValue(code, out var name) ? name : null;
}
