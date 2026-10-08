namespace BeastieBot3.SiteBuild;

/// Finds the taxon of a scientific name in one kingdom, for the status lists. Names are compared
/// with spaces collapsed and without rank markers ("ssp.", "subsp.", "var."), so NatureServe's
/// "Lithobates areolatus circulosus" finds IUCN's "Lithobates areolatus ssp. circulosus". A taxon in
/// the release is found before an old IUCN id with the same name; a name that two taxa in the release
/// have (or, when none is in the release, two other taxa) finds none. Subpopulations, which have
/// their species' scientific name, are never found.
internal sealed class StatusListNameIndex {
    private readonly Dictionary<(string Kingdom, string Name), SiteTaxon?> _byName = new();
    private readonly Dictionary<(string Kingdom, string Name), SiteTaxon?> _byIucnSynonym = new();

    public StatusListNameIndex(IEnumerable<SiteTaxon> taxa) {
        // Taxa in the release first: a later taxon with the same key only makes the key ambiguous
        // when it is in the release too.
        var released = new HashSet<(string, string)>();
        var releasedSynonyms = new HashSet<(string, string)>();
        foreach (var taxon in taxa.Where(t => t.Kind != SiteTaxonKind.Subpopulation).OrderBy(t => t.InRelease ? 0 : 1).ThenBy(t => t.TaxonId)) {
            var kingdom = taxon.Kingdom?.Trim().ToUpperInvariant() ?? string.Empty;
            Add(_byName, released, (kingdom, Key(taxon.ScientificName)), taxon);
            foreach (var synonym in taxon.IucnSynonyms) {
                Add(_byIucnSynonym, releasedSynonyms, (kingdom, Key(synonym.Name)), taxon);
            }
        }
    }

    private static void Add(Dictionary<(string, string), SiteTaxon?> map, HashSet<(string, string)> released,
        (string, string) key, SiteTaxon taxon) {
        if (!map.TryGetValue(key, out var existing)) {
            map[key] = taxon;
            if (taxon.InRelease) {
                released.Add(key);
            }
        } else if (existing is not null && existing.TaxonId != taxon.TaxonId && (taxon.InRelease || !released.Contains(key))) {
            map[key] = null;
        }
    }

    /// The taxon with this scientific name in this kingdom (IUCN's spelling: "ANIMALIA"); with no
    /// kingdom, the one taxon with the name in any kingdom.
    public SiteTaxon? Find(string? kingdom, string name) => Find(_byName, kingdom, name);

    /// The one taxon whose IUCN synonyms include this name.
    public SiteTaxon? FindByIucnSynonym(string? kingdom, string name) => Find(_byIucnSynonym, kingdom, name);

    private static SiteTaxon? Find(Dictionary<(string Kingdom, string Name), SiteTaxon?> map, string? kingdom, string name) {
        var key = Key(name);
        if (kingdom is not null) {
            return map.GetValueOrDefault((kingdom, key));
        }
        SiteTaxon? found = null;
        foreach (var k in Kingdoms) {
            if (map.TryGetValue((k, key), out var taxon)) {
                if (taxon is null || found is not null) {
                    return null;
                }
                found = taxon;
            }
        }
        return found;
    }

    private static readonly string[] Kingdoms = { "ANIMALIA", "PLANTAE", "FUNGI", "CHROMISTA", "PROTISTA", "BACTERIA" };

    /// IUCN's kingdom for a status list's kingdom ("Animalia", "Animal", "Plant"); null when unknown.
    public static string? Kingdom(string? kingdom) => kingdom?.Trim().ToUpperInvariant() switch {
        "ANIMALIA" or "ANIMAL" => "ANIMALIA",
        "PLANTAE" or "PLANT" => "PLANTAE",
        "FUNGI" or "FUNGUS" => "FUNGI",
        "CHROMISTA" => "CHROMISTA",
        _ => null,
    };

    internal static string Key(string name) =>
        string.Join(' ', name.Split(' ', StringSplitOptions.RemoveEmptyEntries).Where(w => w is not ("ssp." or "subsp." or "var.")));
}
