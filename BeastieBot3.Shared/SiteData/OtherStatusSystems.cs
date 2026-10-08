namespace BeastieBot3.Shared.SiteData;

/// The lists other than the IUCN Red List whose statuses the site database holds (other_status).
/// Key: the value of other_status.system. Country: the ISO 3166-1 code of the country whose law or
/// list gives the status. The species page shows the rows of each country together, in the order of
/// All.
public static class OtherStatusSystems {
    /// Australia's Environment Protection and Biodiversity Conservation Act 1999.
    public const string Epbc = "au-epbc";
    public const string AustralianCapitalTerritory = "au-act";
    public const string NewSouthWales = "au-nsw";
    public const string NorthernTerritory = "au-nt";
    public const string Queensland = "au-qld";
    public const string SouthAustralia = "au-sa";
    public const string Tasmania = "au-tas";
    public const string Victoria = "au-vic";
    public const string WesternAustralia = "au-wa";

    public static readonly IReadOnlyList<OtherStatusSystem> All = [
        new(Epbc, "AU"),
        new(AustralianCapitalTerritory, "AU"),
        new(NewSouthWales, "AU"),
        new(NorthernTerritory, "AU"),
        new(Queensland, "AU"),
        new(SouthAustralia, "AU"),
        new(Tasmania, "AU"),
        new(Victoria, "AU"),
        new(WesternAustralia, "AU"),
    ];

    /// The position of a system in All; systems not in All come last.
    public static int Order(string system) {
        for (var i = 0; i < All.Count; i++) {
            if (All[i].Key == system) {
                return i;
            }
        }
        return int.MaxValue;
    }

    public static OtherStatusSystem? Find(string system) => All.FirstOrDefault(s => s.Key == system);

    /// EPBC Act categories as the Act names them, from the category code that epbc_listing stores.
    public static string? EpbcLabel(string? code) => code?.Trim().ToUpperInvariant() switch {
        null or "" => null,
        "EX" => "Extinct",
        "EW" => "Extinct in the wild",
        "CR" => "Critically Endangered",
        "EN" => "Endangered",
        "VU" => "Vulnerable",
        "CD" => "Conservation Dependent",
        var other => other,
    };
}

public sealed record OtherStatusSystem(string Key, string Country);

/// The sources of other_status rows (other_status.source).
public static class OtherStatusSources {
    /// Australia's Species Profile and Threats Database; source_id is the SPRAT taxon id.
    public const string Sprat = "sprat";
}
