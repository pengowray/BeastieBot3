namespace BeastieBot3.Shared.SiteData;

/// The lists other than the IUCN Red List whose statuses the site database holds (other_status).
/// Key: the value of other_status.system. Group: the ISO 3166-1 code of the country whose law or list
/// gives the status, or NatureServeGroup. The species page shows the rows of each group together, in
/// the order of All.
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
    /// The Committee on the Status of Endangered Wildlife in Canada.
    public const string Cosewic = "ca-cosewic";
    /// Canada's Species at Risk Act, Schedule 1.
    public const string Sara = "ca-sara";
    /// The New Zealand Threat Classification System.
    public const string Nztcs = "nz-nztcs";
    /// The United States Endangered Species Act.
    public const string Esa = "us-esa";
    /// NatureServe's global conservation status rank (G rank, with a T rank for an infraspecific taxon).
    public const string NatureServeGlobal = "natureserve-global";

    public const string NatureServeGroup = "natureserve";

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
        new(Cosewic, "CA"),
        new(Sara, "CA"),
        new(Nztcs, "NZ"),
        new(Esa, "US"),
        new(NatureServeGlobal, NatureServeGroup),
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

    /// What a rounded NatureServe rank means, in NatureServe's words; null for a code that is not a
    /// rank (or an unranked or not applicable one, which the site leaves out). A T rank (an
    /// infraspecific taxon's) means what the G rank with the same number means.
    public static string? NatureServeRankMeaning(string? roundedRank) {
        if (string.IsNullOrWhiteSpace(roundedRank)) {
            return null;
        }
        var rank = roundedRank.Trim().ToUpperInvariant();
        if (rank.Length < 2 || rank[0] is not ('G' or 'T')) {
            return null;
        }
        return rank[1..] switch {
            "1" => "Critically Imperiled",
            "2" => "Imperiled",
            "3" => "Vulnerable",
            "4" => "Apparently Secure",
            "5" => "Secure",
            "H" => "Possibly Extinct",
            "X" => "Presumed Extinct",
            "U" => "Unrankable",
            _ => null,
        };
    }

    /// COSEWIC's status for one of the codes NatureServe gives (cosewicCode); null for an unknown code.
    public static string? CosewicLabel(string? code) => code?.Trim() switch {
        "E" => "Endangered",
        "T" => "Threatened",
        "SC" => "Special Concern",
        "X" => "Extinct",
        "XT" => "Extirpated",
        "NAR" => "Not at Risk",
        "DD" => "Data Deficient",
        "Non-active/Nonactive" or "Non-active" or "Nonactive" => "Non-active",
        _ => null,
    };
}

public sealed record OtherStatusSystem(string Key, string Group);

/// The sources of other_status rows (other_status.source).
public static class OtherStatusSources {
    /// Australia's Species Profile and Threats Database; source_id is the SPRAT taxon id.
    public const string Sprat = "sprat";
    /// The US Fish and Wildlife Service's ECOS; source_id is the ECOS Listed Species ID.
    public const string Ecos = "ecos";
    /// NatureServe Explorer; source_id is the element global id.
    public const string NatureServe = "natureserve";
    /// The New Zealand Threat Classification System database; source_id is the NZTCS assessment id.
    public const string Nztcs = "nztcs";

    /// A taxon's SPRAT profile.
    public static string SpratUrl(long spratTaxonId) =>
        "https://www.environment.gov.au/cgi-bin/sprat/public/publicspecies.pl?taxon_id="
        + spratTaxonId.ToString(System.Globalization.CultureInfo.InvariantCulture);
}
