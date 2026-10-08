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
    /// ICMBio's national assessments of Brazil's fauna (SALVE).
    public const string Salve = "br-salve";
    /// The Committee on the Status of Endangered Wildlife in Canada.
    public const string Cosewic = "ca-cosewic";
    /// Canada's Species at Risk Act, Schedule 1.
    public const string Sara = "ca-sara";
    /// The New Zealand Threat Classification System.
    public const string Nztcs = "nz-nztcs";
    /// The Red List of Japan's Ministry of the Environment (Red List 2020 and 5th Red List).
    public const string JapanMoe = "jp-moe";
    /// The United States Endangered Species Act.
    public const string Esa = "us-esa";
    /// The appendices of CITES, the Convention on International Trade in Endangered Species of Wild Fauna
    /// and Flora, from the Checklist of CITES Species.
    public const string Cites = "cites";
    /// National and subnational red lists published on GBIF (`statuses red-lists-import`), one list per
    /// other_status_list row, whose country gives the group.
    public const string NationalRedList = "national-red-list";
    /// JNCC's Conservation Designations for UK Taxa: the UK, Great Britain and UK country red lists, laws
    /// and priority lists, one list per other_status_list row (list_key).
    public const string Jncc = "gb-jncc";
    /// NatureServe's global conservation status rank (G rank, with a T rank for an infraspecific taxon).
    public const string NatureServeGlobal = "natureserve-global";
    /// NatureServe's national rank (N rank) in the country in other_status.country (US, CA).
    public const string NatureServeNational = "natureserve-national";
    /// NatureServe's rank (S rank) in a state, province or territory (other_status.population, by name)
    /// of the country in other_status.country.
    public const string NatureServeSubnational = "natureserve-subnational";

    public const string NatureServeGroup = "natureserve";
    /// The group of international treaties (CITES).
    public const string InternationalGroup = "international";

    public static readonly IReadOnlyList<OtherStatusSystem> All = [
        new(Cites, InternationalGroup),
        new(Epbc, "AU"),
        new(AustralianCapitalTerritory, "AU"),
        new(NewSouthWales, "AU"),
        new(NorthernTerritory, "AU"),
        new(Queensland, "AU"),
        new(SouthAustralia, "AU"),
        new(Tasmania, "AU"),
        new(Victoria, "AU"),
        new(WesternAustralia, "AU"),
        new(Salve, "BR"),
        new(Cosewic, "CA"),
        new(Sara, "CA"),
        new(JapanMoe, "JP"),
        new(Nztcs, "NZ"),
        new(Jncc, "GB"),
        new(Esa, "US"),
        new(NationalRedList, null),
        new(NatureServeNational, null),
        new(NatureServeSubnational, null),
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

    /// The parts of a rounded NatureServe national or subnational rank as NatureServe gives it: "S4B,S5N"
    /// is S4 when breeding and S5 when not breeding; "SNA" is not applicable. Empty for a blank rank or
    /// one that cannot be read.
    public static IReadOnlyList<NatureServeRankPart> NatureServeRankParts(string? roundedRank) {
        var parts = new List<NatureServeRankPart>();
        if (string.IsNullOrWhiteSpace(roundedRank)) {
            return parts;
        }
        foreach (var raw in roundedRank.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)) {
            var token = raw.ToUpperInvariant();
            if (token.Length < 2 || token[0] is not ('N' or 'S')) {
                return [];
            }
            var rest = token[1..];
            string code;
            if (rest.StartsWith("NR", StringComparison.Ordinal) || rest.StartsWith("NA", StringComparison.Ordinal)) {
                code = rest[..2];
            } else if (rest[0] is >= '1' and <= '5' or 'H' or 'X' or 'U' or 'Z') {
                code = rest[..1];
            } else {
                return [];
            }
            var season = rest[code.Length..] switch {
                "" => NatureServeSeason.Any,
                "B" => NatureServeSeason.Breeding,
                "N" => NatureServeSeason.Nonbreeding,
                "M" => NatureServeSeason.Migrant,
                _ => (NatureServeSeason?)null,
            };
            if (season is null) {
                return [];
            }
            parts.Add(new NatureServeRankPart(token[0] + code, code, season.Value));
        }
        return parts;
    }

    /// What a national or subnational rank's code means, in NatureServe's words ("3" Vulnerable, "X"
    /// Presumed Extirpated); null for NR, which is no rank.
    public static string? NatureServeLocalRankMeaning(string code) => code switch {
        "1" => "Critically Imperiled",
        "2" => "Imperiled",
        "3" => "Vulnerable",
        "4" => "Apparently Secure",
        "5" => "Secure",
        "H" => "Possibly Extirpated",
        "X" => "Presumed Extirpated",
        "U" => "Unrankable",
        "Z" => "Zero Occurrences",
        "NA" => "Not Applicable",
        _ => null,
    };

    /// Whether a rounded national or subnational rank has a part with a rank (1 to 5, H, X or U).
    public static bool IsRankedLocally(string? roundedRank) =>
        NatureServeRankParts(roundedRank).Any(p => p.Code is not ("NR" or "NA" or "Z"));

    /// A SALVE category in English; CR with SALVE's possibly extinct flag is "Critically Endangered
    /// (Possibly Extinct)". Null for an unknown code.
    public static string? SalveLabel(string? code, bool possiblyExtinct) => code?.Trim().ToUpperInvariant() switch {
        "EX" => "Extinct",
        "EW" => "Extinct in the Wild",
        "RE" => "Regionally Extinct",
        "CR" => possiblyExtinct ? "Critically Endangered (Possibly Extinct)" : "Critically Endangered",
        "EN" => "Endangered",
        "VU" => "Vulnerable",
        "NT" => "Near Threatened",
        "LC" => "Least Concern",
        "DD" => "Data Deficient",
        "NA" => "Not Applicable",
        _ => null,
    };

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

/// One part of a rounded NatureServe national or subnational rank. Rank: the part without its season
/// letter ("S4"); Code: the rank's code ("4", "H", "NA", "NR").
public sealed record NatureServeRankPart(string Rank, string Code, NatureServeSeason Season);

/// The season a part of a NatureServe rank applies to: B breeding, N nonbreeding, M migrant, or all year.
public enum NatureServeSeason { Any, Breeding, Nonbreeding, Migrant }

/// Group: the ISO code of the system's country, a special group (NatureServeGroup, InternationalGroup),
/// or null for a system that spans countries, whose rows or lists give the country.
public sealed record OtherStatusSystem(string Key, string? Group);

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
    /// SALVE, ICMBio's assessments of Brazil's fauna; source_id is SALVE's sheet id (id_ficha).
    public const string Salve = "salve";
    /// JNCC's Conservation Designations for UK Taxa; source_id is the taxon version key (UK Species Inventory).
    public const string Jncc = "jncc";
    /// Japan's Red List (`statuses japan-import`); source_id is japan_listing.row_id.
    public const string Japan = "japan";
    /// National and subnational red lists from GBIF; source_id is "<dataset key>:<the archive's taxon id>".
    public const string RedLists = "red-lists";
    /// The Checklist of CITES Species (UNEP-WCMC); source_id is the Species+ taxon concept id of the
    /// taxon whose listing it is (the higher taxon's, for a listing that covers the taxon as part of it).
    public const string Cites = "cites";

    /// A taxon's SPRAT profile.
    public static string SpratUrl(long spratTaxonId) =>
        "https://www.environment.gov.au/cgi-bin/sprat/public/publicspecies.pl?taxon_id="
        + spratTaxonId.ToString(System.Globalization.CultureInfo.InvariantCulture);
}
