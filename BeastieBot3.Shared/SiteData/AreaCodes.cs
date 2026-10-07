namespace BeastieBot3.Shared.SiteData;

/// How a taxon came to be in a country or area, as IUCN codes it (the "origin" of a location in an
/// assessment). The values are stored in taxon_area.origin; a lower value is the stronger claim, so
/// two codings of one area for one taxon keep the lower.
public enum AreaOrigin {
    Native = 1,
    Reintroduced = 2,
    Introduced = 3,
    AssistedColonisation = 4,
    Vagrant = 5,
    OriginUncertain = 6,
}

/// Whether a taxon is still in a country or area, as IUCN codes it (the "presence" of a location).
/// Stored in taxon_area.presence; lower is the stronger claim.
public enum AreaPresence {
    Extant = 1,
    PossiblyExtant = 2,
    PresenceUncertain = 3,
    PossiblyExtinct = 4,
    ExtinctPost1500 = 5,
}

public static class AreaCodes {
    /// IUCN's origin text ("Native", "Vagrant"); null for text it does not use.
    public static AreaOrigin? Origin(string? text) => text?.Trim().ToLowerInvariant() switch {
        "native" => AreaOrigin.Native,
        "reintroduced" => AreaOrigin.Reintroduced,
        "introduced" => AreaOrigin.Introduced,
        "assisted colonisation" or "assisted colonization" => AreaOrigin.AssistedColonisation,
        "vagrant" => AreaOrigin.Vagrant,
        "origin uncertain" => AreaOrigin.OriginUncertain,
        _ => null,
    };

    /// IUCN's presence text ("Extant", "Extinct Post-1500"); null for text it does not use.
    public static AreaPresence? Presence(string? text) => text?.Trim().ToLowerInvariant() switch {
        "extant" => AreaPresence.Extant,
        "possibly extant" => AreaPresence.PossiblyExtant,
        "presence uncertain" => AreaPresence.PresenceUncertain,
        "possibly extinct" => AreaPresence.PossiblyExtinct,
        "extinct post-1500" => AreaPresence.ExtinctPost1500,
        _ => null,
    };

    /// An area code IUCN uses for a place: ISO 3166-1 alpha-2 ("BR") or a TDWG code for part of a
    /// country ("HAW-HI", "RU-EU"). IUCN also codes some regions with a 31-character hash, which are
    /// not places a list is of.
    public static bool IsAreaCode(string? code) =>
        code is { Length: >= 2 and <= 7 } && code.All(c => c is >= 'A' and <= 'Z' or '-')
        && (code.Length == 2 || code.Contains('-'));
}
