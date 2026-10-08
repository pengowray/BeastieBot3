using System.Globalization;
using CsvHelper;
using CsvHelper.Configuration;

// The US Fish and Wildlife Service's list of species listed under the Endangered Species Act, as the
// ECOS "pullreports" species report exports it (CSV, one row per listed entity). The default export
// has six columns; the columns= parameter adds the ECOS ids, the population and foreign flags, and
// the ITIS TSN, kingdom, family and group from the report's taxonomy table.
//
// The species page URL ("Scientific Name_url") ends in the ECOS Species ID, which a species listed
// as several populations shares (Lampsilis virescens: Endangered, and an experimental population).
// The ECOS Listed Species ID is the one unique per row.

namespace BeastieBot3.StatusLists;

/// One US Endangered Species Act listing from ECOS.
internal sealed record EcosListing(
    long EntityId,
    long? SpeciesId,
    string ScientificNameRaw,
    string ScientificName,
    string? NameNote,
    string? CommonName,
    string Status,
    string? EntityDescription,
    string? ListingDate,
    bool? IsDps,
    bool? IsForeign,
    string? RangeCountry,
    string? SpeciesGroup,
    long? ItisTsn,
    string? Kingdom,
    string? Family,
    string Url,
    IReadOnlyList<string> Names);

internal static class EcosListedSpecies {
    public const string ReportUrl = "https://ecos.fws.gov/ecp/pullreports/catalog/species/report/species/export";

    // /species@cn,sn,status,desc,listing_date,id,sid,dps,is_foreign,country,gn;/species/taxonomy@tsn,kingdom,family,group
    // filtered to status_category = 'Listed'.
    public const string DownloadUrl = ReportUrl
        + "?format=csv"
        + "&columns=%2Fspecies%40cn%2Csn%2Cstatus%2Cdesc%2Clisting_date%2Cid%2Csid%2Cdps%2Cis_foreign%2Ccountry%2Cgn%3B%2Fspecies%2Ftaxonomy%40tsn%2Ckingdom%2Cfamily%2Cgroup"
        + "&filter=%2Fspecies%40status_category%20%3D%20%27Listed%27";

    public const string Title = "U.S. Fish and Wildlife Service, Environmental Conservation Online System (ECOS)";
    public const string SiteUrl = "https://ecos.fws.gov/ecp/";
    public const string Licence = "Public domain (work of the U.S. federal government)";

    public static string Citation(DateTime accessedUtc) =>
        $"U.S. Fish and Wildlife Service. Listed species. Environmental Conservation Online System (ECOS). https://ecos.fws.gov/ecp/. (Accessed: {accessedUtc.ToString("MMMM d, yyyy", CultureInfo.InvariantCulture)}).";

    // Column headers of the export.
    internal const string CommonName = "Common Name";
    internal const string ScientificName = "Scientific Name";
    internal const string ScientificNameUrl = "Scientific Name_url";
    internal const string Status = "ESA Listing Status";
    internal const string EntityDescription = "Entity Description";
    internal const string ListingDate = "ESA Listing Date";
    internal const string ListedSpeciesId = "ECOS Listed Species ID";
    internal const string SpeciesId = "ECOS Species ID";
    internal const string Dps = "Distinct Population Segment?";
    internal const string IsForeign = "Is Foreign?";
    internal const string Country = "Foreign or Domestic";
    internal const string SpeciesGroup = "Species Group";
    internal const string Tsn = "Taxonomic Serial Number";
    internal const string Kingdom = "Taxonomic Kingdom";
    internal const string Family = "Taxonomic Family";

    private static readonly string[] Required = { ScientificName, ScientificNameUrl, Status, ListedSpeciesId };

    /// Reads the export. Throws InvalidDataException when a column it needs is missing; a row with
    /// no listed species id or no scientific name is skipped and counted in <paramref name="skipped"/>.
    public static IReadOnlyList<EcosListing> Read(TextReader reader, out int skipped) {
        using var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture) {
            HasHeaderRecord = true,
            BadDataFound = null,
            MissingFieldFound = null,
            TrimOptions = TrimOptions.Trim,
        });
        if (!csv.Read() || !csv.ReadHeader() || csv.HeaderRecord is not { } header) {
            throw new InvalidDataException("The ECOS file is empty.");
        }
        var missing = Required.Where(c => !header.Contains(c, StringComparer.Ordinal)).ToList();
        if (missing.Count > 0) {
            throw new InvalidDataException($"The ECOS file has no {string.Join(", ", missing)} column. The report format may have changed.");
        }
        var has = header.ToHashSet(StringComparer.Ordinal);
        string? Field(string column) => has.Contains(column) && csv.GetField(column) is { } value && value.Trim().Length > 0 ? value.Trim() : null;

        var rows = new List<EcosListing>();
        var seen = new HashSet<long>();
        skipped = 0;
        while (csv.Read()) {
            var raw = Field(ScientificName);
            if (raw is null || !long.TryParse(Field(ListedSpeciesId), NumberStyles.Integer, CultureInfo.InvariantCulture, out var entityId)
                || !seen.Add(entityId)) {
                skipped++;
                continue;
            }
            var url = Field(ScientificNameUrl) ?? $"https://ecos.fws.gov/ecp/species/{Field(SpeciesId)}";
            var parts = EcosScientificName.Parse(raw);
            rows.Add(new EcosListing(
                EntityId: entityId,
                SpeciesId: Long(Field(SpeciesId)) ?? SpeciesIdFromUrl(url),
                ScientificNameRaw: raw,
                ScientificName: parts.Name,
                NameNote: parts.Note,
                CommonName: CommonNameOrNull(Field(CommonName)),
                Status: Field(Status) ?? "",
                EntityDescription: Field(EntityDescription),
                ListingDate: IsoDate(Field(ListingDate)),
                IsDps: Bool(Field(Dps)),
                IsForeign: Bool(Field(IsForeign)),
                RangeCountry: Field(Country),
                SpeciesGroup: Field(SpeciesGroup),
                ItisTsn: Long(Field(Tsn)),
                Kingdom: Field(Kingdom),
                Family: Field(Family),
                Url: url,
                Names: parts.Names));
        }
        return rows;
    }

    /// "06-14-1976" (month first, as ECOS writes it) as "1976-06-14"; null when empty or not a date.
    public static string? IsoDate(string? text) =>
        DateTime.TryParseExact(text, new[] { "MM-dd-yyyy", "M-d-yyyy", "MM/dd/yyyy", "yyyy-MM-dd" }, CultureInfo.InvariantCulture,
            DateTimeStyles.None, out var date)
            ? date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)
            : null;

    /// The number at the end of an ECOS species page URL (https://ecos.fws.gov/ecp/species/1470).
    public static long? SpeciesIdFromUrl(string? url) {
        if (string.IsNullOrWhiteSpace(url)) {
            return null;
        }
        var last = url.TrimEnd('/').Split('/')[^1];
        return Long(last);
    }

    // ECOS writes "No common name" where a species has none.
    private static string? CommonNameOrNull(string? name) =>
        name is null || name.Equals("No common name", StringComparison.OrdinalIgnoreCase) ? null : name;

    private static long? Long(string? text) =>
        long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var n) ? n : null;

    private static bool? Bool(string? text) => text?.ToLowerInvariant() switch {
        "true" => true,
        "false" => false,
        _ => null,
    };
}
