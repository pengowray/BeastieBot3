using System.Text.RegularExpressions;
using BeastieBot3.Configuration;
using BeastieBot3.Web.Endpoints;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

// The national and subnational red lists that `statuses red-lists-import` imports, from
// rules/status-lists/national-red-lists.yml. The file's header explains each field and how datasets
// are chosen; docs/status-lists.md lists the datasets left out and why.

namespace BeastieBot3.StatusLists;

/// One red list on GBIF. Categories: English labels for statuses that are not IUCN categories, keyed
/// by the status as the archive writes it (matched by RedListCategories.Label).
internal sealed record RedListDataset(
    string Key,
    string GbifKey,
    string Country,
    string? Region,
    string? RegionCode,
    string Name,
    string? NameEn,
    int? Year,
    string Publisher,
    string Licence,
    string Citation,
    string? Kingdom,
    string? ArchiveUrl,
    string? Notes,
    IReadOnlyDictionary<string, string> Categories);

internal static partial class RedListManifest {
    public const string FileName = "national-red-lists.yml";
    public const string Folder = "status-lists";

    /// The licences the site may use. The import refuses a dataset whose licence is not one of these.
    public static readonly IReadOnlyList<string> Licences = ["CC0 1.0", "CC BY 4.0", "CC BY-NC 4.0"];

    public static string PathFor(PathsService paths) => Path.Combine(RulesPaths.Resolve(paths).SourceRulesDir, Folder, FileName);

    public static IReadOnlyList<RedListDataset> Load(string path) {
        if (!File.Exists(path)) {
            throw new InvalidOperationException($"The list of red lists was not found: {path}");
        }
        return Parse(File.ReadAllText(path), path);
    }

    internal static IReadOnlyList<RedListDataset> Parse(string yaml, string source) {
        var deserializer = new DeserializerBuilder().WithNamingConvention(UnderscoredNamingConvention.Instance).Build();
        var document = deserializer.Deserialize<ManifestDocument?>(yaml) ?? new ManifestDocument();
        var datasets = new List<RedListDataset>();
        foreach (var entry in document.Datasets ?? []) {
            var where = $"{source}: dataset {datasets.Count + 1}{(entry.Key is { Length: > 0 } k ? $" ({k})" : "")}";
            string Required(string? value, string field) =>
                string.IsNullOrWhiteSpace(value) ? throw new InvalidOperationException($"{where} has no {field}.") : value.Trim();
            var key = Required(entry.Key, "key");
            if (!KeyPattern().IsMatch(key)) {
                throw new InvalidOperationException($"{where}: a key is lower-case letters, digits and hyphens.");
            }
            var gbif = Required(entry.Gbif, "gbif");
            if (!Guid.TryParse(gbif, out _)) {
                throw new InvalidOperationException($"{where}: gbif is not a GBIF dataset key.");
            }
            var country = Required(entry.Country, "country");
            if (!CountryPattern().IsMatch(country)) {
                throw new InvalidOperationException($"{where}: country is a two-letter ISO code in capitals.");
            }
            var licence = Required(entry.Licence, "licence");
            if (!Licences.Contains(licence)) {
                throw new InvalidOperationException($"{where}: licence {licence} is not one of {string.Join(", ", Licences)}.");
            }
            datasets.Add(new RedListDataset(key, gbif.ToLowerInvariant(), country, Optional(entry.Region), Optional(entry.RegionCode),
                Required(entry.Name, "name"), Optional(entry.NameEn), entry.Year, Required(entry.Publisher, "publisher"), licence,
                Required(entry.Citation, "citation"), Optional(entry.Kingdom), Optional(entry.ArchiveUrl), Optional(entry.Notes),
                entry.Categories ?? new Dictionary<string, string>()));
        }
        var duplicate = datasets.GroupBy(d => d.Key, StringComparer.Ordinal).FirstOrDefault(g => g.Count() > 1)
                        ?? datasets.GroupBy(d => d.GbifKey, StringComparer.Ordinal).FirstOrDefault(g => g.Count() > 1);
        if (duplicate is not null) {
            throw new InvalidOperationException($"{source}: {duplicate.Key} is listed twice.");
        }
        return datasets;
    }

    private static string? Optional(string? value) => string.IsNullOrWhiteSpace(value) ? null : value.Trim();

    [GeneratedRegex("^[a-z0-9]+(-[a-z0-9]+)*$")]
    private static partial Regex KeyPattern();

    [GeneratedRegex("^[A-Z]{2}$")]
    private static partial Regex CountryPattern();

    private sealed class ManifestDocument {
        public List<ManifestEntry>? Datasets { get; set; }
    }

    private sealed class ManifestEntry {
        public string? Key { get; set; }
        public string? Gbif { get; set; }
        public string? Country { get; set; }
        public string? Region { get; set; }
        public string? RegionCode { get; set; }
        public string? Name { get; set; }
        public string? NameEn { get; set; }
        public int? Year { get; set; }
        public string? Publisher { get; set; }
        public string? Licence { get; set; }
        public string? Citation { get; set; }
        public string? Kingdom { get; set; }
        public string? ArchiveUrl { get; set; }
        public string? Notes { get; set; }
        public Dictionary<string, string>? Categories { get; set; }
    }
}
