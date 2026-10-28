using System.Net;
using System.Text.RegularExpressions;
using BeastieBot3.Configuration;
using BeastieBot3.Web.Endpoints;
using YamlDotNet.Core;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

// Ratings of seafood buying guides (`seafood goodfish-import`, `seafood seafoodwatch-import`). A
// rating is a guide's advice to shoppers about one species (or group of species) from one region,
// caught or farmed one way: GoodFish rates gummy shark from Western Australia Say No because its
// gillnets drown Australian sea lions, while the shark itself is not threatened. A rating is about a
// fishery or farm, never the conservation status of the species, so the site shows the ratings in a
// section of their own and never among the other conservation statuses.
//
// The names a guide gives are kept as written (seafood_rating_name.name) beside the scientific name
// read from them (SeafoodNames.Read): an abbreviated genus expanded from a full name on the same
// rating ("H. laevigata" beside "Haliotis rubra"), the epithet in lower case ("Scortum Barcoo"), and
// misspellings fixed from rules/status-lists/seafood-name-fixes.yml.

namespace BeastieBot3.StatusLists;

internal static class SeafoodGuideKeys {
    /// GoodFish, Australia's Sustainable Seafood Guide (Australian Marine Conservation Society).
    public const string GoodFish = Shared.SiteData.SeafoodGuides.GoodFish;
    /// Seafood Watch (Monterey Bay Aquarium).
    public const string SeafoodWatch = Shared.SiteData.SeafoodGuides.SeafoodWatch;
}

/// One rating of a seafood guide. Ratings: the guide's words ("Say No"); a GoodFish page with two
/// ratings has both, and its Note says which to choose. Fisheries: the fisheries a GoodFish page
/// lists, or the parts of a Seafood Watch recommendation's detail ("Coho fishery below Bonneville
/// Dam", "Zones 1-5"; for a certified product, the certification first).
internal sealed record SeafoodRating(
    string Guide,
    string RatingId,
    string CommonName,
    string? GroupName,
    IReadOnlyList<string> Ratings,
    string? Color,
    string? Score,
    string? Region,
    string? WaterBody,
    IReadOnlyList<string> Methods,
    string? Production,
    IReadOnlyList<string> Fisheries,
    string? Note,
    string Url,
    string? Published,
    string? Modified,
    IReadOnlyList<SeafoodName> Names);

/// A name as the guide writes it, and the scientific name read from it (null for a genus or group:
/// "Sepia spp.", "Various species").
internal sealed record SeafoodName(string Written, string? ScientificName);

internal static partial class SeafoodNames {
    public const string FixesFileName = "seafood-name-fixes.yml";

    public static string FixesPathFor(PathsService paths) =>
        Path.Combine(RulesPaths.Resolve(paths).SourceRulesDir, "status-lists", FixesFileName);

    /// Splits a guide's list of names ("H. conicopora, H. laevigata, Haliotis rubra") on commas,
    /// semicolons and slashes. A part that is one lower-case word ("Sebastes melanostictus/aleutianus")
    /// takes the genus of the part before it.
    public static IReadOnlyList<string> Split(string? names) {
        if (string.IsNullOrWhiteSpace(names)) {
            return [];
        }
        var parts = new List<string>();
        foreach (var raw in names.Split([',', ';', '/'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)) {
            var part = Spaces().Replace(raw, " ");
            if (part.Length == 0) {
                continue;
            }
            if (Epithet().IsMatch(part) && parts.Count > 0 && Words(parts[^1]) is [var genus, ..] && IsGenus(genus)) {
                part = $"{genus} {part}";
            }
            parts.Add(part);
        }
        return parts;
    }

    /// The names of one rating with their scientific names. A bracketed part is left out (a subgenus,
    /// "Penaeus (Fenneropenaeus) indicus", or another name, "Octopus americanus (O. vulgaris)"). An
    /// abbreviated genus ("P. esculentus")
    /// takes the genus of the full names in the list that start with its letter, when they all have
    /// the same genus; the epithets are put in lower case; then a fix for the guide's spelling, when
    /// <paramref name="fixes"/> has one, replaces the name. A name that is not a genus and one to three
    /// lower-case words after that has no scientific name.
    public static IReadOnlyList<SeafoodName> Read(IReadOnlyList<string> written, IReadOnlyDictionary<string, string> fixes) {
        var genera = written.Select(w => Words(Bracketed().Replace(w, " "))).Where(w => w.Length >= 2 && IsGenus(w[0])).Select(w => w[0]).Distinct().ToList();
        return written.Select(w => {
            var words = Words(Bracketed().Replace(w, " "));
            if (words.Length >= 2 && Abbreviation().IsMatch(words[0])) {
                var options = genera.Where(g => g[0] == char.ToUpperInvariant(words[0][0])).ToList();
                words = options.Count == 1 ? [options[0], .. words[1..]] : words;
            }
            string? name = null;
            if (words.Length is >= 2 and <= 4 && IsGenus(words[0])) {
                var epithets = words[1..].Select(e => e.ToLowerInvariant()).ToArray();
                if (epithets.All(e => Epithet().IsMatch(e)) && !epithets.Any(e => e is "spp." or "spp" or "sp." or "sp" or "species")) {
                    name = string.Join(' ', [words[0], .. epithets]);
                }
            }
            if (name is not null && fixes.TryGetValue(name, out var fixedName)) {
                name = fixedName;
            }
            return new SeafoodName(w, name);
        }).ToList();
    }

    /// A name left abbreviated on one rating ("A. truttaceus" on a page with no other Arripis) takes
    /// the name the same abbreviation, or a full name with that initial and epithet, has on the
    /// guide's other ratings, when they all give the same name.
    public static IReadOnlyList<SeafoodRating> ExpandAcrossRatings(IReadOnlyList<SeafoodRating> ratings) {
        var known = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);
        void Add(string key, string scientific) {
            if (!known.TryGetValue(key, out var names)) {
                known[key] = names = new HashSet<string>(StringComparer.Ordinal);
            }
            names.Add(scientific);
        }
        foreach (var name in ratings.SelectMany(r => r.Names)) {
            if (name.ScientificName is not { } scientific) {
                continue;
            }
            if (Words(scientific) is [var genus, var epithet]) {
                Add($"{genus[0]}. {epithet}", scientific);
            }
            // Also as written, so a misspelt abbreviation ("K. Peronei") finds the name its fix gave.
            if (AbbreviationKey(name.Written) is { } written) {
                Add(written, scientific);
            }
        }
        string? Expanded(SeafoodName name) =>
            name.ScientificName is null && AbbreviationKey(name.Written) is { } key && known.TryGetValue(key, out var names) && names.Count == 1
                ? names.First()
                : null;
        return ratings.Select(r => r.Names.Any(n => Expanded(n) is not null)
            ? r with { Names = r.Names.Select(n => Expanded(n) is { } e ? n with { ScientificName = e } : n).ToList() }
            : r).ToList();
    }

    // "K. peronei" for "K. Peronei"; null when the name is not an initial and one epithet.
    private static string? AbbreviationKey(string written) =>
        Words(Bracketed().Replace(written, " ")) is [var initial, var epithet] && Abbreviation().IsMatch(initial)
            ? $"{initial} {epithet.ToLowerInvariant()}"
            : null;

    private static string[] Words(string name) => name.Split(' ', StringSplitOptions.RemoveEmptyEntries);

    private static bool IsGenus(string word) => Genus().IsMatch(word);

    /// The fixes of one guide from the rules file: the name as read (after abbreviations are expanded
    /// and epithets put in lower case) and the name it means. A missing file has none.
    public static IReadOnlyDictionary<string, string> LoadFixes(string path, string guide) {
        if (!File.Exists(path)) {
            return new Dictionary<string, string>();
        }
        try {
            return FixesFromYaml(File.ReadAllText(path), guide);
        } catch (YamlException ex) {
            throw new InvalidOperationException($"{path}: {ex.Message}", ex);
        }
    }

    public static IReadOnlyDictionary<string, string> FixesFromYaml(string yaml, string guide) {
        var file = new DeserializerBuilder().WithNamingConvention(UnderscoredNamingConvention.Instance).Build()
            .Deserialize<FixesFile?>(yaml);
        var fixes = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var fix in file?.Fixes ?? []) {
            if (string.Equals(fix.Guide?.Trim(), guide, StringComparison.Ordinal)
                && !string.IsNullOrWhiteSpace(fix.Name) && !string.IsNullOrWhiteSpace(fix.Means)) {
                fixes[fix.Name.Trim()] = fix.Means.Trim();
            }
        }
        return fixes;
    }

    /// Text of an HTML fragment: tags removed, entities decoded, spaces collapsed.
    public static string? PlainText(string? html) {
        if (html is null) {
            return null;
        }
        var text = Spaces().Replace(WebUtility.HtmlDecode(Tags().Replace(html, " ")), " ").Trim();
        return text.Length == 0 ? null : text;
    }

    private sealed class FixesFile {
        public List<Fix>? Fixes { get; set; }
    }

    private sealed class Fix {
        public string? Guide { get; set; }
        public string? Name { get; set; }
        public string? Means { get; set; }
    }

    [GeneratedRegex(@"\s+")]
    private static partial Regex Spaces();

    [GeneratedRegex(@"<[^>]*>")]
    private static partial Regex Tags();

    [GeneratedRegex(@"\([^)]*\)")]
    private static partial Regex Bracketed();

    [GeneratedRegex(@"^[A-Z]\.$")]
    private static partial Regex Abbreviation();

    [GeneratedRegex(@"^[A-Z][a-z]+$")]
    private static partial Regex Genus();

    [GeneratedRegex(@"^[a-z][a-z-]*\.?$")]
    private static partial Regex Epithet();
}
