using System.Globalization;
using System.Net;
using System.Text.RegularExpressions;
using BeastieBot3.WikipediaLists;

// The decisions `site build-db` makes about single values, kept apart from the reading and writing
// so each one can be tested on its own (BeastieBot3.Tests/SiteBuild/SiteBuildRulesTests.cs).

namespace BeastieBot3.SiteBuild;

internal static class SiteTaxonKind {
    public const string Species = "species";
    public const string Subspecies = "subspecies";
    public const string Variety = "variety";
    public const string Subpopulation = "subpopulation";
}

internal static class SiteNameType {
    public const string Scientific = "scientific";
    public const string Common = "common";
    public const string Synonym = "synonym";
}

internal static class SiteNameSource {
    public const string Iucn = "iucn";
    public const string Col = "col";
    public const string Wikidata = "wikidata";
    /// A Wikipedia article title.
    public const string Wikipedia = "wikipedia";
    /// The English name in a Wikipedia article's taxobox.
    public const string WikipediaTaxobox = "wikipedia-taxobox";
}

internal static class SiteBuildRules {
    /// The scope of a global assessment, as the site compares it.
    public const string GlobalScope = "Global";

    /// The scope code of a global assessment in the API's scopes[].
    public const string GlobalScopeCode = "1";

    private static readonly string[] InfraRankMarkers = { "ssp.", "subsp.", "var." };

    private static readonly Regex HtmlTag = new(@"<[^>]*>", RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private static readonly Regex Whitespace = new(@"\s+", RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private static readonly Regex CriteriaVersionPattern = new(@"^\d+\.\d+$", RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private static readonly Regex ColReleasePattern = new(@"COL(?<version>\d+(?:\.\d+)?)(?:[_ -](?<edition>XR|Base))?",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);

    // ------------------------------------------------------------ taxa

    /// The taxon's kind from the CSV's infraType and subpopulationName. A subpopulation of a
    /// subspecies (4 Zea mays rows in 2026-1 have both) is a subpopulation.
    public static string KindOf(string? infraType, string? subpopulationName) {
        if (!string.IsNullOrWhiteSpace(subpopulationName)) {
            return SiteTaxonKind.Subpopulation;
        }
        return infraType?.Trim().ToLowerInvariant() switch {
            "subspecies" or "subspecies (plantae)" => SiteTaxonKind.Subspecies,
            "variety" => SiteTaxonKind.Variety,
            _ => SiteTaxonKind.Species,
        };
    }

    /// The kind of a taxon from the flags of its IUCN API record (taxon.infrarank,
    /// taxon.subpopulation): a subpopulation; an infraspecific taxon is a variety when its name has
    /// "var.", otherwise a subspecies; anything else is a species.
    public static string KindFromApiFlags(bool infrarank, bool subpopulation, string scientificName) {
        if (subpopulation) {
            return SiteTaxonKind.Subpopulation;
        }
        if (infrarank) {
            return InfraRankMarker(scientificName) == "var." ? SiteTaxonKind.Variety : SiteTaxonKind.Subspecies;
        }
        return SiteTaxonKind.Species;
    }

    /// The rank marker as the scientific name writes it ("ssp.", "subsp.", "var."), or null.
    public static string? InfraRankMarker(string scientificName) {
        foreach (var word in scientificName.Split(' ', StringSplitOptions.RemoveEmptyEntries)) {
            foreach (var marker in InfraRankMarkers) {
                if (string.Equals(word, marker, StringComparison.Ordinal)) {
                    return marker;
                }
            }
        }
        return null;
    }

    /// The name of the taxon a subpopulation belongs to: its scientific name with the
    /// subpopulation name taken off the end ("Zea mays subsp. mexicana Nobogame subpopulation" ->
    /// "Zea mays subsp. mexicana"). Null when the name does not end with it.
    public static string? SubpopulationParentName(string scientificName, string subpopulationName) {
        var name = scientificName.Trim();
        var suffix = subpopulationName.Trim();
        if (suffix.Length == 0 || !name.EndsWith(suffix, StringComparison.Ordinal) || name.Length == suffix.Length) {
            return null;
        }
        var parent = name[..^suffix.Length].Trim();
        return parent.Length == 0 ? null : parent;
    }

    // ------------------------------------------------------------ assessments

    /// The regions in a CSV scopes text, in order: "Global, Europe & Mediterranean" -> Global,
    /// Europe, Mediterranean. Empty when the row has no scope (28 rows of 2026-1).
    public static IReadOnlyList<string> CsvRegions(string? scopes) =>
        string.IsNullOrWhiteSpace(scopes)
            ? Array.Empty<string>()
            : scopes.Split(new[] { ',', '&' }, StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);

    /// The scope of a single CSV assessment row: "Global" when Global is among its regions,
    /// otherwise its first region as IUCN names it; null when it has none.
    public static string? ScopeFromCsv(string? scopes) => AssignCsvScopes(new[] { CsvRegions(scopes) })[0];

    /// The scope of each of one taxon's CSV rows (each row given as its regions). A row that
    /// includes Global is "Global". A regional row gets its first region that no other of the
    /// taxon's rows has already taken, rows with fewer regions choosing first; when every region is
    /// taken, its first region. So when a 2010 "Europe & Mediterranean" assessment and a 2026
    /// "Europe" one are both current (taxon 155717), the 2026 one is the latest for Europe and the
    /// 2010 one the latest for the Mediterranean, and no region gets two latest rows.
    public static IReadOnlyList<string?> AssignCsvScopes(IReadOnlyList<IReadOnlyList<string>> rows) {
        var scopes = new string?[rows.Count];
        var taken = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < rows.Count; i++) {
            if (rows[i].Any(r => string.Equals(r, GlobalScope, StringComparison.OrdinalIgnoreCase))) {
                scopes[i] = GlobalScope;
                taken.Add(GlobalScope);
            }
        }
        var regional = Enumerable.Range(0, rows.Count)
            .Where(i => scopes[i] is null && rows[i].Count > 0)
            .OrderBy(i => rows[i].Count)
            .ThenBy(i => i);
        foreach (var i in regional) {
            var scope = rows[i].FirstOrDefault(r => !taken.Contains(r)) ?? rows[i][0];
            scopes[i] = scope;
            taken.Add(scope);
        }
        return scopes;
    }

    /// The scope of an API assessment header from its scopes[] (code, English description):
    /// "Global" when any code is 1, otherwise the first description; null when there is none.
    public static string? ScopeFromApi(IReadOnlyList<(string? Code, string? Description)> scopes) {
        if (scopes.Any(s => string.Equals(s.Code?.Trim(), GlobalScopeCode, StringComparison.Ordinal))) {
            return GlobalScope;
        }
        foreach (var (_, description) in scopes) {
            if (!string.IsNullOrWhiteSpace(description)) {
                return description.Trim();
            }
        }
        return null;
    }

    /// The IUCN category code for the CSV's redlistCategory text ("Lower Risk/near threatened" ->
    /// "LR/nt", "Critically Endangered" -> "CR"), or null for a text IucnRedlistStatus does not know.
    public static string? CategoryCodeFromCsv(string? redlistCategory) {
        if (string.IsNullOrWhiteSpace(redlistCategory)) {
            return null;
        }
        var text = redlistCategory.Trim();
        var descriptor = IucnRedlistStatus.ResolveFromDatabase(text, "false", "false");
        // ResolveFromDatabase gives the text back as the code when nothing matches.
        return IucnRedlistStatus.TryGetDescriptor(descriptor.Code, out var known)
            && string.Equals(known.Category, text, StringComparison.OrdinalIgnoreCase)
            ? known.Code
            : null;
    }

    /// "3.1", "2.3" as given; null for "Earlier Version" and anything else that is not a version
    /// number (the site's schema keeps NULL for assessments older than the 1994 criteria).
    public static string? CriteriaVersion(string? text) {
        var trimmed = text?.Trim();
        return trimmed is { Length: > 0 } && CriteriaVersionPattern.IsMatch(trimmed) ? trimmed : null;
    }

    /// The assessment date as yyyy-MM-dd in UTC. The CSV writes "2015-08-27 00:00:00 UTC"; the API
    /// writes local midnight with an offset ("2015-08-27T01:00:00.000+01:00", which is the same
    /// instant), so both give 2015-08-27.
    public static string? UtcDate(string? text) {
        if (string.IsNullOrWhiteSpace(text)) {
            return null;
        }
        var trimmed = text.Trim();
        if (trimmed.EndsWith(" UTC", StringComparison.OrdinalIgnoreCase)) {
            trimmed = trimmed[..^4].Trim();
        }
        if (DateTimeOffset.TryParse(trimmed, CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var parsed)) {
            return parsed.UtcDateTime.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        }
        return trimmed.Length >= 10
            && DateOnly.TryParseExact(trimmed[..10], "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var date)
            ? date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)
            : null;
    }

    public static int? Year(string? text) =>
        int.TryParse(text?.Trim(), NumberStyles.None, CultureInfo.InvariantCulture, out var year) ? year : null;

    /// The CSV's possiblyExtinct flags are the text "true" or "false".
    public static bool CsvFlag(string? text) =>
        string.Equals(text?.Trim(), "true", StringComparison.OrdinalIgnoreCase);

    public static string? NullIfBlank(string? text) => string.IsNullOrWhiteSpace(text) ? null : text.Trim();

    // ------------------------------------------------------------ links

    /// The EPBC Act category code for SPRAT's epbc_status text; null when the taxon is not listed
    /// or the text is not one of the six categories.
    public static string? EpbcCode(string? epbcStatus) => epbcStatus?.Trim().ToLowerInvariant() switch {
        "extinct" => "EX",
        "extinct in the wild" => "EW",
        "critically endangered" => "CR",
        "endangered" => "EN",
        "vulnerable" => "VU",
        "conservation dependent" => "CD",
        _ => null,
    };

    /// What a SPRAT scientific name says about the taxon whose name it starts with: the whole
    /// taxon ("Phascolarctos cinereus"), one population of it ("Phascolarctos cinereus (combined
    /// populations of Qld, NSW and the ACT)", "Rhinonicteris aurantia (Pilbara form)"), or neither.
    /// Brackets count as a population only when they are one group at the end of the name and hold
    /// no digit (digits mean a voucher, part of a phrase name: "Acacia sp. Castletower (N.Gibson
    /// TOI345)"). A sense in brackets ("sensu lato", "s.l.", "sensu stricto") is the whole taxon.
    public static SpratNameMatch ClassifySpratName(string spratName, string taxonName) {
        var name = spratName.Trim();
        var taxon = taxonName.Trim();
        if (string.Equals(name, taxon, StringComparison.Ordinal)) {
            return new SpratNameMatch(SpratNameKind.Taxon, null);
        }
        if (!name.StartsWith(taxon + " (", StringComparison.Ordinal)) {
            return new SpratNameMatch(SpratNameKind.None, null);
        }
        var bracketed = name[(taxon.Length + 1)..];
        if (BracketGroup(bracketed) is not { } inner) {
            return new SpratNameMatch(SpratNameKind.NotPopulation, null);
        }
        if (SenseQualifiers.Contains(inner.TrimEnd('.').Trim(), StringComparer.OrdinalIgnoreCase)) {
            return new SpratNameMatch(SpratNameKind.Taxon, null);
        }
        if (inner.Any(char.IsDigit)) {
            return new SpratNameMatch(SpratNameKind.NotPopulation, null);
        }
        return new SpratNameMatch(SpratNameKind.Population, inner);
    }

    private static readonly string[] SenseQualifiers = {
        "sensu lato", "s.l", "s. l", "s. lat", "sens. lat", "sensu stricto", "s.s", "s. str", "s.str", "sens. str",
    };

    // "(text)" as the whole string, with balanced brackets inside: the text, trimmed; else null.
    private static string? BracketGroup(string text) {
        if (text.Length < 3 || text[0] != '(' || text[^1] != ')') {
            return null;
        }
        var depth = 0;
        for (var i = 0; i < text.Length; i++) {
            depth += text[i] switch { '(' => 1, ')' => -1, _ => 0 };
            if (depth == 0 && i < text.Length - 1) {
                return null;
            }
        }
        var inner = text[1..^1].Trim();
        return depth == 0 && inner.Length > 0 ? inner : null;
    }

    /// When several Wikidata items state the same IUCN taxon id (P627), the one whose English label
    /// or taxon name (P225) is the IUCN scientific name, written with or without its rank marker;
    /// failing that, or when several qualify, the lowest item number.
    public static long ChooseP627Item(IReadOnlyCollection<WikidataCandidate> candidates, string scientificName) {
        if (candidates.Count == 0) {
            throw new ArgumentException("No candidates.", nameof(candidates));
        }
        var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase) {
            CollapseWhitespace(scientificName),
            CollapseWhitespace(WithoutRankMarkers(scientificName)),
        };
        var matching = candidates
            .Where(c => (c.Label is not null && names.Contains(CollapseWhitespace(c.Label)))
                || c.TaxonNames.Any(n => names.Contains(CollapseWhitespace(n))))
            .ToList();
        return (matching.Count > 0 ? matching : candidates).Min(c => c.NumericId);
    }

    private static string WithoutRankMarkers(string scientificName) =>
        string.Join(' ', scientificName.Split(' ', StringSplitOptions.RemoveEmptyEntries)
            .Where(w => !InfraRankMarkers.Contains(w, StringComparer.Ordinal)));

    /// "COL26.7 XR" from a path such as ".../col_coldp_COL26.7_XR.sqlite"; null when the file name
    /// names no release.
    public static string? ColReleaseFromPath(string? path) {
        if (string.IsNullOrWhiteSpace(path)) {
            return null;
        }
        var match = ColReleasePattern.Match(Path.GetFileName(path.Trim()));
        if (!match.Success) {
            return null;
        }
        var edition = match.Groups["edition"].Success ? " " + match.Groups["edition"].Value.ToUpperInvariant() : string.Empty;
        return "COL" + match.Groups["version"].Value + edition;
    }

    // ------------------------------------------------------------ names

    /// A name as text for the site: HTML tags removed, entities decoded, runs of whitespace
    /// collapsed, and the stray leading backslashes some Catalogue of Life names carry
    /// ("\\ Woolly Akodont") removed. Empty when nothing is left.
    public static string CleanName(string? text) {
        if (string.IsNullOrEmpty(text)) {
            return string.Empty;
        }
        var decoded = WebUtility.HtmlDecode(HtmlTag.Replace(text, " "));
        return CollapseWhitespace(decoded.TrimStart('\\', ' ', '\t'));
    }

    /// A synonym from the API's taxon record without its authority: built from its parts
    /// ("Thalarctos maritimus" from genus_name, species_name; "Felis pardus ssp. orientalis" with
    /// the rank marker the full name uses, else the usual one for infra_type), and from the full
    /// name only when the parts are missing. Notes such as "[nomen nudum]" are left out.
    public static string? IucnSynonymName(string? genus, string? species, string? infraType, string? infraName,
        string? subpopulationName, string? fullName) {
        var g = CleanName(genus);
        var s = CleanName(species);
        if (g.Length == 0 || s.Length == 0) {
            var whole = CleanName(fullName);
            var bracket = whole.IndexOf(" [", StringComparison.Ordinal);
            if (bracket > 0) {
                whole = whole[..bracket].Trim();
            }
            return whole.Length == 0 ? null : whole;
        }

        var name = g + " " + s;
        var infra = CleanName(infraName);
        if (infra.Length > 0) {
            var marker = MarkerBefore(CleanName(fullName), infra) ?? DefaultMarker(infraType);
            name += marker is null ? " " + infra : $" {marker} {infra}";
        }
        var subpopulation = CleanName(subpopulationName).Trim('(', ')', ' ');
        if (subpopulation.Length > 0) {
            name += " " + subpopulation;
        }
        return name;
    }

    private static readonly string[] SynonymMarkers = { "ssp.", "subsp.", "var.", "subvar.", "f.", "forma", "fo." };

    private static string? MarkerBefore(string fullName, string infraName) {
        var words = fullName.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        for (var i = 1; i < words.Length; i++) {
            if (string.Equals(words[i], infraName, StringComparison.Ordinal)
                && SynonymMarkers.Contains(words[i - 1], StringComparer.Ordinal)) {
                return words[i - 1];
            }
        }
        return null;
    }

    private static string? DefaultMarker(string? infraType) => infraType?.Trim().ToLowerInvariant() switch {
        "subspecies" => "ssp.",
        "subspecies (plantae)" => "subsp.",
        "variety" => "var.",
        "forma" => "f.",
        _ => null,
    };

    private static string CollapseWhitespace(string text) => Whitespace.Replace(text, " ").Trim();

    private static readonly System.Text.RegularExpressions.Regex RankOfGroup = new(
        @"^(?:species|subspecies|genus|variety|form|subgenus) of (?:\w+ )*?(?<group>[^,;()]+?)\s*(?:[,;(]|$)",
        System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.CultureInvariant);

    /// <summary>
    /// True when a Wikidata item's English description ("species of insect", "species of tree frog")
    /// names a group that cannot be in <paramref name="taxonKingdom"/>, judged with the same word
    /// table the Wikipedia matcher uses for bracketed qualifiers (WikiPageKingdom). An unknown group,
    /// or no description, is not evidence.
    /// </summary>
    internal static bool DescribesAnotherKingdom(string? description, string? taxonKingdom) {
        if (string.IsNullOrWhiteSpace(description) || string.IsNullOrWhiteSpace(taxonKingdom)) {
            return false;
        }
        var match = RankOfGroup.Match(description.Trim());
        if (!match.Success) {
            return false;
        }
        var group = match.Groups["group"].Value.Trim();
        var kingdoms = BeastieBot3.Wikipedia.WikiPageKingdom.FromQualifier($"x ({group})");
        if (kingdoms is null && group.EndsWith('s')) {
            // "genus of insects": the word table holds the singular.
            kingdoms = BeastieBot3.Wikipedia.WikiPageKingdom.FromQualifier($"x ({group[..^1]})");
        }
        if (kingdoms is null || kingdoms.Count == 0) {
            return false;
        }
        var own = taxonKingdom.Trim().ToUpperInvariant();
        return !kingdoms.Contains(own, StringComparer.OrdinalIgnoreCase);
    }
}

internal enum SpratNameKind {
    /// The SPRAT name is not the taxon's name, with or without brackets.
    None,
    /// The SPRAT name is the taxon's name, or the name with a sense in brackets.
    Taxon,
    /// The taxon's name with a population in brackets.
    Population,
    /// The taxon's name with brackets that do not name a population: a voucher, or more than one group.
    NotPopulation,
}

/// ClassifySpratName's answer. Population: the text in the brackets, for SpratNameKind.Population.
internal readonly record struct SpratNameMatch(SpratNameKind Kind, string? Population);

/// A Wikidata item that states an IUCN taxon id, with what the tie-break compares.
internal sealed record WikidataCandidate(long NumericId, string? Label, IReadOnlyList<string> TaxonNames);
