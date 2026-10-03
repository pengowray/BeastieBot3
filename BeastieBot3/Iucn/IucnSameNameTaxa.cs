using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Data.Sqlite;
using BeastieBot3.Infrastructure;
using BeastieBot3.WikipediaLists;

// For an IUCN taxon id with no current assessment (an old or merged id), finds the current taxa
// that have the same scientific name, or that list the old name as an IUCN synonym, in the same
// kingdom and with a current assessment in the same scope. Example: 2785 Bettongia penicillata has
// no current assessment; the name is now used by 2790, whose current Global assessment is EN 2025.
//
// Current taxa come from the CSV export (one release per database, every assessment in it is
// current); the synonyms of current taxa and the old ids' last assessments come from the API cache.
// Matching and display are pure; the loaders only read. Used by `iucn api report-no-latest` and the
// audit site's no-latest page, so both apply the same rules.

namespace BeastieBot3.Iucn;

/// <summary>One current assessment of a current taxon. Scopes are region names ("Global", "Europe").</summary>
internal sealed record IucnCurrentAssessment(long AssessmentId, string? StatusCode, string? Year, IReadOnlyList<string> Scopes);

/// <summary>A taxon in the CSV export, with its current assessments.</summary>
internal sealed record IucnCurrentTaxon(long TaxonId, string ScientificName, string? Authority, string? Kingdom,
    IReadOnlyList<IucnCurrentAssessment> Assessments);

/// <summary>
/// A synonym IUCN lists for a current taxon. <see cref="Name"/> is the bare name built from the
/// structured fields (no authority, no subgenus); <see cref="AsWritten"/> is IUCN's full text,
/// authority and notes such as "[in part]" included.
/// </summary>
internal sealed record IucnCurrentSynonym(long TaxonId, string Name, string AsWritten);

/// <summary>A taxon id with no current assessment, and the scopes of its last assessment.</summary>
internal sealed record IucnOldTaxon(long TaxonId, string? ScientificName, string? Authority, string? Kingdom,
    IReadOnlyList<string> LastScopes);

internal enum IucnSameNameMatchKind {
    SameName,
    Synonym,
}

/// <summary>
/// A current taxon matched to an old id, with the current assessment in the old id's scope.
/// For a synonym match, <see cref="ListedAs"/> is IUCN's text of the synonym when it differs from
/// the old taxon's name and authority (a different author, or a note such as "[in part]").
/// </summary>
internal sealed record IucnSameNameMatch(IucnSameNameMatchKind Kind, IucnCurrentTaxon Taxon,
    IucnCurrentAssessment Assessment, string? ListedAs = null) {
    public string Url => IucnUrls.Species(Taxon.TaxonId, Assessment.AssessmentId)!;
}

internal sealed class IucnSameNameTaxa {
    private readonly Dictionary<string, List<IucnCurrentTaxon>> _byName = new(StringComparer.Ordinal);
    private readonly Dictionary<string, List<(IucnCurrentTaxon Taxon, IucnCurrentSynonym Synonym)>> _bySynonym = new(StringComparer.Ordinal);

    public int CurrentTaxonCount { get; }

    /// <summary>False when no synonyms were loaded (API cache absent), so synonym matching did not run.</summary>
    public bool HasSynonyms { get; }

    public IucnSameNameTaxa(IEnumerable<IucnCurrentTaxon> current, IEnumerable<IucnCurrentSynonym>? synonyms = null) {
        var byId = new Dictionary<long, IucnCurrentTaxon>();
        foreach (var taxon in current) {
            byId[taxon.TaxonId] = taxon;
            var key = Key(taxon.ScientificName, taxon.Kingdom);
            if (key is null) {
                continue;
            }
            if (!_byName.TryGetValue(key, out var list)) {
                _byName[key] = list = new List<IucnCurrentTaxon>();
            }
            list.Add(taxon);
        }
        CurrentTaxonCount = byId.Count;

        if (synonyms is null) {
            return;
        }
        HasSynonyms = true;
        foreach (var synonym in synonyms) {
            if (!byId.TryGetValue(synonym.TaxonId, out var taxon)) {
                continue;
            }
            var key = Key(synonym.Name, taxon.Kingdom);
            if (key is null) {
                continue;
            }
            if (!_bySynonym.TryGetValue(key, out var list)) {
                _bySynonym[key] = list = new List<(IucnCurrentTaxon, IucnCurrentSynonym)>();
            }
            list.Add((taxon, synonym));
        }
    }

    /// <summary>
    /// Current taxa (other than the old id) with the same scientific name and kingdom as the old id
    /// and a current assessment in the scope of the old id's last assessment. Ordered by taxon id.
    /// </summary>
    public IReadOnlyList<IucnSameNameMatch> SameName(IucnOldTaxon old) {
        var key = Key(old.ScientificName, old.Kingdom);
        if (key is null || !_byName.TryGetValue(key, out var candidates)) {
            return Array.Empty<IucnSameNameMatch>();
        }
        var matches = new List<IucnSameNameMatch>();
        foreach (var taxon in candidates.Where(t => t.TaxonId != old.TaxonId).OrderBy(t => t.TaxonId)) {
            var assessment = AssessmentInScope(old.LastScopes, taxon.Assessments);
            if (assessment is not null) {
                matches.Add(new IucnSameNameMatch(IucnSameNameMatchKind.SameName, taxon, assessment));
            }
        }
        return matches;
    }

    /// <summary>
    /// Current taxa that list the old id's name as an IUCN synonym, in the same kingdom, with a
    /// current assessment in the old id's scope. Leaves out the taxa <see cref="SameName"/> returns,
    /// so a taxon is never listed twice for one old id. One match per taxon, ordered by taxon id.
    /// </summary>
    public IReadOnlyList<IucnSameNameMatch> ViaSynonym(IucnOldTaxon old) {
        var key = Key(old.ScientificName, old.Kingdom);
        if (key is null || !_bySynonym.TryGetValue(key, out var candidates)) {
            return Array.Empty<IucnSameNameMatch>();
        }
        var sameName = SameName(old).Select(m => m.Taxon.TaxonId).ToHashSet();
        var oldFull = NameKey(JoinNameAndAuthority(old.ScientificName, old.Authority));
        var matches = new List<IucnSameNameMatch>();
        foreach (var group in candidates
                     .Where(c => c.Taxon.TaxonId != old.TaxonId && !sameName.Contains(c.Taxon.TaxonId))
                     .GroupBy(c => c.Taxon.TaxonId)
                     .OrderBy(g => g.Key)) {
            var taxon = group.First().Taxon;
            var assessment = AssessmentInScope(old.LastScopes, taxon.Assessments);
            if (assessment is null) {
                continue;
            }
            // A taxon can list the same name more than once ("Pittier" and "Pitt."). When one of
            // them has the old taxon's own authority there is nothing more to show.
            var exact = group.Any(c => NameKey(c.Synonym.AsWritten) == oldFull);
            var listedAs = exact ? null : group.First().Synonym.AsWritten;
            matches.Add(new IucnSameNameMatch(IucnSameNameMatchKind.Synonym, taxon, assessment, listedAs));
        }
        return matches;
    }

    // ---- Rules ----

    /// <summary>
    /// The comparison form of a scientific name: HTML entities decoded, whitespace trimmed and
    /// collapsed, lower case, and the rank markers "subsp." and "ssp." treated as the same.
    /// Returns "" for a blank name.
    /// </summary>
    public static string NameKey(string? name) {
        if (string.IsNullOrWhiteSpace(name)) {
            return string.Empty;
        }
        var words = WebUtility.HtmlDecode(name)
            .Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries)
            .Select(w => w.ToLowerInvariant())
            .Select(w => w == "subsp." ? "ssp." : w);
        return string.Join(' ', words);
    }

    private static string? Key(string? name, string? kingdom) {
        var nameKey = NameKey(name);
        if (nameKey.Length == 0 || string.IsNullOrWhiteSpace(kingdom)) {
            return null;
        }
        return nameKey + "|" + kingdom.Trim().ToUpperInvariant();
    }

    /// <summary>
    /// The current assessment that covers the scope of the old id's last assessment. When that
    /// assessment was Global, the current one must include Global. When it was regional only, the
    /// current one must include at least one of its regions; the one sharing the most regions wins,
    /// then the most recent. An old assessment with no scope recorded matches nothing, because there
    /// is no scope to compare.
    /// </summary>
    public static IucnCurrentAssessment? AssessmentInScope(IReadOnlyList<string> oldScopes, IReadOnlyList<IucnCurrentAssessment> current) {
        if (oldScopes.Count == 0) {
            return null;
        }
        if (oldScopes.Any(IsGlobal)) {
            return current.FirstOrDefault(a => a.Scopes.Any(IsGlobal));
        }
        var regions = new HashSet<string>(oldScopes.Select(s => s.Trim()), StringComparer.OrdinalIgnoreCase);
        return current
            .Select(a => (Assessment: a, Shared: a.Scopes.Count(regions.Contains)))
            .Where(x => x.Shared > 0)
            .OrderByDescending(x => x.Shared)
            .ThenByDescending(x => int.TryParse(x.Assessment.Year, out var y) ? y : 0)
            .ThenBy(x => x.Assessment.AssessmentId)
            .Select(x => x.Assessment)
            .FirstOrDefault();
    }

    private static bool IsGlobal(string scope) => string.Equals(scope.Trim(), "Global", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Splits the CSV export's scopes text into region names: "Global, Europe &amp; Mediterranean"
    /// gives Global, Europe and Mediterranean. No IUCN region name contains a comma or an ampersand.
    /// </summary>
    public static IReadOnlyList<string> ParseCsvScopes(string? scopes) {
        if (string.IsNullOrWhiteSpace(scopes)) {
            return Array.Empty<string>();
        }
        return WebUtility.HtmlDecode(scopes)
            .Split(new[] { ',', '&' }, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .ToList();
    }

    /// <summary>
    /// The bare name of an API synonym record: genus (subgenus dropped), species, and for an
    /// infraspecific name the rank marker and infraspecific epithet. Null when the record has no
    /// genus or species, or names a subpopulation.
    /// </summary>
    public static string? SynonymBareName(string? genus, string? species, string? infraType, string? infraName, string? subpopulation) {
        if (string.IsNullOrWhiteSpace(genus) || string.IsNullOrWhiteSpace(species) || !string.IsNullOrWhiteSpace(subpopulation)) {
            return null;
        }
        var genusOnly = Regex.Replace(genus, @"\s*\([^)]*\)", "").Trim();
        if (genusOnly.Length == 0) {
            return null;
        }
        var name = $"{genusOnly} {species.Trim()}";
        if (string.IsNullOrWhiteSpace(infraName)) {
            return name;
        }
        var marker = RankMarker(infraType);
        return marker is null ? $"{name} {infraName.Trim()}" : $"{name} {marker} {infraName.Trim()}";
    }

    private static string? RankMarker(string? infraType) => infraType?.Trim().ToLowerInvariant() switch {
        null or "" => null,
        "subspecies" => "ssp.",
        "subspecies (plantae)" => "subsp.",
        "variety" => "var.",
        "forma" => "f.",
        var other => other,
    };

    // ---- Display ----

    /// <summary>
    /// One match as text: the taxon id, then the name and authority when they differ from the old
    /// taxon's (always for a synonym match, since the current name is a different name), then the
    /// current category and year. A synonym IUCN lists with a different author or a note adds the
    /// synonym entry as IUCN wrote it.
    /// </summary>
    public static string Describe(IucnSameNameMatch match, IucnOldTaxon old) {
        var sb = new StringBuilder();
        sb.Append(match.Taxon.TaxonId.ToString(CultureInfo.InvariantCulture));
        if (match.Kind == IucnSameNameMatchKind.Synonym || NameOrAuthorityDiffers(match.Taxon, old)) {
            sb.Append(' ').Append(Clean(JoinNameAndAuthority(match.Taxon.ScientificName, match.Taxon.Authority)));
        }
        sb.Append(" (").Append(StatusAndYear(match.Assessment)).Append(')');
        if (match.ListedAs is { } listedAs) {
            sb.Append(", IUCN synonym: ").Append(Clean(listedAs));
        }
        return sb.ToString();
    }

    /// <summary>All matches for one old id, joined by <see cref="Separator"/>, with a count first when there are several.</summary>
    public static string DescribeAll(IReadOnlyList<IucnSameNameMatch> matches, IucnOldTaxon old) {
        if (matches.Count == 0) {
            return string.Empty;
        }
        var text = string.Join(Separator, matches.Select(m => Describe(m, old)));
        return matches.Count == 1 ? text : SeveralPrefix(matches.Count) + text;
    }

    public const string Separator = "; ";

    public static string SeveralPrefix(int count) => $"{count.ToString(CultureInfo.InvariantCulture)} taxa: ";

    public static string StatusAndYear(IucnCurrentAssessment assessment) {
        var status = string.IsNullOrWhiteSpace(assessment.StatusCode) ? "category unknown" : assessment.StatusCode!.Trim();
        return string.IsNullOrWhiteSpace(assessment.Year) ? status : $"{status}, {assessment.Year!.Trim()}";
    }

    private static bool NameOrAuthorityDiffers(IucnCurrentTaxon taxon, IucnOldTaxon old) =>
        !string.Equals(Clean(taxon.ScientificName), Clean(old.ScientificName), StringComparison.Ordinal)
        || !string.Equals(Clean(taxon.Authority), Clean(old.Authority), StringComparison.OrdinalIgnoreCase);

    private static string JoinNameAndAuthority(string? name, string? authority) {
        var cleanAuthority = Clean(authority);
        return cleanAuthority.Length == 0 ? Clean(name) : $"{Clean(name)} {cleanAuthority}";
    }

    // HTML entities decoded and whitespace collapsed, for display and for comparing authorities.
    private static string Clean(string? value) =>
        string.IsNullOrWhiteSpace(value)
            ? string.Empty
            : string.Join(' ', WebUtility.HtmlDecode(value).Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));

    // ---- Loading ----

    /// <summary>Reads both sources. Pass a null API cache to skip synonym matching.</summary>
    public static IucnSameNameTaxa Load(SqliteConnection csv, SqliteConnection? apiCache) {
        var current = LoadCurrentTaxa(csv);
        var synonyms = apiCache is null ? null : LoadSynonyms(apiCache, current.Select(t => t.TaxonId).ToHashSet());
        return new IucnSameNameTaxa(current, synonyms);
    }

    /// <summary>
    /// Every taxon in the CSV export with its assessments. The export holds only current
    /// assessments, one per taxon and scope. For a subspecies or variety the authority is the
    /// infraspecific authority when the export has one.
    /// </summary>
    public static IReadOnlyList<IucnCurrentTaxon> LoadCurrentTaxa(SqliteConnection csv) {
        const string sql = """
            SELECT t.taxonId, t.scientificName, t.authority, t.infraAuthority, t.kingdomName,
                   a.assessmentId, a.redlistCategory, a.yearPublished, a.scopes,
                   a.possiblyExtinct, a.possiblyExtinctInTheWild
            FROM taxonomy_html t
            JOIN assessments_html a ON a.taxonId = t.taxonId
            ORDER BY t.taxonId, a.assessmentId
            """;
        using var command = csv.CreateCommand();
        command.CommandText = sql;
        command.CommandTimeout = 0;

        var taxa = new Dictionary<long, (IucnCurrentTaxon Taxon, List<IucnCurrentAssessment> Assessments)>();
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            var taxonId = reader.GetInt64(0);
            var name = Text(reader, 1);
            if (name is null) {
                continue;
            }
            if (!taxa.TryGetValue(taxonId, out var entry)) {
                var authority = Text(reader, 3) ?? Text(reader, 2);
                var assessments = new List<IucnCurrentAssessment>();
                entry = (new IucnCurrentTaxon(taxonId, name, authority, Text(reader, 4), assessments), assessments);
                taxa[taxonId] = entry;
            }
            var category = Text(reader, 6);
            var code = category is null ? null : IucnRedlistStatus.ResolveFromDatabase(category, Text(reader, 9), Text(reader, 10)).Code;
            entry.Assessments.Add(new IucnCurrentAssessment(reader.GetInt64(5), code, Text(reader, 7), ParseCsvScopes(Text(reader, 8))));
        }
        return taxa.Values.Select(e => e.Taxon).ToList();
    }

    /// <summary>
    /// The synonyms IUCN lists for the given current taxa, from the API cache's taxa records. The
    /// synonym arrays are pulled out by SQLite's JSON functions, which reads the whole cache in a
    /// few seconds.
    /// </summary>
    public static IReadOnlyList<IucnCurrentSynonym> LoadSynonyms(SqliteConnection apiCache, IReadOnlySet<long> currentTaxonIds) {
        const string sql = """
            SELECT root_sis_id, json_extract(json, '$.taxon.synonyms')
            FROM taxa
            WHERE json_valid(json) AND json_array_length(json, '$.taxon.synonyms') > 0
            """;
        using var command = apiCache.CreateCommand();
        command.CommandText = sql;
        command.CommandTimeout = 0;

        var synonyms = new List<IucnCurrentSynonym>();
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            var taxonId = reader.GetInt64(0);
            if (!currentTaxonIds.Contains(taxonId) || reader.IsDBNull(1)) {
                continue;
            }
            JsonDocument document;
            try { document = JsonDocument.Parse(reader.GetString(1)); } catch (JsonException) { continue; }
            using (document) {
                foreach (var record in document.RootElement.EnumerateArray()) {
                    if (record.ValueKind != JsonValueKind.Object) {
                        continue;
                    }
                    var bare = SynonymBareName(Str(record, "genus_name"), Str(record, "species_name"),
                        Str(record, "infra_type"), Str(record, "infra_name"), Str(record, "subpopulation_name"));
                    if (bare is null) {
                        continue;
                    }
                    var asWritten = Str(record, "name")
                        ?? JoinNameAndAuthority(bare, Str(record, "infrarank_author") ?? Str(record, "species_author"));
                    synonyms.Add(new IucnCurrentSynonym(taxonId, bare, asWritten));
                }
            }
        }
        return synonyms;
    }

    /// <summary>
    /// The old taxon from an API cache taxa record: name, authority and kingdom from "taxon", and
    /// the scopes of the last assessment (the latest year_published; the first one listed wins a
    /// tie, as in both no-latest reports).
    /// </summary>
    public static IucnOldTaxon? OldTaxonFromJson(long rootSisId, string json) {
        try {
            using var document = JsonDocument.Parse(json);
            var root = document.RootElement;
            if (!root.TryGetProperty("taxon", out var taxon) || taxon.ValueKind != JsonValueKind.Object) {
                return null;
            }
            var scopes = new List<string>();
            if (root.TryGetProperty("assessments", out var assessments) && assessments.ValueKind == JsonValueKind.Array) {
                JsonElement? last = null;
                var lastYear = int.MinValue;
                foreach (var assessment in assessments.EnumerateArray()) {
                    if (assessment.ValueKind != JsonValueKind.Object || !HasAssessmentId(assessment)) {
                        continue;
                    }
                    var year = Year(assessment);
                    if (last is null || year > lastYear) {
                        last = assessment;
                        lastYear = year;
                    }
                }
                if (last is { } chosen && chosen.TryGetProperty("scopes", out var scopeArray) && scopeArray.ValueKind == JsonValueKind.Array) {
                    foreach (var scope in scopeArray.EnumerateArray()) {
                        if (scope.ValueKind == JsonValueKind.Object
                            && scope.TryGetProperty("description", out var description)
                            && description.ValueKind == JsonValueKind.Object
                            && Str(description, "en") is { } en) {
                            scopes.Add(en);
                        }
                    }
                }
            }
            return new IucnOldTaxon(rootSisId, Str(taxon, "scientific_name"), Str(taxon, "authority"), Str(taxon, "kingdom_name"), scopes);
        } catch (JsonException) {
            return null;
        }
    }

    private static bool HasAssessmentId(JsonElement assessment) =>
        assessment.TryGetProperty("assessment_id", out var id)
        && (id.ValueKind == JsonValueKind.Number || (id.ValueKind == JsonValueKind.String && long.TryParse(id.GetString(), out _)));

    private static int Year(JsonElement assessment) {
        if (!assessment.TryGetProperty("year_published", out var year)) {
            return int.MinValue;
        }
        var text = year.ValueKind switch {
            JsonValueKind.String => year.GetString(),
            JsonValueKind.Number => year.GetRawText(),
            _ => null,
        };
        return int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var y) ? y : int.MinValue;
    }

    private static string? Str(JsonElement element, string name) =>
        element.TryGetProperty(name, out var prop) && prop.ValueKind == JsonValueKind.String && !string.IsNullOrWhiteSpace(prop.GetString())
            ? prop.GetString()!.Trim()
            : null;

    private static string? Text(SqliteDataReader reader, int ordinal) {
        if (reader.IsDBNull(ordinal)) {
            return null;
        }
        var value = reader.GetValue(ordinal)?.ToString();
        return string.IsNullOrWhiteSpace(value) ? null : value.Trim();
    }
}
