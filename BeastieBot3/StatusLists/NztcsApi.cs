using System.Globalization;
using System.Net;
using System.Text.Json;
using System.Text.RegularExpressions;

// The New Zealand Threat Classification System database (nztcs.org.nz), run by the Department of
// Conservation. Its search pages call two JSON endpoints that answer without a login:
//   POST /rest/assessmentSearch        the assessments of published reports, filtered to the current
//                                      ones; names are HTML with the authority ("<i>Apteryx haastii</i>
//                                      Potts, 1872"); no kingdom.
//   POST /rest/species/findByCriteria  every species record: id, scientific name, authority.
// Both take pageNumber (from 1) and pageSize (1,000 works). The site's content is CC BY 4.0.

namespace BeastieBot3.StatusLists;

/// One current NZTCS assessment. ScientificName: see NztcsApi.ChooseName; null for an informal name
/// or a title no name could be read from.
internal sealed record NztcsAssessment(
    long AssessmentId,
    long SpeciesId,
    string? ScientificName,
    string AssessmentName,
    string? CommonName,
    string? Category,
    string? Status,
    string? Criteria,
    string? Qualifiers,
    long? ReportId,
    string? ReportName,
    int? ReportYear);

internal static partial class NztcsApi {
    public const string SiteUrl = "https://nztcs.org.nz/";
    public const string AssessmentSearchUrl = "https://nztcs.org.nz/rest/assessmentSearch";
    public const string SpeciesSearchUrl = "https://nztcs.org.nz/rest/species/findByCriteria";
    public const int PageSize = 1000;

    public const string Title = "New Zealand Threat Classification System (NZTCS) database, Department of Conservation";
    public const string Licence = "CC BY 4.0";

    public static string Citation(DateTime accessedUtc) =>
        $"Department of Conservation. New Zealand Threat Classification System database. https://nztcs.org.nz/. (Accessed: {accessedUtc.ToString("MMMM d, yyyy", CultureInfo.InvariantCulture)}).";

    public static string AssessmentUrl(long assessmentId) =>
        "https://nztcs.org.nz/assessments/" + assessmentId.ToString(CultureInfo.InvariantCulture);

    /// The request body for one page of current, published assessments.
    public static string AssessmentRequest(int page) => JsonSerializer.Serialize(new {
        showArchived = false,
        categoryCode = Array.Empty<string>(),
        reportEditStatusList = new[] { "PUBLISHED" },
        reportPublishedStatusList = new[] { "CURRENT" },
        pageNumber = page,
        pageSize = PageSize,
        overrideExportWithCurrent = false,
    });

    /// The request body for one page of species records.
    public static string SpeciesRequest(int page) => JsonSerializer.Serialize(new { pageNumber = page, pageSize = PageSize });

    /// The total the page reports and its rows, as JSON elements.
    public static (long Total, IReadOnlyList<JsonElement> Rows) ReadPage(string json) {
        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;
        var total = root.TryGetProperty("total", out var t) && t.ValueKind == JsonValueKind.Number ? t.GetInt64() : 0;
        var rows = root.TryGetProperty("searchResults", out var results) && results.ValueKind == JsonValueKind.Array
            ? results.EnumerateArray().Select(e => e.Clone()).ToList()
            : new List<JsonElement>();
        return (total, rows);
    }

    /// A species record's id and scientific name; null for a record with neither.
    public static (long SpeciesId, string ScientificName)? ReadSpecies(JsonElement row) {
        if (Long(row, "speciesId") is not { } id || Text(row, "scientificName") is not { } name) {
            return null;
        }
        return (id, name);
    }

    /// An assessment row, with its scientific name (ChooseName); null for a row with no assessment
    /// or species id.
    public static NztcsAssessment? ReadAssessment(JsonElement row, IReadOnlyDictionary<long, string> speciesNames) {
        if (Long(row, "assessmentId") is not { } assessmentId || Long(row, "speciesId") is not { } speciesId) {
            return null;
        }
        var title = Text(row, "assessmentName") ?? Text(row, "nztcsSpeciesName") ?? string.Empty;
        return new NztcsAssessment(assessmentId, speciesId, ChooseName(title, speciesNames.GetValueOrDefault(speciesId)), PlainText(title),
            Text(row, "commonName"), Text(row, "categoryTitle"), Text(row, "conservationStatusTitle"), Text(row, "criteriaTitle"),
            Text(row, "qualifiers"), Long(row, "reportId"), Text(row, "reportName"), (int?)Long(row, "reportYear"));
    }

    /// The scientific name of an assessment, from its title (as the report publishes it) and the
    /// name of its species record, which can be out of date: the record that holds the current
    /// assessment of Apteryx australis australis is named Apteryx australis lawryi.
    ///   - An informal name in the title (quotes, "aff.", "cf.", "sp.", "nr.") gives no name, so an
    ///     undescribed or informal taxon never takes the status of the species it is named after.
    ///   - The record's name when the title gives no name, or the same name, or the start of it.
    ///   - Otherwise the title's name.
    public static string? ChooseName(string title, string? recordName) {
        var plain = PlainText(title);
        if (Informal().IsMatch(plain)) {
            return null;
        }
        var fromTitle = NameFromTitle(title);
        if (string.IsNullOrWhiteSpace(recordName) || Informal().IsMatch(recordName)) {
            return fromTitle;
        }
        if (fromTitle is null) {
            return recordName.Trim();
        }
        var record = MatchKey(recordName);
        var titled = MatchKey(fromTitle);
        return record == titled || record.StartsWith(titled + " ", StringComparison.Ordinal) ? recordName.Trim() : fromTitle;
    }

    /// The scientific name at the start of an assessment's title, without its authority: genus,
    /// epithet, then an infraspecific epithet with or without its rank ("Alectryon excelsus subsp.
    /// grandis (Cheeseman) de Lange" gives "Alectryon excelsus subsp. grandis"). Null when the title
    /// does not start with a genus and an epithet, or when a rank comes later in the title than the
    /// name ("Acrolejeunea securifolia (Nees) Steph. subsp. securifolia"), where the name read would
    /// be the species and not the subspecies.
    public static string? NameFromTitle(string title) {
        var words = PlainText(title).Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (words.Length < 2 || !Genus().IsMatch(words[0]) || !Epithet().IsMatch(words[1]) || Particles.Contains(words[1])) {
            return null;
        }
        var name = new List<string> { words[0], words[1] };
        var at = 2;
        if (at + 1 < words.Length && RankMarkers.Contains(words[at]) && Epithet().IsMatch(words[at + 1])) {
            name.Add(words[at]);
            name.Add(words[at + 1]);
            at += 2;
        } else if (at < words.Length && Epithet().IsMatch(words[at]) && !Particles.Contains(words[at])) {
            name.Add(words[at]);
            at++;
        }
        if (words.Skip(at).Any(RankMarkers.Contains)) {
            return null;
        }
        return string.Join(' ', name);
    }

    private static readonly HashSet<string> RankMarkers = new(StringComparer.Ordinal) { "subsp.", "ssp.", "var.", "f.", "forma" };

    // Lower-case words that start an author's name, not an epithet: "de Lange", "van Steenis".
    private static readonly HashSet<string> Particles = new(StringComparer.Ordinal) { "de", "van", "von", "der", "ex", "le", "la", "du", "et", "in" };

    // A name compared without rank markers and spacing.
    private static string MatchKey(string name) =>
        string.Join(' ', name.Split(' ', StringSplitOptions.RemoveEmptyEntries).Where(w => !RankMarkers.Contains(w)));

    /// An NZTCS status as the database's own pages write it: the category, then the status within
    /// it ("Threatened - Nationally Vulnerable", "At Risk - Declining"), or one of them when the two
    /// are the same ("Not Threatened", "Data Deficient"). Null for "Not assessed" and for neither.
    public static string? StatusText(string? category, string? status) {
        category = string.IsNullOrWhiteSpace(category) ? null : category.Trim();
        status = string.IsNullOrWhiteSpace(status) ? null : status.Trim();
        var text = (category, status) switch {
            (null, null) => null,
            (null, { } s) => s,
            ({ } c, null) => c,
            ({ } c, { } s) when string.Equals(c, s, StringComparison.OrdinalIgnoreCase) => s,
            ({ } c, { } s) => $"{c} - {s}",
        };
        return string.Equals(text, "Not assessed", StringComparison.OrdinalIgnoreCase) ? null : text;
    }

    /// HTML as plain text: tags removed, entities decoded, spaces collapsed.
    public static string PlainText(string html) =>
        Whitespace().Replace(WebUtility.HtmlDecode(Tag().Replace(html, " ")), " ").Trim()
            .Replace(" ,", ",", StringComparison.Ordinal);

    private static string? Text(JsonElement row, string property) =>
        row.TryGetProperty(property, out var value) && value.ValueKind == JsonValueKind.String && !string.IsNullOrWhiteSpace(value.GetString())
            ? value.GetString()!.Trim()
            : null;

    private static long? Long(JsonElement row, string property) =>
        row.TryGetProperty(property, out var value) && value.ValueKind == JsonValueKind.Number && value.TryGetInt64(out var n) ? n : null;

    // Quotes, or a qualifier that makes the name informal: "aff.", "cf.", "sp.", "spp.", "nr.".
    [GeneratedRegex(@"[""“”]|(?<![A-Za-z])(?:aff|cf|sp|spp|nr)\.")]
    private static partial Regex Informal();

    [GeneratedRegex("^[A-Z][a-z]+$")]
    private static partial Regex Genus();

    [GeneratedRegex("^[a-z][a-z-]+$")]
    private static partial Regex Epithet();

    [GeneratedRegex("<[^>]*>")]
    private static partial Regex Tag();

    [GeneratedRegex(@"\s+")]
    private static partial Regex Whitespace();
}
