using System.Text;
using System.Text.Json;

namespace BeastieBot3.Shared.SiteData;

// The credits of an assessment as the site database stores them (assessment.credits): the credit
// groups of the IUCN API payload's credits[] array, each a list of credit_name ids, or, when IUCN's
// value[] list is empty, the id of its "full" string (the citation form, "Tolley, K. & Menegon, M.").
//
//   [{"type":"assessor","names":[12,45]},{"type":"evaluator","full":77}]
//
// The names are kept in a table of their own because the same people are credited on thousands of
// assessments: 2.1 million entries in 2026-1, of which about 5% are distinct.

/// IUCN's credit_type_name values, in the order the site shows them (IUCN's assessment pages use the
/// same order).
public static class CreditTypes {
    public const string Assessor = "assessor";
    public const string Evaluator = "evaluator";
    public const string Contributor = "contributor";
    public const string Facilitators = "facilitators";
    public const string Institutions = "institutions";

    public static readonly IReadOnlyList<string> Order = [Assessor, Evaluator, Contributor, Facilitators, Institutions];

    /// The place of a type in Order; a type IUCN may add later comes after the known ones.
    public static int Rank(string type) {
        for (var i = 0; i < Order.Count; i++) {
            if (string.Equals(Order[i], type, StringComparison.Ordinal)) return i;
        }
        return Order.Count;
    }
}

/// One credit group. Exactly one of Names (credit_name ids, in display order) and Full (the id of
/// the group's "full" string) is set.
public sealed record StoredCreditGroup(string Type, IReadOnlyList<long> Names, long? Full) {
    public bool IsFullOnly => Full is not null;
}

public static class StoredCredits {
    public static string ToJson(IReadOnlyList<StoredCreditGroup> groups) {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream)) {
            writer.WriteStartArray();
            foreach (var group in groups) {
                writer.WriteStartObject();
                writer.WriteString("type", group.Type);
                if (group.Full is { } full) {
                    writer.WriteNumber("full", full);
                } else {
                    writer.WriteStartArray("names");
                    foreach (var id in group.Names) writer.WriteNumberValue(id);
                    writer.WriteEndArray();
                }
                writer.WriteEndObject();
            }
            writer.WriteEndArray();
        }
        return Encoding.UTF8.GetString(stream.ToArray());
    }

    /// Reads assessment.credits. Empty for null or text that is not this format; groups with no
    /// type or no names are skipped.
    public static IReadOnlyList<StoredCreditGroup> FromJson(string? json) {
        if (string.IsNullOrWhiteSpace(json)) return [];
        try {
            using var document = JsonDocument.Parse(json);
            if (document.RootElement.ValueKind != JsonValueKind.Array) return [];
            var groups = new List<StoredCreditGroup>();
            foreach (var element in document.RootElement.EnumerateArray()) {
                if (element.ValueKind != JsonValueKind.Object
                    || !element.TryGetProperty("type", out var type) || type.ValueKind != JsonValueKind.String
                    || type.GetString() is not { Length: > 0 } typeText) {
                    continue;
                }
                if (element.TryGetProperty("full", out var full) && full.TryGetInt64(out var fullId)) {
                    groups.Add(new StoredCreditGroup(typeText, [], fullId));
                    continue;
                }
                if (!element.TryGetProperty("names", out var names) || names.ValueKind != JsonValueKind.Array) continue;
                var ids = new List<long>();
                foreach (var id in names.EnumerateArray()) {
                    if (id.ValueKind == JsonValueKind.Number && id.TryGetInt64(out var value)) ids.Add(value);
                }
                if (ids.Count > 0) groups.Add(new StoredCreditGroup(typeText, ids, null));
            }
            return groups;
        } catch (JsonException) {
            return [];
        }
    }

    /// Every credit_name id the groups refer to, each once.
    public static IReadOnlyList<long> NameIds(IReadOnlyList<StoredCreditGroup> groups) =>
        groups.SelectMany(g => g.Full is { } full ? [full] : g.Names).Distinct().ToList();
}
