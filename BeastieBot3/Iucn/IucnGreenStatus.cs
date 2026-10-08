using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text.Json;

// Reads the answer of /api/v4/green_status/all for `iucn api green-status`: every published IUCN
// Green Status of Species assessment in one {"assessments":[...]} object. Each record is kept as
// the API gave it (GetRawText), keyed by its taxon's SIS id and its assessment date. Its url is
// the Red List page the Green Status is shown on (https://www.iucnredlist.org/species/12520/218695618),
// whose last number is a Red List assessment id.
//
// An answer that cannot be keyed in full is refused as a whole, not record by record: the command
// deletes stored rows that are not in the download, so a record skipped here would delete the
// stored copy of the same assessment.

namespace BeastieBot3.Iucn;

/// <summary>One Green Status assessment from /api/v4/green_status/all.</summary>
internal sealed record IucnGreenStatusRecord(
    long SisId,
    string AssessmentDate,
    long? RedListAssessmentId,
    string Json,
    string? ScientificName,
    string? SpeciesRecoveryCategory);

internal enum IucnGreenStatusProblem {
    None,
    NotJson,      // the body is not JSON
    NoList,       // not an object with an "assessments" array
    Empty,        // the array has no records
    NoTaxonId,    // a record has no taxon.sis_id (Index, 1-based, of Total)
    NoDate,       // a record has no assessment_date in yyyy-MM-dd form (Index, 1-based, of Total)
}

/// <summary>
/// The records of one answer, or why the answer cannot be used. DuplicateKeys counts records with
/// the taxon and date of an earlier record; only the last of each is in Records.
/// </summary>
internal sealed record IucnGreenStatusAnswer(
    IReadOnlyList<IucnGreenStatusRecord> Records,
    IucnGreenStatusProblem Problem,
    int Index = 0,
    int Total = 0,
    string? Detail = null,
    int DuplicateKeys = 0) {
    public bool Usable => Problem == IucnGreenStatusProblem.None;
}

internal static class IucnGreenStatusParser {
    public static IucnGreenStatusAnswer Parse(string body) {
        JsonDocument doc;
        try {
            doc = JsonDocument.Parse(body);
        } catch (JsonException ex) {
            return Fail(IucnGreenStatusProblem.NotJson, detail: ex.Message);
        }
        using (doc) {
            if (doc.RootElement.ValueKind != JsonValueKind.Object
                || !doc.RootElement.TryGetProperty("assessments", out var list)
                || list.ValueKind != JsonValueKind.Array) {
                return Fail(IucnGreenStatusProblem.NoList);
            }

            var byKey = new Dictionary<(long, string), IucnGreenStatusRecord>();
            var order = new List<(long, string)>();
            var duplicates = 0;
            var index = 0;
            var total = list.GetArrayLength();
            foreach (var item in list.EnumerateArray()) {
                index++;
                if (item.ValueKind != JsonValueKind.Object || SisIdOf(item) is not { } sisId) {
                    return Fail(IucnGreenStatusProblem.NoTaxonId, index, total);
                }
                if (DateOf(String(item, "assessment_date")) is not { } date) {
                    return Fail(IucnGreenStatusProblem.NoDate, index, total);
                }
                var taxon = item.TryGetProperty("taxon", out var t) && t.ValueKind == JsonValueKind.Object ? t : default;
                var record = new IucnGreenStatusRecord(
                    sisId,
                    date,
                    AssessmentIdFromUrl(String(item, "url")),
                    item.GetRawText(),
                    taxon.ValueKind == JsonValueKind.Object ? String(taxon, "scientific_name") : null,
                    String(item, "species_recovery_category"));
                var key = (sisId, date);
                if (byKey.ContainsKey(key)) {
                    duplicates++;
                } else {
                    order.Add(key);
                }
                byKey[key] = record;
            }
            if (index == 0) return Fail(IucnGreenStatusProblem.Empty);

            var records = new List<IucnGreenStatusRecord>(order.Count);
            foreach (var key in order) records.Add(byKey[key]);
            return new IucnGreenStatusAnswer(records, IucnGreenStatusProblem.None, DuplicateKeys: duplicates);
        }
    }

    /// <summary>The Red List assessment id at the end of a Red List page URL, or null.</summary>
    internal static long? AssessmentIdFromUrl(string? url) {
        if (string.IsNullOrWhiteSpace(url)) return null;
        var path = url.Trim();
        var cut = path.IndexOfAny(new[] { '?', '#' });
        if (cut >= 0) path = path[..cut];
        path = path.TrimEnd('/');
        var slash = path.LastIndexOf('/');
        var last = slash >= 0 ? path[(slash + 1)..] : path;
        return long.TryParse(last, NumberStyles.None, CultureInfo.InvariantCulture, out var id) && id > 0 ? id : null;
    }

    /// <summary>The date as yyyy-MM-dd, from "2023-10-31" or a longer timestamp that starts with it.</summary>
    internal static string? DateOf(string? text) {
        if (text is null || text.Trim() is not { Length: >= 10 } trimmed) return null;
        var day = trimmed[..10];
        return DateOnly.TryParseExact(day, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out _) ? day : null;
    }

    private static long? SisIdOf(JsonElement item) {
        if (!item.TryGetProperty("taxon", out var taxon) || taxon.ValueKind != JsonValueKind.Object
            || !taxon.TryGetProperty("sis_id", out var id)) {
            return null;
        }
        return id.ValueKind switch {
            JsonValueKind.Number when id.TryGetInt64(out var n) && n > 0 => n,
            JsonValueKind.String when long.TryParse(id.GetString(), NumberStyles.None, CultureInfo.InvariantCulture, out var n) && n > 0 => n,
            _ => null,
        };
    }

    private static string? String(JsonElement obj, string name) =>
        obj.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String ? value.GetString() : null;

    private static IucnGreenStatusAnswer Fail(IucnGreenStatusProblem problem, int index = 0, int total = 0, string? detail = null) =>
        new(Array.Empty<IucnGreenStatusRecord>(), problem, index, total, detail);
}

/// <summary>The release name in the answer of /api/v4/information/red_list_version.</summary>
internal static class IucnRedListVersion {
    /// <summary>"2026-1" from {"red_list_version":"2026-1"}; null when the answer has no such string.</summary>
    public static string? Parse(string body) {
        try {
            using var doc = JsonDocument.Parse(body);
            return doc.RootElement.ValueKind == JsonValueKind.Object
                   && doc.RootElement.TryGetProperty("red_list_version", out var v)
                   && v.ValueKind == JsonValueKind.String
                   && v.GetString()?.Trim() is { Length: > 0 } version
                ? version
                : null;
        } catch (JsonException) {
            return null;
        }
    }
}
