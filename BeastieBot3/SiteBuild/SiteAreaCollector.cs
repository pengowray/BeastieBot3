using System.Text.Json;
using BeastieBot3.Shared.SiteData;

// The countries and areas of each taxon's latest global assessment, for the site database's area and
// taxon_area tables (the status update page compares a list with one area). Read from the assessment
// payload's locations[]: code, description.en, origin, presence and is_endemic.

namespace BeastieBot3.SiteBuild;

internal sealed class SiteAreaCollector {
    private readonly Dictionary<(string Area, long TaxonId), (AreaOrigin Origin, AreaPresence Presence, bool Endemic)> _rows = new();
    private readonly Dictionary<string, string> _names = new(StringComparer.Ordinal);
    // For each part of a country, how often each country is coded beside it, to find the country it is in.
    private readonly Dictionary<string, Dictionary<string, int>> _beside = new(StringComparer.Ordinal);

    public int Taxa { get; private set; }
    public int Rows => _rows.Count;
    public int Areas => _names.Count;

    /// Reads the payload's locations for the taxon. A location whose origin or presence IUCN does
    /// not name, or whose code is not an area (a hash-coded region), is left out.
    public void Add(long taxonId, JsonElement root) {
        if (root.ValueKind != JsonValueKind.Object || !root.TryGetProperty("locations", out var locations)
            || locations.ValueKind != JsonValueKind.Array) {
            return;
        }
        var codes = new List<string>();
        foreach (var location in locations.EnumerateArray()) {
            var code = String(location, "code")?.Trim().ToUpperInvariant();
            if (!AreaCodes.IsAreaCode(code) || AreaCodes.Origin(String(location, "origin")) is not { } origin
                || AreaCodes.Presence(String(location, "presence")) is not { } presence) {
                continue;
            }
            var endemic = location.TryGetProperty("is_endemic", out var e) && e.ValueKind == JsonValueKind.True;
            var key = (code!, taxonId);
            _rows[key] = _rows.TryGetValue(key, out var known)
                ? ((AreaOrigin)Math.Min((int)known.Origin, (int)origin), (AreaPresence)Math.Min((int)known.Presence, (int)presence), known.Endemic || endemic)
                : (origin, presence, endemic);
            if (location.TryGetProperty("description", out var description) && description.ValueKind == JsonValueKind.Object
                && String(description, "en") is { Length: > 0 } name) {
                _names.TryAdd(code!, name.Trim());
            }
            codes.Add(code!);
        }
        if (codes.Count == 0) {
            return;
        }
        Taxa++;
        var countries = codes.Where(c => c.Length == 2).Distinct().ToList();
        foreach (var part in codes.Where(c => c.Contains('-')).Distinct()) {
            if (!_beside.TryGetValue(part, out var counts)) {
                _beside[part] = counts = new Dictionary<string, int>(StringComparer.Ordinal);
            }
            foreach (var country in countries) {
                counts[country] = counts.GetValueOrDefault(country) + 1;
            }
        }
    }

    /// area rows: code, name, and for part of a country the country coded beside it most often.
    public IEnumerable<object?[]> AreaRows() =>
        _names.OrderBy(n => n.Key, StringComparer.Ordinal).Select(n => new object?[] {
            n.Key, n.Value,
            n.Key.Contains('-') && _beside.TryGetValue(n.Key, out var counts) && counts.Count > 0
                ? counts.OrderByDescending(c => c.Value).ThenBy(c => c.Key, StringComparer.Ordinal).First().Key
                : null,
        });

    /// taxon_area rows: area, taxon id, origin, presence, endemic. IUCN flags endemism only on
    /// countries (about 100,000 flags in 2026-1, none on parts of countries), so a part's flag is
    /// derived: the taxon is endemic to the country, and the part is the only part of that country
    /// it is recorded in (the Tasmanian devil in Tasmania; not the koala, recorded in four states).
    public IEnumerable<object?[]> TaxonAreaRows() {
        var countryOf = AreaRows().ToDictionary(r => (string)r[0]!, r => (string?)r[2], StringComparer.Ordinal);
        var partsByTaxon = _rows.Keys.Where(k => k.Area.Contains('-')).GroupBy(k => k.TaxonId)
            .ToDictionary(g => g.Key, g => g.Select(k => k.Area).ToList());
        bool EndemicToPart(string part, long taxonId) =>
            countryOf.GetValueOrDefault(part) is { } country
            && _rows.TryGetValue((country, taxonId), out var countryRecord) && countryRecord.Endemic
            && partsByTaxon[taxonId].Count(p => countryOf.GetValueOrDefault(p) == country) == 1;
        return _rows.OrderBy(r => r.Key.Area, StringComparer.Ordinal).ThenBy(r => r.Key.TaxonId)
            .Select(r => new object?[] {
                r.Key.Area, r.Key.TaxonId, (int)r.Value.Origin, (int)r.Value.Presence,
                r.Value.Endemic || (r.Key.Area.Contains('-') && EndemicToPart(r.Key.Area, r.Key.TaxonId)) ? 1 : 0,
            });
    }

    private static string? String(JsonElement element, string name) =>
        element.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String ? value.GetString() : null;
}
