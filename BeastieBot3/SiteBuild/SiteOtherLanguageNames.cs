using System.Globalization;
using System.Text.Json;
using Microsoft.Data.Sqlite;

// Common names in languages other than English for `site build-db`, beside IUCN's (which come from
// the API's taxon records and are kept as they are):
//   - the Catalogue of Life's vernacular names (vernacularname) of each taxon's CoL id (col_id),
//     source 'col';
//   - from each taxon's Wikidata item in the Wikidata cache, its taxon common name (P1843)
//     statements other than deprecated ones, and its labels and aliases, source 'wikidata';
//   - the titles of the item's Wikipedia sitelinks, as names in that Wikipedia's language, source
//     'wikipedia'.
// English names are left out here: they come from the common names store. Language codes go
// through SiteLanguageCodes, and names through OtherLanguageNameRules; SiteNameSet then leaves out
// junk and repeats when the names are written. Both readers run after every synonym list is read,
// because the rules compare names with the synonyms.

namespace BeastieBot3.SiteBuild;

/// A common name in a language other than English, from the Catalogue of Life, Wikidata or Wikipedia.
internal readonly record struct SiteOtherName(string Name, string Language, string Source);

/// What the readers did with each source's names, for the build summary.
internal sealed class OtherNameCounts {
    /// Names read for the site's taxa, before any rule.
    public int Read;
    /// English names (left out: the English names come from the common names store).
    public int English;
    /// Names with no language, several languages, or a code with no ISO 639 language the site can name.
    public int LanguageLeftOut;
    public readonly Dictionary<string, int> LanguagesLeftOut = new(StringComparer.Ordinal);
    public readonly Dictionary<OtherNameDrop, int> Dropped = new();
    /// Kept for the taxon (before SiteNameSet removes junk and repeats).
    public int Kept;
}

internal static class SiteOtherLanguageNames {
    /// The Catalogue of Life's vernacular names of each taxon's CoL id, in languages other than
    /// English. False when the database has no vernacularname table.
    public static bool ReadCol(string path, IReadOnlyDictionary<long, SiteTaxon> taxa, OtherNameCounts counts,
        CancellationToken cancellationToken) {
        var byColId = new Dictionary<string, List<SiteTaxon>>(StringComparer.Ordinal);
        foreach (var taxon in taxa.Values) {
            if (taxon.ColId is { } colId) {
                if (!byColId.TryGetValue(colId, out var list)) {
                    byColId[colId] = list = new List<SiteTaxon>();
                }
                list.Add(taxon);
            }
        }
        var keys = new Dictionary<long, TaxonScientificKeys>();
        using var connection = SiteLinkReaders.OpenReadOnly(path);
        using (var exists = connection.CreateCommand()) {
            exists.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'vernacularname'";
            if (Convert.ToInt64(exists.ExecuteScalar(), CultureInfo.InvariantCulture) == 0) {
                return false;
            }
        }
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT taxonID, name, language FROM vernacularname WHERE name IS NOT NULL AND name <> ''";
        command.CommandTimeout = 0;
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            cancellationToken.ThrowIfCancellationRequested();
            if (reader.IsDBNull(0) || !byColId.TryGetValue(reader.GetString(0), out var colTaxa)) {
                continue;
            }
            var name = OtherLanguageNameRules.TrimDirectionMarks(reader.GetString(1));
            var code = reader.IsDBNull(2) ? null : reader.GetString(2);
            foreach (var taxon in colTaxa) {
                if (!keys.TryGetValue(taxon.TaxonId, out var taxonKeys)) {
                    keys[taxon.TaxonId] = taxonKeys = TaxonScientificKeys.For(taxon);
                }
                Consider(taxon, name, code, SiteNameSource.Col, taxonKeys, counts, null);
            }
        }
        return true;
    }

    /// The names on each taxon's Wikidata item: P1843 statements, labels and aliases (source
    /// 'wikidata') and Wikipedia sitelink titles (source 'wikipedia'). Reads every downloaded item's
    /// JSON once, in the cache's order, and parses only the items of the site's taxa.
    public static void ReadWikidata(string path, IReadOnlyDictionary<long, SiteTaxon> taxa, OtherNameCounts wikidata,
        OtherNameCounts wikipedia, CancellationToken cancellationToken) {
        var byItem = new Dictionary<long, List<SiteTaxon>>();
        foreach (var taxon in taxa.Values) {
            if (taxon.WikidataQid is { Length: > 1 } qid
                && long.TryParse(qid.AsSpan(1), NumberStyles.None, CultureInfo.InvariantCulture, out var id)) {
                if (!byItem.TryGetValue(id, out var list)) {
                    byItem[id] = list = new List<SiteTaxon>();
                }
                list.Add(taxon);
            }
        }
        using var connection = SiteLinkReaders.OpenReadOnly(path);
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT entity_numeric_id, json FROM wikidata_entities WHERE json_downloaded = 1";
        command.CommandTimeout = 0;
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            cancellationToken.ThrowIfCancellationRequested();
            if (!byItem.TryGetValue(reader.GetInt64(0), out var itemTaxa) || reader.IsDBNull(1)) {
                continue;
            }
            var item = WikidataItemNames.Read(reader.GetString(1));
            if (item is null) {
                continue;
            }
            foreach (var taxon in itemTaxa) {
                var taxonKeys = TaxonScientificKeys.For(taxon).With(item.ScientificNames);
                // Repeats within the item (a label that is also a P1843 value) are counted once.
                var seen = new HashSet<(string, string, string)>();
                foreach (var (name, code) in item.Names) {
                    Consider(taxon, OtherLanguageNameRules.TrimDirectionMarks(name), code, SiteNameSource.Wikidata, taxonKeys, wikidata, seen);
                }
                foreach (var (site, title) in item.Sitelinks) {
                    if (OtherLanguageNameRules.WikipediaLanguage(site) is not { } code) {
                        continue;
                    }
                    Consider(taxon, OtherLanguageNameRules.WithoutDisambiguation(title), code, SiteNameSource.Wikipedia, taxonKeys,
                        wikipedia, seen);
                }
            }
        }
    }

    private static void Consider(SiteTaxon taxon, string name, string? code, string source, TaxonScientificKeys keys,
        OtherNameCounts counts, HashSet<(string, string, string)>? seen) {
        if (name.Length == 0) {
            return;
        }
        var language = SiteLanguageCodes.Normalise(code);
        if (seen is not null && !seen.Add((name, language ?? code ?? string.Empty, source))) {
            return;
        }
        counts.Read++;
        if (language is null) {
            if (IsEnglish(code)) {
                counts.English++;
            } else {
                counts.LanguageLeftOut++;
                var shown = string.IsNullOrWhiteSpace(code) ? "(none)" : code.Trim().ToLowerInvariant();
                counts.LanguagesLeftOut[shown] = counts.LanguagesLeftOut.GetValueOrDefault(shown) + 1;
            }
            return;
        }
        var drop = OtherLanguageNameRules.Check(name, keys);
        if (drop != OtherNameDrop.None) {
            counts.Dropped[drop] = counts.Dropped.GetValueOrDefault(drop) + 1;
            return;
        }
        counts.Kept++;
        taxon.OtherLanguageNames.Add(new SiteOtherName(name, language, source));
    }

    private static bool IsEnglish(string? code) {
        if (code is null) {
            return false;
        }
        var lower = code.Trim().ToLowerInvariant();
        return lower is "en" or "eng" or "simple" || lower.StartsWith("en-", StringComparison.Ordinal);
    }
}

/// The names on one Wikidata item, from its JSON (wbgetentities form, or the entity object itself).
internal sealed record WikidataItemNames(
    IReadOnlyList<(string Name, string Language)> Names,
    IReadOnlyList<(string Site, string Title)> Sitelinks,
    IReadOnlyList<string> ScientificNames) {
    /// Labels, aliases and taxon common name (P1843) values other than deprecated ones (in that
    /// order), sitelink titles, and the taxon name (P225) values; null for JSON that cannot be read.
    public static WikidataItemNames? Read(string json) {
        try {
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            var entity = root.TryGetProperty("entities", out var entities) && entities.ValueKind == JsonValueKind.Object
                ? entities.EnumerateObject().Select(p => p.Value).FirstOrDefault()
                : root;
            if (entity.ValueKind != JsonValueKind.Object) {
                return null;
            }
            var names = new List<(string, string)>();
            if (entity.TryGetProperty("labels", out var labels) && labels.ValueKind == JsonValueKind.Object) {
                foreach (var label in labels.EnumerateObject()) {
                    if (Text(label.Value, "value") is { } value) {
                        names.Add((value, label.Name));
                    }
                }
            }
            if (entity.TryGetProperty("aliases", out var aliases) && aliases.ValueKind == JsonValueKind.Object) {
                foreach (var language in aliases.EnumerateObject()) {
                    if (language.Value.ValueKind != JsonValueKind.Array) {
                        continue;
                    }
                    foreach (var alias in language.Value.EnumerateArray()) {
                        if (Text(alias, "value") is { } value) {
                            names.Add((value, language.Name));
                        }
                    }
                }
            }
            var scientific = new List<string>();
            if (entity.TryGetProperty("claims", out var claims) && claims.ValueKind == JsonValueKind.Object) {
                foreach (var value in StatementValues(claims, "P1843")) {
                    if (value.ValueKind == JsonValueKind.Object && Text(value, "text") is { } text && Text(value, "language") is { } language) {
                        names.Add((text, language));
                    }
                }
                foreach (var value in StatementValues(claims, "P225")) {
                    if (value.ValueKind == JsonValueKind.String && value.GetString() is { Length: > 0 } taxonName) {
                        scientific.Add(taxonName);
                    }
                }
            }
            var sitelinks = new List<(string, string)>();
            if (entity.TryGetProperty("sitelinks", out var links) && links.ValueKind == JsonValueKind.Object) {
                foreach (var link in links.EnumerateObject()) {
                    if (Text(link.Value, "title") is { } title) {
                        sitelinks.Add((link.Name, title));
                    }
                }
            }
            return new WikidataItemNames(names, sitelinks, scientific);
        } catch (JsonException) {
            return null;
        }
    }

    // The datavalue values of a property's statements, leaving out deprecated statements and
    // statements with no value or an unknown value.
    private static IEnumerable<JsonElement> StatementValues(JsonElement claims, string property) {
        if (!claims.TryGetProperty(property, out var statements) || statements.ValueKind != JsonValueKind.Array) {
            yield break;
        }
        foreach (var statement in statements.EnumerateArray()) {
            if (statement.TryGetProperty("rank", out var rank) && rank.ValueKind == JsonValueKind.String && rank.GetString() == "deprecated") {
                continue;
            }
            if (statement.TryGetProperty("mainsnak", out var snak) && snak.TryGetProperty("datavalue", out var datavalue)
                && datavalue.TryGetProperty("value", out var value)) {
                yield return value;
            }
        }
    }

    private static string? Text(JsonElement element, string property) =>
        element.ValueKind == JsonValueKind.Object && element.TryGetProperty(property, out var value) && value.ValueKind == JsonValueKind.String
            ? value.GetString()
            : null;
}
