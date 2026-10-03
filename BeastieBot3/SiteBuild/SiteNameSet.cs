using BeastieBot3.CommonNames;
using BeastieBot3.Shared.SiteData;

// The names of one taxon on their way into the site database's `name` table, without repeats.
//
// Two names are the same when their folded forms (SiteNameKey.Fold: case, accents and spacing
// ignored) are equal and:
//   - scientific names and synonyms: they have the same type. A synonym that folds to one of the
//     taxon's scientific names is dropped.
//   - common names: they also have the same language and the same source. The source stays in the
//     key because the species page lists, for each English name, every source that gives it
//     ("Polar bear": IUCN Red List, Wikidata, Catalogue of Life), and it can only do that from one
//     row per source.
// The first spelling added is kept; a later copy can only turn is_preferred on.
// Common names in every language go through CommonNameQuality first: junk (wiki markup, author
// citations, OCR errors, names cut off at a bracket) is left out, and a name with a fixable extra
// ("Sunda slow loris{sfn|...}", "Mountain_gorilla") is added repaired.

namespace BeastieBot3.SiteBuild;

internal sealed record SiteName(string Name, string NameType, string? Language, string Source, bool IsPreferred);

internal sealed class SiteNameSet {
    private readonly List<SiteName> _names = new();
    private readonly Dictionary<(string Key, string Type, string? Language, string? Source), int> _index = new();
    private readonly HashSet<string> _scientificKeys = new(StringComparer.Ordinal);

    public IReadOnlyList<SiteName> Names => _names;

    /// Adds the name unless it repeats one already added; true when it was added. Empty names
    /// (after cleaning) are never added. A common name that CommonNameQuality finds is junk is not
    /// added, and one it can repair is added repaired.
    public bool Add(string? name, string nameType, string? language, string source, bool isPreferred = false) {
        var cleaned = SiteBuildRules.CleanName(name);
        if (cleaned.Length == 0) {
            return false;
        }
        if (nameType == SiteNameType.Common) {
            var quality = CommonNameQuality.Assess(cleaned, language);
            if (quality.IsJunk) {
                return false;
            }
            cleaned = quality.Name;
        }
        var key = SiteNameKey.Fold(cleaned);
        if (key.Length == 0) {
            return false;
        }
        if (nameType == SiteNameType.Synonym && _scientificKeys.Contains(key)) {
            return false;
        }
        var lookup = nameType == SiteNameType.Common
            ? (key, nameType, language, source)
            : (key, nameType, (string?)null, (string?)null);
        if (_index.TryGetValue(lookup, out var at)) {
            if (isPreferred && !_names[at].IsPreferred) {
                _names[at] = _names[at] with { IsPreferred = true };
            }
            return false;
        }
        if (nameType == SiteNameType.Scientific) {
            _scientificKeys.Add(key);
            // A synonym added before the scientific name it equals is removed.
            var synonym = (key, SiteNameType.Synonym, (string?)null, (string?)null);
            if (_index.Remove(synonym, out var synonymAt)) {
                _names.RemoveAt(synonymAt);
                RebuildIndex();
            }
        }
        _index[lookup] = _names.Count;
        _names.Add(new SiteName(cleaned, nameType, language, source, isPreferred));
        return true;
    }

    private void RebuildIndex() {
        _index.Clear();
        for (var i = 0; i < _names.Count; i++) {
            var n = _names[i];
            var key = SiteNameKey.Fold(n.Name);
            _index[n.NameType == SiteNameType.Common ? (key, n.NameType, n.Language, n.Source) : (key, n.NameType, null, null)] = i;
        }
    }
}
