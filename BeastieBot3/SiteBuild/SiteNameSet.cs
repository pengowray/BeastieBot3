using BeastieBot3.CommonNames;
using BeastieBot3.Shared.SiteData;

// The names of one taxon on their way into the site database's `name` table, without repeats.
//
// Two names are the same when their folded forms (SiteNameKey.Fold: case, accents and spacing
// ignored) are equal and:
//   - scientific names: they have the same type.
//   - synonyms: they also have the same source, so the species page can list every source that
//     gives a synonym. A synonym that folds to one of the taxon's scientific names is dropped. A
//     synonym's authority is kept from the first copy that has one.
//   - common names: they also have the same language and the same source. The source stays in the
//     key because the species page lists, for each English name, every source that gives it
//     ("Polar bear": IUCN Red List, Wikidata, Catalogue of Life), and it can only do that from one
//     row per source.
// The first spelling added is kept; a later copy can only turn is_preferred on.
// Common names in every language go through CommonNameQuality first: junk (wiki markup, author
// citations, OCR errors, names cut off at a bracket) is left out, and a name with a fixable extra
// ("Sunda slow loris{sfn|...}", "Mountain_gorilla") is added repaired. The set counts both for the
// build summary: junk once per name, language and source (the store repeats IUCN's names, so the
// same junk name can be offered twice), and a repaired name only when it is added.

namespace BeastieBot3.SiteBuild;

internal sealed record SiteName(string Name, string NameType, string? Language, string Source, bool IsPreferred, string? Authority = null);

internal sealed class SiteNameSet {
    private readonly List<SiteName> _names = new();
    private readonly Dictionary<(string Key, string Type, string? Language, string? Source), int> _index = new();
    private readonly HashSet<string> _scientificKeys = new(StringComparer.Ordinal);
    private readonly HashSet<(string Key, string? Language, string Source)> _junkCommonNames = new();

    public IReadOnlyList<SiteName> Names => _names;

    /// Common names left out because CommonNameQuality found them to be junk, counted once per
    /// folded name, language and source.
    public int JunkCommonNames => _junkCommonNames.Count;

    /// Common names added with CommonNameQuality's repair. A repaired name that repeats one already
    /// added is not added, so is not counted. SiteBuildRules.CleanName's tidying, which comes first
    /// (HTML tags and entities, leading backslashes), is not counted as a repair.
    public int RepairedCommonNames { get; private set; }

    /// Adds the name unless it repeats one already added; true when it was added. Empty names
    /// (after cleaning) are never added. A common name that CommonNameQuality finds is junk is not
    /// added, and one it can repair is added repaired.
    public bool Add(string? name, string nameType, string? language, string source, bool isPreferred = false, string? authority = null) {
        var cleaned = SiteBuildRules.CleanName(name);
        if (cleaned.Length == 0) {
            return false;
        }
        var repaired = false;
        if (nameType == SiteNameType.Common) {
            var quality = CommonNameQuality.Assess(cleaned, language);
            if (quality.IsJunk) {
                _junkCommonNames.Add((SiteNameKey.Fold(cleaned), language, source));
                return false;
            }
            repaired = quality.IsRepaired;
            cleaned = quality.Name;
        }
        var key = SiteNameKey.Fold(cleaned);
        if (key.Length == 0) {
            return false;
        }
        if (nameType == SiteNameType.Synonym && _scientificKeys.Contains(key)) {
            return false;
        }
        var cleanedAuthority = nameType == SiteNameType.Synonym ? SiteBuildRules.NullIfBlank(SiteBuildRules.CleanName(authority)) : null;
        var lookup = LookupKey(key, nameType, language, source);
        if (_index.TryGetValue(lookup, out var at)) {
            if (isPreferred && !_names[at].IsPreferred) {
                _names[at] = _names[at] with { IsPreferred = true };
            }
            if (cleanedAuthority is not null && _names[at].Authority is null) {
                _names[at] = _names[at] with { Authority = cleanedAuthority };
            }
            return false;
        }
        if (nameType == SiteNameType.Scientific) {
            _scientificKeys.Add(key);
            // A synonym added before the scientific name it equals is removed, from every source.
            if (_names.RemoveAll(n => n.NameType == SiteNameType.Synonym && SiteNameKey.Fold(n.Name) == key) > 0) {
                RebuildIndex();
            }
        }
        _index[lookup] = _names.Count;
        _names.Add(new SiteName(cleaned, nameType, language, source, isPreferred, cleanedAuthority));
        if (repaired) {
            RepairedCommonNames++;
        }
        return true;
    }

    private static (string Key, string Type, string? Language, string? Source) LookupKey(string key, string nameType, string? language,
        string source) => nameType switch {
        SiteNameType.Common => (key, nameType, language, source),
        SiteNameType.Synonym => (key, nameType, null, source),
        _ => (key, nameType, null, null),
    };

    private void RebuildIndex() {
        _index.Clear();
        for (var i = 0; i < _names.Count; i++) {
            var n = _names[i];
            _index[LookupKey(SiteNameKey.Fold(n.Name), n.NameType, n.Language, n.Source)] = i;
        }
    }
}
