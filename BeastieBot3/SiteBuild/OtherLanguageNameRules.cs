using System.Text.RegularExpressions;
using BeastieBot3.Shared.SiteData;

// Which names from the Catalogue of Life, Wikidata and Wikipedia site build-db keeps as a taxon's
// common names in languages other than English. Many of them are the scientific name: in most
// languages Wikidata's label of a taxon item is its scientific name, set by bots, and bots made
// hundreds of thousands of Cebuano, Waray, Swedish, Dutch and Vietnamese Wikipedia articles titled
// with it. A name is not a common name when, with case, accents and spacing ignored
// (SiteNameKey.Fold), it is
//   - the taxon's scientific name (also without IUCN's rank marker: "Panthera tigris sumatrae"),
//     one of its synonyms from any source, or a scientific name of its Wikidata item (P225);
//   - its genus;
// or when it starts with the genus, as written, a space and a lower-case word that is an epithet
// (a binomial or trinomial: "Panthera tigris altaica", "Carya illinoiensis" misspelt), or with the
// genus's initial, a full stop, a space and the species epithet ("P. tigris"). The epithet test
// keeps real names that begin with the genus ("Tragopan de Cabot", "Veronica delle paludi"); a word
// counts as an epithet when it is the taxon's own or an epithet of any name in the common names
// store (NameWordSets.IsEpithet). Without that word list, any lower-case word counts.
// CommonNameQuality's junk test runs after these, in SiteNameSet.
//
// Names are tidied first: Wikidata's Asturian labels wrap the scientific name in left-to-right
// marks (U+200E), which are removed from the ends, and a Wikipedia title loses a bracketed
// disambiguation at its end ("Tigre (animal)" is "Tigre").

namespace BeastieBot3.SiteBuild;

/// Why a name from another source is not kept as a common name.
internal enum OtherNameDrop {
    None,
    /// The taxon's scientific name or one of its synonyms.
    ScientificName,
    /// The taxon's genus.
    Genus,
    /// The genus, a space and a lower-case word.
    Binomial,
    /// The genus's initial, a full stop and the species epithet.
    AbbreviatedBinomial,
}

/// One taxon's scientific names for the rule: folded keys of its names and synonyms, its genus and
/// species epithet.
internal sealed class TaxonScientificKeys {
    private readonly HashSet<string> _keys;

    public TaxonScientificKeys(string? genus, string? speciesEpithet, IEnumerable<string> names) {
        Genus = string.IsNullOrWhiteSpace(genus) ? null : genus.Trim();
        SpeciesEpithet = string.IsNullOrWhiteSpace(speciesEpithet) ? null : speciesEpithet.Trim();
        _keys = new HashSet<string>(StringComparer.Ordinal);
        foreach (var name in names) {
            var key = SiteNameKey.Fold(name);
            if (key.Length > 0) {
                _keys.Add(key);
            }
        }
    }

    public string? Genus { get; }
    public string? SpeciesEpithet { get; }

    public bool Contains(string foldedName) => _keys.Contains(foldedName);

    /// A copy with more names (the scientific names of the taxon's Wikidata item).
    public TaxonScientificKeys With(IEnumerable<string> more) {
        var copy = new TaxonScientificKeys(Genus, SpeciesEpithet, []);
        copy._keys.UnionWith(_keys);
        foreach (var name in more) {
            var key = SiteNameKey.Fold(name);
            if (key.Length > 0) {
                copy._keys.Add(key);
            }
        }
        return copy;
    }

    /// The keys of a site taxon: its scientific name as IUCN writes it and without the rank marker,
    /// and every synonym the build has read for it.
    public static TaxonScientificKeys For(SiteTaxon taxon) {
        var names = new List<string> { taxon.ScientificName };
        if (taxon.Genus is { } genus && taxon.SpeciesEpithet is { } epithet) {
            names.Add(taxon.InfraName is { } infra ? $"{genus} {epithet} {infra}" : $"{genus} {epithet}");
        }
        names.AddRange(taxon.IucnSynonyms.Select(s => s.Name));
        names.AddRange(taxon.ColSynonyms.Select(s => s.Name));
        names.AddRange(taxon.WikidataSynonyms.Select(s => s.Name));
        names.AddRange(taxon.WikipediaSynonyms.Select(s => s.Name));
        names.AddRange(taxon.ChecklistNames.Where(n => n.Type != "common").Select(n => n.Name));
        return new TaxonScientificKeys(taxon.Genus, taxon.SpeciesEpithet, names);
    }
}

internal static partial class OtherLanguageNameRules {
    /// Why the name is not a common name of the taxon, or None when it can be one. isEpithet: whether a
    /// lower-case word is an epithet of some scientific name; null takes any lower-case word as one.
    public static OtherNameDrop Check(string name, TaxonScientificKeys taxon, Func<string, bool>? isEpithet = null) {
        var key = SiteNameKey.Fold(name);
        if (taxon.Contains(key)) {
            return OtherNameDrop.ScientificName;
        }
        if (taxon.Genus is not { } genus) {
            return OtherNameDrop.None;
        }
        if (key == SiteNameKey.Fold(genus)) {
            return OtherNameDrop.Genus;
        }
        var trimmed = name.Trim();
        if (trimmed.Length > genus.Length + 1 && trimmed.StartsWith(genus, StringComparison.Ordinal)
            && trimmed[genus.Length] == ' ' && char.IsLower(trimmed[genus.Length + 1])) {
            var word = trimmed[(genus.Length + 1)..].Split(' ', 2)[0];
            if (isEpithet is null || word == taxon.SpeciesEpithet || isEpithet(word)) {
                return OtherNameDrop.Binomial;
            }
        }
        if (taxon.SpeciesEpithet is { } epithet) {
            var abbreviated = $"{genus[0]}. {epithet}";
            if (trimmed.StartsWith(abbreviated, StringComparison.Ordinal)
                && (trimmed.Length == abbreviated.Length || trimmed[abbreviated.Length] == ' ')) {
                return OtherNameDrop.AbbreviatedBinomial;
            }
        }
        return OtherNameDrop.None;
    }

    /// The name with direction marks (U+200E, U+200F and the embedding and isolate controls)
    /// removed from its ends and its spaces trimmed. Marks inside a name are kept; the zero-width
    /// non-joiner (U+200C) is part of Persian and Malayalam spelling and is never removed.
    public static string TrimDirectionMarks(string name) => name.Trim().Trim(DirectionMarks).Trim();

    private static readonly char[] DirectionMarks = [
        '‎', '‏', '‪', '‫', '‬', '‭', '‮', '⁦', '⁧', '⁨', '⁩', '﻿',
    ];

    /// A Wikipedia article title without a bracketed disambiguation at its end ("Tigre (animal)" is
    /// "Tigre"); the title as it is when nothing would be left.
    public static string WithoutDisambiguation(string title) {
        var trimmed = TrimDirectionMarks(title);
        var match = TrailingBracket().Match(trimmed);
        return match.Success && match.Index > 0 ? trimmed[..match.Index].TrimEnd() : trimmed;
    }

    [GeneratedRegex(@"\s*\([^()]*\)\s*$")]
    private static partial Regex TrailingBracket();

    /// The language code of a Wikipedia site id from an item's sitelinks ("frwiki" -> "fr",
    /// "zh_yuewiki" -> "zh-yue"), or null for a site that is not a language's Wikipedia
    /// (commonswiki, specieswiki, metawiki, frwikiquote ...). The code still goes through
    /// SiteLanguageCodes.Normalise.
    public static string? WikipediaLanguage(string site) {
        if (!site.EndsWith("wiki", StringComparison.Ordinal) || site.Length <= 4 || NotLanguageWikis.Contains(site)) {
            return null;
        }
        var code = site[..^4];
        return LanguageSite().IsMatch(code) ? code.Replace('_', '-') : null;
    }

    // Sites named "...wiki" that are not a Wikipedia in one language.
    private static readonly HashSet<string> NotLanguageWikis = new(StringComparer.Ordinal) {
        "commonswiki", "specieswiki", "metawiki", "wikidatawiki", "sourceswiki", "abstractwiki", "incubatorwiki", "mediawikiwiki",
        "outreachwiki", "wikimaniawiki", "foundationwiki", "wikifunctionswiki", "testwiki", "test2wiki", "testwikidatawiki",
        "nostalgiawiki", "tenwiki", "labswiki", "donatewiki", "strategywiki", "usabilitywiki", "votewiki", "advisorywiki",
    };

    [GeneratedRegex("^[a-z]{2,3}(_[a-z]+)*$")]
    private static partial Regex LanguageSite();
}
