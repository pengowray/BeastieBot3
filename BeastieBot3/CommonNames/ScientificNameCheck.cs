using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using BeastieBot3.Taxonomy;

// Decides whether a name that `common-names aggregate` is about to store as a common name is the
// taxon's scientific name instead: a Wikipedia article title (wikipedia_title), a taxobox name
// (wikipedia_taxobox) or a Wikidata English label (wikidata_label).
//
// It used to judge by the shape of the name alone (LooksLikeScientificName: a capital, then
// lower-case words with Latin endings). That dropped about 10,700 article titles that are common
// names in sentence case ("Pygmy hippopotamus", "Bluntnose sixgill shark") and kept about 5,000
// that are scientific names ("Arundo donax", genus titles such as "Techmarscincus"), and the lists
// then showed no common name for those taxa.
//
// The check now compares the name with the taxon's own names first. A name that is none of them
// can still be a scientific name: Wikipedia often titles an article with a newer or older
// combination than the store knows ("Rubroshorea ovata" for Shorea ovata, "Tliltocatl vagans" for
// Brachypelma vagans), and the lists cannot catch that later, because their check only compares
// a name with the taxon's own binomial. So a name shaped like a scientific name is also tested
// word by word against the store's genus names, epithets and the words of English common names.

namespace BeastieBot3.CommonNames;

/// <summary>
/// One taxon's scientific names, normalised with <see cref="ScientificNameNormalizer.Normalize"/>:
/// the accepted (canonical) name and its synonyms. For a Wikidata item the item's own scientific
/// names count as synonyms.
/// </summary>
internal sealed record TaxonScientificNames(string? Canonical, IReadOnlyCollection<string> Synonyms) {
    public static readonly TaxonScientificNames None = new(null, Array.Empty<string>());

    public bool IsEmpty => string.IsNullOrEmpty(Canonical) && Synonyms.Count == 0;

    public IEnumerable<string> All => string.IsNullOrEmpty(Canonical) ? Synonyms : Synonyms.Prepend(Canonical);

    public TaxonScientificNames WithSynonyms(IEnumerable<string> more) =>
        this with { Synonyms = Synonyms.Concat(more).Distinct(StringComparer.Ordinal).ToArray() };
}

/// <summary>
/// The words the check compares a name's words with: genus names and epithets from the store's
/// scientific names, and words that appear in English common names from IUCN and the Catalogue of
/// Life. All lower case.
/// </summary>
internal sealed class NameWordSets {
    /// <summary>
    /// A word counts as English when at least this many different English common names use it.
    /// CoL's English rows include a few scientific names ("Gobio gobio"); a word that only one or two
    /// names use is more likely to come from one of those than to be English.
    /// </summary>
    internal const int MinCommonNamesForEnglishWord = 3;

    public static readonly NameWordSets Empty = new(new HashSet<string>(), new HashSet<string>(), new HashSet<string>());

    private readonly HashSet<string> _genera;
    private readonly HashSet<string> _epithets;
    private readonly HashSet<string> _english;

    public NameWordSets(HashSet<string> genera, HashSet<string> epithets, HashSet<string> englishWords) {
        _genera = genera;
        _epithets = epithets;
        _english = englishWords;
    }

    public int GenusCount => _genera.Count;
    public int EpithetCount => _epithets.Count;
    public int EnglishWordCount => _english.Count;

    public bool IsGenus(string word) => _genera.Contains(word);
    public bool IsEpithet(string word) => _epithets.Contains(word);

    /// <summary>True when the word, or one part of a hyphenated word, is an English word.</summary>
    public bool IsEnglish(string word) =>
        _english.Contains(word) || (word.Contains('-') && word.Split('-').Any(_english.Contains));

    private static readonly Regex CleanScientificName = new(@"^[a-z]+( [a-z][a-z\-]*){0,2}$", RegexOptions.Compiled);
    private static readonly Regex EnglishWord = new(@"[a-z]+(?:-[a-z]+)*", RegexOptions.Compiled);

    /// <summary>
    /// Builds the sets. <paramref name="scientificNames"/> are normalised names (canonical names and
    /// synonyms); only a genus with up to two lower-case epithets is used, so subpopulation names
    /// ("salmo salar eastern cape breton subpopulation") and working names ("haplochromis sp. nov.
    /// 'yellow'") add no English words to the epithets. <paramref name="englishCommonNames"/> are
    /// English common names as stored.
    /// </summary>
    public static NameWordSets Build(IEnumerable<string> scientificNames, IEnumerable<string> englishCommonNames) {
        var genera = new HashSet<string>(StringComparer.Ordinal);
        var epithets = new HashSet<string>(StringComparer.Ordinal);
        foreach (var name in scientificNames) {
            var words = ScientificNameCheck.WithoutRankMarkers(name.Split(' ', StringSplitOptions.RemoveEmptyEntries));
            if (words.Count == 0 || !CleanScientificName.IsMatch(string.Join(' ', words))) {
                continue;
            }
            genera.Add(words[0]);
            for (var i = 1; i < words.Count; i++) {
                epithets.Add(words[i]);
            }
        }

        var namesUsingWord = new Dictionary<string, int>(StringComparer.Ordinal);
        var wordsInName = new HashSet<string>(StringComparer.Ordinal);
        foreach (var name in englishCommonNames) {
            wordsInName.Clear();
            foreach (Match match in EnglishWord.Matches(name.ToLowerInvariant())) {
                wordsInName.Add(match.Value);
                if (match.Value.Contains('-')) {
                    wordsInName.UnionWith(match.Value.Split('-'));
                }
            }
            foreach (var word in wordsInName) {
                namesUsingWord[word] = namesUsingWord.GetValueOrDefault(word) + 1;
            }
        }
        var english = new HashSet<string>(
            namesUsingWord.Where(kv => kv.Value >= MinCommonNamesForEnglishWord).Select(kv => kv.Key), StringComparer.Ordinal);

        return new NameWordSets(genera, epithets, english);
    }
}

internal static class ScientificNameCheck {
    private static readonly Regex GenusWord = new(@"^[A-Z][a-z]+$", RegexOptions.Compiled);
    private static readonly Regex EpithetWord = new(@"^[a-z][a-z\-]*$", RegexOptions.Compiled);

    private static readonly HashSet<string> RankMarkers = new(StringComparer.OrdinalIgnoreCase) {
        "var.", "subsp.", "ssp.", "f.", "fo.", "forma", "subf.", "cv.", "nothosubsp.", "nothovar."
    };

    /// <summary>
    /// True when <paramref name="name"/> (a Wikipedia title without its disambiguation, a taxobox
    /// name or a Wikidata label) is a scientific name rather than a common name of the taxon with
    /// <paramref name="taxon"/> as its names. In order:
    /// <list type="number">
    /// <item>The name is one of the taxon's names, or the first word or words of one (the genus of a
    /// monotypic genus, the species of a subspecies), ignoring case, rank markers and a subgenus. A
    /// genus taken from a synonym is not counted when it is an English word ("Orca" for Orcinus
    /// orca).</item>
    /// <item>Otherwise only a name shaped like a scientific name can be one: two to four words, the
    /// first a capitalised word of plain letters, the rest lower case. Anything else is a common
    /// name.</item>
    /// <item>The first word is one of the taxon's genera ("Gobio gobio" for Gobio latus): a scientific
    /// name.</item>
    /// <item>A word is an English word, and not a genus (first word) or an epithet (later words) in
    /// the store or in the taxon's names ("Pygmy hippopotamus", "Alligator gar"): a common name.</item>
    /// <item>A word is a genus or epithet in the store, or one of the taxon's epithets (a different
    /// combination of the same species, "Rubroshorea ovata"): a scientific name. An epithet that is
    /// also the taxon's genus does not count, so "Western gorilla" is a common name of Gorilla
    /// gorilla.</item>
    /// <item>Otherwise, a common name; but when the taxon's names are not known, the shape alone
    /// decides, and the name is taken to be a scientific name.</item>
    /// </list>
    /// </summary>
    public static bool IsScientificName(string? name, TaxonScientificNames taxon, NameWordSets words) {
        if (string.IsNullOrWhiteSpace(name)) {
            return false;
        }
        var normalized = ScientificNameNormalizer.Normalize(name);
        if (normalized is null) {
            return false;
        }

        // 1. One of the taxon's own names, or the first words of one.
        foreach (var own in taxon.All) {
            if (own == normalized) {
                return true;
            }
            if (own.Length > normalized.Length && own.StartsWith(normalized + " ", StringComparison.Ordinal)) {
                var isSingleWord = !normalized.Contains(' ');
                if (!isSingleWord || own == taxon.Canonical || !words.IsEnglish(normalized)) {
                    return true;
                }
            }
        }

        // 2. Shaped like a scientific name.
        var parts = name.Trim().Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length is < 2 or > 4 || !GenusWord.IsMatch(parts[0])) {
            return false;
        }
        var later = WithoutRankMarkers(parts.Skip(1));
        if (later.Count == 0 || later.Any(w => !EpithetWord.IsMatch(w))) {
            return false;
        }
        var first = parts[0].ToLowerInvariant();

        var ownGenera = new HashSet<string>(StringComparer.Ordinal);
        var ownEpithets = new HashSet<string>(StringComparer.Ordinal);
        foreach (var own in taxon.All) {
            var ownWords = WithoutRankMarkers(own.Split(' ', StringSplitOptions.RemoveEmptyEntries));
            if (ownWords.Count == 0) {
                continue;
            }
            ownGenera.Add(ownWords[0]);
            ownEpithets.UnionWith(ownWords.Skip(1));
        }

        // 3. Starts with the taxon's own genus.
        if (ownGenera.Contains(first)) {
            return true;
        }

        // 4. Any English word that is not a scientific name word in its place: a common name.
        var firstIsLatin = words.IsGenus(first);
        if (!firstIsLatin && words.IsEnglish(first)) {
            return false;
        }
        if (later.Any(w => !ownEpithets.Contains(w) && !words.IsEpithet(w) && words.IsEnglish(w))) {
            return false;
        }

        // 5. Evidence that the words are a genus and epithets.
        if (firstIsLatin || later.Any(w => words.IsEpithet(w) || (ownEpithets.Contains(w) && !ownGenera.Contains(w)))) {
            return true;
        }

        // 6. No evidence either way.
        return taxon.IsEmpty;
    }

    internal static List<string> WithoutRankMarkers(IEnumerable<string> words) =>
        words.Where(w => !RankMarkers.Contains(w)).ToList();
}
