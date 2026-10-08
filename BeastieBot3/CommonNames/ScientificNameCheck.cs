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
/// Life, with how many names use each. All lower case.
/// </summary>
internal sealed class NameWordSets {
    /// <summary>
    /// A word counts as English when at least this many different English common names use it.
    /// CoL's English rows include a few scientific names ("Gobio gobio"); a word that only one or two
    /// names use is more likely to come from one of those than to be English.
    /// </summary>
    internal const int MinCommonNamesForEnglishWord = 3;

    /// <summary>
    /// A word that is also an epithet somewhere in the store counts as English only when at least
    /// this many English common names use it. Many epithets come from vernacular names ("gecko",
    /// "chub", "tetra", "gazelle": hundreds of names each), while an epithet that appears in a few
    /// English rows is usually part of a scientific name in them ("montana": 7, "elegans": 6).
    /// </summary>
    internal const int MinCommonNamesForEnglishEpithet = 10;

    public static readonly NameWordSets Empty = new(new HashSet<string>(), new HashSet<string>(), new Dictionary<string, int>());

    private readonly HashSet<string> _genera;
    private readonly HashSet<string> _epithets;
    private readonly Dictionary<string, int> _namesUsingWord;

    /// <param name="namesUsingWord">For each word, how many English common names use it.</param>
    public NameWordSets(HashSet<string> genera, HashSet<string> epithets, Dictionary<string, int> namesUsingWord) {
        _genera = genera;
        _epithets = epithets;
        _namesUsingWord = namesUsingWord;
    }

    public bool IsGenus(string word) => _genera.Contains(word);
    public bool IsEpithet(string word) => _epithets.Contains(word);

    /// <summary>
    /// True when the word is an English word: used in at least
    /// <see cref="MinCommonNamesForEnglishWord"/> English names, or hyphenated with every part an
    /// English word ("blue-eye"; not "walter-tillii", an epithet named after a person).
    /// </summary>
    public bool IsEnglish(string word) => IsEnglish(word, MinCommonNamesForEnglishWord);

    /// <summary>
    /// <see cref="IsEnglish(string)"/> with the higher bar for a word that is an epithet in the store
    /// (<see cref="MinCommonNamesForEnglishEpithet"/>); a word that is no epithet uses the lower bar.
    /// </summary>
    public bool IsEnglishRatherThanEpithet(string word) =>
        IsEnglish(word, IsEpithet(word) ? MinCommonNamesForEnglishEpithet : MinCommonNamesForEnglishWord);

    /// <summary>
    /// <see cref="IsEnglish(string)"/> with the higher bar for a word that is a genus or an epithet in
    /// the store (<see cref="MinCommonNamesForEnglishEpithet"/>), for a Wikipedia title that is a
    /// genus name: "gorilla" (20 names) and "caracara" (25) are English, "drepana" (none) and "rana"
    /// (8) are not.
    /// </summary>
    public bool IsEnglishRatherThanScientific(string word) =>
        IsEnglish(word, IsGenus(word) || IsEpithet(word) ? MinCommonNamesForEnglishEpithet : MinCommonNamesForEnglishWord);

    private bool IsEnglish(string word, int minNames) =>
        _namesUsingWord.GetValueOrDefault(word) >= minNames
        || (word.Contains('-') && word.Split('-').All(part => _namesUsingWord.GetValueOrDefault(part) >= minNames));

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

        return new NameWordSets(genera, epithets, namesUsingWord);
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
    /// <item>The name is one of the taxon's names, one of them followed by an authority or a note
    /// ("Myristica fatua Sw."), or the first word or words of one (the genus of a monotypic genus,
    /// the species of a subspecies), ignoring case, rank markers and a subgenus. A
    /// genus taken from a synonym is not counted when it is an English word ("Orca" for Orcinus
    /// orca).</item>
    /// <item>Otherwise only a name shaped like a scientific name can be one: two to four words, the
    /// first a capitalised word of plain letters, the rest lower case. A single word is a scientific
    /// name when it is a genus in the store and not an English word ("Strumigenys" for Kyidris
    /// media; not "Platypus"). Anything else is a common name. Double quotes around a genus
    /// ("\"Hyla\" nicefori"), a hybrid marker ("Yucca × schottii", <see cref="WithoutHybridMarker"/>)
    /// and a period inside an epithet ("Cyanea st.-johnii") are ignored throughout.</item>
    /// <item>The first word is one of the taxon's genera ("Gobio gobio" for Gobio latus): a scientific
    /// name.</item>
    /// <item>A word is an English word ("Pygmy hippopotamus", "Alligator gar"): a common name. A
    /// first word that is a genus in the store is not counted as English, nor is a later word that is
    /// one of the taxon's epithets; a later word that is an epithet elsewhere in the store must be
    /// used in more English names to count (<see cref="NameWordSets.MinCommonNamesForEnglishEpithet"/>:
    /// "gazelle" in "Dorcas gazelle" counts, "montana" in "Aiouea montana" does not).</item>
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
        // Wikipedia puts a genus in double quotes when the species no longer belongs in it
        // ("\"Hyla\" nicefori").
        // Some Wikidata labels have a no-break space between the words ("Lycodon cathaya").
        name = WithoutHybridMarker(name.Replace("\"", "").Replace(' ', ' '));
        name = PeriodInEpithet.Replace(name, "");
        var normalized = ScientificNameNormalizer.Normalize(name);
        if (normalized is null) {
            return false;
        }

        // 1. One of the taxon's own names, the first words of one, or one followed by an authority
        // or a note ("Myristica fatua Sw.", "Andrena pilipes s.s.").
        foreach (var own in taxon.All) {
            if (own == normalized || normalized.StartsWith(own + " ", StringComparison.Ordinal)) {
                return true;
            }
            if (own.Length > normalized.Length && own.StartsWith(normalized + " ", StringComparison.Ordinal)) {
                var isSingleWord = !normalized.Contains(' ');
                if (!isSingleWord || own == taxon.Canonical || !words.IsEnglish(normalized)) {
                    return true;
                }
            }
        }

        // 2. Shaped like a scientific name. One word is a genus name when the store has that genus
        // and English names do not use the word ("Strumigenys", but not "Platypus" or "Orca").
        var parts = name.Trim().Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length == 1) {
            var word = parts[0].ToLowerInvariant();
            return GenusWord.IsMatch(parts[0]) && words.IsGenus(word) && !words.IsEnglish(word);
        }
        if (parts.Length > 4 || !GenusWord.IsMatch(parts[0])) {
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
        if (later.Any(w => !ownEpithets.Contains(w) && words.IsEnglishRatherThanEpithet(w))) {
            return false;
        }

        // 5. Evidence that the words are a genus and epithets.
        if (firstIsLatin || later.Any(w => words.IsEpithet(w) || (ownEpithets.Contains(w) && !ownGenera.Contains(w)))) {
            return true;
        }

        // 6. No evidence either way.
        return taxon.IsEmpty;
    }

    /// <summary>
    /// The name without a hybrid marker: the hybrid sign × anywhere ("Yucca × schottii",
    /// "×Chitalpa tashkentensis"), and an "x" written for it between a genus and an epithet
    /// ("Yucca x schottii"). An "x" anywhere else is kept, so a common name for a hybrid such as
    /// "Eurasian Teal x Green-winged Teal" is unchanged.
    /// </summary>
    internal static string WithoutHybridMarker(string name) =>
        AsciiHybridMarker.Replace(HybridSign.Replace(name, " ").Trim(), "$1 ");

    private static readonly Regex HybridSign = new(@"\s*×\s*", RegexOptions.Compiled);
    private static readonly Regex AsciiHybridMarker = new(@"^([A-Z][a-z]+) x (?=[a-z])", RegexOptions.Compiled);

    // A period inside an epithet, before a hyphen: Wikipedia titles the page for Cyanea st-johnii
    // "Cyanea st.-johnii".
    private static readonly Regex PeriodInEpithet = new(@"(?<=\b[a-z]+)\.(?=-[a-z])", RegexOptions.Compiled);

    internal static List<string> WithoutRankMarkers(IEnumerable<string> words) =>
        words.Where(w => !RankMarkers.Contains(w)).ToList();
}
