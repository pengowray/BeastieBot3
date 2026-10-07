using BeastieBot3.Shared.SiteData;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Site.Data;

/// The words of the site's names (name_word), in memory, for spelling suggestions when a search
/// finds nothing: about 300,000 words, grouped by length and sorted, so a word is found by binary
/// search and similar words are looked for only among words of about the same length.
public sealed class NameWordIndex {
    private readonly Dictionary<int, (string[] Words, int[] Uses)> _byLength;

    public NameWordIndex(IEnumerable<(string Word, int Uses)> words) {
        _byLength = words
            .GroupBy(w => w.Word.Length)
            .ToDictionary(g => g.Key, g => {
                var sorted = g.OrderBy(w => w.Word, StringComparer.Ordinal).ToArray();
                return (sorted.Select(w => w.Word).ToArray(), sorted.Select(w => w.Uses).ToArray());
            });
        Count = _byLength.Values.Sum(v => v.Words.Length);
    }

    public int Count { get; }

    public static NameWordIndex Load(SqliteConnection connection) {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT word, uses FROM name_word";
        using var reader = command.ExecuteReader();
        var words = new List<(string, int)>();
        while (reader.Read()) {
            words.Add((reader.GetString(0), reader.GetInt32(1)));
        }
        return new NameWordIndex(words);
    }

    /// How many names have the word; 0 when none has it.
    public int Uses(string word) {
        if (!_byLength.TryGetValue(word.Length, out var bucket)) {
            return 0;
        }
        var i = Array.BinarySearch(bucket.Words, word, StringComparer.Ordinal);
        return i >= 0 ? bucket.Uses[i] : 0;
    }

    /// The most letters a word of this length may differ by from a suggestion: 1 up to 4 letters, else 2.
    public static int MaxDistance(int length) => length <= 4 ? 1 : 2;

    /// The words within MaxDistance of the word (a letter added, removed, changed, or two
    /// neighbouring letters swapped, each counting 1), closest first, then the most used first.
    public IReadOnlyList<(string Word, int Distance, int Uses)> Similar(string word, int limit) {
        var max = MaxDistance(word.Length);
        var found = new List<(string Word, int Distance, int Uses)>();
        for (var length = Math.Max(NameWords.MinLength, word.Length - max); length <= word.Length + max; length++) {
            if (!_byLength.TryGetValue(length, out var bucket)) {
                continue;
            }
            for (var i = 0; i < bucket.Words.Length; i++) {
                var distance = Distance(word, bucket.Words[i], max);
                if (distance > 0 && distance <= max) {
                    found.Add((bucket.Words[i], distance, bucket.Uses[i]));
                }
            }
        }
        return found.OrderBy(f => f.Distance).ThenByDescending(f => f.Uses).ThenBy(f => f.Word, StringComparer.Ordinal).Take(limit).ToList();
    }

    /// Optimal string alignment distance, or max + 1 as soon as it must be more than max.
    internal static int Distance(string a, string b, int max) {
        if (Math.Abs(a.Length - b.Length) > max) {
            return max + 1;
        }
        var previous2 = new int[b.Length + 1];
        var previous = new int[b.Length + 1];
        var current = new int[b.Length + 1];
        for (var j = 0; j <= b.Length; j++) {
            previous[j] = j;
        }
        for (var i = 1; i <= a.Length; i++) {
            current[0] = i;
            var rowMin = current[0];
            for (var j = 1; j <= b.Length; j++) {
                var cost = a[i - 1] == b[j - 1] ? 0 : 1;
                var d = Math.Min(Math.Min(previous[j] + 1, current[j - 1] + 1), previous[j - 1] + cost);
                if (i > 1 && j > 1 && a[i - 1] == b[j - 2] && a[i - 2] == b[j - 1]) {
                    d = Math.Min(d, previous2[j - 2] + 1);
                }
                current[j] = d;
                rowMin = Math.Min(rowMin, d);
            }
            if (rowMin > max) {
                return max + 1;
            }
            (previous2, previous, current) = (previous, current, previous2);
        }
        return previous[b.Length];
    }
}

/// Search texts like one that found nothing, with misspelled words replaced by similar words of
/// the site's names. The caller keeps the ones that find something.
public static class SpellingSuggestions {
    /// Similar words tried for each misspelled word.
    public const int WordsPerWord = 3;

    /// Candidate search texts, most likely first: fewest letters changed, then the most used words.
    /// Empty when every word is already a word of the site's names, or a misspelled word has no
    /// similar word.
    public static IReadOnlyList<string> Candidates(string text, NameWordIndex index, int limit) {
        var folded = SiteNameKey.Fold(text);
        var options = new List<(int Start, int Length, IReadOnlyList<(string Word, int Distance, int Uses)> Words)>();
        foreach (var (start, length) in NameWords.Find(folded)) {
            var word = folded.Substring(start, length);
            if (index.Uses(word) > 0) {
                continue;
            }
            var similar = index.Similar(word, WordsPerWord);
            if (similar.Count == 0) {
                return [];
            }
            options.Add((start, length, similar));
        }
        if (options.Count == 0) {
            return [];
        }
        // Every combination of the similar words (3 misspelled words: 27 at most).
        IEnumerable<(string Text, int Distance, double Weight)> combinations = [(folded, 0, 0.0)];
        foreach (var option in Enumerable.Reverse(options)) {
            var o = option;
            combinations = combinations.SelectMany(c => o.Words.Select(w => (
                c.Text[..o.Start] + w.Word + c.Text[(o.Start + o.Length)..],
                c.Distance + w.Distance,
                c.Weight + Math.Log(1 + w.Uses))));
        }
        return combinations
            .OrderBy(c => c.Distance).ThenByDescending(c => c.Weight).ThenBy(c => c.Text, StringComparer.Ordinal)
            .Select(c => c.Text).Distinct(StringComparer.Ordinal).Take(limit).ToList();
    }
}
