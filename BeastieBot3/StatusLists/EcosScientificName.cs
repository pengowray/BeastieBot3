using System.Text.RegularExpressions;

// Reads the brackets in an ECOS scientific name. ECOS writes the names as they appear in the listing
// documents, with earlier names in brackets after the word they replace:
//
//   Papasula (=Sula) abbotti                    Papasula abbotti, Sula abbotti
//   Harrisia (=Cereus) aboriginum (=gracilis)   Harrisia aboriginum, Cereus aboriginum, Harrisia gracilis, Cereus gracilis
//   Pediocactus (=Echinocactus,=Utahia) sileri  Pediocactus sileri, Echinocactus sileri, Utahia sileri
//   Hemileuca maia menyanthevora (=H. iroquois) Hemileuca maia menyanthevora, Hemileuca iroquois
//   Andrias japonicus (=davidianus j.)          Andrias japonicus, Andrias davidianus japonicus
//   Icaricia (Plebejus) shasta charlestonensis  Icaricia shasta charlestonensis, Plebejus shasta charlestonensis
//
// A bracket that starts with "=" gives one or more earlier names, separated by commas:
//   - one word replaces the word before the bracket (a genus or an epithet);
//   - several words starting with a capital letter are a whole name; a one-letter abbreviation in it
//     ("H.", "E. e.", "L.g.") stands for the word of the main name in the same place, else for the
//     word before the bracket, else for the first word of the main name with that initial;
//   - several words starting with a small letter replace the word before the bracket, and a
//     one-letter abbreviation in them stands for that word ("(=megaspila c.)" after civettina).
// A capitalised word in brackets right after the genus, without "=", is a subgenus or another genus
// (Icaricia (Plebejus)). Every other bracket is a note ("entire genus", "incl. D. cascus",
// "all subsp. except coryi") and gives no name. Names from different brackets are combined, except
// whole names, which stand alone. Rank words (ssp., var.) are kept as written.

namespace BeastieBot3.StatusLists;

internal sealed record EcosNameParts(string Name, IReadOnlyList<string> Names, string? Note);

internal static class EcosScientificName {
    // At most this many names are returned for one listing (the most in October 2026 is 4).
    private const int MaxNames = 16;

    private static readonly Regex Abbreviation = new(@"^(?:[A-Za-z]\.)+$", RegexOptions.CultureInvariant);
    private static readonly Regex GenusWord = new(@"^[A-Z][a-z]+(?:-[a-z]+)?$", RegexOptions.CultureInvariant);

    public static EcosNameParts Parse(string raw) {
        var (words, brackets) = Split(raw ?? "");
        var name = string.Join(' ', words);
        if (words.Count == 0) {
            return new EcosNameParts(name, Array.Empty<string>(), null);
        }

        // For each bracket with names: the replacements it offers for one word of the main name.
        var choices = new List<(int Index, List<List<string>> Replacements)>();
        var wholeNames = new List<string>();
        var notes = new List<string>();
        foreach (var (index, content) in brackets) {
            if (content.StartsWith('=')) {
                var replacements = new List<List<string>>();
                foreach (var part in content.Split(',')) {
                    var alternative = part.Trim().TrimStart('=').Trim();
                    if (alternative.Length == 0 || index < 0) {
                        continue;
                    }
                    var tokens = Tokens(alternative);
                    if (tokens.Count == 1 && !Abbreviation.IsMatch(tokens[0])) {
                        replacements.Add(tokens);
                    } else if (char.IsUpper(tokens[0][0])) {
                        if (WholeName(tokens, words, index) is { } whole) {
                            wholeNames.Add(whole);
                        }
                    } else {
                        replacements.Add(tokens.Select(t => Abbreviation.IsMatch(t) ? words[index] : t).ToList());
                    }
                }
                if (replacements.Count > 0) {
                    choices.Add((index, replacements));
                }
            } else if (index == 0 && GenusWord.IsMatch(content)) {
                choices.Add((0, new List<List<string>> { new() { content } }));
            } else if (content.Length > 0) {
                notes.Add(content);
            }
        }

        var names = new List<string> { name };
        foreach (var combination in Combinations(choices)) {
            if (names.Count >= MaxNames) {
                break;
            }
            var result = new List<string>(words);
            // From the last word back, so a replacement of several words leaves earlier indexes as they are.
            foreach (var (index, replacement) in combination.OrderByDescending(c => c.Index)) {
                result.RemoveAt(index);
                result.InsertRange(index, replacement);
            }
            Add(names, string.Join(' ', result));
        }
        foreach (var whole in wholeNames) {
            if (names.Count >= MaxNames) {
                break;
            }
            Add(names, whole);
        }
        return new EcosNameParts(name, names, notes.Count == 0 ? null : string.Join("; ", notes));
    }

    private static void Add(List<string> names, string name) {
        if (!names.Contains(name, StringComparer.Ordinal)) {
            names.Add(name);
        }
    }

    // Every way of taking at most one replacement from each bracket, the main name (no replacement
    // at all) left out.
    private static IEnumerable<List<(int Index, List<string> Replacement)>> Combinations(
        IReadOnlyList<(int Index, List<List<string>> Replacements)> choices) {
        var picks = new List<List<(int Index, List<string> Replacement)>> { new() };
        foreach (var (index, replacements) in choices) {
            var next = new List<List<(int Index, List<string> Replacement)>>();
            foreach (var pick in picks) {
                next.Add(pick);
                foreach (var replacement in replacements) {
                    next.Add(new List<(int, List<string>)>(pick) { (index, replacement) });
                }
            }
            picks = next;
        }
        // One replacement before two, and the genus's before the epithet's.
        return picks.Where(p => p.Count > 0).OrderBy(p => p.Count).ThenBy(p => p.Min(c => c.Index));
    }

    // A whole earlier name such as "H. iroquois", "E. e. wrighti" or "Meliphaga c.", with its
    // abbreviations written out. Null when an abbreviation fits no word of the main name.
    private static string? WholeName(IReadOnlyList<string> tokens, IReadOnlyList<string> words, int bracketIndex) {
        var expanded = new List<string>();
        for (var i = 0; i < tokens.Count; i++) {
            var token = tokens[i];
            if (!Abbreviation.IsMatch(token)) {
                expanded.Add(token);
                continue;
            }
            var initial = char.ToLowerInvariant(token[0]);
            bool Fits(string word) => word.Length > 1 && char.ToLowerInvariant(word[0]) == initial;
            string? word = i < words.Count && Fits(words[i]) ? words[i]
                : Fits(words[bracketIndex]) ? words[bracketIndex]
                : words.FirstOrDefault(Fits);
            if (word is null) {
                return null;
            }
            expanded.Add(word);
        }
        return string.Join(' ', expanded);
    }

    // The words of an earlier name; "L.g." is two abbreviations, "L." and "g.".
    private static List<string> Tokens(string text) {
        var tokens = new List<string>();
        foreach (var token in text.Split(' ', StringSplitOptions.RemoveEmptyEntries)) {
            if (Abbreviation.IsMatch(token) && token.Length > 2) {
                for (var i = 0; i + 1 < token.Length; i += 2) {
                    tokens.Add(token.Substring(i, 2));
                }
            } else {
                tokens.Add(token);
            }
        }
        return tokens;
    }

    // The words outside brackets, and each bracket's text with the index of the word before it
    // (-1 when the name starts with a bracket). A bracket left open runs to the end of the name.
    private static (List<string> Words, List<(int Index, string Content)> Brackets) Split(string raw) {
        var words = new List<string>();
        var brackets = new List<(int, string)>();
        var i = 0;
        while (i < raw.Length) {
            var c = raw[i];
            if (char.IsWhiteSpace(c)) {
                i++;
            } else if (c == '(') {
                var depth = 1;
                var start = ++i;
                while (i < raw.Length && depth > 0) {
                    if (raw[i] == '(') depth++;
                    else if (raw[i] == ')') depth--;
                    i++;
                }
                var end = depth == 0 ? i - 1 : raw.Length;
                brackets.Add((words.Count - 1, Regex.Replace(raw[start..end], @"\s+", " ").Trim()));
            } else {
                var start = i;
                while (i < raw.Length && !char.IsWhiteSpace(raw[i]) && raw[i] != '(') {
                    i++;
                }
                var word = raw[start..i].TrimEnd(')');
                if (word.Length > 0) {
                    words.Add(word);
                }
            }
        }
        return (words, brackets);
    }
}
