// Repairs author names whose non-ASCII letters were lost when IUCN's data passed through the wrong
// character encoding: "Kry\uFFFDtufek, B." (U+FFFD, the replacement character) and "Kry?tufek, B.",
// "U?ur Kaya", "Yusuf Kumluta?" (a question mark in place of the letter). CS1 shows U+FFFD as a red
// "replacement character" error, and both forms misspell the name.
//
// The repair comes from the data, not from a list: the same person is credited correctly in other
// assessments ("Kryštufek, B." 329 times). A damaged character matches exactly one character,
// and only a letter outside ASCII, because an encoding failure only ever loses those; this keeps
// "U?ur Kaya" from matching "Ugur Kaya", which IUCN also has. The name is repaired when exactly one
// undamaged assessor-credit name matches; with none or several it stays as it is, and the parse
// reports it so `site check-citations` can list it.
//
// The site build fills the pool from parsed citations with AddFrom, an extension method in
// IucnCitationPartsParser.cs, because it reads the site build's IucnCitationParse.

namespace BeastieBot3.Iucn.Citations;

/// An author name with a damaged character, and the name it was repaired to.
internal sealed record AuthorNameRepair(string From, string To);

/// Every undamaged author name read from an assessor credit, for repairing damaged ones.
internal sealed class AssessorNamePool {
    private const char Replacement = '\uFFFD';

    private readonly Dictionary<int, HashSet<string>> _byLength = new();
    private readonly Dictionary<string, string?> _repairs = new(StringComparer.Ordinal);

    public int Count { get; private set; }

    /// True when the name has U+FFFD, or a "?" next to a letter.
    public static bool IsDamaged(string name) {
        for (var i = 0; i < name.Length; i++) {
            if (IsDamagedAt(name, i)) {
                return true;
            }
        }
        return false;
    }

    private static bool IsDamagedAt(string name, int i) =>
        name[i] == Replacement
        || (name[i] == '?' && ((i > 0 && char.IsLetter(name[i - 1])) || (i + 1 < name.Length && char.IsLetter(name[i + 1]))));

    /// Adds a name as the parser displays it; damaged names are left out.
    public void Add(string name) {
        if (name.Length == 0 || IsDamaged(name)) {
            return;
        }
        if (!_byLength.TryGetValue(name.Length, out var names)) {
            _byLength[name.Length] = names = new HashSet<string>(StringComparer.Ordinal);
        }
        if (names.Add(name)) {
            Count++;
            _repairs.Clear();
        }
    }

    /// The one undamaged name the damaged name can be, or null when there is none or more than one.
    public string? Repair(string damaged) {
        if (_repairs.TryGetValue(damaged, out var cached)) {
            return cached;
        }
        string? found = null;
        var matches = 0;
        if (_byLength.TryGetValue(damaged.Length, out var candidates)) {
            foreach (var candidate in candidates) {
                if (!Fits(damaged, candidate)) {
                    continue;
                }
                found = candidate;
                if (++matches > 1) {
                    break;
                }
            }
        }
        var result = matches == 1 ? found : null;
        _repairs[damaged] = result;
        return result;
    }

    private static bool Fits(string damaged, string candidate) {
        for (var i = 0; i < damaged.Length; i++) {
            if (IsDamagedAt(damaged, i)) {
                if (candidate[i] <= '\u007F' || !char.IsLetter(candidate[i])) {
                    return false;
                }
            } else if (candidate[i] != damaged[i]) {
                return false;
            }
        }
        return true;
    }
}
