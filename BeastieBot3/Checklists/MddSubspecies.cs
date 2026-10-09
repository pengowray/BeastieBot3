using System.Text.RegularExpressions;
using BeastieBot3.Shared.SiteData;

// The `subspecies` column of the Mammal Diversity Database's species file (1,429 of 6,904 species in
// v2.5): the subspecies separated by ";", each written with the genus and species abbreviated and
// the whole name in underscores, then its authority, then brackets with a note and the subspecies'
// synonyms:
//   _T. a. acanthion_ (Collett, 1885) (synonyms: _ineptus_ Thomas, 1906); _T. a. lawesii_ Ramsay, 1877
//   _S. h. dixonae_ Werdelin, 1987 (fossil)
//   _O. r. altus_ (Owen, 1874) (fossil; synonyms: _birdselli_ (Tedford, 1967), _cooperi_ Owen, 1874)
// The notes are "fossil", "recently extinct" and "holocene". The synonyms are left out. An entry that
// does not have this form, whose initials are not the species', or that has other bracketed text
// after the authority is not read and is counted.

namespace BeastieBot3.Checklists;

internal static partial class MddSubspecies {
    public const string Fossil = "fossil";
    public const string RecentlyExtinct = "recently extinct";
    public const string Holocene = "holocene";
    private static readonly string[] NoteWords = [Fossil, RecentlyExtinct, Holocene];

    /// The subspecies of species ("Tachyglossus aculeatus") in the text of its subspecies column, and
    /// the number of entries not read. "NA" or an empty text has none.
    public static (List<ChecklistInfraspecific> Subspecies, int NotRead) Parse(string species, string? text) {
        var subspecies = new List<ChecklistInfraspecific>();
        if (string.IsNullOrWhiteSpace(text) || text.Trim() == "NA") {
            return (subspecies, 0);
        }
        var entries = SplitOutsideBrackets(text);
        var words = species.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (words.Length != 2) {
            return (subspecies, entries.Count);
        }
        var notRead = 0;
        foreach (var entry in entries) {
            if (Read(words[0], words[1], entry) is { } s) {
                subspecies.Add(s);
            } else {
                notRead++;
            }
        }
        return (subspecies, notRead);
    }

    private static ChecklistInfraspecific? Read(string genus, string epithet, string entry) {
        var m = Entry().Match(entry);
        if (!m.Success || m.Groups["g"].Value[0] != genus[0] || m.Groups["s"].Value[0] != epithet[0]) {
            return null;
        }
        var rest = m.Groups["rest"].Value.Trim();
        string? note = null;
        // Take off the bracketed notes and synonym lists at the end; what is left is the authority.
        while (rest.EndsWith(')') && OpeningOfLastBracket(rest) is { } open) {
            var inner = rest[(open + 1)..^1].Trim();
            if (!IsNoteOrSynonyms(inner, out var word)) {
                break;
            }
            if (word is not null) {
                if (note is not null) {
                    return null;
                }
                note = word;
            }
            rest = rest[..open].TrimEnd();
        }
        if (rest.Contains('_') || !Authority().IsMatch(rest)) {
            return null;
        }
        return new ChecklistInfraspecific($"{genus} {epithet}", $"{genus} {epithet} {m.Groups["e"].Value}", InfraspecificNames.Subspecies,
            rest.Length == 0 ? null : rest, note);
    }

    // "synonyms: ...", "fossil", "recently extinct; synonyms: ...": word is the note, or null for a
    // synonym list.
    private static bool IsNoteOrSynonyms(string inner, out string? word) {
        word = null;
        if (inner.StartsWith("synonyms:", StringComparison.Ordinal)) {
            return true;
        }
        var semicolon = inner.IndexOf(';');
        var head = (semicolon < 0 ? inner : inner[..semicolon]).Trim();
        var tail = semicolon < 0 ? "" : inner[(semicolon + 1)..].Trim();
        if (tail.Length > 0 && !tail.StartsWith("synonyms:", StringComparison.Ordinal)) {
            return false;
        }
        word = NoteWords.FirstOrDefault(n => string.Equals(n, head, StringComparison.OrdinalIgnoreCase));
        return word is not null;
    }

    // The index of the bracket that the final ")" closes; null when the brackets do not balance.
    private static int? OpeningOfLastBracket(string text) {
        var depth = 0;
        for (var i = text.Length - 1; i >= 0; i--) {
            if (text[i] == ')') {
                depth++;
            } else if (text[i] == '(' && --depth == 0) {
                return i;
            }
        }
        return null;
    }

    // Splits at ";" outside brackets: a note's synonym list has its own ";".
    internal static List<string> SplitOutsideBrackets(string text) {
        var parts = new List<string>();
        var depth = 0;
        var start = 0;
        for (var i = 0; i < text.Length; i++) {
            switch (text[i]) {
                case '(' or '[':
                    depth++;
                    break;
                case ')' or ']':
                    depth--;
                    break;
                case ';' when depth == 0:
                    parts.Add(text[start..i]);
                    start = i + 1;
                    break;
            }
        }
        parts.Add(text[start..]);
        return [.. parts.Select(p => p.Trim()).Where(p => p.Length > 0)];
    }

    // "_T. a. acanthion_ rest": the initials of genus and species, the epithet (lower-case letters
    // and hyphens) and what follows.
    [GeneratedRegex(@"^_(?<g>\p{Lu})\.\s*(?<s>\p{Ll})\.\s*(?<e>\p{Ll}[\p{Ll}\p{Mn}-]*)_\s*(?<rest>.*)$")]
    private static partial Regex Entry();

    // An authority with or without brackets ("(Collett, 1885)", "Thomas & Rothschild, 1922"), or none.
    [GeneratedRegex(@"^(?:\([^()]+\)|[^()]+)?$")]
    private static partial Regex Authority();
}
