using System;
using System.Collections.Generic;
using System.Text;

// Reduces a stored name to the name itself, dropping the authority and any nomenclatural note.
//
// IUCN synonyms are stored complete: "Eumeces schneideri (Daudin, 1802) [orth. error]",
// "Hexanchus griseus ssp. australis de Buen, 1960". Those strings are correct as records, but they
// are not what any other database holds, and they are never a Wikipedia article title: of 68,169
// candidate titles carrying an authority, 11 ever resolved to a page. Feeding them to the matcher
// filled a quarter of the download queue with titles that cannot exist.
//
// The authority is found by walking forward from the genus for as long as the tokens still look
// like parts of a name, rather than by trying to recognise author names, which vary far too much.
// Anything that does not parse as a name is returned unchanged.

namespace BeastieBot3.Taxonomy;

internal static class BareScientificName {
    // Rank markers that sit between epithets and stay part of the name.
    private static readonly HashSet<string> RankMarkers = new(StringComparer.OrdinalIgnoreCase) {
        "var.", "var", "subsp.", "subsp", "ssp.", "ssp", "spp.", "f.", "fo.", "forma", "form",
        "subf.", "subvar.", "nothosubsp.", "nothovar.", "cv.", "sect.", "ser.", "subg.",
    };

    // Lowercase particles that open a surname ("de Buen, 1960", "van der Hoeven"). Without these
    // the first word of the authority reads as an infraspecific epithet.
    private static readonly HashSet<string> AuthorParticles = new(StringComparer.Ordinal) {
        "de", "del", "della", "der", "des", "di", "do", "dos", "du", "da", "das",
        "van", "von", "vander", "ten", "ter", "den", "le", "la", "el", "bin", "ibn", "af", "av",
        "ex", "et", "in", "sensu", "auct", "auct.", "non", "nec",
    };

    /// The name without its authority or bracketed note. Returns the input trimmed when it does
    /// not parse as a scientific name, so higher taxa ("Felidae") pass through untouched.
    public static string Strip(string? raw) {
        if (string.IsNullOrWhiteSpace(raw)) {
            return string.Empty;
        }

        var text = RemoveNotesAndMarkup(raw);
        var tokens = text.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (tokens.Length == 0) {
            return string.Empty;
        }

        // Token 0 is the genus (or a higher taxon on its own). Anything else at the front, such as
        // a stray "cf." or a lowercase word, means this is not a name we can safely cut.
        if (!IsCapitalisedWord(tokens[0])) {
            return text;
        }

        var kept = new List<string> { tokens[0] };
        var index = 1;

        // "Genus (Subgenus) species": the one parenthesised group that is part of the name.
        if (index < tokens.Length && IsSubgenus(tokens[index])) {
            kept.Add(tokens[index]);
            index++;
        }

        // The specific epithet. Without one there is nothing more a name can hold.
        if (index >= tokens.Length || !IsEpithet(tokens[index])) {
            return string.Join(' ', kept);
        }
        kept.Add(tokens[index]);
        index++;

        // Below species a name holds either rank-marked epithets ("subsp. x var. y") or a single
        // unmarked one ("Ursus arctos horribilis"). Everything after that is the authority, which
        // is what stops "de Buen, 1960" being read as two more epithets.
        var rankMarked = false;
        while (index < tokens.Length) {
            var token = tokens[index];
            if (RankMarkers.Contains(token) && index + 1 < tokens.Length && IsEpithet(tokens[index + 1])) {
                kept.Add(token);
                kept.Add(tokens[index + 1]);
                index += 2;
                rankMarked = true;
                continue;
            }
            if (!rankMarked && IsEpithet(token) && !AuthorParticles.Contains(token)) {
                kept.Add(token);
                rankMarked = true;   // one unmarked infraspecific epithet, and no more
                index++;
                continue;
            }
            break;
        }

        return string.Join(' ', kept);
    }

    /// True when stripping actually removed something, i.e. the stored name carried an authority
    /// or a note. Used to report how much of a queue is names that were never article titles.
    public static bool CarriesAuthorityOrNote(string? raw) {
        if (string.IsNullOrWhiteSpace(raw)) {
            return false;
        }
        var stripped = Strip(raw);
        return stripped.Length > 0 && !string.Equals(stripped, raw.Trim(), StringComparison.Ordinal);
    }

    private static string RemoveNotesAndMarkup(string raw) {
        var sb = new StringBuilder(raw.Length);
        var depth = 0;
        foreach (var c in raw) {
            switch (c) {
                case '[':
                case '<':
                    depth++;
                    sb.Append(' ');
                    break;
                case ']':
                case '>':
                    if (depth > 0) depth--;
                    sb.Append(' ');
                    break;
                default:
                    sb.Append(depth > 0 ? ' ' : (char.IsWhiteSpace(c) ? ' ' : c));
                    break;
            }
        }
        return sb.ToString().Trim();
    }

    private static bool IsCapitalisedWord(string token) {
        if (token.Length < 2 || !char.IsUpper(token[0])) {
            return false;
        }
        foreach (var c in token) {
            if (!char.IsLetter(c) && c != '-' && c != '×') {
                return false;
            }
        }
        return true;
    }

    // "(Subgenus)" exactly: a capitalised word wrapped in parentheses.
    private static bool IsSubgenus(string token) =>
        token.Length > 2 && token[0] == '(' && token[^1] == ')' && IsCapitalisedWord(token[1..^1]);

    // A specific or infraspecific epithet: lowercase letters, optionally hyphenated, and the
    // hybrid sign. Anything with a digit, a dot or a capital is where the authority begins.
    private static bool IsEpithet(string token) {
        if (token.Length < 2) {
            return false;
        }
        if (token[0] == '×' && token.Length > 2) {
            token = token[1..];      // "×hybrida"
        }
        if (!char.IsLower(token[0])) {
            return false;
        }
        foreach (var c in token) {
            if (!char.IsLower(c) && c != '-') {
                return false;
            }
        }
        return true;
    }
}
