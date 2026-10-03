using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using BeastieBot3.Shared.SiteData;
using BeastieBot3.Shared.Wikitext;

// Full given names for the authors of an IUCN citation, from the assessor credit's value[] list, so
// {{cite iucn}} can name "Sayer, Catherine" where the citation prints "Sayer, C." (the public site's
// "full given names" option; IucnCitationPartsParser calls Match for each author).
//
// value[] lists each credited person once, given name first, often with an affiliation:
//   "full": "Sayer, C. & Lajus, D."   "value": ["Catherine Sayer (IUCN Red List Unit)", "Dmitry Lajus"]
// in any order, mixed with email-only entries ("false.email@globaltrees.org"), postal addresses and
// organisations. A name is used only when the match is certain:
//
//   - The author is a person IUCN wrote with initials: "Sayer, C.", "Paulson, D.R.", "Samain, M.-S.",
//     "DoNascimiento, CD", "Lowry II, P.P.", "Tamanyan K.", "N.H. Rakotoarivelo". Not an organisation,
//     not a name kept as published, and not "Mohd Yusof, Nur Adillah", which has its given names.
//   - The entry, with every trailing note in brackets removed and then a trailing Jr., Sr., II, III or
//     IV, ends with the author's surname as a whole word, compared without case or accents ("Chris van
//     Swaay" for "van Swaay, C."). What comes before the surname is the given part. A trailing
//     particle there ("Rogier de" from "Rogier de Kok" for "Kok, R.") belongs to the surname as
//     value[] writes it and is dropped.
//   - The given part has no digit, "@", bracket, comma, semicolon, slash or "&", no letter lost to
//     an encoding error ("Jos? Ralison"), no organisation word and no name starting with a small
//     letter ("kelly Hare"), and is not only initials ("D.R. Paulson").
//   - Each published initial, in order, is the start of the next given name ("J.-P." and "J.P." both
//     fit "Jean-Pierre"; "Th." fits "Thomas"). Particles among the initials ("C. de C.") are skipped
//     on both sides. People who go by a middle name fail here: "Liddle, T.A." is "Adam Liddle".
//   - Exactly one entry fits, counting entries that differ only in their notes as one. "Alemu, S."
//     twice, with entries for Shambel Alemu and Sisay Alemu, fits both, so neither gets a name.
//
// When the counts differ:
//   - value[] gives fewer given names than there are initials ("Dennis Paulson" for "Paulson, D.R."):
//     the initials left over are kept as IUCN printed them, giving "Dennis R." ("Alejandra C.D." for
//     "Fuentes, A.C.D.", "Cristiano de C." for "Nogueira, C. de C."). Both parts come from IUCN, and
//     each given name value[] does give has been checked against its initial.
//   - value[] gives more given names than initials ("Catherine Anne Sayer" for "Sayer, C."): only
//     the given names the initials stand for are kept ("Catherine"), since IUCN's citation names the
//     person by those. A hyphenated given name is kept whole ("Jean-Pierre" for "J.").
//
// GivenNames keeps a middle initial value[] gives ("Susana C." for "Gonçalves, S.C.") and is
// otherwise written as value[] writes it. It never holds a generational suffix: CiteIucnRenderer
// adds the suffix from Initials.

namespace BeastieBot3.Iucn.Citations;

/// The result of looking for one author's given names in value[].
internal enum GivenNameOutcome {
    /// One given name for each initial.
    Matched,
    /// value[] gives fewer given names than initials: the rest of the initials kept ("Dennis R.").
    MatchedFewerGivenNames,
    /// value[] gives more given names than initials: only the matched ones kept.
    MatchedMoreGivenNames,
    /// Not a person written with initials: an organisation, a name kept as published, or a person
    /// whose given names IUCN already printed.
    NotInitials,
    /// The credit has no usable value[] entries (none, or only email addresses).
    NoValueList,
    /// No entry ends with the author's surname.
    NoSurnameMatch,
    /// An entry ends with the surname, but its given names don't fit the initials.
    InitialsDisagree,
    /// Two different entries fit.
    Ambiguous,
    /// The entry with the surname gives only initials.
    EntryInitialsOnly,
    /// The entry with the surname has digits, "@", brackets, a comma, a lost letter ("?" or U+FFFD),
    /// an organisation word or a name starting with a small letter in its given part (or a
    /// semicolon, slash or "&").
    EntryNotAName,
}

/// One value[] entry with its notes in brackets removed. Raw is the entry as IUCN wrote it.
internal sealed record ValueName(string Raw, string Name);

internal readonly record struct GivenNameMatch(GivenNameOutcome Outcome, string? GivenNames, string? Entry) {
    public bool IsMatch => Outcome is GivenNameOutcome.Matched or GivenNameOutcome.MatchedFewerGivenNames or GivenNameOutcome.MatchedMoreGivenNames;
}

internal static partial class AssessorGivenNames {
    // Lower-case only: capitalised "Das", "Van" are names in their own right.
    private static readonly HashSet<string> Particles = new(StringComparer.Ordinal) {
        "de", "da", "das", "do", "dos", "du", "van", "von", "der", "den", "la", "las", "le", "les", "lo", "los",
        "di", "del", "della", "bin", "binti", "al", "el", "y", "e", "ter", "ten", "zu", "dela",
    };

    /// The entries of every assessor credit's value[], cleaned, distinct, email-only entries left out.
    public static IReadOnlyList<ValueName> ReadValues(IEnumerable<JsonElement> credits) {
        var list = new List<ValueName>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var credit in credits) {
            if (!credit.TryGetProperty("value", out var value) || value.ValueKind != JsonValueKind.Array) continue;
            foreach (var entry in value.EnumerateArray()) {
                if (entry.ValueKind != JsonValueKind.String) continue;
                if (Clean(entry.GetString()) is not { } cleaned || !seen.Add(cleaned.Name)) continue;
                list.Add(cleaned);
            }
        }
        return list;
    }

    /// The entry with HTML entities decoded, whitespace collapsed and trailing notes in brackets
    /// removed; null for an empty entry or an email address.
    internal static ValueName? Clean(string? raw) {
        if (string.IsNullOrWhiteSpace(raw)) return null;
        var text = Whitespace().Replace(WebUtility.HtmlDecode(raw), " ").Trim().Normalize(NormalizationForm.FormC);
        if (EmailOnly().IsMatch(text)) return null;
        var name = StripTrailingNotes(text);
        return name.Length == 0 ? null : new ValueName(text, name);
    }

    /// Looks for the author's given names among the entries; see the file comment.
    public static GivenNameMatch Match(CitationAuthor author, AuthorNameShape shape, IReadOnlyList<ValueName> entries) {
        if (author.Kind != CitationAuthorKind.Person
            || shape is not (AuthorNameShape.SurnameInitials or AuthorNameShape.SurnameInitialsNoDots
                or AuthorNameShape.SurnameInitialsSuffix or AuthorNameShape.CompactSurnameFirst or AuthorNameShape.CompactInitialsFirst)
            || string.IsNullOrWhiteSpace(author.Last) || string.IsNullOrWhiteSpace(author.Initials)) {
            return new GivenNameMatch(GivenNameOutcome.NotInitials, null, null);
        }
        if (entries.Count == 0) return new GivenNameMatch(GivenNameOutcome.NoValueList, null, null);

        var initialsText = WithoutSuffix(author.Initials.Normalize(NormalizationForm.FormC));
        var initials = ReadInitials(initialsText);
        if (initials.Count == 0) return new GivenNameMatch(GivenNameOutcome.NotInitials, null, null);
        var surname = FoldName(Whitespace().Replace(author.Last, " ").Trim().Normalize(NormalizationForm.FormC));

        GivenNameMatch? found = null;
        // Entries that fit, by name without notes: one person listed twice with different notes
        // ("Catherine Sayer (IUCN)", "Catherine Sayer (Red List Unit)") is one fit.
        var fits = new HashSet<string>(StringComparer.Ordinal);
        // The most telling failure, when nothing fits: an entry with the surname that failed a check.
        GivenNameMatch? failure = null;
        foreach (var entry in entries) {
            var result = MatchEntry(entry, surname, initialsText, initials);
            if (result is not { } r) continue;
            if (r.IsMatch) {
                fits.Add(entry.Name);
                found ??= r;
            } else if (failure is null || Rank(r.Outcome) < Rank(failure.Value.Outcome)) {
                failure = r;
            }
        }
        if (fits.Count > 1) return new GivenNameMatch(GivenNameOutcome.Ambiguous, null, null);
        if (found is { } match) return match;
        return failure ?? new GivenNameMatch(GivenNameOutcome.NoSurnameMatch, null, null);
    }

    private static int Rank(GivenNameOutcome outcome) => outcome switch {
        GivenNameOutcome.InitialsDisagree => 0,
        GivenNameOutcome.EntryInitialsOnly => 1,
        _ => 2,
    };

    // Null when the entry doesn't end with the surname.
    private static GivenNameMatch? MatchEntry(ValueName entry, string foldedSurname, string initialsText, IReadOnlyList<InitialToken> initials) {
        var name = WithoutTrailingSuffix(entry.Name);
        var folded = FoldName(name);
        if (!folded.EndsWith(foldedSurname, StringComparison.Ordinal)) return null;
        var givenLength = folded.Length - foldedSurname.Length;
        if (givenLength == 0 || folded[givenLength - 1] != ' ') return null;

        // FoldName keeps one character for each character of the cleaned name, so the given part is
        // the same span of the name as written.
        var given = name[..(givenLength - 1)].Trim();
        var words = given.Split(' ', StringSplitOptions.RemoveEmptyEntries).ToList();
        while (words.Count > 0 && Particles.Contains(words[^1])) words.RemoveAt(words.Count - 1);
        if (words.Count == 0) return null;
        given = string.Join(' ', words);

        // A name written in small letters ("kelly Hare", "hai-Ning Qin") would print that way.
        var smallLetter = words.Any(w => !Particles.Contains(w) && char.IsLower(w[0]));
        if (smallLetter || NotAName().IsMatch(given) || IucnAuthorNameParser.HasOrganisationWord(given)) {
            return new GivenNameMatch(GivenNameOutcome.EntryNotAName, null, entry.Raw);
        }
        if (words.All(IsInitialWord)) {
            return new GivenNameMatch(GivenNameOutcome.EntryInitialsOnly, null, entry.Raw);
        }

        // Each given word's hyphenated parts, with the word each part belongs to; particles skipped.
        var parts = new List<(string Part, int Word)>();
        for (var w = 0; w < words.Count; w++) {
            if (Particles.Contains(words[w])) continue;
            foreach (var part in words[w].Split(HyphenChars, StringSplitOptions.RemoveEmptyEntries)) {
                parts.Add((FoldName(part).TrimEnd('.'), w));
            }
        }
        var letters = initials.Where(i => !i.IsParticle).ToList();
        var used = 0;
        foreach (var initial in letters) {
            if (used == parts.Count) break;
            if (!parts[used].Part.StartsWith(initial.Folded, StringComparison.Ordinal)) {
                return new GivenNameMatch(GivenNameOutcome.InitialsDisagree, null, entry.Raw);
            }
            used++;
        }
        if (used == 0) return new GivenNameMatch(GivenNameOutcome.InitialsDisagree, null, entry.Raw);

        // Whole words up to the word of the last part matched, from the first word on.
        var lastWord = parts[used - 1].Word;
        var kept = string.Join(' ', words.Take(lastWord + 1));
        if (used < letters.Count) {
            // The initials after the last one matched, as published: "C.D." from "A.C.D.", "de C."
            // from "C. de C.", "-P." from "J.-P.".
            var rest = initialsText[letters[used - 1].End..].Trim();
            var joined = rest.StartsWith('-') ? kept + rest : $"{kept} {rest}";
            return new GivenNameMatch(GivenNameOutcome.MatchedFewerGivenNames, joined, entry.Raw);
        }
        var moreWords = words.Skip(lastWord + 1).Any(w => !Particles.Contains(w));
        return new GivenNameMatch(moreWords ? GivenNameOutcome.MatchedMoreGivenNames : GivenNameOutcome.Matched, kept, entry.Raw);
    }

    // ------------------------------------------------------------ initials

    /// One initial or particle; End is the position just after it in the initials text.
    private sealed record InitialToken(string Text, string Folded, bool IsParticle, int End);

    // "D.R." -> D. R.; "J.-P." -> J. P.; "C. de C." -> C. de C.; "CD" -> C D; "Th." -> Th.; "h." -> h.
    private static List<InitialToken> ReadInitials(string initials) {
        var tokens = new List<InitialToken>();
        foreach (Match m in InitialChunk().Matches(initials)) {
            var text = m.Value;
            if (m.Groups["particle"].Success) {
                tokens.Add(new InitialToken(text, text, IsParticle: true, m.Index + m.Length));
            } else if (m.Groups["caps"].Success) {
                // A run of capitals without dots: one initial each ("CD").
                for (var i = 0; i < text.Length; i++) {
                    tokens.Add(new InitialToken(text[i].ToString(), FoldName(text[i].ToString()), false, m.Index + i + 1));
                }
            } else {
                tokens.Add(new InitialToken(text, FoldName(text.TrimEnd('.')), false, m.Index + m.Length));
            }
        }
        return tokens;
    }

    private static string WithoutSuffix(string initials) => InitialsSuffix().Replace(initials, string.Empty).Trim();

    private static string WithoutTrailingSuffix(string name) => NameSuffix().Replace(name, string.Empty).Trim();

    private static bool IsInitialWord(string word) => InitialWord().IsMatch(word);

    // ------------------------------------------------------------ text

    private static readonly char[] HyphenChars = { '-', '‐', '‑', '‒', '–' };

    // Lower case, no accents, every hyphen and apostrophe one character. One output character per
    // input character, so a position in the folded text is the same position in the input.
    internal static string FoldName(string text) {
        var sb = new StringBuilder(text.Length);
        foreach (var c in text) {
            var mapped = c switch {
                '‐' or '‑' or '‒' or '–' => '-',
                '’' or 'ʼ' or '`' => '\'',
                _ when char.IsWhiteSpace(c) => ' ',
                _ => c,
            };
            var folded = SiteNameKey.Fold(mapped.ToString());
            sb.Append(folded.Length == 1 ? folded[0] : char.ToLowerInvariant(mapped));
        }
        return sb.ToString();
    }

    // Removes trailing "(...)" notes, nested ones included, until none is left.
    private static string StripTrailingNotes(string text) {
        var result = text.Trim();
        while (result.EndsWith(')')) {
            var depth = 0;
            var open = -1;
            for (var i = result.Length - 1; i >= 0; i--) {
                if (result[i] == ')') depth++;
                else if (result[i] == '(' && --depth == 0) {
                    open = i;
                    break;
                }
            }
            if (open < 0) break;
            result = result[..open].TrimEnd();
        }
        return result;
    }

    [GeneratedRegex(@"\s+")]
    private static partial Regex Whitespace();

    [GeneratedRegex(@"^\S+@\S+$")]
    private static partial Regex EmailOnly();

    [GeneratedRegex(@"[\d@()\[\],;/&?\uFFFD]")]
    private static partial Regex NotAName();

    // "D.", "D", "DR", "D.R.", "N.S", "J.-P.", "Th."; not "Evan" or "Al".
    [GeneratedRegex(@"^\p{Lu}(?:\.?-?\p{Lu})*\.?$|^(?:\p{Lu}\p{Ll}?\.-?)+$")]
    private static partial Regex InitialWord();

    [GeneratedRegex(@"(?<particle>\b(?:de|da|das|do|dos|du|van|von|der|den|la|le|di|del|el|y|e)\b(?!\.))|(?<caps>\p{Lu}{2,}(?![\p{Ll}.]))|(?<one>\p{L}\p{Ll}?\.?)")]
    private static partial Regex InitialChunk();

    [GeneratedRegex(@"\s*,?\s*(?:Jr|Jnr|Sr|Snr|II|III|IV)\.?$")]
    private static partial Regex InitialsSuffix();

    [GeneratedRegex(@"\s*,?\s+(?:Jr|Jnr|Sr|Snr|II|III|IV)\.?$")]
    private static partial Regex NameSuffix();
}
