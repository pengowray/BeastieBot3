using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

// Splits one IUCN credits[].full string into the names it lists. The Wikidata status dry run
// (IucnAssessmentCitationParser.ReadCredits) and the public site's citation parser
// (IucnCitationPartsParser, which then reads each name with IucnAuthorNameParser) both use it.
//
// `full` holds the names as the citation prints them, often many people in one string
// ("Stuart, B.L., Grismer, L. & Achyuthan, N.S."). `value` holds one entry per credited person or
// organisation, but in a different order and mixed with email addresses and postal addresses, so
// only its count of distinct entries is used, as a count that can confirm an otherwise doubtful
// split (DistinctValueCount).
//
// The same short name can belong to two people: "Alemu, S., Alemu, S." is Shambel Alemu and Sisay
// Alemu. A name listed twice in one `full` string is kept twice when value[] has as many distinct
// entries as the split has names, and listed once otherwise. value[] sometimes repeats one person
// word for word ("Suzanne Livingstone (GMSA)" twice), which is why the count is of distinct entries.
// A few payloads repeat a whole credits block, sometimes reordered or with a name added; a repeated
// block of a type adds only the names the earlier blocks of that type don't already hold
// (AddNamesNotYetHeld).
//
// IUCN writes credit lists citation-style: "Surname, I., Surname, I. & Surname, I.". Real
// strings also carry organisations ("BirdLife International & IUCN SSC Hornbill Specialist
// Group"), affiliation notes in parentheses, typos and loose punctuation. The rules, strictest
// first, each tried only when the one before fails:
//
//  1. Pairs. Every comma/&/"and"/semicolon-separated token pairs up as surname then initials
//     ("Malabrigo Jr., P.L.", "Nogueira, C. de C.", "Lee, Y-W", "Davidson, ZD"). Also accepted
//     inside that list: a name already written as one token ("Tamanyan K.", "N.H. Rakotoarivelo"),
//     a suffix token after a pair ("Golamco, A., Jr."), a missing comma between two people
//     ("Ng, P. Yeo, D." reads as Ng, P. and Yeo, D.), and parenthetical notes after the initials,
//     which are dropped ("Suhling, F. (SSC Odonata Specialist Group)" -> "Suhling, F.").
//  2. Confirmed by count. When the number of distinct entries in the credit's value[] is known, a
//     parse that also allows stand-alone names (organisations) or a full given name after the
//     comma ("Rogers, Alex") is accepted only if it yields exactly that many names.
//  3. People written given-name first. With no count to check against, a list with no pairs at
//     all splits only if every part looks like a person's name: 2-4 capitalised words and none of
//     the words organisations use ("Neil Cox and Helen Temple").
//  4. People and organisations. With no count, a list with pairs and stand-alone names splits if
//     every stand-alone name has a word only organisations use ("Eudey, A. & Members of the
//     Primate Specialist Group"). Other stand-alone names are not split off: "Mantasoa" in
//     "Loiselle, P. & participants of the ... workshop, Mantasoa, Madagascar 2001" is part of a
//     workshop's name, and "Eastern Arc Mountains & Coastal Forests ..." is one organisation.
//  5. Otherwise the whole string is one name. So "Tortoise & Freshwater Turtle Specialist Group",
//     "Royal Botanic Gardens, Kew", "Jaffré, T. et al." and "Qin, Hai-Ning & Kohorn, L." stay whole.

namespace BeastieBot3.Iucn.Citations;

/// Which rule of the ladder above produced a split.
internal enum CreditSplitRule {
    /// Nothing left after cleaning.
    Empty,
    /// "et al." in the string: kept whole.
    EtAl,
    /// One token: kept whole (most organisations).
    Single,
    /// Rule 1, surname and initials pairs.
    Strict,
    /// Rule 2, stand-alone names allowed, confirmed by the value[] count.
    CountStandalone,
    /// Rule 2, given names after the comma allowed, confirmed by the value[] count.
    CountGiven,
    /// Rule 3, every part written given name first.
    GivenFirst,
    /// Rule 4, surname and initials pairs plus stand-alone organisation names, with no count.
    PairsAndOrganisations,
    /// Rule 5, kept whole: no rule applied and there was no count to check against.
    Whole,
    /// Rule 5, kept whole: no split gave as many names as value[] has entries.
    WholeCountMismatch,
}

internal readonly record struct CreditSplit(IReadOnlyList<string> Names, CreditSplitRule Rule);

internal static class CreditNameSplitter {
    private const string Particle = @"(?:de|da|do|dos|das|du|van|von|der|den|la|le|di|del|el|y)";
    private const string InitialChunk = @"(?:\p{Lu}{1,2}\.?|\p{L}\p{Ll}?\.)";
    private const string NameWord = @"\p{Lu}[\p{L}'’\-]+";

    private static readonly Regex InitialsPattern = new(
        $@"^{InitialChunk}(?:(?:\s*-\s*|\s+|)(?:{InitialChunk}|{Particle}\b\.?))*$", RegexOptions.CultureInvariant | RegexOptions.Compiled);

    private static readonly Regex UppercaseRun = new(@"\p{Lu}{4,}", RegexOptions.CultureInvariant | RegexOptions.Compiled);

    // "Tamanyan K.", "de Bélair G.", "Luke W.R.Q."
    private static readonly Regex CompactSurnameFirst = new(
        $@"^(?:{Particle}\s+)?{NameWord}(?:\s+{NameWord})?\s+(?:\p{{Lu}}\.\s?){{1,3}}$", RegexOptions.CultureInvariant | RegexOptions.Compiled);

    // "N.H. Rakotoarivelo", "M. Rhazi"
    private static readonly Regex CompactInitialsFirst = new(
        $@"^(?:\p{{Lu}}\.\s?-?){{1,3}}\s*{NameWord}(?:\s+{NameWord})?$", RegexOptions.CultureInvariant | RegexOptions.Compiled);

    // "J. Staissny" where a comma is missing: the initials of one person, then the next surname.
    // Dotted initials only, so the pattern can't backtrack through "COL-HUA-COAH-JAUM" letter by letter.
    private static readonly Regex GluedInitials = new(
        $@"^((?:\p{{Lu}}\.-?){{1,3}})\s+((?:{Particle}\s+)?{NameWord}(?:\s+{NameWord})?)$", RegexOptions.CultureInvariant | RegexOptions.Compiled);

    private static readonly Regex NameSuffix = new(@"^(?:Jr|Jnr|Sr|Snr|II|III|IV)\.?$", RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private static readonly Regex EtAl = new(@"\bet\s+al\b", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private static readonly Regex TrailingNote = new(@"\s*\([^()]*\)\.?$", RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private static readonly Regex HtmlTag = new(@"<[^>]*>", RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private static readonly Regex Whitespace = new(@"\s+", RegexOptions.CultureInvariant | RegexOptions.Compiled);

    // Organisation names with a comma in them, kept whole before the string is tokenised.
    private static readonly string[] CommaNames = { "Royal Botanic Gardens, Kew" };

    // Organisation words too common in other text to show on their own that a name is an
    // organisation's (rule 4).
    private static readonly HashSet<string> WeakOrganisationWords = new(StringComparer.OrdinalIgnoreCase) {
        "the", "of", "for", "and",
    };

    private static readonly Regex Letters = new(@"\p{L}+", RegexOptions.CultureInvariant | RegexOptions.Compiled);

    // Lower-case only: capitalised "Das", "Do", "Van" are surnames in their own right.
    private static readonly HashSet<string> Particles = new(StringComparer.Ordinal) {
        "de", "da", "das", "do", "dos", "du", "van", "von", "der", "den", "la", "las", "le", "los", "di", "del", "della",
        "bin", "binti", "al", "el", "y", "e", "ter", "ten",
    };

    private static readonly HashSet<string> OrganisationWords = new(StringComparer.OrdinalIgnoreCase) {
        "group", "groups", "specialist", "department", "society", "university", "institute", "institution",
        "international", "garden", "gardens", "museum", "park", "centre", "center", "unit", "authority", "team",
        "project", "workshop", "participants", "programme", "program", "network", "foundation", "trust",
        "council", "committee", "commission", "service", "survey", "agency", "ministry", "conservation",
        "research", "biodiversity", "fisheries", "wildlife", "herbarium", "herbarios", "herbario", "laboratory",
        "zoo", "aquarium", "office", "association", "alliance", "partnership", "iucn", "ssc", "uicn", "birdlife",
        "natureserve", "red", "list", "the", "of", "for", "and", "assessment", "volunteers", "members", "biopark",
        "national", "botanic", "botanical", "partners", "working", "coordinating", "secretariat", "government",
        "expert", "experts",
    };

    internal static IReadOnlyList<string> Split(string full) => Split(full, null);

    /// Splits one credits[].full string into names. expectedCount is the number of distinct entries
    /// in that credit's value[] array when known (0 counts as unknown); it confirms a doubtful split,
    /// and a name the split finds twice is kept twice only when the count confirms it.
    internal static IReadOnlyList<string> Split(string full, int? expectedCount) =>
        SplitWithRule(full, expectedCount).Names;

    internal static CreditSplit SplitWithRule(string full, int? expectedCount) {
        var text = NormalizeCreditText(full);
        if (text.Length == 0) return new CreditSplit(Array.Empty<string>(), CreditSplitRule.Empty);
        if (EtAl.IsMatch(text)) return new CreditSplit(new[] { text }, CreditSplitRule.EtAl);

        var tokens = TokenizeCredit(text);
        if (tokens.Count <= 1) return new CreditSplit(new[] { text }, CreditSplitRule.Single);

        var strict = PairNames(tokens, allowGivenNames: false, allowStandalone: false);
        if (strict is not null) return new CreditSplit(KeepConfirmedRepeats(strict.Names, expectedCount), CreditSplitRule.Strict);

        if (expectedCount is > 0) {
            // Both rules here accept a split only when its name count equals expectedCount, so any
            // name repeated in it is confirmed.
            var standalone = PairNames(tokens, allowGivenNames: false, allowStandalone: true);
            if (standalone is not null && standalone.Names.Count == expectedCount) {
                return new CreditSplit(standalone.Names, CreditSplitRule.CountStandalone);
            }
            var given = PairNames(tokens, allowGivenNames: true, allowStandalone: true);
            if (given is not null && given.Names.Count == expectedCount) {
                return new CreditSplit(given.Names, CreditSplitRule.CountGiven);
            }
            return new CreditSplit(new[] { text }, CreditSplitRule.WholeCountMismatch);
        }

        var loose = PairNames(tokens, allowGivenNames: false, allowStandalone: true);
        if (loose is not null && !loose.AnyPaired && loose.Names.All(LooksLikePersonName)) {
            return new CreditSplit(Distinct(loose.Names), CreditSplitRule.GivenFirst);
        }
        if (loose is not null && loose.AnyPaired && loose.Standalone.Count > 0 && loose.Standalone.All(HasOrganisationWord)) {
            return new CreditSplit(Distinct(loose.Names), CreditSplitRule.PairsAndOrganisations);
        }
        return new CreditSplit(new[] { text }, CreditSplitRule.Whole);
    }

    /// The number of distinct non-blank entries in a credit's value[] (one per credited person or
    /// organisation), or null when the credit has no value[] array. Entries are compared after
    /// trimming; IUCN sometimes repeats one person's entry word for word.
    internal static int? DistinctValueCount(JsonElement credit) {
        if (!credit.TryGetProperty("value", out var value) || value.ValueKind != JsonValueKind.Array) return null;
        var distinct = new HashSet<string>(StringComparer.Ordinal);
        foreach (var entry in value.EnumerateArray()) {
            if (entry.ValueKind != JsonValueKind.String) continue;
            var text = entry.GetString()?.Trim();
            if (!string.IsNullOrEmpty(text)) distinct.Add(text);
        }
        return distinct.Count;
    }

    /// Adds the names of a later credits block of the same type, skipping each name as many times as
    /// the earlier blocks already hold it. A whole block repeated (in any order) adds nothing; a
    /// block that repeats the list with one more person adds that person.
    internal static void AddNamesNotYetHeld(List<string> held, IReadOnlyList<string> block) {
        var heldCounts = held.GroupBy(n => n, StringComparer.Ordinal).ToDictionary(g => g.Key, g => g.Count(), StringComparer.Ordinal);
        var blockCounts = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (var name in block) {
            blockCounts[name] = blockCounts.GetValueOrDefault(name) + 1;
            if (blockCounts[name] > heldCounts.GetValueOrDefault(name)) held.Add(name);
        }
    }

    /// True for the initials part of a "Surname, Initials" name: "R.", "J.-P.", "C. de C.", "CD".
    internal static bool IsInitials(string token) =>
        token.Length is > 0 and <= 16 && !UppercaseRun.IsMatch(token) && InitialsPattern.IsMatch(token);

    /// Standalone: the names that paired with nothing and were not a person written in one token.
    private sealed record PairedNames(List<string> Names, bool AnyPaired, List<string> Standalone);

    private static PairedNames? PairNames(IReadOnlyList<string> input, bool allowGivenNames, bool allowStandalone) {
        var tokens = input.ToList();
        var names = new List<string>();
        var standalone = new List<string>();
        var anyPaired = false;
        var lastWasPair = false;
        var i = 0;
        while (i < tokens.Count) {
            var token = tokens[i];
            var next = i + 1 < tokens.Count ? StripNotes(tokens[i + 1]) : null;

            // "Golamco, A., Jr."; a note after the suffix is dropped, as after initials:
            // "Kirkland, G.L., Jr. (Rodent Specialist Group)".
            var suffix = StripNotes(token);
            if (lastWasPair && NameSuffix.IsMatch(suffix) && !(next is not null && IsInitials(next))) {
                names[^1] = $"{names[^1]}, {suffix}";
                i++;
                continue;
            }

            if (next is not null && LooksLikeSurname(token)) {
                // "Driggers, III, W.B.": the suffix written between surname and initials.
                if (NameSuffix.IsMatch(next) && i + 2 < tokens.Count && IsInitials(StripNotes(tokens[i + 2]))) {
                    names.Add($"{token}, {next}, {StripNotes(tokens[i + 2])}");
                    anyPaired = lastWasPair = true;
                    i += 3;
                    continue;
                }
                if (IsInitials(next)) {
                    names.Add($"{token}, {next}");
                    anyPaired = lastWasPair = true;
                    i += 2;
                    continue;
                }
                var glued = GluedInitials.Match(next);
                if (glued.Success && i + 2 < tokens.Count && IsInitials(StripNotes(tokens[i + 2]))) {
                    names.Add($"{token}, {glued.Groups[1].Value}");
                    tokens[i + 1] = glued.Groups[2].Value;
                    anyPaired = lastWasPair = true;
                    i++;
                    continue;
                }
                if (allowGivenNames && LooksLikeGivenNames(next)) {
                    names.Add($"{token}, {next}");
                    anyPaired = lastWasPair = true;
                    i += 2;
                    continue;
                }
            }

            var bare = StripNotes(token);
            if (CompactSurnameFirst.IsMatch(bare) || CompactInitialsFirst.IsMatch(bare)) {
                names.Add(bare);
                lastWasPair = false;
                i++;
                continue;
            }
            if (!allowStandalone || IsInitials(bare)) return null;
            names.Add(token);
            standalone.Add(token);
            lastWasPair = false;
            i++;
        }
        return new PairedNames(names, anyPaired, standalone);
    }

    private static string NormalizeCreditText(string? full) {
        if (string.IsNullOrWhiteSpace(full)) return string.Empty;
        var text = WebUtility.HtmlDecode(HtmlTag.Replace(full, string.Empty));
        text = Whitespace.Replace(text, " ").Trim();
        text = text.Replace(" .", ".", StringComparison.Ordinal);
        while (text.Contains("..", StringComparison.Ordinal)) {
            text = text.Replace("..", ".", StringComparison.Ordinal);
        }
        return text.TrimEnd(' ', ',', ';', '&');
    }

    // Splits on commas, semicolons, "&" and " and " outside parentheses; drops empty tokens, tokens
    // that are only a parenthetical note, and stray leading dots/dashes (from "S,. Urdiales").
    private static List<string> TokenizeCredit(string text) {
        var tokens = new List<string>();
        var current = new StringBuilder();
        var depth = 0;
        var i = 0;
        while (i < text.Length) {
            if (depth == 0) {
                var known = CommaNames.FirstOrDefault(n => string.CompareOrdinal(text, i, n, 0, n.Length) == 0);
                if (known is not null) {
                    current.Append(known);
                    i += known.Length;
                    continue;
                }
            }
            var c = text[i];
            if (c == '(') depth++;
            else if (c == ')' && depth > 0) depth--;

            if (depth == 0 && (c == ',' || c == ';' || c == '&')) {
                AddToken(tokens, current);
                i++;
                continue;
            }
            if (depth == 0 && string.CompareOrdinal(text, i, " and ", 0, 5) == 0) {
                AddToken(tokens, current);
                i += 5;
                continue;
            }
            current.Append(c);
            i++;
        }
        AddToken(tokens, current);
        return tokens;
    }

    private static void AddToken(List<string> tokens, StringBuilder current) {
        var token = current.ToString().TrimStart('.', ' ', '-', '–').Trim();
        current.Clear();
        if (token.Length == 0) return;
        if (token[0] == '(' && StripNotes(token).Length == 0) return;
        tokens.Add(token);
    }

    private static string StripNotes(string token) {
        string previous;
        do {
            previous = token;
            token = TrailingNote.Replace(token, string.Empty).Trim();
        } while (token.Length != previous.Length);
        return token;
    }

    private static bool LooksLikeSurname(string token) {
        if (token.Length == 0 || !char.IsLetter(token[0])) return false;
        if (token.Any(char.IsDigit) || token.Contains('(') || token.Contains('/')) return false;
        if (token.Contains('.') && IsInitials(token)) return false;
        if (NameSuffix.IsMatch(token)) return false;
        var words = token.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        var coreWords = words.Count(w => !Particles.Contains(w));
        return coreWords is >= 1 and <= 4 && words.Length <= 6;
    }

    // "Alex", "Hai-Ning", "Nur Adillah": only ever accepted when a count confirms the split.
    private static bool LooksLikeGivenNames(string token) {
        if (token.Any(char.IsDigit) || token.Contains('(') || token.Contains('/') || token.Contains('.')) return false;
        var words = token.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        return words.Length is >= 1 and <= 3
            && words.All(w => char.IsUpper(w[0]) && !OrganisationWords.Contains(w));
    }

    private static bool LooksLikePersonName(string token) {
        if (token.Any(char.IsDigit) || token.Contains('(') || token.Contains('/')) return false;
        if (CompactSurnameFirst.IsMatch(token) || CompactInitialsFirst.IsMatch(token)) return true;
        var words = token.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (words.Length is < 2 or > 4) return false;
        foreach (var word in words) {
            if (OrganisationWords.Contains(word.TrimEnd('.'))) return false;
            if (Particles.Contains(word)) continue;
            if (!char.IsUpper(word[0])) return false;
        }
        return !words[^1].EndsWith('.');
    }

    // A word only organisations use, leaving out "the", "of", "for" and "and", and leaving out a note
    // in parentheses: "Ng Wai Chuen (Grouper & Wrasse Specialist Group)" is a person with an
    // affiliation, while "NatureServe (Hammerson, G.)" is an organisation.
    private static bool HasOrganisationWord(string token) =>
        Letters.Matches(StripNotes(token)).Any(m => OrganisationWords.Contains(m.Value) && !WeakOrganisationWords.Contains(m.Value));

    private static IReadOnlyList<string> Distinct(List<string> names) =>
        names.Distinct(StringComparer.Ordinal).ToList();

    // A name listed twice is two people when value[] has as many distinct entries as there are
    // names ("Harold, A. & Harold, A." is Anthony and Antony Harold); otherwise it is one person
    // listed twice.
    private static IReadOnlyList<string> KeepConfirmedRepeats(List<string> names, int? expectedCount) =>
        expectedCount is > 0 && names.Count == expectedCount ? names : Distinct(names);
}
