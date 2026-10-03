using System.Text.RegularExpressions;
using BeastieBot3.Shared.Wikitext;

// Reads one author name, as CreditNameSplitter.Split returns it, into a CitationAuthor: a person
// with surname and initials, an organisation kept whole, or a name kept as published because it
// can't be split with confidence.
//
// Person. IUCN writes people "Surname, Initials". The surname may carry particles ("de Kok, R.",
// "van Swaay, C."), several words ("Martínez Salas, E."), hyphens and apostrophes; the initials may
// carry particles ("Nogueira, C. de C."), hyphens ("Samain, M.-S.") or no dots ("DoNascimiento, CD").
// A full given name after the comma ("Mohd Yusof, Nur Adillah") is a person too: Initials then holds
// the given name, as |first= would. Two people written given name first have the same shape
// ("Djoko Iskandar, Mumpuni"), so IucnCitationPartsParser keeps this read only when the credit's
// value[] count confirmed the split. Names already in one token are people when they fit the compact
// patterns the splitter accepts ("Tamanyan K.", "N.H. Rakotoarivelo").
//
// Generational suffixes ("Lowry II, P.P.", "Brownell Jr., R.L.", "Golamco, A., Jr.") follow the CS1
// convention for |first= ("Firstname M., Sr."): Last is the bare surname and Initials ends with the
// suffix, so Last = "Lowry", Initials = "P.P., II". Display keeps IUCN's spelling.
//
// The person test runs before the organisation test, because some surnames are organisation words
// ("Park, S.-H.", "Garden, A."); an organisation word only rules out a surname of two or more words.
//
// Organisation. A name with an organisation word ("BirdLife International", "IUCN SSC Amphibian
// Specialist Group", "Royal Botanic Gardens, Kew", "Ministry of the Environment, Japan") or written in
// capitals only ("WCMC").
//
// Verbatim. Everything else: names written given name first ("Neil Cox", and Ethiopian names, which
// are a given name and a father's name with no surname: "Sebsebe Demissew"), single names
// ("Kadarusman"), typos ("Gadsden. H.", "Disi, M., A.M.") and whole lists the splitter could not split.
// A list kept whole that names a person as "Surname, I." outside parentheses is Verbatim even when it
// also has an organisation word ("Loiselle, P. & participants of the ... workshop, Mantasoa,
// Madagascar 2001"): it is several names, and the site asks editors to check names kept as published.
// A person in parentheses after an organisation ("NatureServe (Hammerson, G.)") is a credit note, so
// that name stays an organisation.

namespace BeastieBot3.Iucn.Citations;

/// How a name was read; finer than CitationAuthorKind, for reports.
internal enum AuthorNameShape {
    /// "Wiig, Ø.", "Nogueira, C. de C.", "Allen, G.R"
    SurnameInitials,
    /// Initials with no dot at all: "DoNascimiento, CD", "Reis, R"
    SurnameInitialsNoDots,
    /// "Lowry II, P.P.", "Golamco, A., Jr."
    SurnameInitialsSuffix,
    /// "Mohd Yusof, Nur Adillah", "Rogers, Alex"
    SurnameGivenNames,
    /// The SurnameGivenNames shape where no value[] count confirmed the split, so possibly two
    /// people: "Djoko Iskandar, Mumpuni". Kept as published (IucnCitationPartsParser decides this,
    /// since only it knows the count).
    SurnameGivenNamesUnconfirmed,
    /// "Tamanyan K."
    CompactSurnameFirst,
    /// "N.H. Rakotoarivelo"
    CompactInitialsFirst,
    Organisation,
    /// "Neil Cox", "Sebsebe Demissew"
    GivenNameFirst,
    /// "Kadarusman"
    SingleName,
    /// Anything else, kept as published.
    Unknown,
}

internal readonly record struct ParsedAuthorName(CitationAuthor Author, AuthorNameShape Shape);

internal static class IucnAuthorNameParser {
    private const string NameWord = @"\p{Lu}[\p{L}'’\-]+";
    private const string Particle = @"(?:de|da|do|dos|das|du|van|von|der|den|la|le|di|del|el|y)";

    private static readonly Regex Whitespace = new(@"\s+", RegexOptions.CultureInvariant | RegexOptions.Compiled);

    private static readonly Regex SuffixPattern = new(
        @"^(?:Jr|Jnr|Sr|Snr|II|III|IV)\.?$", RegexOptions.CultureInvariant | RegexOptions.Compiled);

    private static readonly Regex SurnameWithSuffix = new(
        @"^(?<surname>.+?)\s+(?<suffix>(?:Jr|Jnr|Sr|Snr|II|III|IV)\.?)$", RegexOptions.CultureInvariant | RegexOptions.Compiled);

    private static readonly Regex InitialsWithSuffix = new(
        @"^(?<initials>.+?)\s+(?<suffix>(?:Jr|Jnr|Sr|Snr|II|III|IV)\.?)$", RegexOptions.CultureInvariant | RegexOptions.Compiled);

    private static readonly Regex CompactSurnameFirst = new(
        $@"^(?<last>(?:{Particle}\s+)?{NameWord}(?:\s+{NameWord})?)\s+(?<initials>(?:\p{{Lu}}\.\s?){{1,3}})$",
        RegexOptions.CultureInvariant | RegexOptions.Compiled);

    private static readonly Regex CompactInitialsFirst = new(
        $@"^(?<initials>(?:\p{{Lu}}\.\s?-?){{1,4}})\s*(?<last>{NameWord}(?:\s+{NameWord})?)$",
        RegexOptions.CultureInvariant | RegexOptions.Compiled);

    // Letters with any combining accents ("c" + U+030C), apostrophes and any dash ("Pérez‐Miranda" has U+2010).
    private static readonly Regex GivenNameWord = new(@"^\p{Lu}[\p{L}\p{M}'’\p{Pd}]*$", RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private static readonly Regex SurnameWord = new(@"^[\p{L}\p{M}'’\p{Pd}]+\.?$", RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private static readonly Regex Capitals = new(@"^[\p{Lu}\d&\-]{2,12}$", RegexOptions.CultureInvariant | RegexOptions.Compiled);
    // One word with two capitals in a row: "CNCFlora", "WWF-Malaysia".
    private static readonly Regex Acronym = new(@"^[^\s]*\p{Lu}{2}[^\s]*$", RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private static readonly Regex MiddleInitial = new(@"^\p{Lu}\.$", RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private static readonly Regex Words = new(@"[\p{L}]+", RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private static readonly Regex Parenthesised = new(@"\([^()]*\)", RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private static readonly Regex ListSeparator = new(@"[,;&]|\s+and\s+", RegexOptions.CultureInvariant | RegexOptions.Compiled);

    // Lower-case only, as in the splitter: capitalised "Das", "Do", "Van" are surnames in their own right.
    private static readonly HashSet<string> Particles = new(StringComparer.Ordinal) {
        "de", "da", "das", "do", "dos", "du", "van", "von", "der", "den", "la", "las", "le", "les", "lo", "los",
        "di", "del", "della", "bin", "binti", "al", "el", "y", "e", "ter", "ten", "zu",
    };

    // Words that only organisations use. "park" and "garden" are left out of the person test (they
    // are surnames too) by testing people first; see the file comment.
    private static readonly HashSet<string> OrganisationWords = new(StringComparer.OrdinalIgnoreCase) {
        "group", "groups", "specialist", "subcommittee", "committee", "commission", "council", "department",
        "departamento", "society", "sociedad", "sociedade", "university", "universidad", "universidade",
        "institute", "instituto", "institut", "institution", "international", "garden", "gardens", "jardín",
        "jardim", "museum", "museo", "museu", "centre", "center", "centro", "unit", "authority", "team",
        "project", "workshop", "workshops", "participants", "programme", "program", "network", "foundation",
        "fundación", "fundação", "trust", "service", "servicio", "serviço", "survey", "agency", "ministry",
        "ministerio", "ministério", "conservation", "conservação", "conservación", "research", "biodiversity",
        "biodiversidade", "fisheries", "wildlife", "herbarium", "herbario", "laboratory", "zoo", "aquarium",
        "office", "association", "alliance", "partnership", "iucn", "ssc", "uicn", "birdlife", "natureserve",
        "botanic", "botanical", "botánico", "secretariat", "government", "corporation", "company", "members",
        "volunteers", "partners", "experts", "expert", "working", "fishbase", "asociación", "associação",
        "associazione",
    };

    public static ParsedAuthorName Parse(string name) {
        var display = Clean(name);
        if (TryPerson(display) is { } person) return person;
        if (NamesAPersonOutsideParentheses(display)) return Make(CitationAuthorKind.Verbatim, display, AuthorNameShape.Unknown);
        if (IsOrganisation(display)) return Make(CitationAuthorKind.Organisation, display, AuthorNameShape.Organisation);

        var compact = CompactSurnameFirst.Match(display);
        if (compact.Success) {
            return Make(CitationAuthorKind.Person, display, AuthorNameShape.CompactSurnameFirst,
                compact.Groups["last"].Value, compact.Groups["initials"].Value.Trim());
        }
        compact = CompactInitialsFirst.Match(display);
        if (compact.Success) {
            return Make(CitationAuthorKind.Person, display, AuthorNameShape.CompactInitialsFirst,
                compact.Groups["last"].Value, compact.Groups["initials"].Value.Trim());
        }

        var words = display.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (words.Length == 1 && GivenNameWord.IsMatch(words[0])) {
            return Make(CitationAuthorKind.Verbatim, display, AuthorNameShape.SingleName);
        }
        // "Neil Cox", "Thomas K. Kristensen"
        if (words.Length is >= 2 and <= 4
            && words.Select((w, i) => Particles.Contains(w) || GivenNameWord.IsMatch(w)
                || (i > 0 && i < words.Length - 1 && MiddleInitial.IsMatch(w))).All(ok => ok)
            && GivenNameWord.IsMatch(words[^1])) {
            return Make(CitationAuthorKind.Verbatim, display, AuthorNameShape.GivenNameFirst);
        }
        return Make(CitationAuthorKind.Verbatim, display, AuthorNameShape.Unknown);
    }

    /// The name with whitespace collapsed (narrow and ordinary no-break spaces included), no space
    /// before a comma and no stray separators at either end.
    internal static string Clean(string name) {
        var text = Whitespace.Replace(name ?? string.Empty, " ").Trim();
        text = text.Replace(" ,", ",", StringComparison.Ordinal);
        return text.Trim(' ', ',', ';', '&').Trim();
    }

    private static ParsedAuthorName? TryPerson(string display) {
        var parts = display.Split(',').Select(p => p.Trim()).ToArray();
        string surname;
        string initials;
        string? suffix = null;
        switch (parts.Length) {
            case 2:
                (surname, initials) = (parts[0], parts[1]);
                break;
            case 3 when SuffixPattern.IsMatch(parts[2]):
                // "Golamco, A., Jr."
                (surname, initials, suffix) = (parts[0], parts[1], parts[2]);
                break;
            case 3 when SuffixPattern.IsMatch(parts[1]):
                // "Driggers, III, W.B."
                (surname, suffix, initials) = (parts[0], parts[1], parts[2]);
                break;
            default:
                return null;
        }
        if (surname.Length == 0 || initials.Length == 0) return null;
        if (suffix is null) {
            var suffixed = SurnameWithSuffix.Match(surname);
            if (suffixed.Success) {
                surname = suffixed.Groups["surname"].Value;
                suffix = suffixed.Groups["suffix"].Value;
            }
        }
        if (suffix is null) {
            // "Guerrero, R.D. III"
            var suffixed = InitialsWithSuffix.Match(initials);
            if (suffixed.Success && CreditNameSplitter.IsInitials(suffixed.Groups["initials"].Value)) {
                initials = suffixed.Groups["initials"].Value;
                suffix = suffixed.Groups["suffix"].Value;
            }
        }
        if (!LooksLikeSurname(surname)) return null;
        // "G, Ntakimazi" is a typo for one name; no real surname is a single letter.
        if (surname.Count(char.IsLetter) == 1) return null;

        AuthorNameShape shape;
        if (CreditNameSplitter.IsInitials(initials)) {
            shape = suffix is not null ? AuthorNameShape.SurnameInitialsSuffix
                : initials.Contains('.') ? AuthorNameShape.SurnameInitials
                : AuthorNameShape.SurnameInitialsNoDots;
        } else if (LooksLikeGivenNames(initials) && !HasOrganisationWord(surname)) {
            shape = AuthorNameShape.SurnameGivenNames;
        } else {
            return null;
        }
        var first = suffix is null ? initials : $"{initials}, {suffix}";
        return Make(CitationAuthorKind.Person, display, shape, surname, first);
    }

    // True when, with any text in parentheses removed, a surname is followed by initials somewhere in
    // the list: "Carter, R.L., Hayes, W.K. & West Indian Iguana Specialist Group".
    private static bool NamesAPersonOutsideParentheses(string display) {
        var text = display;
        string previous;
        do {
            previous = text;
            text = Parenthesised.Replace(text, " ");
        } while (text.Length != previous.Length);
        var tokens = ListSeparator.Split(text).Select(t => t.Trim()).ToArray();
        for (var i = 0; i + 1 < tokens.Length; i++) {
            if (tokens[i].Length > 0 && LooksLikeSurname(tokens[i]) && CreditNameSplitter.IsInitials(tokens[i + 1])) {
                return true;
            }
        }
        return false;
    }

    private static bool LooksLikeSurname(string surname) {
        if (!char.IsLetter(surname[0])) return false;
        var words = surname.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (words.Length > 6 || !words.All(SurnameWord.IsMatch)) return false;
        return words.Length == 1 || !HasOrganisationWord(surname);
    }

    // "Alex", "Hai-Ning", "Nur Adillah"
    private static bool LooksLikeGivenNames(string text) {
        var words = text.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        return words.Length is >= 1 and <= 3 && words.All(GivenNameWord.IsMatch) && !HasOrganisationWord(text);
    }

    private static bool IsOrganisation(string display) =>
        HasOrganisationWord(display)
        || display.Contains("red list", StringComparison.OrdinalIgnoreCase)
        || Capitals.IsMatch(display)
        || Acronym.IsMatch(display);

    private static bool HasOrganisationWord(string text) =>
        Words.Matches(text).Any(m => OrganisationWords.Contains(m.Value));

    private static ParsedAuthorName Make(CitationAuthorKind kind, string display, AuthorNameShape shape,
        string? last = null, string? initials = null) =>
        new(new CitationAuthor(kind, display, last, initials), shape);
}
