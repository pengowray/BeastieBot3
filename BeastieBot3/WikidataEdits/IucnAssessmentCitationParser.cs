using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

// Reads one cached /api/v4/assessment/{id} payload into an IucnGlobalAssessment: the fields a
// Wikidata P141 statement and its reference need, including the citation and the people credited.
// IucnAssessmentJsonParser covers the CSV-shaped projection and ignores all of that.
//
// Field names checked against the 2026-1 cache (Aug 2026): url, citation, year_published (a string),
// assessment_date ("2019-09-02T01:00:00.000+01:00"), criteria, red_list_category {code, version},
// possibly_extinct, possibly_extinct_in_the_wild, scopes [{code}], errata [{reason}],
// credits [{credit_type_name, full, value[]}], taxon {sis_id, scientific_name, infrarank}.
//
// Credits. `full` holds the names as the citation prints them, often many people in one string
// ("Stuart, B.L., Grismer, L. & Achyuthan, N.S."). `value` holds one entry per credited person or
// organisation, but in a different order and mixed with email addresses and postal addresses, so
// only its count of distinct entries is used, as a count that can confirm an otherwise doubtful
// split. IUCN's type names are "assessor", "evaluator", "contributor", "facilitators" and
// "institutions" (plural, as sent).
//
// The same short name can belong to two people: "Alemu, S., Alemu, S." is Shambel Alemu and Sisay
// Alemu. A name listed twice in one `full` string is kept twice when value[] has as many distinct
// entries as the split has names, and listed once otherwise. value[] sometimes repeats one person
// word for word ("Suzanne Livingstone (GMSA)" twice), which is why the count is of distinct entries.
// A few payloads repeat a whole credits block, sometimes reordered or with a name added; a repeated
// block of a type adds only the names the earlier blocks of that type don't already hold.

namespace BeastieBot3.WikidataEdits;

internal static class IucnAssessmentCitationParser {
    private const string GlobalScopeCode = "1";

    public static IucnGlobalAssessment? Parse(string json, DateTime downloadedAtUtc) {
        if (string.IsNullOrWhiteSpace(json)) return null;
        JsonDocument document;
        try {
            document = JsonDocument.Parse(json);
        } catch (JsonException) {
            return null;
        }
        using (document) {
            return Parse(document.RootElement, downloadedAtUtc);
        }
    }

    /// Null when the assessment has no Global scope, or lacks an id, taxon id, name or category.
    internal static IucnGlobalAssessment? Parse(JsonElement root, DateTime downloadedAtUtc) {
        if (root.ValueKind != JsonValueKind.Object) return null;
        if (!HasGlobalScope(root)) return null;

        var assessmentId = GetLong(root, "assessment_id");
        var taxon = root.TryGetProperty("taxon", out var taxonEl) && taxonEl.ValueKind == JsonValueKind.Object
            ? taxonEl
            : (JsonElement?)null;
        var taxonId = GetLong(root, "sis_taxon_id") ?? (taxon is { } t1 ? GetLong(t1, "sis_id") : null);
        var scientificName = (taxon is { } t2 ? GetString(t2, "scientific_name") : null)
            ?? GetString(root, "taxon_scientific_name");
        var (categoryCode, criteriaVersion) = ReadCategory(root);
        if (assessmentId is null || taxonId is null
            || string.IsNullOrWhiteSpace(scientificName) || string.IsNullOrWhiteSpace(categoryCode)) {
            return null;
        }

        var rawCitation = GetString(root, "citation");
        var citation = string.IsNullOrWhiteSpace(rawCitation) ? null : StripAccessedOn(rawCitation);
        var url = GetString(root, "url");

        return new IucnGlobalAssessment {
            TaxonId = taxonId.Value,
            AssessmentId = assessmentId.Value,
            ScientificName = scientificName.Trim(),
            IsInfrarank = taxon is { } t3 && GetBool(t3, "infrarank"),
            CategoryCode = categoryCode.Trim(),
            PossiblyExtinct = GetBool(root, "possibly_extinct"),
            PossiblyExtinctInTheWild = GetBool(root, "possibly_extinct_in_the_wild"),
            Criteria = NullIfBlank(GetString(root, "criteria")),
            CriteriaVersion = NullIfBlank(criteriaVersion),
            YearPublished = int.TryParse(GetString(root, "year_published"), NumberStyles.Integer, CultureInfo.InvariantCulture, out var year) ? year : null,
            AssessmentDate = ParseAssessmentDate(GetString(root, "assessment_date")),
            Url = string.IsNullOrWhiteSpace(url)
                ? $"https://www.iucnredlist.org/species/{taxonId.Value}/{assessmentId.Value}"
                : url.Trim(),
            Citation = string.IsNullOrWhiteSpace(citation) ? null : citation,
            Doi = ExtractDoi(rawCitation),
            Credits = ReadCredits(root),
            DownloadedAtUtc = downloadedAtUtc,
            IsAmended = root.TryGetProperty("errata", out var errata)
                && errata.ValueKind == JsonValueKind.Array && errata.GetArrayLength() > 0,
        };
    }

    /// True when scopes[] has an entry with code "1" (Global). Works on a full assessment and on
    /// the assessment headers embedded in a /taxa payload, which carry the same scopes array.
    internal static bool HasGlobalScope(JsonElement assessment) {
        if (!assessment.TryGetProperty("scopes", out var scopes) || scopes.ValueKind != JsonValueKind.Array) return false;
        foreach (var scope in scopes.EnumerateArray()) {
            if (scope.ValueKind == JsonValueKind.Object && GetString(scope, "code") == GlobalScopeCode) return true;
        }
        return false;
    }

    /// True when scopes[] is missing or empty: the handful of assessments IUCN publishes with no scope.
    internal static bool HasBlankScope(JsonElement assessment) =>
        !assessment.TryGetProperty("scopes", out var scopes)
        || scopes.ValueKind != JsonValueKind.Array
        || scopes.GetArrayLength() == 0;

    // ------------------------------------------------------------ citation

    private static readonly Regex AccessedOnPattern = new(
        @"\s*Accessed on\b[^.]*\.?\s*$", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);

    private static readonly Regex DoiPattern = new(
        @"doi\.org/(10\.\d{4,9}/\S+)", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);

    /// Removes IUCN's trailing "Accessed on 18 August 2026." (the date the payload was fetched).
    public static string StripAccessedOn(string citation) =>
        AccessedOnPattern.Replace(citation ?? string.Empty, string.Empty).Trim();

    /// The DOI a citation links to ("10.2305/IUCN.UK.2025-2.RLTS.T22732931A250382422.en"), or null.
    /// The DOI's own last segment is ".en"/".es"; a dot after that is the sentence's.
    public static string? ExtractDoi(string? citation) {
        if (string.IsNullOrWhiteSpace(citation)) return null;
        var match = DoiPattern.Match(citation);
        if (!match.Success) return null;
        var doi = match.Groups[1].Value.TrimEnd('.', ',', ';', ')');
        return doi.Length > "10.1/".Length ? doi : null;
    }

    private static DateOnly? ParseAssessmentDate(string? text) {
        if (string.IsNullOrWhiteSpace(text)) return null;
        // The offset is IUCN's server clock (+00:00 or +01:00 around midnight); the date as written is
        // the date IUCN means, so take it before any conversion to UTC can move it.
        if (DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.None, out var parsed)) {
            return DateOnly.FromDateTime(parsed.DateTime);
        }
        return DateOnly.TryParse(text, CultureInfo.InvariantCulture, out var date) ? date : null;
    }

    private static (string? Code, string? Version) ReadCategory(JsonElement root) {
        if (root.TryGetProperty("red_list_category", out var category) && category.ValueKind == JsonValueKind.Object) {
            return (GetString(category, "code"), GetString(category, "version"));
        }
        return (GetString(root, "red_list_category_code"), null);
    }

    // ------------------------------------------------------------ credits

    /// Assessors first, then the other types in the order they first appear; Order counts from 1
    /// within each type.
    internal static IReadOnlyList<IucnCredit> ReadCredits(JsonElement root) {
        if (!root.TryGetProperty("credits", out var credits) || credits.ValueKind != JsonValueKind.Array) {
            return Array.Empty<IucnCredit>();
        }

        var byType = new Dictionary<string, List<string>>(StringComparer.Ordinal);
        var typeOrder = new List<string>();
        foreach (var credit in credits.EnumerateArray()) {
            if (credit.ValueKind != JsonValueKind.Object) continue;
            var type = GetString(credit, "credit_type_name")?.Trim().ToLowerInvariant();
            var full = GetString(credit, "full");
            if (string.IsNullOrEmpty(type) || string.IsNullOrWhiteSpace(full)) continue;

            if (!byType.TryGetValue(type, out var names)) {
                names = new List<string>();
                byType[type] = names;
                typeOrder.Add(type);
            }
            AddNamesNotYetHeld(names, SplitCreditNames(full, DistinctValueCount(credit)));
        }

        var result = new List<IucnCredit>();
        foreach (var type in typeOrder.OrderBy(t => t == "assessor" ? 0 : 1)) {
            var order = 0;
            foreach (var name in byType[type]) {
                result.Add(new IucnCredit(type, name, ++order));
            }
        }
        return result;
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

    // ------------------------------------------------------------ credit name splitting
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

    public static IReadOnlyList<string> SplitCreditNames(string full) => SplitCreditNames(full, null);

    /// Splits one credits[].full string into names. expectedCount is the number of distinct entries
    /// in that credit's value[] array when known (0 counts as unknown); it confirms a doubtful split,
    /// and a name the split finds twice is kept twice only when the count confirms it.
    public static IReadOnlyList<string> SplitCreditNames(string full, int? expectedCount) =>
        SplitCreditNamesWithRule(full, expectedCount).Names;

    internal static CreditSplit SplitCreditNamesWithRule(string full, int? expectedCount) {
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

    /// True for the initials part of a "Surname, Initials" name: "R.", "J.-P.", "C. de C.", "CD".
    internal static bool IsInitials(string token) =>
        token.Length is > 0 and <= 16 && !UppercaseRun.IsMatch(token) && InitialsPattern.IsMatch(token);

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

    // ------------------------------------------------------------ JSON helpers

    private static string? NullIfBlank(string? value) => string.IsNullOrWhiteSpace(value) ? null : value.Trim();

    private static bool GetBool(JsonElement element, string property) {
        if (!element.TryGetProperty(property, out var value)) return false;
        return value.ValueKind switch {
            JsonValueKind.True => true,
            JsonValueKind.String => bool.TryParse(value.GetString(), out var parsed) && parsed,
            _ => false,
        };
    }

    private static long? GetLong(JsonElement element, string property) {
        if (!element.TryGetProperty(property, out var value)) return null;
        return value.ValueKind switch {
            JsonValueKind.Number when value.TryGetInt64(out var n) => n,
            JsonValueKind.String when long.TryParse(value.GetString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var n) => n,
            _ => null,
        };
    }

    private static string? GetString(JsonElement element, string property) {
        if (!element.TryGetProperty(property, out var value)) return null;
        return value.ValueKind switch {
            JsonValueKind.String => value.GetString(),
            JsonValueKind.Number => value.GetRawText(),
            _ => null,
        };
    }
}
