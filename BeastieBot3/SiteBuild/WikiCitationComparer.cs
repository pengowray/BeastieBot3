using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using BeastieBot3.Shared.Wikitext;

// Compares the citation parts the site builds with one {{cite iucn}} in an en-wiki article that
// cites the same assessment: the author list, the DOI, and the year, errata and amends years.
// Pure; `site check-citations` tallies the results.
//
// Author lists are compared twice: exactly (after reducing wiki markup to text and collapsing
// whitespace), and loosely, ignoring case, accents, dots, hyphens, apostrophes, commas and spaces,
// because {{make cite IUCN}} and Monkbot added a dot after a final initial ("Reis, R" became
// "Reis, R."). Disagreements are classed by the first rule that explains them, so the report can
// separate en-wiki conventions and edits from parser mistakes.

namespace BeastieBot3.SiteBuild;

internal enum AuthorAgreement {
    /// The same names in the same order.
    Same,
    /// The same once case, accents, dots, hyphens, apostrophes and spaces are ignored.
    SameIgnoringPunctuation,
    /// The template names no authors.
    WikiNoAuthors,
    /// One wiki author parameter holds several of our names ("Butchart, S.H.M. & Symes, A.").
    WikiSeveralInOneParameter,
    /// Our last name's parenthesised part is in |collaboration= ({{make cite IUCN}}'s split of
    /// "Botanic Gardens Conservation International (BGCI)").
    WikiCollaborationParameter,
    /// We kept a list of names whole (the splitter could not split it) where the wiki splits it.
    OursKeptWhole,
    /// The wiki splits one of our names in two ("Brownell Jr. | R.L." for "Brownell Jr., R.L.").
    WikiSplitsAName,
    /// The wiki names are the first of ours, the rest left out.
    WikiFewer,
    /// Our names are the first of the wiki's.
    WikiMore,
    /// The same names in another order.
    OtherOrder,
    /// The wiki writes the names given name first ("A. Harold" for "Harold, A.").
    WikiGivenNameFirst,
    /// The wiki gives only the surnames ("Reeves | Pitman | Ford").
    WikiSurnamesOnly,
    /// The same surnames in the same order, with other initials or given names ("Woinarski, J." for
    /// "Woinarski, J.C.Z.", "Foord, Stefan" for "Foord, S.").
    OtherInitials,
    /// Some surnames are the same and some are not (typos, a name added or replaced).
    SomeNamesDiffer,
    /// No surname in common: usually the authors of an older assessment of the taxon.
    NoNameInCommon,
}

internal enum DoiAgreement {
    /// Neither has a DOI.
    BothNone,
    Same,
    /// Only the wiki has a DOI.
    WikiOnly,
    /// Only we have a DOI.
    OursOnly,
    /// Both have a DOI and they differ.
    Different,
    /// The wiki's |doi= is not an IUCN Red List DOI.
    WikiMalformed,
}

internal enum YearAgreement { Same, Different, WikiMissing, OursMissing, BothMissing }

internal sealed record WikiCitationComparison(
    AuthorAgreement Authors,
    IReadOnlyList<string> OurAuthors,
    IReadOnlyList<string> WikiAuthors,
    DoiAgreement Doi,
    string? WikiDoi,
    /// The verdict IucnDoiSelector gives the wiki's DOI for this assessment.
    DoiVerdict WikiDoiVerdict,
    /// Both DOIs name the same taxon and assessment and differ only in release or language.
    bool DoiDiffersOnlyInRelease,
    YearAgreement Year,
    YearAgreement Errata,
    YearAgreement Amends,
    int? WikiYear,
    int? WikiErrata,
    int? WikiAmends);

internal static class WikiCitationComparer {
    private static readonly Regex NumberedAuthorParameter = new(@"^(?:author|last|first|surname|given)(?<n>\d+)$", RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private static readonly Regex YearPattern = new(@"\b(?<y>1[89]\d\d|20\d\d)\b", RegexOptions.CultureInvariant | RegexOptions.Compiled);

    public static WikiCitationComparison Compare(IucnCitationParts parts, WikiCiteIucn cite, IReadOnlyCollection<long>? predecessorAssessmentIds) {
        var ours = parts.Authors.Select(a => a.Display).ToList();
        var wiki = WikiAuthors(cite);
        var oursHasUnsplitList = parts.Authors.Any(a =>
            a.Kind == CitationAuthorKind.Verbatim && (a.Display.Count(c => c == ',') >= 2 || a.Display.Contains('&')));
        var authors = CompareAuthors(ours, wiki, CiteIucnTemplateScanner.PlainText(cite.Get("collaboration")), oursHasUnsplitList);

        var wikiDoiRaw = cite.Get("doi");
        var wikiDoi = IucnDoiSelector.Normalise(wikiDoiRaw);
        var verdict = IucnDoiSelector.Check(parts, wikiDoiRaw, predecessorAssessmentIds);
        DoiAgreement doi;
        var onlyRelease = false;
        if (wikiDoiRaw is null) {
            doi = parts.Doi is null ? DoiAgreement.BothNone : DoiAgreement.OursOnly;
        } else if (wikiDoi is null) {
            doi = DoiAgreement.WikiMalformed;
        } else if (parts.Doi is null) {
            doi = DoiAgreement.WikiOnly;
        } else if (string.Equals(parts.Doi, wikiDoi, StringComparison.OrdinalIgnoreCase)) {
            doi = DoiAgreement.Same;
        } else {
            doi = DoiAgreement.Different;
            var a = IucnDoiSelector.TryParse(parts.Doi);
            var b = IucnDoiSelector.TryParse(wikiDoi);
            onlyRelease = a is { } x && b is { } y && x.TaxonId == y.TaxonId && x.AssessmentId == y.AssessmentId;
        }

        var wikiYear = ReadYear(cite.Get("year") ?? cite.Get("date"));
        var wikiErrata = ReadYear(cite.Get("errata"));
        var wikiAmends = ReadYear(cite.Get("amends"));
        return new WikiCitationComparison(
            authors, ours, wiki, doi, wikiDoiRaw, verdict, onlyRelease,
            CompareYear(parts.Year, wikiYear),
            CompareYear(parts.ErrataYear, wikiErrata),
            CompareYear(parts.AmendsYear, wikiAmends),
            wikiYear, wikiErrata, wikiAmends);
    }

    /// The author names a {{cite iucn}} gives, in order: |authorN= as written, or "|lastN=, |firstN=".
    /// |authors= and |vauthors= (several names in one parameter) come first, as one entry.
    public static IReadOnlyList<string> WikiAuthors(WikiCiteIucn cite) {
        var names = new List<string>();
        foreach (var combined in new[] { "authors", "vauthors" }) {
            if (cite.Get(combined) is { } value) names.Add(CiteIucnTemplateScanner.PlainText(value));
        }
        var max = 1;
        foreach (var key in cite.Parameters.Keys) {
            var numbered = NumberedAuthorParameter.Match(key);
            if (numbered.Success && int.TryParse(numbered.Groups["n"].Value, NumberStyles.None, CultureInfo.InvariantCulture, out var n)) {
                max = Math.Max(max, Math.Min(n, 500));
            }
        }
        for (var n = 1; n <= max; n++) {
            var author = n == 1 ? cite.Get("author1") ?? cite.Get("author") : cite.Get($"author{n}");
            var last = n == 1
                ? cite.Get("last1") ?? cite.Get("last") ?? cite.Get("surname1") ?? cite.Get("surname")
                : cite.Get($"last{n}") ?? cite.Get($"surname{n}");
            var first = n == 1
                ? cite.Get("first1") ?? cite.Get("first") ?? cite.Get("given1") ?? cite.Get("given")
                : cite.Get($"first{n}") ?? cite.Get($"given{n}");
            if (author is not null) {
                names.Add(CiteIucnTemplateScanner.PlainText(author));
            } else if (last is not null) {
                var plainLast = CiteIucnTemplateScanner.PlainText(last);
                names.Add(first is null ? plainLast : $"{plainLast}, {CiteIucnTemplateScanner.PlainText(first)}");
            }
        }
        return names.Where(n => n.Length > 0).ToList();
    }

    /// oursHasUnsplitList: one of our names is a list the splitter kept whole.
    internal static AuthorAgreement CompareAuthors(IReadOnlyList<string> ours, IReadOnlyList<string> wiki,
        string? collaboration, bool oursHasUnsplitList = false) {
        if (wiki.Count == 0) return ours.Count == 0 ? AuthorAgreement.Same : AuthorAgreement.WikiNoAuthors;
        if (ours.SequenceEqual(wiki, StringComparer.Ordinal)) return AuthorAgreement.Same;

        var oursLoose = ours.Select(Loose).ToList();
        var wikiLoose = wiki.Select(Loose).ToList();
        if (oursLoose.SequenceEqual(wikiLoose, StringComparer.Ordinal)) return AuthorAgreement.SameIgnoringPunctuation;

        if (!string.IsNullOrEmpty(collaboration)) {
            var withCollaboration = wiki.ToList();
            withCollaboration[^1] = $"{withCollaboration[^1]} ({collaboration})";
            if (withCollaboration.Select(Loose).SequenceEqual(oursLoose, StringComparer.Ordinal)) {
                return AuthorAgreement.WikiCollaborationParameter;
            }
        }

        // The same text, cut into a different number of names.
        if (wiki.Count < ours.Count && ContainsAll(string.Concat(wikiLoose), oursLoose)) return AuthorAgreement.WikiSeveralInOneParameter;
        if (wiki.Count > ours.Count && ContainsAll(string.Concat(oursLoose), wikiLoose)) {
            return oursHasUnsplitList ? AuthorAgreement.OursKeptWhole : AuthorAgreement.WikiSplitsAName;
        }

        if (wiki.Count < ours.Count && oursLoose.Take(wiki.Count).SequenceEqual(wikiLoose, StringComparer.Ordinal)) return AuthorAgreement.WikiFewer;
        if (ours.Count < wiki.Count && wikiLoose.Take(ours.Count).SequenceEqual(oursLoose, StringComparer.Ordinal)) return AuthorAgreement.WikiMore;
        if (ours.Count == wiki.Count
            && oursLoose.OrderBy(s => s, StringComparer.Ordinal).SequenceEqual(wikiLoose.OrderBy(s => s, StringComparer.Ordinal), StringComparer.Ordinal)) {
            return AuthorAgreement.OtherOrder;
        }

        var oursSurnames = ours.Select(Surname).ToList();
        var wikiSurnames = wiki.Select(Surname).ToList();
        if (ours.Count == wiki.Count) {
            if (Enumerable.Range(0, ours.Count).All(i => !wiki[i].Contains(',') && Loose(GivenNameFirst(ours[i])) == wikiLoose[i])) {
                return AuthorAgreement.WikiGivenNameFirst;
            }
            if (wiki.All(w => !w.Contains(',')) && wikiLoose.SequenceEqual(oursSurnames, StringComparer.Ordinal)) {
                return AuthorAgreement.WikiSurnamesOnly;
            }
            if (oursSurnames.SequenceEqual(wikiSurnames, StringComparer.Ordinal)) return AuthorAgreement.OtherInitials;
        }
        return oursSurnames.Intersect(wikiSurnames, StringComparer.Ordinal).Any()
            ? AuthorAgreement.SomeNamesDiffer
            : AuthorAgreement.NoNameInCommon;
    }

    /// Lower case, accents removed, letters and digits only.
    internal static string Loose(string name) {
        var decomposed = name.Normalize(NormalizationForm.FormD);
        var builder = new StringBuilder(decomposed.Length);
        foreach (var c in decomposed) {
            if (char.IsLetterOrDigit(c)) builder.Append(char.ToLowerInvariant(c));
        }
        return builder.ToString();
    }

    private static bool ContainsAll(string joined, IEnumerable<string> parts) =>
        parts.All(p => p.Length > 0 && joined.Contains(p, StringComparison.Ordinal));

    // The loose surname: the text before the first comma.
    private static string Surname(string name) {
        var comma = name.IndexOf(',');
        return Loose(comma < 0 ? name : name[..comma]);
    }

    // "Harold, A." as "A. Harold".
    private static string GivenNameFirst(string name) {
        var comma = name.IndexOf(',');
        return comma < 0 ? name : $"{name[(comma + 1)..].Trim()} {name[..comma].Trim()}";
    }

    private static int? ReadYear(string? value) {
        if (value is null) return null;
        var match = YearPattern.Match(CiteIucnTemplateScanner.PlainText(value));
        return match.Success ? int.Parse(match.Groups["y"].Value, CultureInfo.InvariantCulture) : null;
    }

    private static YearAgreement CompareYear(int? ours, int? wiki) => (ours, wiki) switch {
        (null, null) => YearAgreement.BothMissing,
        (null, _) => YearAgreement.OursMissing,
        (_, null) => YearAgreement.WikiMissing,
        _ => ours == wiki ? YearAgreement.Same : YearAgreement.Different,
    };
}
