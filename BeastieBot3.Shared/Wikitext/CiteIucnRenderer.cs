using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;

namespace BeastieBot3.Shared.Wikitext;

// {{cite iucn}} for one assessment. The parameter order follows {{make cite IUCN}}, the tool
// Template:Cite IUCN/doc points editors to (Module:Cite IUCN, make_cite_iucn):
//   authors, |display-authors=, |name-list-style=, |year=, |errata=, |amends=, |title=, |volume=,
//   |article-number=, |doi=, |access-date=
// |page= is deprecated (a maintenance message) and |url= is built by the module from the article
// number, so neither is written. |language= is left to the module, which reads it from the DOI.
//
// Module:Cite IUCN (revision of 2026-05-12) shows a red error and adds Category:Cite IUCN errors when:
// - the title still holds "(errata version published in YYYY)" or "(amended version of YYYY
//   assessment)": those become |errata= and |amends=;
// - the DOI does not end in T<taxon>A<assessment>.<en|es|fr|pt>;
// - the DOI's taxon id differs from the article number's, or its assessment id differs and |errata=
//   is not set (an errata citation's DOI carries the assessment it corrects).
// The DOI is left out in those cases rather than producing an error. With both |errata= and |amends=
// the module handles only errata and passes |amends= on to CS1 as an unknown parameter, so only
// |errata= is written then.

public static partial class CiteIucnRenderer {
    /// {{cite iucn}} for one assessment, on one line.
    public static string Render(IucnCitationParts parts, CiteIucnOptions? options = null) {
        options ??= new CiteIucnOptions();

        var rawName = WikitextValue.Clean(parts.ScientificName);
        rawName = StripAnnotations(rawName, out var errataFromTitle, out var amendsFromTitle, out var scopeFromTitle);
        var errataYear = parts.ErrataYear ?? errataFromTitle;
        var amendsYear = errataYear is null ? parts.AmendsYear ?? amendsFromTitle : null;
        var scope = WikitextValue.Clean(parts.RegionalScope);
        if (scope.Length == 0) {
            scope = scopeFromTitle ?? string.Empty;
        }

        var title = ScientificNameMarkup.ToWikitext(rawName, WikitextValue.Clean(parts.SubpopulationName));
        if (scope.Length > 0) {
            title += $" ({scope} assessment)";
        }

        var p = new List<(string Name, string Value)>();
        var authorCount = AddAuthors(p, parts, options.AuthorStyle, options.FullGivenNames, out var etAlInNames);
        if (authorCount > 0 && (parts.AuthorsEtAl || etAlInNames)) {
            p.Add(("display-authors", "etal"));
        }
        if (authorCount > 1 && options.NameListStyleAmp) {
            p.Add(("name-list-style", "amp"));
        }

        var year = parts.Year.ToString(CultureInfo.InvariantCulture);
        p.Add(("year", year));
        if (errataYear is not null) {
            p.Add(("errata", errataYear.Value.ToString(CultureInfo.InvariantCulture)));
        }
        if (amendsYear is not null) {
            p.Add(("amends", amendsYear.Value.ToString(CultureInfo.InvariantCulture)));
        }
        p.Add(("title", title));
        p.Add(("volume", year));
        p.Add(("article-number", parts.ArticleNumber));

        var doi = AcceptableDoi(parts.Doi, parts.TaxonId, parts.AssessmentId, errataYear is not null);
        if (doi is not null) {
            p.Add(("doi", doi));
        }
        if (options.AccessDate is { } accessed) {
            p.Add(("access-date", accessed.ToString("d MMMM yyyy", CultureInfo.InvariantCulture)));
        }

        var sb = new StringBuilder("{{cite iucn");
        foreach (var (name, value) in p) {
            sb.Append(" |").Append(name).Append('=').Append(value);
        }
        sb.Append("}}");
        var template = sb.ToString();

        if (!options.WrapInRef) {
            return template;
        }
        var refName = SanitizeRefName(options.RefName);
        return refName.Length == 0
            ? $"<ref>{template}</ref>"
            : $"<ref name=\"{refName}\">{template}</ref>";
    }

    // Writes the author parameters and returns how many authors were written. An "et al." written
    // inside a name (IUCN's "Jaffré, T. <i>et al.</i>") is removed from the name and reported, since
    // CS1 flags a name containing it.
    //
    // With fullGivenNames, a person with GivenNames is named by them instead of the initials:
    // |author=Sayer, Catherine, or |last1=Sayer |first1=Catherine. A generational suffix follows the
    // given names as it follows the initials ("Lowry, Porter P. II"). A name IUCN wrote initials
    // first ("N.H. Rakotoarivelo") becomes "Rakotoarivelo, Nirina Hasina". Everyone else keeps
    // IUCN's form.
    private static int AddAuthors(List<(string Name, string Value)> p, IucnCitationParts parts,
        CiteAuthorStyle style, bool fullGivenNames, out bool etAlInNames) {
        etAlInNames = false;
        var n = 0;
        foreach (var author in parts.Authors) {
            var display = StripEtAl(WikitextValue.Clean(author.Display), ref etAlInNames);
            var last = StripEtAl(WikitextValue.Clean(author.Last), ref etAlInNames);
            var initials = WikitextValue.Clean(author.Initials);
            var given = fullGivenNames && author.Kind == CitationAuthorKind.Person && last.Length > 0
                ? WikitextValue.Clean(author.GivenNames)
                : string.Empty;
            // The given names with the suffix the initials carry ("Porter P., II").
            var first = given.Length > 0 ? WithSuffixOf(given, initials) : initials;

            if (style == CiteAuthorStyle.LastFirst && author.Kind == CitationAuthorKind.Person && last.Length > 0) {
                n++;
                p.Add(($"last{n}", last));
                if (first.Length > 0) {
                    p.Add(($"first{n}", SuffixWithoutComma(first)));
                }
                continue;
            }
            if (given.Length > 0) {
                display = $"{last}, {SuffixWithoutComma(first)}";
            }

            if (display.Length == 0) {
                // Fall back to the parsed parts when the published form is missing.
                display = initials.Length > 0 && last.Length > 0 ? $"{last}, {initials}" : last;
            }
            if (display.Length == 0) {
                continue;
            }
            if (NeedsAcceptAsWritten(author.Kind, display)) {
                // ((...)) tells CS1 to take the name as written, so "Royal Botanic Gardens, Kew, UK" or
                // "Golamco, A., Jr." is not reported as several names run together, and a workshop
                // name with a date is not reported as a numeric name.
                display = $"(({display}))";
            }
            n++;
            var name = style == CiteAuthorStyle.AuthorN && n == 1 ? "author" : $"author{n}";
            p.Add((name, display));
        }
        return n;
    }

    // IucnCitationParts stores a generational suffix after the initials with a comma ("P.P., II",
    // "A., Jr."). CS1 allows no comma at all in |firstN= and adds "CS1 maint: multiple names" for one,
    // so the suffix is written as MOS:JR does: "P.P. II", "A. Jr.". Done here rather than in the parser
    // so that site databases built before this change render correctly too.
    private static string SuffixWithoutComma(string initials) => TrailingSuffix().Replace(initials, " ${suffix}");

    // The given names with the generational suffix the initials end with, in the stored form
    // ("Porter P., II").
    private static string WithSuffixOf(string given, string initials) {
        var suffix = TrailingSuffix().Match(initials);
        return suffix.Success ? $"{given}, {suffix.Groups["suffix"].Value}" : given;
    }

    // CS1 reports "multiple names" for a name with more than one comma or any semicolon, and "numeric
    // names" for one containing a digit. Those are false alarms for an organisation or for one person
    // whose name the parser recognised; a verbatim name with several commas probably is several
    // names, so it keeps CS1's warning.
    private static bool NeedsAcceptAsWritten(CitationAuthorKind kind, string name) {
        if (kind == CitationAuthorKind.Verbatim) {
            return false;
        }
        var severalNames = name.Count(c => c == ',') > 1 || name.Contains(';');
        var numeric = kind == CitationAuthorKind.Organisation && name.Any(char.IsDigit);
        return severalNames || numeric;
    }

    private static string StripEtAl(string name, ref bool found) {
        var match = EtAl().Match(name);
        if (!match.Success) {
            return name;
        }
        found = true;
        return name[..match.Index].TrimEnd(' ', ',', ';');
    }

    // Removes IUCN's title annotations and returns what they said. Each one is removed wherever it
    // appears, so a name that arrives with them (contrary to IucnCitationParts' contract) still
    // renders without the module's "title has extraneous text" error.
    private static string StripAnnotations(string name, out int? errataYear, out int? amendsYear, out string? scope) {
        errataYear = null;
        amendsYear = null;
        scope = null;

        var errata = ErrataAnnotation().Match(name);
        if (errata.Success) {
            errataYear = ParseYear(errata.Groups[1].Value);
            name = name.Remove(errata.Index, errata.Length);
        }
        var amends = AmendsAnnotation().Match(name);
        if (amends.Success) {
            amendsYear = ParseYear(amends.Groups[1].Value);
            name = name.Remove(amends.Index, amends.Length);
        }
        var regional = ScopeAnnotation().Match(name);
        if (regional.Success) {
            scope = regional.Groups[1].Value.Trim();
            name = name.Remove(regional.Index, regional.Length);
        }
        return WikitextValue.CollapseWhitespace(name);
    }

    private static int? ParseYear(string text) =>
        int.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out var year) ? year : null;

    // The DOI as {{cite iucn}} will accept it, or null. Resolver prefixes are removed; the rest must
    // end like Module:Cite IUCN's pattern and name this assessment (or, for an errata version, the
    // assessment it corrects, which has the same taxon id).
    private static string? AcceptableDoi(string? doi, long taxonId, long assessmentId, bool isErrata) {
        if (string.IsNullOrWhiteSpace(doi)) {
            return null;
        }
        var value = DoiPrefix().Replace(doi.Trim(), string.Empty);
        var match = IucnDoi().Match(value);
        if (!match.Success) {
            return null;
        }
        if (!long.TryParse(match.Groups["taxon"].Value, NumberStyles.None, CultureInfo.InvariantCulture, out var doiTaxon)
            || !long.TryParse(match.Groups["assessment"].Value, NumberStyles.None, CultureInfo.InvariantCulture, out var doiAssessment)) {
            return null;
        }
        if (doiTaxon != taxonId) {
            return null;
        }
        if (doiAssessment != assessmentId && !isErrata) {
            return null;
        }
        return value;
    }

    // Quotes and angle brackets would end the attribute or the tag; "/" would make <ref name=x/>.
    // Cite rejects a name made only of the digits 0-9 ("Cite error: The <ref> tag name cannot be a
    // simple integer"), so such a name gets a prefix rather than being dropped, which would break any
    // reuse the editor meant: "1" becomes "iucn-1".
    internal static string SanitizeRefName(string? name) {
        if (string.IsNullOrWhiteSpace(name)) {
            return string.Empty;
        }
        var sb = new StringBuilder(name.Length);
        foreach (var c in name) {
            if (c is '"' or '\'' or '<' or '>' or '/') {
                continue;
            }
            sb.Append(c);
        }
        var cleaned = WikitextValue.CollapseWhitespace(sb.ToString());
        return cleaned.Length > 0 && cleaned.All(char.IsAsciiDigit) ? "iucn-" + cleaned : cleaned;
    }

    [GeneratedRegex(@"\s*\(errata version published in\s*(\d{4})?\s*\)", RegexOptions.IgnoreCase)]
    private static partial Regex ErrataAnnotation();

    [GeneratedRegex(@"\s*\(amended version of\s*(\d{4})?\s*assessment\)", RegexOptions.IgnoreCase)]
    private static partial Regex AmendsAnnotation();

    // "(Europe assessment)"; not "(Green Status assessment)", which is a different kind of assessment
    // and stays in the title.
    [GeneratedRegex(@"\s*\((?!Green Status\b)([^()]+?)\s+assessment\)\s*$", RegexOptions.IgnoreCase)]
    private static partial Regex ScopeAnnotation();

    // CS1's own "et al." patterns, simplified: "et al", "et al.", "et alii", "and others" at the end.
    [GeneratedRegex(@"[;,]?\s*\b(?:et\.?\s*al(?:ii|ia|iae)?\.?|and others)\s*$", RegexOptions.IgnoreCase)]
    private static partial Regex EtAl();

    // The suffixes the site-build name parser recognises (IucnAuthorNameParser.SuffixPattern).
    [GeneratedRegex(@"\s*,\s*(?<suffix>(?:Jr|Jnr|Sr|Snr|II|III|IV)\.?)$")]
    private static partial Regex TrailingSuffix();

    [GeneratedRegex(@"^(?:https?://(?:dx\.)?doi\.org/|doi:\s*)", RegexOptions.IgnoreCase)]
    private static partial Regex DoiPrefix();

    // 10.<registrant>/<suffix ending in T<taxon>A<assessment>.<lang>>, as Module:Cite IUCN requires.
    [GeneratedRegex(@"^10\.\d{4,9}/\S+?[Tt](?<taxon>\d+)[Aa](?<assessment>\d+)\.(?:en|es|fr|pt)$")]
    private static partial Regex IucnDoi();
}
