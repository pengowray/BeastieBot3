using System.Globalization;
using System.Net;
using System.Text.Json;
using System.Text.RegularExpressions;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.WikidataEdits;

// Reads one cached /api/v4/assessment/{id} payload into the IucnCitationParts the public site
// renders {{cite iucn}} from. Pure: the caller passes the payload, when it was downloaded and,
// for errata versions, the assessments it may have replaced (IucnTaxaHeaders.PredecessorIds).
//
// IUCN's citation has one shape (checked over all 366,029 cached payloads, Oct 2026):
//
//   <authors> <year>. <title>. The IUCN Red List of Threatened Species <year>: e.T<taxon>A<assessment>.
//   [https://dx.doi.org/<doi>.] Accessed on <date>.
//
// The year is year_published. The title is taxon.scientific_name (with "ssp.", "subsp.", "var." or
// "<name> subpopulation" already in it), followed by up to two of IUCN's annotations:
// "(<Region> assessment)" on a regional assessment, then "(amended version of YYYY assessment)" or
// "(errata version published in YYYY)"; three amended and one errata title leave the year out.
// The title is found by searching for "<year>. <scientific name>" from the right, never by splitting
// at the first year: the author list of aid 9303466 contains "Madagascar 2001.".
//
// Authors come from the assessor credit's `full`, which matches the citation's author prefix in all
// but two of 179,191 latest global assessments (both have no assessor credit; the prefix is the
// fallback). They are split by IucnAssessmentCitationParser.SplitCreditNames with the count of
// distinct value[] entries, then read one by one by IucnAuthorNameParser. "et al." is taken off and
// sets AuthorsEtAl; the names before it are split without a count, since value[] counts everyone.
//
// The DOI in the citation goes into the parts only when IucnDoiSelector accepts it for this
// assessment. A payload with no year_published is an unpublished draft and gives no parts.

namespace BeastieBot3.SiteBuild;

/// Why a payload gave no citation parts.
internal enum CitationParseFailure {
    None,
    /// Not a JSON object, or no assessment id or taxon id.
    MissingIds,
    /// No taxon.scientific_name.
    MissingName,
    /// No year_published: IUCN has not published the assessment (42 non-latest rows in Oct 2026).
    Unpublished,
    /// No citation text.
    NoCitation,
    /// The citation doesn't end "The IUCN Red List of Threatened Species YYYY: e.T…A…." (then an
    /// optional DOI and access date).
    CitationFormat,
    /// The citation's e.T…A… names another taxon or assessment than the payload does.
    IdMismatch,
    /// "<year>. <scientific name>" doesn't appear before the Red List sentence.
    TitleMismatch,
    /// Text after the scientific name that is not one of IUCN's annotations.
    UnknownTitleSuffix,
}

/// Where the author names were read from.
internal enum CitationAuthorSource {
    None,
    AssessorCredit,
    /// No assessor credit with a name: the text before the year in the citation.
    CitationPrefix,
}

/// The parts, or why there are none, with what a check report needs to know about how they were read.
internal sealed record IucnCitationParse {
    public IucnCitationParts? Parts { get; init; }
    public CitationParseFailure Failure { get; init; }
    /// The text that failed: the unknown title annotation, or the citation.
    public string? FailureDetail { get; init; }

    public long? AssessmentId { get; init; }
    public long? TaxonId { get; init; }
    public string? ScientificName { get; init; }

    public CitationAuthorSource AuthorSource { get; init; }
    /// The splitter rule for the assessor string; EtAl when "et al." was taken off first.
    public IucnAssessmentCitationParser.CreditSplitRule? SplitRule { get; init; }
    /// One per author, in order.
    public IReadOnlyList<AuthorNameShape> AuthorShapes { get; init; } = [];
    /// The assessor credit and the citation's author prefix differ after HTML and whitespace cleanup.
    public bool CreditDiffersFromCitation { get; init; }
    /// Further assessor credits that added names to the first (a repeated credits block that adds a person).
    public int ExtraAssessorBlocksAddingNames { get; init; }

    /// "(amended version of … assessment)" present, with or without a year.
    public bool HasAmendedAnnotation { get; init; }
    /// "(errata version published in …)" present, with or without a year.
    public bool HasErrataAnnotation { get; init; }
    /// The year after "Threatened Species" differs from year_published.
    public bool VolumeDiffersFromYear { get; init; }
    /// scopes[0].description.en, which IUCN's "(<Region> assessment)" repeats.
    public string? FirstScopeDescription { get; init; }

    /// The DOI in the citation text, canonical when it is an IUCN Red List DOI, else as written.
    public string? CitationDoi { get; init; }
    public DoiVerdict CitationDoiVerdict { get; init; } = DoiVerdict.Missing;
}

internal static class IucnCitationPartsParser {
    private static readonly Regex Tail = new(
        @"^(?<head>.*)\.\s+The IUCN Red List of Threatened Species\s*(?<volume>\d{4})?\s*:\s*e\.T(?<pt>\d+)A(?<pa>\d+)\.?"
        + @"(?:\s*(?<doi>(?:https?://)?(?:dx\.)?doi\.org/\S+?))?\.?\s*(?:(?:Accessed|Downloaded) on\b[^.]*\.?)?\s*$",
        RegexOptions.Singleline | RegexOptions.CultureInvariant | RegexOptions.Compiled);

    private static readonly Regex Annotations = new(
        @"^(?:\((?!amended version of|errata version published in)(?<scope>[^()]+?) assessment\))?\s*"
        + @"(?:\(amended version of\s*(?<amends>\d{4})?\s*assessment\)(?<amended>)|\(errata version published in\s*(?<errata>\d{4})?\s*\)(?<erratum>))?$",
        RegexOptions.CultureInvariant | RegexOptions.Compiled);

    private static readonly Regex EtAl = new(@",?\s*\bet\s+al\b\.?", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private static readonly Regex HtmlTag = new(@"<[^>]*>", RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private static readonly Regex Whitespace = new(@"\s+", RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private static readonly Regex SubpopulationWord = new(@"\s*\bsubpopulation$", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);

    /// The parts of one assessment's citation, or null (see Parse for why).
    public static IucnCitationParts? ParseParts(JsonElement assessment, DateTime? downloadedAtUtc,
        IReadOnlyCollection<long>? predecessorAssessmentIds = null) =>
        Parse(assessment, downloadedAtUtc, predecessorAssessmentIds).Parts;

    public static IucnCitationParse Parse(JsonElement assessment, DateTime? downloadedAtUtc,
        IReadOnlyCollection<long>? predecessorAssessmentIds = null) {
        if (assessment.ValueKind != JsonValueKind.Object) return Fail(CitationParseFailure.MissingIds);

        var taxon = assessment.TryGetProperty("taxon", out var t) && t.ValueKind == JsonValueKind.Object ? t : (JsonElement?)null;
        var assessmentId = ReadLong(assessment, "assessment_id");
        var taxonId = ReadLong(assessment, "sis_taxon_id") ?? (taxon is { } t1 ? ReadLong(t1, "sis_id") : null);
        if (assessmentId is null || taxonId is null) return Fail(CitationParseFailure.MissingIds);

        var result = new IucnCitationParse {
            AssessmentId = assessmentId,
            TaxonId = taxonId,
            FirstScopeDescription = FirstScopeDescription(assessment),
        };
        var scientificName = CollapseWhitespace(taxon is { } t2 ? ReadString(t2, "scientific_name") : null);
        if (scientificName.Length == 0) return result with { Failure = CitationParseFailure.MissingName };
        result = result with { ScientificName = scientificName };

        var yearText = ReadString(assessment, "year_published")?.Trim();
        if (string.IsNullOrEmpty(yearText)
            || !int.TryParse(yearText, NumberStyles.None, CultureInfo.InvariantCulture, out var year)) {
            return result with { Failure = CitationParseFailure.Unpublished };
        }

        var citation = ReadString(assessment, "citation");
        if (string.IsNullOrWhiteSpace(citation)) return result with { Failure = CitationParseFailure.NoCitation };

        var tail = Tail.Match(citation);
        if (!tail.Success) return result with { Failure = CitationParseFailure.CitationFormat, FailureDetail = citation };
        if (tail.Groups["pt"].Value != taxonId.Value.ToString(CultureInfo.InvariantCulture)
            || tail.Groups["pa"].Value != assessmentId.Value.ToString(CultureInfo.InvariantCulture)) {
            return result with { Failure = CitationParseFailure.IdMismatch, FailureDetail = citation };
        }

        // "<authors> <year>. <title>": the title is the last "<year>. <scientific name>".
        var head = CleanText(tail.Groups["head"].Value);
        var titleAt = FindTitle(head, yearText, scientificName);
        if (titleAt < 0) return result with { Failure = CitationParseFailure.TitleMismatch, FailureDetail = citation };
        var authorPrefix = head[..titleAt].Trim();
        var annotationText = head[(titleAt + yearText.Length + 2 + scientificName.Length)..].Trim();
        var annotations = Annotations.Match(annotationText);
        if (!annotations.Success) {
            return result with { Failure = CitationParseFailure.UnknownTitleSuffix, FailureDetail = annotationText };
        }

        var authors = ReadAuthors(assessment, authorPrefix);
        var parts = new IucnCitationParts {
            TaxonId = taxonId.Value,
            AssessmentId = assessmentId.Value,
            Year = year,
            ScientificName = scientificName,
            SubpopulationName = SubpopulationName(taxon),
            RegionalScope = NullIfEmpty(annotations.Groups["scope"].Value),
            ErrataYear = ParseYear(annotations.Groups["errata"].Value),
            AmendsYear = ParseYear(annotations.Groups["amends"].Value),
            Authors = authors.Names.Select(n => n.Author).ToList(),
            AuthorsEtAl = authors.EtAl,
            IucnCitationText = CleanText(IucnAssessmentCitationParser.StripAccessedOn(citation)),
            DownloadedAtUtc = downloadedAtUtc,
        };

        var rawDoi = NullIfEmpty(tail.Groups["doi"].Value);
        var verdict = IucnDoiSelector.Check(parts, rawDoi, predecessorAssessmentIds);
        if (IucnDoiSelector.IsAccepted(verdict)) {
            parts = parts with { Doi = IucnDoiSelector.Normalise(rawDoi), DoiSource = DoiSource.Citation };
        }

        return result with {
            Parts = parts,
            AuthorSource = authors.Source,
            SplitRule = authors.Rule,
            AuthorShapes = authors.Names.Select(n => n.Shape).ToList(),
            CreditDiffersFromCitation = authors.Source == CitationAuthorSource.AssessorCredit
                && !string.Equals(authors.CreditText, CleanText(authorPrefix), StringComparison.Ordinal),
            ExtraAssessorBlocksAddingNames = authors.ExtraBlocksAddingNames,
            HasAmendedAnnotation = annotations.Groups["amended"].Success,
            HasErrataAnnotation = annotations.Groups["erratum"].Success,
            VolumeDiffersFromYear = tail.Groups["volume"].Value != yearText,
            CitationDoi = rawDoi is null ? null : IucnDoiSelector.Normalise(rawDoi) ?? rawDoi,
            CitationDoiVerdict = verdict,
        };
    }

    private static IucnCitationParse Fail(CitationParseFailure failure) => new() { Failure = failure };

    // ------------------------------------------------------------ title

    /// Where "<year>. <scientific name>" starts in the cleaned head, searching from the right; -1 when absent.
    private static int FindTitle(string head, string year, string scientificName) {
        var needle = $"{year}. {scientificName}";
        var at = head.Length;
        while (at > 0) {
            at = head.LastIndexOf(needle, at - 1, StringComparison.Ordinal);
            if (at < 0) return -1;
            var startsWord = at == 0 || head[at - 1] == ' ';
            var end = at + needle.Length;
            var endsWord = end == head.Length || head[end] == ' ';
            if (startsWord && endsWord) return at;
        }
        return -1;
    }

    private static string? SubpopulationName(JsonElement? taxon) {
        if (taxon is not { } t) return null;
        var name = CollapseWhitespace(ReadString(t, "subpopulation_name"));
        if (name.Length == 0) return null;
        return NullIfEmpty(SubpopulationWord.Replace(name, string.Empty).Trim());
    }

    private static int? ParseYear(string text) =>
        int.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out var year) ? year : null;

    // ------------------------------------------------------------ authors

    private sealed record AuthorRead(
        IReadOnlyList<ParsedAuthorName> Names,
        bool EtAl,
        CitationAuthorSource Source,
        IucnAssessmentCitationParser.CreditSplitRule? Rule,
        string? CreditText,
        int ExtraBlocksAddingNames);

    private static AuthorRead ReadAuthors(JsonElement assessment, string authorPrefix) {
        var blocks = AssessorCredits(assessment);
        string text;
        int? count;
        CitationAuthorSource source;
        if (blocks.Count > 0) {
            text = ReadString(blocks[0], "full")!;
            count = IucnAssessmentCitationParser.DistinctValueCount(blocks[0]);
            source = CitationAuthorSource.AssessorCredit;
        } else if (authorPrefix.Length > 0) {
            text = authorPrefix;
            count = null;
            source = CitationAuthorSource.CitationPrefix;
        } else {
            return new AuthorRead(Array.Empty<ParsedAuthorName>(), false, CitationAuthorSource.None, null, null, 0);
        }

        var cleaned = CleanText(text);
        var etAl = EtAl.IsMatch(cleaned);
        List<string> names;
        IucnAssessmentCitationParser.CreditSplitRule rule;
        var extraBlocks = 0;
        if (etAl) {
            var before = EtAl.Replace(cleaned, string.Empty).Trim().TrimEnd(',', ';', '&').Trim();
            names = IucnAssessmentCitationParser.SplitCreditNames(before, null).ToList();
            rule = IucnAssessmentCitationParser.CreditSplitRule.EtAl;
        } else {
            var split = IucnAssessmentCitationParser.SplitCreditNamesWithRule(text, count);
            names = split.Names.ToList();
            rule = split.Rule;
            foreach (var block in blocks.Skip(1)) {
                var before = names.Count;
                IucnAssessmentCitationParser.AddNamesNotYetHeld(names,
                    IucnAssessmentCitationParser.SplitCreditNames(ReadString(block, "full")!, IucnAssessmentCitationParser.DistinctValueCount(block)));
                if (names.Count > before) extraBlocks++;
            }
        }

        var parsed = names.Select(IucnAuthorNameParser.Parse).ToList();
        return new AuthorRead(parsed, etAl, source, rule,
            source == CitationAuthorSource.AssessorCredit ? cleaned : null, extraBlocks);
    }

    private static List<JsonElement> AssessorCredits(JsonElement assessment) {
        var blocks = new List<JsonElement>();
        if (!assessment.TryGetProperty("credits", out var credits) || credits.ValueKind != JsonValueKind.Array) return blocks;
        foreach (var credit in credits.EnumerateArray()) {
            if (credit.ValueKind != JsonValueKind.Object) continue;
            var type = ReadString(credit, "credit_type_name")?.Trim();
            if (!string.Equals(type, "assessor", StringComparison.OrdinalIgnoreCase)) continue;
            if (string.IsNullOrWhiteSpace(ReadString(credit, "full"))) continue;
            blocks.Add(credit);
        }
        return blocks;
    }

    // ------------------------------------------------------------ text helpers

    /// HTML tags removed ("<i>et al.</i>"), entities decoded, whitespace (newlines, no-break and
    /// narrow no-break spaces) collapsed to single spaces, trimmed.
    internal static string CleanText(string? text) {
        if (string.IsNullOrEmpty(text)) return string.Empty;
        return CollapseWhitespace(WebUtility.HtmlDecode(HtmlTag.Replace(text, string.Empty)));
    }

    private static string CollapseWhitespace(string? text) =>
        string.IsNullOrEmpty(text) ? string.Empty : Whitespace.Replace(text, " ").Trim();

    private static string? NullIfEmpty(string? text) => string.IsNullOrWhiteSpace(text) ? null : text.Trim();

    private static string? FirstScopeDescription(JsonElement assessment) {
        if (!assessment.TryGetProperty("scopes", out var scopes) || scopes.ValueKind != JsonValueKind.Array) return null;
        foreach (var scope in scopes.EnumerateArray()) {
            if (scope.ValueKind == JsonValueKind.Object
                && scope.TryGetProperty("description", out var description)
                && description.ValueKind == JsonValueKind.Object) {
                return NullIfEmpty(ReadString(description, "en"));
            }
            return null;
        }
        return null;
    }

    private static long? ReadLong(JsonElement element, string property) {
        if (!element.TryGetProperty(property, out var value)) return null;
        return value.ValueKind switch {
            JsonValueKind.Number when value.TryGetInt64(out var n) => n,
            JsonValueKind.String when long.TryParse(value.GetString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var n) => n,
            _ => null,
        };
    }

    private static string? ReadString(JsonElement element, string property) {
        if (!element.TryGetProperty(property, out var value)) return null;
        return value.ValueKind switch {
            JsonValueKind.String => value.GetString(),
            JsonValueKind.Number => value.GetRawText(),
            _ => null,
        };
    }
}
