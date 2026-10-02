using System.Globalization;
using System.Text;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.WikidataEdits;

// Counts and examples for `site check-citations`, and the Markdown report built from them.
// CitationCheckTally is filled as the command reads the cache; CitationCheckReport only formats.

namespace BeastieBot3.SiteBuild;

/// Which scope a latest assessment has, from the taxa header.
internal enum AssessmentScopeKind { Global, Regional, NoScope }

internal sealed class CitationCheckTally {
    public CitationCheckTally(int examplesPerClass) {
        ExamplesPerClass = examplesPerClass;
    }

    public int ExamplesPerClass { get; }

    // ------------------------------------------------------------ input

    public int TaxaRows { get; set; }
    public int TaxaRowsUnreadable { get; set; }
    public Dictionary<AssessmentScopeKind, int> LatestByScope { get; } = new();
    public int LatestSkippedByLimit { get; set; }
    public int PayloadMissing { get; set; }
    public int PayloadUnreadable { get; set; }
    /// The assessment payload says latest=false although the taxa header says latest.
    public int PayloadLatestFlagFalse { get; set; }

    // ------------------------------------------------------------ parsing

    public int Parsed { get; private set; }
    public Dictionary<AssessmentScopeKind, int> ParsedByScope { get; } = new();
    public Dictionary<CitationParseFailure, int> Failures { get; } = new();
    public Dictionary<CitationParseFailure, List<string>> FailureExamples { get; } = new();

    public Dictionary<CitationAuthorSource, int> AuthorSources { get; } = new();
    public Dictionary<IucnAssessmentCitationParser.CreditSplitRule, int> SplitRules { get; } = new();
    public Dictionary<IucnAssessmentCitationParser.CreditSplitRule, List<string>> SplitRuleExamples { get; } = new();
    public Dictionary<CitationAuthorKind, int> AuthorKinds { get; } = new();
    public Dictionary<AuthorNameShape, int> AuthorShapes { get; } = new();
    /// Distinct names read as Verbatim, with how often each occurs, by shape.
    public Dictionary<AuthorNameShape, Dictionary<string, int>> VerbatimNames { get; } = new();
    public int AllAuthorsStructured { get; private set; }
    public int SomeAuthorsVerbatim { get; private set; }
    public int NoAuthors { get; private set; }
    public int EtAl { get; private set; }
    public int MoreThan8Authors { get; private set; }
    public int MoreThan20Authors { get; private set; }
    public int MaxAuthors { get; private set; }
    public int RepeatedNamesKept { get; private set; }
    public List<string> RepeatedNameExamples { get; } = new();
    public int CreditDiffersFromCitation { get; private set; }
    public List<string> CreditDiffersExamples { get; } = new();
    public int ExtraAssessorBlocks { get; private set; }

    public int Regional { get; private set; }
    public int RegionalScopeDiffers { get; private set; }
    public List<string> RegionalScopeExamples { get; } = new();
    public int AmendedWithYear { get; private set; }
    public int AmendedNoYear { get; private set; }
    public int ErrataWithYear { get; private set; }
    public int ErrataNoYear { get; private set; }
    public int Subpopulations { get; private set; }
    public int VolumeDiffers { get; private set; }

    public Dictionary<DoiVerdict, int> CitationDoiVerdicts { get; } = new();
    public Dictionary<DoiVerdict, List<string>> CitationDoiExamples { get; } = new();

    // ------------------------------------------------------------ en-wiki

    public bool WikiCompared { get; set; }
    public int WikiPagesScanned { get; set; }
    public int WikiPagesWithTemplate { get; set; }
    public int WikiTemplates { get; set; }
    public int WikiTemplatesWithoutIds { get; set; }
    /// Templates whose assessment is not one of the latest assessments read (older assessments,
    /// or left out by --limit).
    public int WikiTemplatesOtherAssessment { get; set; }
    public int WikiTemplatesMatched { get; private set; }
    public HashSet<long> WikiAssessmentsMatched { get; } = new();

    public Dictionary<AuthorAgreement, int> AuthorAgreements { get; } = new();
    public Dictionary<AuthorAgreement, List<string>> AuthorAgreementExamples { get; } = new();
    public Dictionary<DoiAgreement, int> DoiAgreements { get; } = new();
    public Dictionary<(DoiAgreement, DoiVerdict), int> DoiAgreementVerdicts { get; } = new();
    public Dictionary<string, List<string>> DoiExamples { get; } = new();
    public int DoiDifferOnlyInRelease { get; private set; }
    public Dictionary<YearAgreement, int> YearAgreements { get; } = new();
    public Dictionary<YearAgreement, int> ErrataAgreements { get; } = new();
    public Dictionary<YearAgreement, int> AmendsAgreements { get; } = new();
    public Dictionary<string, List<string>> YearExamples { get; } = new();

    public void AddLatest(AssessmentScopeKind scope) => Increment(LatestByScope, scope);

    public void AddParse(IucnCitationParse parse, AssessmentScopeKind scope) {
        var parts = parse.Parts;
        if (parts is null) {
            Increment(Failures, parse.Failure);
            AddExample(FailureExamples, parse.Failure,
                $"{parse.AssessmentId} {parse.ScientificName}: {Shorten(parse.FailureDetail)}");
            return;
        }
        Parsed++;
        Increment(ParsedByScope, scope);
        Increment(AuthorSources, parse.AuthorSource);
        if (parse.SplitRule is { } rule) {
            Increment(SplitRules, rule);
            AddExample(SplitRuleExamples, rule, $"{parts.AssessmentId}: {AuthorList(parts)}");
        }

        var structured = true;
        for (var i = 0; i < parts.Authors.Count; i++) {
            var author = parts.Authors[i];
            Increment(AuthorKinds, author.Kind);
            var shape = i < parse.AuthorShapes.Count ? parse.AuthorShapes[i] : AuthorNameShape.Unknown;
            Increment(AuthorShapes, shape);
            if (author.Kind == CitationAuthorKind.Verbatim) {
                structured = false;
                if (!VerbatimNames.TryGetValue(shape, out var names)) VerbatimNames[shape] = names = new();
                names[author.Display] = names.GetValueOrDefault(author.Display) + 1;
            }
        }
        if (parts.Authors.Count == 0) NoAuthors++;
        else if (structured) AllAuthorsStructured++;
        else SomeAuthorsVerbatim++;
        if (parts.AuthorsEtAl) EtAl++;
        if (parts.Authors.Count > 8) MoreThan8Authors++;
        if (parts.Authors.Count > 20) MoreThan20Authors++;
        MaxAuthors = Math.Max(MaxAuthors, parts.Authors.Count);
        if (parts.Authors.GroupBy(a => a.Display, StringComparer.Ordinal).Any(g => g.Count() > 1)) {
            RepeatedNamesKept++;
            AddExample(RepeatedNameExamples, $"{parts.AssessmentId} {parts.ScientificName}: {AuthorList(parts)}");
        }
        if (parse.CreditDiffersFromCitation) {
            CreditDiffersFromCitation++;
            AddExample(CreditDiffersExamples, $"{parts.AssessmentId}: {Shorten(parts.IucnCitationText)}");
        }
        if (parse.ExtraAssessorBlocksAddingNames > 0) ExtraAssessorBlocks++;

        if (parts.RegionalScope is not null) {
            Regional++;
            if (!string.Equals(parts.RegionalScope, parse.FirstScopeDescription, StringComparison.Ordinal)) {
                RegionalScopeDiffers++;
                AddExample(RegionalScopeExamples, $"{parts.AssessmentId}: title \"{parts.RegionalScope}\", scopes[0] \"{parse.FirstScopeDescription}\"");
            }
        }
        if (parse.HasAmendedAnnotation) {
            if (parts.AmendsYear is null) AmendedNoYear++; else AmendedWithYear++;
        }
        if (parse.HasErrataAnnotation) {
            if (parts.ErrataYear is null) ErrataNoYear++; else ErrataWithYear++;
        }
        if (parts.SubpopulationName is not null) Subpopulations++;
        if (parse.VolumeDiffersFromYear) VolumeDiffers++;

        Increment(CitationDoiVerdicts, parse.CitationDoiVerdict);
        if (parse.CitationDoiVerdict != DoiVerdict.Missing) {
            AddExample(CitationDoiExamples, parse.CitationDoiVerdict,
                $"{parts.AssessmentId} {parts.ScientificName}{Annotation(parts)}: {parse.CitationDoi}");
        }
    }

    public void AddComparison(IucnCitationParts parts, string articleTitle, WikiCitationComparison comparison) {
        WikiTemplatesMatched++;
        WikiAssessmentsMatched.Add(parts.AssessmentId);
        var where = $"[[{articleTitle}]] {parts.AssessmentId} {parts.ScientificName}{Annotation(parts)}";

        Increment(AuthorAgreements, comparison.Authors);
        if (comparison.Authors is not (AuthorAgreement.Same or AuthorAgreement.SameIgnoringPunctuation)) {
            AddExample(AuthorAgreementExamples, comparison.Authors,
                $"{where}: ours \"{string.Join(" | ", comparison.OurAuthors)}\"; wiki \"{string.Join(" | ", comparison.WikiAuthors)}\"");
        }

        Increment(DoiAgreements, comparison.Doi);
        Increment(DoiAgreementVerdicts, (comparison.Doi, comparison.WikiDoiVerdict));
        if (comparison.Doi == DoiAgreement.Different && comparison.DoiDiffersOnlyInRelease) DoiDifferOnlyInRelease++;
        if (comparison.Doi is not (DoiAgreement.Same or DoiAgreement.BothNone)) {
            var key = comparison.Doi is DoiAgreement.WikiOnly or DoiAgreement.Different
                ? $"{comparison.Doi}: {comparison.WikiDoiVerdict}"
                : comparison.Doi.ToString();
            AddExample(DoiExamples, key, $"{where}: ours {parts.Doi ?? "none"}; wiki {comparison.WikiDoi ?? "none"}");
        }

        Increment(YearAgreements, comparison.Year);
        Increment(ErrataAgreements, comparison.Errata);
        Increment(AmendsAgreements, comparison.Amends);
        AddYearExample("Year", "|year= or |date=", comparison.Year, parts.Year, comparison.WikiYear, where);
        AddYearExample("Errata", "|errata=", comparison.Errata, parts.ErrataYear, comparison.WikiErrata, where);
        AddYearExample("Amends", "|amends=", comparison.Amends, parts.AmendsYear, comparison.WikiAmends, where);
    }

    private void AddYearExample(string what, string parameter, YearAgreement agreement, int? ours, int? wiki, string where) {
        var key = agreement switch {
            YearAgreement.Different => $"{what}: {parameter} differs from IUCN's",
            YearAgreement.WikiMissing => $"{what}: the wiki has no {parameter}",
            YearAgreement.OursMissing => $"{what}: the wiki has {parameter} but IUCN's citation has no such year",
            _ => null,
        };
        if (key is null) return;
        AddExample(YearExamples, key, $"{where}: IUCN {ours?.ToString(CultureInfo.InvariantCulture) ?? "none"}, wiki {wiki?.ToString(CultureInfo.InvariantCulture) ?? "none"}");
    }

    internal static string AuthorList(IucnCitationParts parts) {
        var names = string.Join(" | ", parts.Authors.Select(a => $"{a.Display} ({KindLetter(a.Kind)})"));
        return parts.AuthorsEtAl ? $"{names} | et al." : names;
    }

    private static string KindLetter(CitationAuthorKind kind) => kind switch {
        CitationAuthorKind.Person => "P",
        CitationAuthorKind.Organisation => "O",
        _ => "V",
    };

    private static string Annotation(IucnCitationParts parts) {
        var notes = new List<string>();
        if (parts.RegionalScope is not null) notes.Add(parts.RegionalScope);
        if (parts.ErrataYear is not null) notes.Add($"errata {parts.ErrataYear}");
        if (parts.AmendsYear is not null) notes.Add($"amends {parts.AmendsYear}");
        return notes.Count == 0 ? string.Empty : $" ({string.Join(", ", notes)})";
    }

    private static string Shorten(string? text) {
        if (string.IsNullOrEmpty(text)) return string.Empty;
        var flat = text.Replace('\n', ' ').Replace('\r', ' ');
        return flat.Length <= 300 ? flat : flat[..300] + "…";
    }

    private static void Increment<TKey>(Dictionary<TKey, int> counts, TKey key) where TKey : notnull =>
        counts[key] = counts.GetValueOrDefault(key) + 1;

    private void AddExample<TKey>(Dictionary<TKey, List<string>> examples, TKey key, string example) where TKey : notnull {
        if (!examples.TryGetValue(key, out var list)) examples[key] = list = new List<string>();
        AddExample(list, example);
    }

    private void AddExample(List<string> list, string example) {
        if (list.Count < ExamplesPerClass) list.Add(example);
    }
}

internal sealed record CitationCheckInputs(
    string ApiCachePath,
    string? WikiCachePath,
    int? Limit,
    DateTimeOffset Generated,
    TimeSpan Elapsed);

internal static class CitationCheckReport {
    public static string Build(CitationCheckTally t, CitationCheckInputs inputs) {
        var sb = new StringBuilder();
        var latest = t.LatestByScope.Values.Sum();
        var read = latest - t.PayloadMissing - t.PayloadUnreadable;
        var failed = t.Failures.Values.Sum();

        sb.AppendLine("# IUCN citation parts check");
        sb.AppendLine();
        sb.AppendLine("Checks how `site build-db` will read IUCN's citation of each latest assessment into the parts the public site uses for");
        sb.AppendLine("`{{cite iucn}}`: the authors, the title annotations and the DOI. It then compares those parts with the `{{cite iucn}}`");
        sb.AppendLine("templates in cached English Wikipedia articles that cite the same assessment.");
        sb.AppendLine();
        sb.AppendLine($"- **Generated:** {inputs.Generated:yyyy-MM-dd HH:mm} ({inputs.Elapsed.TotalSeconds:F0} s)");
        sb.AppendLine($"- **IUCN API cache:** `{inputs.ApiCachePath}`");
        sb.AppendLine($"- **Wikipedia cache:** {(inputs.WikiCachePath is null ? "not read" : $"`{inputs.WikiCachePath}`")}");
        if (inputs.Limit is { } limit) sb.AppendLine($"- **Limit:** the first {N(limit)} latest assessments; {N(t.LatestSkippedByLimit)} more were not read");
        sb.AppendLine();

        sb.AppendLine("## Summary");
        sb.AppendLine();
        sb.AppendLine("| Measure | Count | Share |");
        sb.AppendLine("| --- | ---: | ---: |");
        Row(sb, "Latest assessments read", read, null);
        Row(sb, "Read into citation parts", t.Parsed, read);
        Row(sb, "Not read into parts (see Parse failures)", failed, read);
        Row(sb, "Every author read as a person or organisation", t.AllAuthorsStructured, t.Parsed);
        Row(sb, "At least one author kept as published", t.SomeAuthorsVerbatim, t.Parsed);
        Row(sb, "DOI in IUCN's citation, accepted", Count(t.CitationDoiVerdicts, DoiVerdict.Accepted) + Count(t.CitationDoiVerdicts, DoiVerdict.AcceptedPredecessor), t.Parsed);
        if (t.WikiCompared) {
            var sameAuthors = Count(t.AuthorAgreements, AuthorAgreement.Same);
            var looseAuthors = sameAuthors + Count(t.AuthorAgreements, AuthorAgreement.SameIgnoringPunctuation);
            var withAuthors = t.WikiTemplatesMatched - Count(t.AuthorAgreements, AuthorAgreement.WikiNoAuthors);
            var bothDoi = Count(t.DoiAgreements, DoiAgreement.Same) + Count(t.DoiAgreements, DoiAgreement.Different);
            Row(sb, "En-wiki {{cite iucn}} templates citing an assessment read here", t.WikiTemplatesMatched, null);
            Row(sb, "… author lists identical (of templates with authors)", sameAuthors, withAuthors);
            Row(sb, "… author lists the same ignoring punctuation", looseAuthors, withAuthors);
            Row(sb, "… DOIs identical (where both have one)", Count(t.DoiAgreements, DoiAgreement.Same), bothDoi);
        }
        sb.AppendLine();

        Input(sb, t, latest);
        Failures(sb, t);
        Authors(sb, t);
        Titles(sb, t);
        Dois(sb, t);
        if (t.WikiCompared) Wiki(sb, t);
        return sb.ToString();
    }

    private static void Input(StringBuilder sb, CitationCheckTally t, int latest) {
        sb.AppendLine("## Input");
        sb.AppendLine();
        sb.AppendLine("\"Latest\" comes from the assessment list in each cached taxon record, not from the flag in the assessment's own");
        sb.AppendLine("record, which is out of date when the record was downloaded before a newer assessment was published.");
        sb.AppendLine();
        sb.AppendLine("| | Count |");
        sb.AppendLine("| --- | ---: |");
        sb.AppendLine($"| Taxon records | {N(t.TaxaRows)} |");
        if (t.TaxaRowsUnreadable > 0) sb.AppendLine($"| Taxon records that could not be read | {N(t.TaxaRowsUnreadable)} |");
        sb.AppendLine($"| Latest assessments | {N(latest)} |");
        foreach (var scope in Enum.GetValues<AssessmentScopeKind>()) {
            sb.AppendLine($"| … {ScopeLabel(scope)} | {N(Count(t.LatestByScope, scope))} |");
        }
        sb.AppendLine($"| Assessment record not downloaded | {N(t.PayloadMissing)} |");
        sb.AppendLine($"| Assessment record not valid JSON | {N(t.PayloadUnreadable)} |");
        sb.AppendLine($"| Assessment record says it is not the latest (downloaded before a newer one was published) | {N(t.PayloadLatestFlagFalse)} |");
        sb.AppendLine();
        sb.AppendLine("Read into parts, by scope:");
        sb.AppendLine();
        sb.AppendLine("| Scope | Read into parts |");
        sb.AppendLine("| --- | ---: |");
        foreach (var scope in Enum.GetValues<AssessmentScopeKind>()) {
            sb.AppendLine($"| {ScopeLabel(scope)} | {N(Count(t.ParsedByScope, scope))} |");
        }
        sb.AppendLine();
    }

    private static void Failures(StringBuilder sb, CitationCheckTally t) {
        sb.AppendLine("## Parse failures");
        sb.AppendLine();
        if (t.Failures.Count == 0) {
            sb.AppendLine("None: every assessment record read gave citation parts.");
            sb.AppendLine();
            return;
        }
        sb.AppendLine("| Reason | Assessments | Examples |");
        sb.AppendLine("| --- | ---: | --- |");
        foreach (var (failure, count) in t.Failures.OrderByDescending(p => p.Value)) {
            sb.AppendLine($"| {FailureLabel(failure)} | {N(count)} | {Cell(t.FailureExamples.GetValueOrDefault(failure))} |");
        }
        sb.AppendLine();
    }

    private static void Authors(StringBuilder sb, CitationCheckTally t) {
        sb.AppendLine("## Authors");
        sb.AppendLine();
        sb.AppendLine("Names come from the assessor credit, which matches the author part of IUCN's citation; when there is no assessor");
        sb.AppendLine("credit, from the citation itself.");
        sb.AppendLine();
        sb.AppendLine("| Source | Assessments |");
        sb.AppendLine("| --- | ---: |");
        foreach (var source in Enum.GetValues<CitationAuthorSource>()) {
            sb.AppendLine($"| {SourceLabel(source)} | {N(Count(t.AuthorSources, source))} |");
        }
        sb.AppendLine($"| Assessor credit differs from the citation's author text | {N(t.CreditDiffersFromCitation)} |");
        sb.AppendLine($"| A repeated assessor credit added names | {N(t.ExtraAssessorBlocks)} |");
        sb.AppendLine();
        Examples(sb, "Assessor credit differs from the citation", t.CreditDiffersExamples);

        sb.AppendLine("### How the author string was split");
        sb.AppendLine();
        sb.AppendLine("Rules of `IucnAssessmentCitationParser.SplitCreditNames`, strictest first. P = person, O = organisation, V = kept as published.");
        sb.AppendLine();
        sb.AppendLine("| Rule | Assessments | Examples |");
        sb.AppendLine("| --- | ---: | --- |");
        foreach (var (rule, count) in t.SplitRules.OrderByDescending(p => p.Value)) {
            sb.AppendLine($"| {RuleLabel(rule)} | {N(count)} | {Cell(t.SplitRuleExamples.GetValueOrDefault(rule)?.Take(3))} |");
        }
        sb.AppendLine();

        sb.AppendLine("### Kinds of name");
        sb.AppendLine();
        sb.AppendLine("| Kind | Names |");
        sb.AppendLine("| --- | ---: |");
        foreach (var kind in Enum.GetValues<CitationAuthorKind>()) {
            sb.AppendLine($"| {kind} | {N(Count(t.AuthorKinds, kind))} |");
        }
        sb.AppendLine();
        sb.AppendLine("| Shape | Kind | Names |");
        sb.AppendLine("| --- | --- | ---: |");
        foreach (var (shape, count) in t.AuthorShapes.OrderByDescending(p => p.Value)) {
            sb.AppendLine($"| {ShapeLabel(shape)} | {ShapeKind(shape)} | {N(count)} |");
        }
        sb.AppendLine();
        sb.AppendLine("Names kept as published, most frequent first:");
        sb.AppendLine();
        sb.AppendLine("| Shape | Distinct names | Most frequent (occurrences) |");
        sb.AppendLine("| --- | ---: | --- |");
        foreach (var (shape, names) in t.VerbatimNames.OrderByDescending(p => p.Value.Values.Sum())) {
            var top = names.OrderByDescending(p => p.Value).ThenBy(p => p.Key, StringComparer.Ordinal)
                .Take(t.ExamplesPerClass + 5).Select(p => $"{p.Key} ({N(p.Value)})");
            sb.AppendLine($"| {ShapeLabel(shape)} | {N(names.Count)} | {Cell(top)} |");
        }
        sb.AppendLine();

        sb.AppendLine("### Per citation");
        sb.AppendLine();
        sb.AppendLine("| | Assessments |");
        sb.AppendLine("| --- | ---: |");
        sb.AppendLine($"| Every author a person or organisation | {N(t.AllAuthorsStructured)} |");
        sb.AppendLine($"| At least one author kept as published | {N(t.SomeAuthorsVerbatim)} |");
        sb.AppendLine($"| No authors | {N(t.NoAuthors)} |");
        sb.AppendLine($"| Ends \"et al.\" | {N(t.EtAl)} |");
        sb.AppendLine($"| More than 8 authors | {N(t.MoreThan8Authors)} |");
        sb.AppendLine($"| More than 20 authors | {N(t.MoreThan20Authors)} |");
        sb.AppendLine($"| Most authors in one citation | {N(t.MaxAuthors)} |");
        sb.AppendLine($"| The same name twice, kept because value[] lists both people | {N(t.RepeatedNamesKept)} |");
        sb.AppendLine();
        Examples(sb, "The same name twice", t.RepeatedNameExamples);
    }

    private static void Titles(StringBuilder sb, CitationCheckTally t) {
        sb.AppendLine("## Title annotations");
        sb.AppendLine();
        sb.AppendLine("| Annotation | Assessments |");
        sb.AppendLine("| --- | ---: |");
        sb.AppendLine($"| (<Region> assessment) | {N(t.Regional)} |");
        sb.AppendLine($"| … region differs from the assessment's first scope | {N(t.RegionalScopeDiffers)} |");
        sb.AppendLine($"| (amended version of YYYY assessment) | {N(t.AmendedWithYear)} |");
        sb.AppendLine($"| (amended version of assessment), no year | {N(t.AmendedNoYear)} |");
        sb.AppendLine($"| (errata version published in YYYY) | {N(t.ErrataWithYear)} |");
        sb.AppendLine($"| (errata version published in), no year | {N(t.ErrataNoYear)} |");
        sb.AppendLine($"| Subpopulation | {N(t.Subpopulations)} |");
        sb.AppendLine($"| Year after \"Threatened Species\" differs from the year published | {N(t.VolumeDiffers)} |");
        sb.AppendLine();
        Examples(sb, "Region differs from the first scope", t.RegionalScopeExamples);
    }

    private static void Dois(StringBuilder sb, CitationCheckTally t) {
        sb.AppendLine("## DOIs in IUCN's citations");
        sb.AppendLine();
        sb.AppendLine("A DOI is accepted when it names this taxon and this assessment, or, for an errata version, an assessment it replaced.");
        sb.AppendLine();
        sb.AppendLine("| Verdict | Assessments | Examples |");
        sb.AppendLine("| --- | ---: | --- |");
        foreach (var (verdict, count) in t.CitationDoiVerdicts.OrderByDescending(p => p.Value)) {
            sb.AppendLine($"| {VerdictLabel(verdict)} | {N(count)} | {Cell(t.CitationDoiExamples.GetValueOrDefault(verdict)?.Take(3))} |");
        }
        sb.AppendLine();
    }

    private static void Wiki(StringBuilder sb, CitationCheckTally t) {
        sb.AppendLine("## Comparison with English Wikipedia");
        sb.AppendLine();
        sb.AppendLine("| | Count |");
        sb.AppendLine("| --- | ---: |");
        sb.AppendLine($"| Cached articles scanned | {N(t.WikiPagesScanned)} |");
        sb.AppendLine($"| … with a {{{{cite iucn}}}} | {N(t.WikiPagesWithTemplate)} |");
        sb.AppendLine($"| {{{{cite iucn}}}} templates | {N(t.WikiTemplates)} |");
        sb.AppendLine($"| … with no e.T…A… in article-number or page | {N(t.WikiTemplatesWithoutIds)} |");
        sb.AppendLine($"| … citing an assessment not read here (an older one, or left out by --limit) | {N(t.WikiTemplatesOtherAssessment)} |");
        sb.AppendLine($"| … citing an assessment read here | {N(t.WikiTemplatesMatched)} |");
        sb.AppendLine($"| Distinct assessments cited | {N(t.WikiAssessmentsMatched.Count)} |");
        sb.AppendLine();

        sb.AppendLine("### Authors");
        sb.AppendLine();
        sb.AppendLine("Each template is counted under the first class that explains the difference.");
        sb.AppendLine();
        sb.AppendLine("| Class | Templates | Share |");
        sb.AppendLine("| --- | ---: | ---: |");
        foreach (var (agreement, count) in t.AuthorAgreements.OrderByDescending(p => p.Value)) {
            sb.AppendLine($"| {AgreementLabel(agreement)} | {N(count)} | {Share(count, t.WikiTemplatesMatched)} |");
        }
        sb.AppendLine();
        foreach (var (agreement, examples) in t.AuthorAgreementExamples.OrderByDescending(p => Count(t.AuthorAgreements, p.Key))) {
            Examples(sb, AgreementLabel(agreement), examples);
        }

        sb.AppendLine("### DOIs");
        sb.AppendLine();
        sb.AppendLine("\"Ours\" is the DOI from IUCN's citation when it is accepted. The wiki's DOI is also checked against the same rule.");
        sb.AppendLine();
        sb.AppendLine("| Class | Wiki DOI verdict | Templates |");
        sb.AppendLine("| --- | --- | ---: |");
        foreach (var ((agreement, verdict), count) in t.DoiAgreementVerdicts.OrderBy(p => p.Key.Item1).ThenByDescending(p => p.Value)) {
            var verdictText = agreement is DoiAgreement.BothNone or DoiAgreement.OursOnly ? "" : VerdictLabel(verdict);
            sb.AppendLine($"| {DoiLabel(agreement)} | {verdictText} | {N(count)} |");
        }
        sb.AppendLine($"| … of the different DOIs, same taxon and assessment, other release or language | | {N(t.DoiDifferOnlyInRelease)} |");
        sb.AppendLine();
        foreach (var (key, examples) in t.DoiExamples.OrderBy(p => p.Key, StringComparer.Ordinal)) {
            Examples(sb, key, examples);
        }

        sb.AppendLine("### Year, errata and amends");
        sb.AppendLine();
        sb.AppendLine("| | Same | Different | Wiki leaves out | We have none | Neither |");
        sb.AppendLine("| --- | ---: | ---: | ---: | ---: | ---: |");
        YearRow(sb, "Year (|year= or |date=)", t.YearAgreements);
        YearRow(sb, "|errata=", t.ErrataAgreements);
        YearRow(sb, "|amends=", t.AmendsAgreements);
        sb.AppendLine();
        foreach (var (key, examples) in t.YearExamples.OrderBy(p => p.Key, StringComparer.Ordinal)) {
            Examples(sb, key, examples);
        }
    }

    private static void YearRow(StringBuilder sb, string label, Dictionary<YearAgreement, int> counts) =>
        sb.AppendLine($"| {label.Replace("|", "\\|", StringComparison.Ordinal)} | {N(Count(counts, YearAgreement.Same))} | {N(Count(counts, YearAgreement.Different))} | "
            + $"{N(Count(counts, YearAgreement.WikiMissing))} | {N(Count(counts, YearAgreement.OursMissing))} | {N(Count(counts, YearAgreement.BothMissing))} |");

    private static void Examples(StringBuilder sb, string heading, IReadOnlyCollection<string>? examples) {
        if (examples is null || examples.Count == 0) return;
        sb.AppendLine($"**{heading}**, examples:");
        sb.AppendLine();
        foreach (var example in examples) sb.AppendLine($"- {Escape(example)}");
        sb.AppendLine();
    }

    private static void Row(StringBuilder sb, string label, int count, int? of) =>
        sb.AppendLine($"| {label} | {N(count)} | {(of is { } total ? Share(count, total) : "")} |");

    private static string Share(int count, int total) =>
        total <= 0 ? "" : (100.0 * count / total).ToString("F1", CultureInfo.InvariantCulture) + "%";

    private static string Cell(IEnumerable<string>? items) =>
        items is null ? "" : string.Join("<br>", items.Select(i => Escape(i).Replace("|", "\\|", StringComparison.Ordinal)));

    private static string Escape(string text) =>
        text.Replace("<", "&lt;", StringComparison.Ordinal).Replace("\n", " ", StringComparison.Ordinal);

    private static string N(int value) => value.ToString("N0", CultureInfo.InvariantCulture);

    private static int Count<TKey>(Dictionary<TKey, int> counts, TKey key) where TKey : notnull => counts.GetValueOrDefault(key);

    private static string ScopeLabel(AssessmentScopeKind scope) => scope switch {
        AssessmentScopeKind.Global => "Global",
        AssessmentScopeKind.Regional => "Regional only",
        _ => "No scope",
    };

    private static string FailureLabel(CitationParseFailure failure) => failure switch {
        CitationParseFailure.MissingIds => "No assessment id or taxon id",
        CitationParseFailure.MissingName => "No scientific name",
        CitationParseFailure.Unpublished => "No year published (not published)",
        CitationParseFailure.NoCitation => "No citation text",
        CitationParseFailure.CitationFormat => "Citation doesn't end with the Red List sentence",
        CitationParseFailure.IdMismatch => "Citation names another taxon or assessment",
        CitationParseFailure.TitleMismatch => "\"<year>. <scientific name>\" not found in the citation",
        CitationParseFailure.UnknownTitleSuffix => "Unknown text after the scientific name",
        _ => failure.ToString(),
    };

    private static string SourceLabel(CitationAuthorSource source) => source switch {
        CitationAuthorSource.AssessorCredit => "Assessor credit",
        CitationAuthorSource.CitationPrefix => "Citation text (no assessor credit)",
        _ => "None",
    };

    private static string RuleLabel(IucnAssessmentCitationParser.CreditSplitRule rule) => rule switch {
        IucnAssessmentCitationParser.CreditSplitRule.Strict => "Surname, initials pairs",
        IucnAssessmentCitationParser.CreditSplitRule.Single => "One name",
        IucnAssessmentCitationParser.CreditSplitRule.CountStandalone => "Pairs and stand-alone names, confirmed by value[] count",
        IucnAssessmentCitationParser.CreditSplitRule.CountGiven => "Pairs with given names, confirmed by value[] count",
        IucnAssessmentCitationParser.CreditSplitRule.GivenFirst => "Every name given name first",
        IucnAssessmentCitationParser.CreditSplitRule.EtAl => "\"et al.\" taken off, the rest split",
        IucnAssessmentCitationParser.CreditSplitRule.Whole => "Kept whole: no rule applied, no count",
        IucnAssessmentCitationParser.CreditSplitRule.WholeCountMismatch => "Kept whole: no split matched the value[] count",
        IucnAssessmentCitationParser.CreditSplitRule.Empty => "Empty",
        _ => rule.ToString(),
    };

    private static string ShapeLabel(AuthorNameShape shape) => shape switch {
        AuthorNameShape.SurnameInitials => "Surname, initials",
        AuthorNameShape.SurnameInitialsNoDots => "Surname, initials without dots",
        AuthorNameShape.SurnameInitialsSuffix => "Surname, initials with Jr. or II",
        AuthorNameShape.SurnameGivenNames => "Surname, given names",
        AuthorNameShape.CompactSurnameFirst => "Surname initials (no comma)",
        AuthorNameShape.CompactInitialsFirst => "Initials surname",
        AuthorNameShape.Organisation => "Organisation",
        AuthorNameShape.GivenNameFirst => "Given name first",
        AuthorNameShape.SingleName => "Single name",
        _ => "Other",
    };

    private static string ShapeKind(AuthorNameShape shape) => shape switch {
        AuthorNameShape.Organisation => "Organisation",
        AuthorNameShape.GivenNameFirst or AuthorNameShape.SingleName or AuthorNameShape.Unknown => "Verbatim",
        _ => "Person",
    };

    private static string VerdictLabel(DoiVerdict verdict) => verdict switch {
        DoiVerdict.Accepted => "Accepted: this assessment",
        DoiVerdict.AcceptedPredecessor => "Accepted: an assessment this errata version replaced",
        DoiVerdict.Missing => "No DOI",
        DoiVerdict.Malformed => "Not an IUCN Red List DOI",
        DoiVerdict.TaxonMismatch => "Rejected: another taxon",
        DoiVerdict.AssessmentMismatch => "Rejected: another assessment",
        DoiVerdict.PredecessorWithoutErrata => "Rejected: an earlier assessment, and this is not an errata version",
        _ => verdict.ToString(),
    };

    private static string DoiLabel(DoiAgreement agreement) => agreement switch {
        DoiAgreement.BothNone => "Neither has a DOI",
        DoiAgreement.Same => "Same DOI",
        DoiAgreement.WikiOnly => "Only the wiki has a DOI",
        DoiAgreement.OursOnly => "Only ours has a DOI",
        DoiAgreement.Different => "Different DOIs",
        DoiAgreement.WikiMalformed => "Wiki DOI is not an IUCN Red List DOI",
        _ => agreement.ToString(),
    };

    private static string AgreementLabel(AuthorAgreement agreement) => agreement switch {
        AuthorAgreement.Same => "Identical",
        AuthorAgreement.SameIgnoringPunctuation => "Same ignoring case, accents and punctuation",
        AuthorAgreement.WikiNoAuthors => "Wiki names no authors",
        AuthorAgreement.WikiSeveralInOneParameter => "Wiki puts several names in one parameter",
        AuthorAgreement.WikiCollaborationParameter => "Wiki moves the bracketed part to |collaboration=",
        AuthorAgreement.OursKeptWhole => "We kept a list whole; the wiki splits it",
        AuthorAgreement.WikiSplitsAName => "Wiki splits one name in two",
        AuthorAgreement.WikiFewer => "Wiki lists only the first names",
        AuthorAgreement.WikiMore => "Wiki lists more names after ours",
        AuthorAgreement.OtherOrder => "Same names, another order",
        AuthorAgreement.WikiGivenNameFirst => "Wiki writes names given name first",
        AuthorAgreement.WikiSurnamesOnly => "Wiki gives surnames only",
        AuthorAgreement.OtherInitials => "Same surnames, other initials or given names",
        AuthorAgreement.SomeNamesDiffer => "Some surnames differ (typos, a name added or replaced)",
        AuthorAgreement.NoNameInCommon => "No surname in common",
        _ => agreement.ToString(),
    };
}
