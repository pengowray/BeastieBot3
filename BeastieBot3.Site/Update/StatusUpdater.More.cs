using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Display;
using BeastieBot3.Site.Pages;

namespace BeastieBot3.Site.Update;

// List lines, {{Species table/row}} and {{cite iucn}} citations: the items found by looking at a
// sample of English Wikipedia lists (List of mammals of India and of Madagascar write
// "* [[Aye-aye]], ''Daubentonia madagascariensis'' {{IUCN status|EN}}"; the family lists such as
// List of felids and List of mustelids use {{Species table/row |binomial=C. temminckii |iucn-status=VU}}).
public sealed partial class StatusUpdater {
    // ---------------------------------------------------------------- list lines

    // Whether the position is on a line that starts with a list marker (*, #, : or ;).
    private static bool IsListLine(WikitextScanner s, int position) {
        var start = s.Text.LastIndexOf('\n', Math.Max(0, position - 1)) + 1;
        return start < s.Text.Length && s.Text[start] is '*' or '#' or ':' or ';';
    }

    // {{IUCN status|EN}} on a list line: the taxon is the one the scientific names on the line,
    // before the template, name.
    private StatusFinding ListLine(WikitextScanner s, WikiTemplate template, List<Edit> edits) {
        var line = s.LineOf(template.Span.Start);
        var before = s.Original(template.Span);
        var (taxon, failure) = ResolveNames(LineNames(s, template));
        if (taxon is null) {
            return new StatusFinding(StatusItemKind.ListLine, line, StatusOutcome.NotUpdated, before, null, null,
                [failure!.Kind == StatusNoteKind.NoName ? new StatusNote(StatusNoteKind.NoName) : failure]);
        }
        var latest = taxon.LatestGlobal;
        StatusFinding Fail(StatusNoteKind kind, string? detail = null) =>
            new(StatusItemKind.ListLine, line, StatusOutcome.NotUpdated, before, null, taxon, [new StatusNote(kind, detail)]);
        if (latest is null) {
            return Fail(StatusNoteKind.NoGlobalAssessment);
        }
        if (!IucnCategories.HasStatusTemplateCode(latest)) {
            return Fail(StatusNoteKind.NoCode, latest.Category);
        }
        var code = IucnStatusTemplate.ToTemplateCode(latest.Category, latest.PossiblyExtinct, latest.PossiblyExtinctInTheWild);
        var notes = new List<StatusNote>();
        if (template.Positional(1) is { } codeParam) {
            ReplaceCore(s, codeParam.Value, code, edits, ignoreCase: true);
        }
        AddIdsAndYear(s, template, taxon, latest, code, hasIds: false, edits, notes);
        return Finish(s, StatusItemKind.ListLine, line, template.Span, edits, taxon, notes);
    }

    // The scientific names on a list line before the template. An abbreviated name ("''G. aurita''")
    // takes its genus from the nearest "Genus ''[[Geogale]]''" line above it, as the lists by
    // country write it.
    private List<string> LineNames(WikitextScanner s, WikiTemplate template) {
        var lineStart = s.Text.LastIndexOf('\n', Math.Max(0, template.Span.Start - 1)) + 1;
        var span = new TextSpan(lineStart, template.Span.Start);
        var names = NamesIn(s, span).ToList();
        var text = s.Masked[span.Start..span.End];
        foreach (Match m in AbbreviatedInText().Matches(text)) {
            if (GenusAbove(s, lineStart) is { } genus && genus.StartsWith(m.Groups["initial"].Value, StringComparison.Ordinal)) {
                names.Add($"{genus} {m.Groups["rest"].Value}");
            }
        }
        // "[[Large-eared tenrec]] (Geogale aurita)": a binomial in brackets with no italics.
        foreach (Match m in BracketedBinomial().Matches(text)) {
            names.Add(m.Groups["name"].Value);
        }
        return names.Where(IsScientificNameShape).Distinct().ToList();
    }

    // The genus of the nearest "Genus ''[[Name]]''" line before a position. The lines are found once
    // per text, so a long list costs one pass.
    private List<(int Position, string Genus)>? _genusLines;

    private string? GenusAbove(WikitextScanner s, int position) {
        _genusLines ??= GenusLine().Matches(s.Masked).Select(m => (m.Index, m.Groups["genus"].Value)).ToList();
        var lo = 0;
        var hi = _genusLines.Count - 1;
        string? found = null;
        while (lo <= hi) {
            var mid = (lo + hi) / 2;
            if (_genusLines[mid].Position < position) {
                found = _genusLines[mid].Genus;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return found;
    }

    // The taxon the scientific names in a template's table row, or on its list line, name; null when
    // they name none or several.
    private StatusTaxon? NameNear(WikitextScanner s, WikiTemplate template) {
        List<string> names;
        if (_rowOf.TryGetValue(template, out var row)) {
            names = row.Cells.Concat(row.Spanning).SelectMany(c => NamesIn(s, c.Content)).Distinct().ToList();
        } else if (IsListLine(s, template.Span.Start)) {
            names = LineNames(s, template);
        } else {
            return null;
        }
        var (taxon, _) = ResolveNames(names);
        return taxon;
    }

    /// The text a link shows: "[[Caracal (genus)|Caracal]]" -> "Caracal"; then CleanName.
    internal static string LinkText(string text) =>
        CleanName(Link().Replace(text, l => l.Groups["label"].Success ? l.Groups["label"].Value : l.Groups["target"].Value));

    // ---------------------------------------------------------------- {{Species table/row}}

    private StatusFinding SpeciesRow(WikitextScanner s, SpeciesRowCandidate candidate, List<Edit> edits) {
        var row = candidate.Template;
        var status = row.Named("iucn-status")!;
        var core = s.Core(status.Value);
        var line = s.LineOf(status.PipePosition);
        var before = s.Original(new TextSpan(status.PipePosition, core.End)).TrimEnd();
        StatusFinding Fail(StatusNoteKind kind, string? detail = null, StatusTaxon? taxon = null) =>
            new(StatusItemKind.SpeciesTableRow, line, StatusOutcome.NotUpdated, before, null, taxon, [new StatusNote(kind, detail)]);

        var current = BareCode(s.CoreText(status.Value));
        if (current is null && s.CoreText(status.Value).Length > 0) {
            return Fail(StatusNoteKind.UnknownStatusCode, s.CoreText(status.Value));
        }
        var names = new List<string>();
        if (row.Named("binomial") is { } binomial) {
            var name = CleanName(s.CoreText(binomial.Value));
            var abbreviated = AbbreviatedGenus().Match(name);
            if (abbreviated.Success) {
                var genus = candidate.Genus is { Length: > 0 } g ? BracketedSuffix().Replace(g, string.Empty).Trim() : null;
                if (genus is null || !genus.StartsWith(abbreviated.Groups["initial"].Value, StringComparison.Ordinal)) {
                    return Fail(StatusNoteKind.NoGenus, name);
                }
                name = genus + " " + abbreviated.Groups["rest"].Value;
            }
            if (IsScientificNameShape(name)) {
                names.Add(name);
            }
        }
        if (row.Named("name") is { } nameParam) {
            names.AddRange(NamesIn(s, nameParam.Value));
        }
        var (taxon, failure) = ResolveNames(names.Distinct().ToList());
        if (taxon is null) {
            return new StatusFinding(StatusItemKind.SpeciesTableRow, line, StatusOutcome.NotUpdated, before, null, null, [failure!]);
        }
        var latest = taxon.LatestGlobal;
        if (latest is null) {
            return Fail(StatusNoteKind.NoGlobalAssessment, taxon: taxon);
        }
        if (!IucnCategories.HasStatusTemplateCode(latest)) {
            return Fail(StatusNoteKind.NoCode, latest.Category, taxon);
        }
        var code = IucnStatusTemplate.ToTemplateCode(latest.Category, latest.PossiblyExtinct, latest.PossiblyExtinctInTheWild);
        var notes = new List<StatusNote>();
        if (BareCodeFor(current, code, notes) is { } written) {
            edits.Add(new Edit(core.Start, core.End, written));
        }
        var shown = new List<TemplateParameter> { status };
        if (row.Named("direction") is { } direction) {
            Direction(s, direction, latest.PopulationTrend, edits, notes);
            shown.Add(direction);
            shown.Sort((a, b) => a.PipePosition.CompareTo(b.PipePosition));
        }
        var shownBefore = ParamLines(s.Text, shown, []);
        if (edits.Count == 0) {
            return new StatusFinding(StatusItemKind.SpeciesTableRow, line, StatusOutcome.Current, shownBefore, null, taxon, notes);
        }
        return new StatusFinding(StatusItemKind.SpeciesTableRow, line, StatusOutcome.Updated, shownBefore, ParamLines(s.Text, shown, edits),
            taxon, notes);
    }

    // The direction parameter of a species table row: the population trend template, replaced when
    // the latest assessment's trend differs. The text around the template (its <ref>) is kept, and a
    // template with the same trend is kept as written, whatever its label or capitals.
    private static void Direction(WikitextScanner s, TemplateParameter direction, string? trend, List<Edit> edits,
        List<StatusNote> notes) {
        var wanted = trend?.Trim().ToLowerInvariant() switch {
            "decreasing" => Trend.Decreasing,
            "stable" => Trend.Stable,
            "increasing" => Trend.Increasing,
            "unknown" => Trend.Unknown,
            _ => (Trend?)null,
        };
        if (wanted is not { } w) {
            notes.Add(new StatusNote(StatusNoteKind.NoPopulationTrend));
            return;
        }
        var core = s.Core(direction.Value);
        if (core.Length == 0) {
            var at = direction.Value.Start;
            edits.Add(new Edit(at, at, TrendTemplate(w)));
            return;
        }
        foreach (var template in s.TemplatesWithin(direction.Value)) {
            if (TrendOf(template.Name) is { } found) {
                if (found != w) {
                    edits.Add(new Edit(template.Span.Start, template.Span.End, TrendTemplate(w)));
                }
                return;
            }
        }
        notes.Add(new StatusNote(StatusNoteKind.DirectionNotRecognised, TrendTemplate(w)));
    }

    private enum Trend { Decreasing, Stable, Increasing, Unknown }

    // What the species tables on English Wikipedia write, such as List of felids.
    private static string TrendTemplate(Trend trend) => trend switch {
        Trend.Decreasing => "{{decrease|Population declining}}",
        Trend.Stable => "{{steady|Population steady}}",
        Trend.Increasing => "{{increase|Population increasing}}",
        _ => "{{population change unknown}}",
    };

    // The trend templates and their redirects, by normalized name.
    private static Trend? TrendOf(string name) => name switch {
        "decrease" or "loss" or "down" or "diminution" or "decreasenegative" or "negative decrease" => Trend.Decreasing,
        "increase" or "gain" or "profit" or "growth" or "up" or "augmentation" or "increasepositive" or "positive increase" => Trend.Increasing,
        "steady" or "nochange" or "unchanged" or "no change" or "same" or "stable" => Trend.Stable,
        "population change unknown" => Trend.Unknown,
        _ => null,
    };

    // ---------------------------------------------------------------- {{cite iucn}}

    // A citation is matched to its taxon by the T…A… id in article-number, id, url or doi. Only a
    // citation of a global assessment is checked: one of a regional assessment is not reported.
    private StatusFinding? CitationFinding(WikitextScanner s, WikiTemplate cite, List<Edit> edits) {
        var line = s.LineOf(cite.Span.Start);
        var before = s.Original(cite.Span);
        string Value(string name) => cite.Named(name) is { } p ? s.CoreText(p.Value) : string.Empty;
        Match? ids = null;
        foreach (var name in new[] { "article-number", "id", "url", "doi" }) {
            var m = AssessmentInText().Match(Value(name));
            if (m.Success && m.Groups["t"].Success) {
                ids = m;
                break;
            }
        }
        if (ids is null) {
            return new StatusFinding(StatusItemKind.Citation, line, StatusOutcome.NotUpdated, before, null, null,
                [new StatusNote(StatusNoteKind.CitationWithoutIds)]);
        }
        var cited = long.Parse(ids.Groups["a"].Value, CultureInfo.InvariantCulture);
        var scope = _lookup.AssessmentScope(cited);
        if (scope is not null && !string.Equals(scope.Trim(), "Global", StringComparison.OrdinalIgnoreCase)) {
            return null;
        }
        var taxonId = long.Parse(ids.Groups["t"].Value, CultureInfo.InvariantCulture);
        var notes = new List<StatusNote>();
        var taxon = _lookup.GetTaxon(taxonId);
        if (taxon is null) {
            return new StatusFinding(StatusItemKind.Citation, line, StatusOutcome.NotUpdated, before, null, null,
                [new StatusNote(StatusNoteKind.TaxonNotFound, Id: taxonId)]);
        }
        if (!taxon.InRelease) {
            var current = taxon.CurrentTaxonId is { } currentId ? _lookup.GetTaxon(currentId) : null;
            if (current is null || !current.InRelease) {
                return new StatusFinding(StatusItemKind.Citation, line, StatusOutcome.NotUpdated, before, null, taxon,
                    [new StatusNote(StatusNoteKind.NotInRelease, Id: taxonId)]);
            }
            notes.Add(new StatusNote(StatusNoteKind.UsedCurrentTaxon, Id: taxonId));
            taxon = current;
        }
        if (taxon.LatestGlobal is not { } latest) {
            return new StatusFinding(StatusItemKind.Citation, line, StatusOutcome.NotUpdated, before, null, taxon,
                [new StatusNote(StatusNoteKind.NoGlobalAssessment)]);
        }
        var parts = ReadCitationParts(latest.CitationJson);
        if (CitesLatest(s, cite, latest, parts)) {
            return new StatusFinding(StatusItemKind.Citation, line, StatusOutcome.Current, before, null, taxon, notes);
        }
        if (parts is null) {
            notes.Add(new StatusNote(StatusNoteKind.NoCitation));
            return new StatusFinding(StatusItemKind.Citation, line, StatusOutcome.NotUpdated, before, null, taxon, notes);
        }
        if (!_options.UpdateCitations) {
            notes.Add(new StatusNote(StatusNoteKind.CitationOlder));
            return new StatusFinding(StatusItemKind.Citation, line, StatusOutcome.NotUpdated, before, null, taxon, notes);
        }
        DateOnly? downloaded = parts.DownloadedAtUtc is { } at ? DateOnly.FromDateTime(at) : null;
        var options = WikitextOptions.Default.ToCiteIucnOptions(_today, downloaded) with { WrapInRef = false };
        edits.Add(new Edit(cite.Span.Start, cite.Span.End, CiteIucnRenderer.Render(parts, options)));
        notes.Add(new StatusNote(StatusNoteKind.CitationUpdated));
        return Finish(s, StatusItemKind.Citation, line, cite.Span, edits, taxon, notes);
    }

    private static IucnCitationParts? ReadCitationParts(string? json) {
        try {
            return IucnCitationParts.FromJson(json);
        } catch (JsonException) {
            return null;
        }
    }

    [GeneratedRegex(@"''(?<initial>\p{Lu})\.\s*(?<rest>[\p{Ll}-]+(?: [\p{Ll}-]+)?)''")]
    private static partial Regex AbbreviatedInText();

    // A line naming a genus: "**** Genus: ''[[Daubentonia]]''", "*** Genus ''[[Mirounga]]''".
    [GeneratedRegex(@"Genus\W{0,10}\[\[(?:[^\]|]*\|)?(?<genus>\p{Lu}\p{Ll}+)")]
    private static partial Regex GenusLine();

    [GeneratedRegex(@"\((?<name>\p{Lu}\p{Ll}+ \p{Ll}[\p{Ll}-]+)\)")]
    private static partial Regex BracketedBinomial();

    // "C. temminckii" -> initial "C", rest "temminckii".
    [GeneratedRegex(@"^(?<initial>\p{Lu})\.\s*(?<rest>[\p{Ll}-]+(?: [\p{Ll}-]+)?)$")]
    private static partial Regex AbbreviatedGenus();
}
