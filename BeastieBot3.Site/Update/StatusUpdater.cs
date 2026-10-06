using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Display;
using BeastieBot3.Site.Lists;
using BeastieBot3.Site.Pages;

namespace BeastieBot3.Site.Update;

/// Brings the IUCN statuses in pasted wikitext up to date with the latest global assessments in the
/// site database, and reports what it found. Six kinds of item are updated:
/// 1. {{IUCN status|CODE|taxonId/assessmentId|1|year=YYYY}}, found by the taxon id it names;
/// 2. a cell in a wikitable column whose header names the status, holding a code ("EN") or
///    {{IUCN status|EN}} with no ids, found by the scientific name in the same row;
/// 3. the status, status_system and status_ref lines of a taxobox, found by the taxobox's name;
/// 4. {{IUCN status}} with no ids on a list line, found by the scientific name on the line;
/// 5. the iucn-status parameter of {{Species table/row}}, found by its binomial;
/// 6. {{cite iucn}} citations of an older global assessment (StatusUpdateOptions.UpdateCitations).
/// It also adds statuses where there are none (StatusUpdater.Add.cs): {{IUCN status}} on list lines
/// and a status column in wikitables, when the reader asks for them.
/// StatusUpdateOptions turns on the changes that are off by default. Only the values that change
/// are replaced, so the text outside them comes back byte for byte.
public sealed partial class StatusUpdater {
    /// The most items checked in one text: as many as one Wikipedia page can hold.
    public const int DefaultMaxItems = GroupList.MaxLines;

    // Longer cells are not read as a status code, a status heading or a status template, or for
    // names: real ones are far shorter, and the limits keep a cell that runs to the end of a
    // 2 MB text (an unclosed table) from being copied and searched once per table.
    private const int MaxCodeLength = 20;
    private const int MaxStatusCellLength = 1_000;
    private const int MaxNameCellLength = 20_000;

    private static readonly HashSet<string> TaxoboxNames = ["speciesbox", "taxobox", "automatic taxobox", "subspeciesbox", "infraspeciesbox"];

    private readonly IStatusLookup _lookup;
    private readonly int _maxItems;
    private readonly DateOnly _today;
    private readonly StatusUpdateOptions _options;
    // The table row each {{IUCN status}} in a table cell is in, for finding its taxon by name when its
    // taxon id is unknown. Filled by Update.
    private readonly Dictionary<WikiTemplate, TableRow> _rowOf = [];
    // Species table rows whose population differs from IUCN's. Filled by Update.
    private readonly List<PopulationSuggestion> _populations = [];
    // How the current item's taxon was found by name, when not by its scientific name (a synonym or
    // a common name), or a common name that would have found it. Set by ResolveNames; Update adds it
    // to the item's notes, so every finding of the item has it, whichever way it ends.
    private StatusNote? _nameNote;
    private readonly StatusTaxonResolver _resolver;

    public StatusUpdater(IStatusLookup lookup, DateOnly today, int maxItems = DefaultMaxItems, StatusUpdateOptions? options = null) {
        _lookup = lookup;
        _today = today;
        _maxItems = maxItems;
        _options = options ?? new StatusUpdateOptions();
        _resolver = new StatusTaxonResolver(lookup, _options.MatchCommonNames);
    }

    private sealed record Edit(int Start, int End, string Replacement);

    private abstract record Candidate(int Position);
    private sealed record TemplateCandidate(WikiTemplate Template) : Candidate(Template.Span.Start);
    private sealed record CellCandidate(TableRow Row, TableCell Cell, WikiTemplate? Template) : Candidate(Cell.Content.Start);
    private sealed record TaxoboxCandidate(WikiTemplate Template) : Candidate(Template.Span.Start);
    private sealed record SpeciesRowCandidate(WikiTemplate Template, string? Genus) : Candidate(Template.Span.Start);
    private sealed record CitationCandidate(WikiTemplate Template) : Candidate(Template.Span.Start);

    public StatusUpdateResult Update(string text) {
        var scanner = new WikitextScanner(text);
        _rowOf.Clear();
        _populations.Clear();
        _genusLines = null;
        _resolver.ReadReferences(scanner);
        var candidates = new List<Candidate>();
        // {{IUCN status}} templates inside a taxobox's status parameters belong to the taxobox.
        var claimed = new HashSet<WikiTemplate>();

        foreach (var box in scanner.Templates.Where(t => TaxoboxNames.Contains(t.Name))) {
            if (box.Named("status") is null) {
                continue;
            }
            candidates.Add(new TaxoboxCandidate(box));
            foreach (var name in new[] { "status", "status_system", "status_ref" }) {
                if (box.Named(name) is { } p) {
                    claimed.UnionWith(scanner.TemplatesWithin(p.Whole));
                }
            }
        }

        var cellTemplates = new HashSet<WikiTemplate>();
        var tables = WikiTables.Find(scanner);
        foreach (var table in tables) {
            foreach (var row in table.Rows.Where(r => !r.IsHeaderRow)) {
                foreach (var cell in row.Cells) {
                    if (cell.Content.Length <= MaxStatusCellLength) {
                        foreach (var t in scanner.TemplatesWithin(cell.Content).Where(t => t.Name == "iucn status")) {
                            _rowOf[t] = row;
                        }
                    }
                }
            }
            foreach (var (row, cell, template) in StatusCells(scanner, table)) {
                candidates.Add(new CellCandidate(row, cell, template));
                if (template is not null) {
                    cellTemplates.Add(template);
                }
            }
        }

        foreach (var template in scanner.Templates.Where(t => t.Name == "iucn status")) {
            if (cellTemplates.Contains(template) || claimed.Contains(template)) {
                continue;
            }
            candidates.Add(new TemplateCandidate(template));
        }

        // {{Species table |genus=[[Catopuma]] ...}} heads the rows under it, which abbreviate the genus.
        string? genus = null;
        foreach (var template in scanner.Templates.OrderBy(t => t.Span.Start)) {
            if (template.Name == "species table") {
                genus = template.Named("genus") is { } g ? LinkText(scanner.CoreText(g.Value)) : null;
            } else if (template.Name == "species table/row" && template.Named("iucn-status") is not null) {
                candidates.Add(new SpeciesRowCandidate(template, genus));
            }
        }

        foreach (var cite in scanner.Templates.Where(t => t.Name == "cite iucn" && !claimed.Contains(t))) {
            candidates.Add(new CitationCandidate(cite));
        }

        var (missingLines, missingTables) = FindMissing(scanner, tables, candidates);

        candidates.Sort((a, b) => a.Position.CompareTo(b.Position));
        var findings = new List<StatusFinding>();
        var edits = new List<Edit>();
        foreach (var candidate in candidates.Take(_maxItems)) {
            // A table's new column is one item: its header and all its cells are added together.
            if (candidate is TableAddCandidate tableCandidate) {
                var (tableFindings, tableEdits) = AddColumn(scanner, tableCandidate.Table);
                findings.AddRange(tableFindings);
                edits.AddRange(tableEdits);
                continue;
            }
            var itemEdits = new List<Edit>();
            _nameNote = null;
            StatusFinding? finding = candidate switch {
                TemplateCandidate t => StatusTemplate(scanner, t.Template, itemEdits),
                CellCandidate c => TableCell(scanner, c, itemEdits),
                TaxoboxCandidate b => Taxobox(scanner, b.Template, itemEdits),
                SpeciesRowCandidate r => SpeciesRow(scanner, r, itemEdits),
                CitationCandidate c => CitationFinding(scanner, c.Template, itemEdits),
                LineAddCandidate l => AddToLine(scanner, l.Line, itemEdits),
                _ => throw new InvalidOperationException(),
            };
            // A citation of a regional assessment is not an item.
            if (finding is null) {
                continue;
            }
            if (_nameNote is { } nameNote) {
                finding = finding with { Notes = [nameNote, .. finding.Notes] };
            }
            findings.Add(finding);
            edits.AddRange(itemEdits);
        }
        _lastText = text;
        _lastEdits = edits;
        return new StatusUpdateResult(Apply(text, edits), findings, Math.Max(0, candidates.Count - _maxItems), [.. _populations],
            missingLines, missingTables, Members(scanner, findings));
    }

    // The text and edits of the last Update, for TextWith.
    private string _lastText = string.Empty;
    private List<Edit> _lastEdits = [];

    /// The last Update's text with its edits and these insertions (positions in the text as it was
    /// pasted). Apply orders edits by position and is stable, so an insertion at the same position
    /// as an edit (a status added at the end of a line) comes after it.
    public string TextWith(IEnumerable<(int Position, string Text)> insertions) =>
        Apply(_lastText, [.. _lastEdits, .. insertions.Select(i => new Edit(i.Position, i.Position, i.Text))]);

    // ---------------------------------------------------------------- {{IUCN status}} with ids

    private StatusFinding StatusTemplate(WikitextScanner s, WikiTemplate template, List<Edit> edits) {
        var line = s.LineOf(template.Span.Start);
        var before = s.Original(template.Span);
        StatusFinding Fail(StatusNoteKind kind, string? detail = null, long? id = null, StatusTaxon? taxon = null) =>
            new(StatusItemKind.StatusTemplate, line, StatusOutcome.NotUpdated, before, null, taxon, [new StatusNote(kind, detail, id)]);

        var idsParam = template.Positional(2);
        var idsText = idsParam is null ? string.Empty : s.CoreText(idsParam.Value);
        if (idsText.Length == 0) {
            return IsListLine(s, template.Span.Start) ? ListLine(s, template, edits) : Fail(StatusNoteKind.NoTaxonId);
        }
        var ids = IdsPattern().Match(idsText);
        if (!ids.Success || !long.TryParse(ids.Groups[1].Value, NumberStyles.None, CultureInfo.InvariantCulture, out var taxonId)) {
            return Fail(StatusNoteKind.BadTaxonId, idsText);
        }

        var notes = new List<StatusNote>();
        var taxon = _lookup.GetTaxon(taxonId);
        if (taxon is { InRelease: false }) {
            var current = taxon.CurrentTaxonId is { } currentId ? _lookup.GetTaxon(currentId) : null;
            if (current is { InRelease: true }) {
                notes.Add(new StatusNote(StatusNoteKind.UsedCurrentTaxon, Id: taxonId));
                taxon = current;
            }
        }
        if (taxon is not { InRelease: true }) {
            // An id the release does not have (often an old BirdLife id): the scientific name in the
            // table row or on the list line can still find the taxon.
            var byName = NameNear(s, template);
            if (byName is null) {
                return taxon is null ? Fail(StatusNoteKind.TaxonNotFound, id: taxonId) : Fail(StatusNoteKind.NotInRelease, id: taxonId, taxon: taxon);
            }
            notes.Add(new StatusNote(taxon is null ? StatusNoteKind.IdNotFoundMatchedByName : StatusNoteKind.IdNotInReleaseMatchedByName,
                byName.ScientificName, taxonId));
            taxon = byName;
        }
        var latest = taxon.LatestGlobal;
        if (latest is null) {
            return Fail(StatusNoteKind.NoGlobalAssessment, taxon: taxon);
        }
        if (!IucnCategories.HasStatusTemplateCode(latest)) {
            return Fail(StatusNoteKind.NoCode, latest.Category, taxon: taxon);
        }

        var code = IucnStatusTemplate.ToTemplateCode(latest.Category, latest.PossiblyExtinct, latest.PossiblyExtinctInTheWild);
        if (template.Positional(1) is { } codeParam) {
            ReplaceCore(s, codeParam.Value, code, edits, ignoreCase: true);
        }
        // A template that names only the taxon ("2467") keeps that form unless the reader asks for ids.
        var fullIds = $"{taxon.TaxonId}/{latest.AssessmentId}";
        if (ids.Groups[2].Success || _options.AddIds) {
            ReplaceCore(s, idsParam!.Value, fullIds, edits);
            if (!ids.Groups[2].Success) {
                notes.Add(new StatusNote(StatusNoteKind.IdsAdded, fullIds));
            }
        } else {
            ReplaceCore(s, idsParam!.Value, taxon.TaxonId.ToString(CultureInfo.InvariantCulture), edits);
            notes.Add(new StatusNote(StatusNoteKind.AssessmentIdNotAdded, fullIds));
        }
        UpdateYear(s, template, code, latest.YearPublished, edits);
        AddIdsAndYear(s, template, taxon, latest, code, hasIds: true, edits, notes);
        return Finish(s, StatusItemKind.StatusTemplate, line, template.Span, edits, taxon, notes);
    }

    // year= (and label= when it holds a year) follow the assessment; EX and EW have no year, as
    // IucnStatusTemplate.Render writes them. A template with neither keeps having neither.
    private static void UpdateYear(WikitextScanner s, WikiTemplate template, string code, int? year, List<Edit> edits) {
        var extinct = code is "EX" or "EW";
        foreach (var name in new[] { "year", "label" }) {
            var p = template.Named(name);
            if (p is null) {
                continue;
            }
            var value = s.CoreText(p.Value);
            if (name == "label" && !YearPattern().IsMatch(value)) {
                continue;
            }
            if (extinct) {
                edits.Add(new Edit(p.PipePosition, p.Whole.End, string.Empty));
            } else if (year is { } y) {
                ReplaceCore(s, p.Value, y.ToString(CultureInfo.InvariantCulture), edits);
            }
        }
    }

    // The code to write where a cell or a species table row holds a bare code (current), or null
    // when it stays. A plain CR stays CR for a possibly extinct taxon unless the reader asks for
    // CR(PE) and CR(PEW): tables usually write the category alone.
    private string? BareCodeFor(string? current, string code, List<StatusNote> notes) {
        if (current == "CR" && code is "CR(PE)" or "CR(PEW)" && !_options.PossiblyExtinctCodes) {
            notes.Add(new StatusNote(StatusNoteKind.PossiblyExtinctKept, code));
            return null;
        }
        return string.Equals(current, code, StringComparison.OrdinalIgnoreCase) ? null : code;
    }

    // Ids for a template that has none, and a year for one with neither year= nor label=, when the
    // options ask for them; otherwise a note says what they would add. EX and EW get no year.
    private void AddIdsAndYear(WikitextScanner s, WikiTemplate template, StatusTaxon taxon, AssessmentRow latest, string code,
        bool hasIds, List<Edit> edits, List<StatusNote> notes) {
        var end = s.Core(new TextSpan(template.Span.Start, template.Span.End - 2)).End;
        if (!hasIds && template.Positional(1) is { } codeParam) {
            var ids = $"{taxon.TaxonId}/{latest.AssessmentId}";
            if (_options.AddIds) {
                var at = s.Core(codeParam.Value).End;
                edits.Add(new Edit(at, at, $"|{ids}|1"));
                notes.Add(new StatusNote(StatusNoteKind.IdsAdded, ids));
            } else {
                notes.Add(new StatusNote(StatusNoteKind.IdsNotAdded, ids));
            }
        }
        if (code is "EX" or "EW" || latest.YearPublished is not { } year || template.Named("year") is not null
            || template.Named("label") is not null) {
            return;
        }
        var yearText = year.ToString(CultureInfo.InvariantCulture);
        if (_options.AddYear) {
            edits.Add(new Edit(end, end, $"|year={yearText}"));
            notes.Add(new StatusNote(StatusNoteKind.YearAdded, yearText));
        } else {
            notes.Add(new StatusNote(StatusNoteKind.YearNotAdded, yearText));
        }
    }

    // ---------------------------------------------------------------- table cells

    private static IEnumerable<(TableRow Row, TableCell Cell, WikiTemplate? Template)> StatusCells(WikitextScanner s, WikiTable table) {
        var statusColumns = new HashSet<int>();
        foreach (var row in table.Rows.Where(r => r.IsHeaderRow)) {
            foreach (var cell in row.Cells) {
                if (cell.Content.Length <= MaxStatusCellLength && IsStatusHeader(s.Masked[cell.Content.Start..cell.Content.End])) {
                    for (var c = cell.Column; c < cell.Column + cell.Colspan; c++) {
                        statusColumns.Add(c);
                    }
                }
            }
        }
        if (statusColumns.Count == 0) {
            yield break;
        }
        foreach (var row in table.Rows.Where(r => !r.IsHeaderRow)) {
            foreach (var cell in row.Cells.Where(c => statusColumns.Contains(c.Column))) {
                var core = s.Core(cell.Content);
                if (core.Length <= MaxCodeLength && BareCode(s.Masked[core.Start..core.End]) is not null) {
                    yield return (row, cell, null);
                    continue;
                }
                var template = core.Length > MaxStatusCellLength ? null : s.TemplatesWithin(core).FirstOrDefault(t => t.Span == core);
                if (template is not null && template.Name == "iucn status"
                    && (template.Positional(2) is not { } ids || s.CoreText(ids.Value).Length == 0)) {
                    yield return (row, cell, template);
                }
            }
        }
    }

    // A header that names the IUCN status: it mentions IUCN or the Red List, or says "status" and
    // names no other list ("EPBC status", "CITES status").
    internal static bool IsStatusHeader(string header) {
        var text = header.ToLowerInvariant();
        if (text.Contains("iucn", StringComparison.Ordinal) || text.Contains("red list", StringComparison.Ordinal)
            || text.Contains("redlist", StringComparison.Ordinal)) {
            return true;
        }
        return text.Contains("status", StringComparison.Ordinal) && !OtherSystem().IsMatch(text);
    }

    private static readonly Dictionary<string, string> BareCodes = new(StringComparer.OrdinalIgnoreCase) {
        ["EX"] = "EX", ["EW"] = "EW", ["CR"] = "CR", ["EN"] = "EN", ["VU"] = "VU", ["NT"] = "NT", ["LC"] = "LC",
        ["DD"] = "DD", ["NE"] = "NE", ["CR(PE)"] = "CR(PE)", ["CR(PEW)"] = "CR(PEW)", ["PE"] = "CR(PE)", ["PEW"] = "CR(PEW)",
        ["LR/cd"] = "LR/cd", ["LR/nt"] = "LR/nt", ["LR/lc"] = "LR/lc",
    };

    /// The {{IUCN status}} code a cell's text is, or null: "EN", "lr/nt", "CR (PE)".
    internal static string? BareCode(string text) {
        var compact = text.Replace(" ", string.Empty, StringComparison.Ordinal);
        return compact.Length is > 0 and <= 7 && BareCodes.TryGetValue(compact, out var code) ? code : null;
    }

    private StatusFinding TableCell(WikitextScanner s, CellCandidate candidate, List<Edit> edits) {
        var core = s.Core(candidate.Cell.Content);
        var line = s.LineOf(core.Start);
        var before = s.Original(core);
        var names = candidate.Row.Cells.Concat(candidate.Row.Spanning).Where(c => c != candidate.Cell)
            .SelectMany(c => NamesIn(s, c.Content)).Distinct().ToList();
        // Only the status cell's own references: another cell can cite another taxon's assessment.
        var notEvaluated = candidate.Template is { } cellTemplate
            ? IsNotEvaluated(s, cellTemplate)
            : BareCode(s.Masked[core.Start..core.End]) == "NE";
        var articles = ArticleTitles(s, candidate.Row.Cells.Concat(candidate.Row.Spanning).Where(c => c != candidate.Cell).Select(c => c.Content));
        var (taxon, failure) = ResolveNames(names, s, [candidate.Cell.Content], notEvaluated, articles);
        if (taxon is null) {
            return new StatusFinding(StatusItemKind.TableCell, line, StatusOutcome.NotUpdated, before, null, null, [failure!]);
        }
        var latest = taxon.LatestGlobal;
        StatusFinding Fail(StatusNoteKind kind, string? detail = null) =>
            new(StatusItemKind.TableCell, line, StatusOutcome.NotUpdated, before, null, taxon, [new StatusNote(kind, detail)]);
        if (latest is null) {
            return Fail(StatusNoteKind.NoGlobalAssessment);
        }
        if (!IucnCategories.HasStatusTemplateCode(latest)) {
            return Fail(StatusNoteKind.NoCode, latest.Category);
        }
        var code = IucnStatusTemplate.ToTemplateCode(latest.Category, latest.PossiblyExtinct, latest.PossiblyExtinctInTheWild);
        var notes = new List<StatusNote>();
        if (candidate.Template is { } template) {
            if (template.Positional(1) is { } codeParam) {
                ReplaceCore(s, codeParam.Value, code, edits, ignoreCase: true);
            }
            AddIdsAndYear(s, template, taxon, latest, code, hasIds: false, edits, notes);
        } else {
            var current = BareCode(s.Masked[core.Start..core.End]);
            if (BareCodeFor(current, code, notes) is { } written) {
                edits.Add(new Edit(core.Start, core.End, written));
            }
        }
        return Finish(s, StatusItemKind.TableCell, line, core, edits, taxon, notes);
    }

    // ---------------------------------------------------------------- taxoboxes

    private StatusFinding Taxobox(WikitextScanner s, WikiTemplate box, List<Edit> edits) {
        var status = box.Named("status")!;
        var system = box.Named("status_system");
        var statusRef = box.Named("status_ref");
        var line = s.LineOf(status.PipePosition);
        TemplateParameter[] shown = [.. new[] { status, system, statusRef }.OfType<TemplateParameter>().OrderBy(p => p.PipePosition)];
        var before = ParamLines(s.Text, shown, []);
        StatusFinding Fail(StatusNoteKind kind, string? detail = null, StatusTaxon? taxon = null) =>
            new(StatusItemKind.Taxobox, line, StatusOutcome.NotUpdated, before, null, taxon, [new StatusNote(kind, detail)]);

        var systemText = system is null ? string.Empty : s.CoreText(system.Value);
        if (systemText.Length > 0 && !systemText.Contains("iucn", StringComparison.OrdinalIgnoreCase)) {
            return Fail(StatusNoteKind.OtherStatusSystem, systemText);
        }
        var statusText = s.CoreText(status.Value);
        if (statusText.Length > 0 && BareCode(statusText) is null && !statusText.Equals("NR", StringComparison.OrdinalIgnoreCase)
            && !statusText.Equals("NA", StringComparison.OrdinalIgnoreCase)) {
            return Fail(StatusNoteKind.UnknownStatusCode, statusText);
        }
        var names = TaxoboxNamesOf(s, box);
        if (names.Count == 0) {
            return Fail(StatusNoteKind.NoName);
        }
        var (taxon, failure) = ResolveNames(names, s, statusRef is null ? null : [statusRef.Whole],
            statusText.Equals("NE", StringComparison.OrdinalIgnoreCase));
        if (taxon is null) {
            return new StatusFinding(StatusItemKind.Taxobox, line, StatusOutcome.NotUpdated, before, null, null, [failure!]);
        }
        var latest = taxon.LatestGlobal;
        if (latest is null) {
            return Fail(StatusNoteKind.NoGlobalAssessment, taxon: taxon);
        }
        if (!IucnCategories.HasTaxoboxCode(latest)) {
            return Fail(StatusNoteKind.NoCode, latest.Category, taxon);
        }

        var notes = new List<StatusNote>();
        var code = SpeciesboxStatus.ToStatusCode(latest.Category, latest.PossiblyExtinct, latest.PossiblyExtinctInTheWild);
        var newSystem = SpeciesboxStatus.ToStatusSystem(code, latest.CriteriaVersion);
        var statusChanged = ReplaceCore(s, status.Value, code, edits, ignoreCase: true);
        if (system is null) {
            // Written in the style of the status parameter: "| status = EN" gets "| status_system = IUCN3.1".
            var statusLine = s.Text[status.PipePosition..status.Whole.End];
            var layout = StatusLayout().Match(statusLine);
            var core = s.Core(status.Value);
            var gap = s.Text[core.End..status.Whole.End];
            // On its own line when the status parameter ends its line, else on the same line.
            var newline = gap.Contains("\r\n", StringComparison.Ordinal) ? "\r\n" : gap.Contains('\n') ? "\n" : string.Empty;
            var insert = layout.Success
                ? $"{newline}|{layout.Groups["pre"].Value}status_system{layout.Groups["mid"].Value}{newSystem}"
                : $"{newline}| status_system = {newSystem}";
            edits.Add(new Edit(core.End, core.End, insert));
            notes.Add(new StatusNote(StatusNoteKind.StatusSystemAdded));
        } else if (!string.Equals(Compact(systemText), newSystem, StringComparison.OrdinalIgnoreCase)) {
            ReplaceCore(s, system.Value, newSystem, edits);
        }

        if (statusRef is null) {
            if (statusChanged) {
                notes.Add(new StatusNote(StatusNoteKind.NoStatusRef));
            }
        } else {
            UpdateStatusRef(s, statusRef, latest, statusChanged, edits, notes);
        }
        var after = ParamLines(s.Text, shown, edits);
        var outcome = edits.Count > 0 ? StatusOutcome.Updated
            : notes.Any(n => n.Kind == StatusNoteKind.NoCitation) ? StatusOutcome.NotUpdated
            : StatusOutcome.Current;
        return new StatusFinding(StatusItemKind.Taxobox, line, outcome, before, edits.Count > 0 ? after : null, taxon, notes);
    }

    private void UpdateStatusRef(WikitextScanner s, TemplateParameter statusRef, AssessmentRow latest, bool statusChanged,
        List<Edit> edits, List<StatusNote> notes) {
        var cite = s.TemplatesWithin(statusRef.Value).FirstOrDefault(t => t.Name == "cite iucn");
        if (cite is null) {
            var refText = s.CoreText(statusRef.Value);
            var reuse = NamedRefReuse().Match(refText);
            if (reuse.Success) {
                notes.Add(new StatusNote(StatusNoteKind.RefDefinedElsewhere, reuse.Groups["name"].Value.Trim()));
            } else if (statusChanged || refText.Length > 0) {
                notes.Add(new StatusNote(StatusNoteKind.RefNotChecked));
            }
            return;
        }
        var parts = ReadParts(latest.CitationJson);
        if (CitesLatest(s, cite, latest, parts)) {
            return;
        }
        if (parts is null) {
            notes.Add(new StatusNote(StatusNoteKind.NoCitation));
            return;
        }
        edits.Add(new Edit(cite.Span.Start, cite.Span.End, ReplacementCitation(latest, parts)));
        notes.Add(new StatusNote(StatusNoteKind.CitationReplaced));
    }

    // Whether a {{cite iucn}} cites the latest assessment: by the assessment id in |article-number=,
    // |id= or |url=, else by the one in |doi= (an errata version's DOI names the assessment it
    // corrects, which counts when the citation has |errata=), else by |year= or |volume=.
    internal static bool CitesLatest(WikitextScanner s, WikiTemplate cite, AssessmentRow latest, IucnCitationParts? parts) {
        string Value(string name) => cite.Named(name) is { } p ? s.CoreText(p.Value) : string.Empty;
        foreach (var name in new[] { "article-number", "id", "url" }) {
            var m = AssessmentInText().Match(Value(name));
            if (m.Success) {
                return m.Groups["a"].Value == latest.AssessmentId.ToString(CultureInfo.InvariantCulture);
            }
        }
        var doi = AssessmentInText().Match(Value("doi"));
        if (doi.Success) {
            var cited = doi.Groups["a"].Value;
            if (cited == latest.AssessmentId.ToString(CultureInfo.InvariantCulture)) {
                return true;
            }
            var corrected = parts?.ErrataYear is not null && parts.Doi is { } latestDoi ? AssessmentInText().Match(latestDoi) : null;
            return corrected is { Success: true } && corrected.Groups["a"].Value == cited && Value("errata").Length > 0;
        }
        var year = Value("year");
        if (year.Length == 0) {
            year = Value("volume");
        }
        return latest.YearPublished is { } y && year == y.ToString(CultureInfo.InvariantCulture);
    }

    private static IucnCitationParts? ReadParts(string? json) {
        try {
            return IucnCitationParts.FromJson(json);
        } catch (JsonException) {
            return null;
        }
    }

    // The names a taxobox gives its taxon, best first.
    private static List<string> TaxoboxNamesOf(WikitextScanner s, WikiTemplate box) {
        string Value(string name) => box.Named(name) is { } p ? CleanName(s.CoreText(p.Value)) : string.Empty;
        var genus = BracketedSuffix().Replace(Value("genus"), string.Empty).Trim();
        var species = Value("species");
        var infra = Value("subspecies") is { Length: > 0 } sub ? sub : Value("variety");
        var names = new List<string>();
        void Add(string name) {
            if (name.Contains(' ') && !names.Contains(name)) {
                names.Add(name);
            }
        }
        switch (box.Name) {
            case "subspeciesbox" or "infraspeciesbox":
                if (genus.Length > 0 && species.Length > 0 && infra.Length > 0) {
                    Add($"{genus} {species} {infra}");
                }
                Add(Value("taxon"));
                break;
            case "taxobox":
                Add(Value("trinomial"));
                Add(Value("binomial"));
                break;
            default:
                Add(Value("taxon"));
                if (genus.Length > 0 && species.Length > 0) {
                    Add($"{genus} {species}");
                }
                Add(Value("binomial"));
                break;
        }
        // Only the first name the taxobox gives counts: a trinomial wins over the binomial.
        return names.Take(1).ToList();
    }

    // ---------------------------------------------------------------- names

    // The scientific names a cell writes in italics, links or a {{sp}} or {{taxlink}} template.
    private static IEnumerable<string> NamesIn(WikitextScanner s, TextSpan content) =>
        NameOccurrencesIn(s, content).Select(o => o.Name);

    // The scientific names in a span, each with the span of the link, italics or template it is in.
    private static List<(string Name, TextSpan Span)> NameOccurrencesIn(WikitextScanner s, TextSpan content) {
        if (content.Length > MaxNameCellLength) {
            return [];
        }
        var text = s.Masked[content.Start..content.End];
        var found = new List<(string Name, TextSpan Span)>();
        TextSpan At(Match m) => new(content.Start + m.Index, content.Start + m.Index + m.Length);
        foreach (Match m in Link().Matches(text)) {
            found.Add((m.Groups["target"].Value.Split('#')[0].TrimStart(':'), At(m)));
            if (m.Groups["label"].Success) {
                found.Add((m.Groups["label"].Value, At(m)));
            }
        }
        foreach (Match m in Italic().Matches(text)) {
            found.Add((Link().Replace(m.Groups["inner"].Value, l => l.Groups["label"].Success ? l.Groups["label"].Value : l.Groups["target"].Value), At(m)));
        }
        foreach (var t in s.TemplatesWithin(content)) {
            if (t.Name is "sp" or "taxlink" or "taxon link") {
                var parts = t.Parameters.Where(p => p.Name is null).Select(p => s.CoreText(p.Value)).ToList();
                found.Add((t.Name == "sp" ? string.Join(' ', parts.Take(3)) : parts.FirstOrDefault() ?? string.Empty, t.Span));
            }
        }
        return found.Select(o => (CleanName(o.Item1), o.Item2)).Where(o => IsScientificNameShape(o.Item1)).ToList();
    }

    // The articles linked in a span: each wikilink's target, with its span. Links to other
    // namespaces ("File:", "Category:", "wikt:") and to sections only are left out.
    private static List<(string Title, TextSpan Span)> ArticleLinks(WikitextScanner s, TextSpan content) {
        if (content.Length > MaxNameCellLength) {
            return [];
        }
        var found = new List<(string, TextSpan)>();
        foreach (Match m in Link().Matches(s.Masked[content.Start..content.End])) {
            var target = m.Groups["target"].Value.Split('#')[0].Trim().Replace('_', ' ');
            if (target.Length == 0 || target.Contains(':')) {
                continue;
            }
            found.Add((target, new TextSpan(content.Start + m.Index, content.Start + m.Index + m.Length)));
        }
        return found;
    }

    private static List<string> ArticleTitles(WikitextScanner s, IEnumerable<TextSpan> spans) =>
        spans.SelectMany(span => ArticleLinks(s, span)).Select(l => l.Title).Distinct(StringComparer.OrdinalIgnoreCase).ToList();

    internal static string CleanName(string text) {
        var name = HtmlTag().Replace(text, " ");
        name = name.Replace("'''", string.Empty, StringComparison.Ordinal).Replace("''", string.Empty, StringComparison.Ordinal)
            .Replace("[[", string.Empty, StringComparison.Ordinal).Replace("]]", string.Empty, StringComparison.Ordinal)
            .Replace("&nbsp;", " ", StringComparison.OrdinalIgnoreCase).Replace(' ', ' ').Replace("†", string.Empty, StringComparison.Ordinal)
            .Replace('_', ' ');
        return Whitespace().Replace(name, " ").Trim(' ', '*', ',', ';', ':', '.');
    }

    internal static bool IsScientificNameShape(string name) => NameShape().IsMatch(name);

    // Finds the item's taxon by name (StatusTaxonResolver) and keeps how it was found for Update to
    // add to the item's notes.
    private (StatusTaxon? Taxon, StatusNote? Failure) ResolveNames(IReadOnlyList<string> names, WikitextScanner? s = null,
        IReadOnlyList<TextSpan>? context = null, bool notEvaluated = false, IReadOnlyList<string>? articles = null) {
        var match = _resolver.Resolve(names, s, context, notEvaluated, articles);
        _nameNote = match.HowFound;
        return (match.Taxon, match.Failure);
    }

    // ---------------------------------------------------------------- edits

    /// Replaces the trimmed value of a span (comments and whitespace around it kept) when it differs.
    /// Returns whether it differs.
    private static bool ReplaceCore(WikitextScanner s, TextSpan span, string value, List<Edit> edits, bool ignoreCase = false) {
        var core = s.Core(span);
        var current = s.Masked[core.Start..core.End];
        var comparison = ignoreCase ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
        if (string.Equals(Compact(current), Compact(value), comparison)) {
            return false;
        }
        edits.Add(new Edit(core.Start, core.End, value));
        return true;
    }

    private static string Compact(string text) => Whitespace().Replace(text, string.Empty);

    private static StatusFinding Finish(WikitextScanner s, StatusItemKind kind, int line, TextSpan span, List<Edit> edits,
        StatusTaxon taxon, List<StatusNote> notes) {
        var before = s.Original(span);
        if (edits.Count == 0) {
            return new StatusFinding(kind, line, StatusOutcome.Current, before, null, taxon, notes);
        }
        var after = Apply(s.Text, edits, span);
        return new StatusFinding(kind, line, StatusOutcome.Updated, before, after, taxon, notes);
    }

    // The taxobox's status parameters one per line, as "| status = EN", with the edits applied.
    private static string ParamLines(string text, IReadOnlyList<TemplateParameter> parameters, List<Edit> edits) =>
        string.Join("\n", parameters.Select(p => Apply(text, edits, new TextSpan(p.PipePosition, p.Whole.End)).TrimEnd()));

    /// The text with the edits applied. An edit that overlaps an earlier one is skipped.
    private static string Apply(string text, IEnumerable<Edit> edits, TextSpan? within = null) {
        var range = within ?? new TextSpan(0, text.Length);
        var sb = new StringBuilder();
        var at = range.Start;
        foreach (var edit in edits.Where(e => e.Start >= range.Start && e.End <= range.End).OrderBy(e => e.Start).ThenBy(e => e.End)) {
            if (edit.Start < at) {
                continue;
            }
            sb.Append(text, at, edit.Start - at).Append(edit.Replacement);
            at = edit.End;
        }
        sb.Append(text, at, range.End - at);
        return sb.ToString();
    }

    [GeneratedRegex(@"^(\d+)\s*(?:/\s*(\d+))?$")]
    private static partial Regex IdsPattern();

    [GeneratedRegex(@"^\d{4}$")]
    private static partial Regex YearPattern();

    [GeneratedRegex(@"\b(epbc|cites|natureserve|cosewic|esa|nztcs|state|national|federal|local|regional|wwf|tnc)\b")]
    private static partial Regex OtherSystem();

    [GeneratedRegex(@"^\|(?<pre>\s*)status(?<mid>\s*=\s*)")]
    private static partial Regex StatusLayout();

    [GeneratedRegex(@"^<ref\s+name\s*=\s*[""']?(?<name>[^""'/>]+)[""']?\s*/>$", RegexOptions.IgnoreCase)]
    private static partial Regex NamedRefReuse();

    private static Regex AssessmentInText() => StatusTaxonResolver.AssessmentInText();

    [GeneratedRegex(@"\s*\([^()]*\)$")]
    private static partial Regex BracketedSuffix();

    [GeneratedRegex(@"\[\[(?<target>[^\[\]|]+)(?:\|(?<label>[^\[\]]*))?\]\]")]
    private static partial Regex Link();

    [GeneratedRegex(@"'{2,5}(?<inner>[^'\n]+?)'{2,5}")]
    private static partial Regex Italic();

    [GeneratedRegex(@"<[^>]*>")]
    private static partial Regex HtmlTag();

    [GeneratedRegex(@"\s+")]
    private static partial Regex Whitespace();

    // "Genus species", "Genus species subspecies", "Genus species ssp. subspecies".
    [GeneratedRegex(@"^\p{Lu}[\p{Ll}-]+ [\p{Ll}-]+(?: (?:(?:ssp|subsp|var)\. )?[\p{Ll}-]+)?$")]
    private static partial Regex NameShape();
}
