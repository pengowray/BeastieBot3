using System.Globalization;
using System.Text.RegularExpressions;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Display;

namespace BeastieBot3.Site.Update;

// Statuses added where the text has none: {{IUCN status}} after the scientific name on a list line
// (StatusUpdateOptions.AddToListLines), and an "IUCN status" column in a wikitable whose rows name
// taxa (AddStatusColumns). Both are found on every run. With the option off they are only counted,
// for the offer above the result, and are not items, so they never push the existing statuses past
// the item limit.
public sealed partial class StatusUpdater {
    // A table needs this many data rows, at least half of them naming a taxon, to get a column:
    // smaller tables are legends and summaries more often than lists of taxa.
    private const int MinTableRows = 3;

    /// The header of an added status column.
    public const string StatusColumnHeader = "IUCN status";

    private sealed record LineAddCandidate(LineAddition Line) : Candidate(Line.Span.Start);
    private sealed record TableAddCandidate(TableAddition Table) : Candidate(Table.Table.Span.Start);

    /// A list line with no status whose names name one taxon. Insert: where the template goes.
    /// StatusText: a status the line gives some other way ("(EN)", a status image), or null.
    private sealed record LineAddition(TextSpan Span, StatusTaxonResolver.NameMatch Match, int Insert, string? StatusText);

    /// A table with no status column whose rows name taxa. Column: the 0-based column the new
    /// column goes after. Layout: why the first row that keeps a column from being
    /// added (a ColumnLayout note), or null when it can be added.
    private sealed record TableAddition(WikiTable Table, IReadOnlyList<(TableRow Row, StatusTaxonResolver.NameMatch Match)> Rows,
        int Column, StatusNote? Layout);

    // Finds the list lines and tables that have no status. With the options on they become
    // candidates; with them off, the ones that would get a status are counted.
    private (int Lines, int Tables) FindMissing(WikitextScanner s, IReadOnlyList<WikiTable> tables, List<Candidate> candidates) {
        _bareMembers.Clear();
        var lines = 0;
        foreach (var line in BareListLines(s, tables)) {
            _bareMembers.Add(Member(line.Match.Taxon!, s.LineOf(line.Span.Start), line.Match.HowFound, null) with { OnListLine = true });
            if (_options.AddToListLines) {
                candidates.Add(new LineAddCandidate(line));
            } else if (line.StatusText is null && line.Match.Taxon?.LatestGlobal is { } latest && IucnCategories.HasStatusTemplateCode(latest)) {
                lines++;
            }
        }
        var tableCount = 0;
        foreach (var table in TablesWithoutStatus(s, tables)) {
            foreach (var (row, match) in table.Rows.Where(r => r.Match.Taxon is not null)) {
                _bareMembers.Add(Member(match.Taxon!, s.LineOf(row.Cells[0].Whole.Start), match.HowFound, null));
            }
            if (_options.AddStatusColumns) {
                candidates.Add(new TableAddCandidate(table));
            } else {
                tableCount++;
            }
        }
        return (lines, tableCount);
    }

    // ---------------------------------------------------------------- members

    // The taxa named on list lines and in table rows that have no status, found whether or not the
    // options add one. Filled by FindMissing.
    private readonly List<ListMember> _bareMembers = [];

    private static ListMember Member(StatusTaxon taxon, int line, StatusNote? howFound, string? code) =>
        new(taxon, line, howFound is { Kind: StatusNoteKind.MatchedBySynonym or StatusNoteKind.MatchedByCommonName or StatusNoteKind.MatchedByArticle, Detail: { } name }
            ? name : taxon.ScientificName, code);

    private static readonly HashSet<StatusItemKind> MemberKinds = [
        StatusItemKind.StatusTemplate, StatusItemKind.TableCell, StatusItemKind.ListLine, StatusItemKind.SpeciesTableRow,
        StatusItemKind.ListLineAdded, StatusItemKind.TableRowAdded,
    ];

    // The taxa the text lists: the items' taxa and the lines and rows with no status, one per taxon
    // and line.
    private List<ListMember> Members(WikitextScanner s, IReadOnlyList<StatusFinding> findings) {
        var members = new List<ListMember>();
        var seen = new HashSet<(long, int)>();
        foreach (var finding in findings.Where(f => MemberKinds.Contains(f.Kind) && f.Taxon is not null)) {
            if (seen.Add((finding.Taxon!.TaxonId, finding.Line))) {
                var howFound = finding.Notes.FirstOrDefault(n => n.Kind is StatusNoteKind.MatchedBySynonym or StatusNoteKind.MatchedByCommonName
                    or StatusNoteKind.MatchedByArticle);
                var code = finding.Kind is StatusItemKind.ListLineAdded or StatusItemKind.TableRowAdded ? null : EditSummary.CodeIn(finding.Before);
                // An {{IUCN status}} with ids on a "*" line (the generated lists) is a list line too;
                // a template in a table cell is on a line that starts with "|".
                var onListLine = finding.Kind is StatusItemKind.ListLine or StatusItemKind.ListLineAdded
                    || (finding.Kind == StatusItemKind.StatusTemplate && IsListLine(s, s.LineStart(finding.Line)));
                var hasStatus = onListLine && (finding.Kind != StatusItemKind.ListLineAdded || finding.Outcome == StatusOutcome.Updated);
                members.Add(Member(finding.Taxon, finding.Line, howFound, code) with { OnListLine = onListLine, HasStatusTemplate = hasStatus });
            }
        }
        foreach (var member in _bareMembers) {
            if (seen.Add((member.Taxon.TaxonId, member.Line))) {
                members.Add(member);
            }
        }
        members.Sort((a, b) => a.Line.CompareTo(b.Line));
        return members;
    }

    // ---------------------------------------------------------------- list lines

    // Lines starting with "*" or "#", outside templates (except ListWrappers) and tables and outside sections such as
    // References and External links, with no {{IUCN status}}, whose text before its first <ref>
    // writes exactly one scientific name outside external links, which names one taxon. At most
    // _maxItems lines are looked up.
    private IEnumerable<LineAddition> BareListLines(WikitextScanner s, IReadOnlyList<WikiTable> tables) {
        var masked = s.Masked;
        var tableSpans = tables.Select(t => t.Span).OrderBy(t => t.Start).ToList();
        var nextTable = 0;
        var looked = 0;
        var statusTemplateStarts = s.Templates.Where(t => t.Name == "iucn status").Select(t => t.Span.Start).ToList();
        var skippedSections = SkippedSections(masked);
        for (var start = 0; start < masked.Length && looked < _maxItems;) {
            var newline = masked.IndexOf('\n', start);
            var end = newline < 0 ? masked.Length : newline;
            var lineStart = start;
            start = end + 1;
            if (masked[lineStart] is not ('*' or '#')
                || (s.OuterTemplateAt(lineStart) is { } outer && !ListWrappers.Contains(outer.Name))) {
                continue;
            }
            while (nextTable < tableSpans.Count && tableSpans[nextTable].End <= lineStart) {
                nextTable++;
            }
            if (tableSpans.Skip(nextTable).TakeWhile(t => t.Start <= lineStart).Any(t => t.Contains(lineStart))) {
                continue;
            }
            if (HasTemplateStartIn(statusTemplateStarts, lineStart, end) || skippedSections.Any(span => span.Contains(lineStart))
                || RankLine().IsMatch(masked[lineStart..end])) {
                continue;
            }
            var refAt = masked.IndexOf("<ref", lineStart, end - lineStart, StringComparison.OrdinalIgnoreCase);
            var nameSpan = new TextSpan(lineStart, refAt < 0 ? end : refAt);
            // A name in the label of an external link ("[https://... Kew Species Profile: ''Asparagus
            // officinalis'']") is the title of the linked page.
            var externalLinks = ExternalLink().Matches(masked[nameSpan.Start..nameSpan.End])
                .Select(m => new TextSpan(nameSpan.Start + m.Index, nameSpan.Start + m.Index + m.Length)).ToList();
            var occurrences = LineNameOccurrences(s, nameSpan)
                .Where(o => !externalLinks.Any(l => l.Start <= o.Span.Start && o.Span.End <= l.End)).ToList();
            // One name only: a line naming two species ("''Felis catus'' and ''Felis silvestris''")
            // would otherwise get the status of whichever of them IUCN has. A line with no
            // scientific name ("*[[Black crested gibbon]]") is found by the article it links.
            // A common name in a link can look like a scientific name ("[[Kashmir pygmy shrew]]"), so a
            // name counts when it is a scientific name or synonym of a taxon, or is written in italics.
            var italics = Italic().Matches(masked[nameSpan.Start..nameSpan.End])
                .Select(m => new TextSpan(nameSpan.Start + m.Index, nameSpan.Start + m.Index + m.Length)).ToList();
            bool InItalics(TextSpan span) => italics.Any(i => i.Start <= span.Start && span.End <= i.End);
            occurrences = [.. occurrences.Where(o => occurrences.Any(p => p.Name == o.Name && InItalics(p.Span)) || IsKnownName(o.Name))];
            var nameCount = occurrences.Select(o => o.Name).Distinct().Count();
            if (nameCount > 1) {
                continue;
            }
            // A link labelled with a family or genus name ("[[Basking shark|Cetorhinidae]]") is about
            // the group, even when its article is the group's only species.
            var links = ArticleLinks(s, nameSpan).Where(l => !IsGroupLink(masked[l.Span.Start..l.Span.End], InItalics(l.Span))).ToList();
            if (nameCount == 0 && links.Count == 0) {
                continue;
            }
            looked++;
            var match = _resolver.Resolve(nameCount == 1 ? [occurrences[0].Name] : [], s, null, notEvaluated: false,
                [.. links.Select(l => l.Title).Distinct(StringComparer.OrdinalIgnoreCase)]);
            if (match.Taxon is null) {
                continue;
            }
            if (nameCount == 0) {
                var title = match.HowFound?.Detail;
                occurrences = [.. links.Where(l => string.Equals(l.Title, title, StringComparison.OrdinalIgnoreCase)).Take(1)];
                if (occurrences.Count == 0) {
                    continue;
                }
            }
            var insert = InsertAfterName(s, occurrences, end);
            // "** ''S. vagrans'' complex": the line heads a group of species.
            if (GroupAfterName().IsMatch(masked[insert..end])) {
                continue;
            }
            var lineText = masked[lineStart..end];
            var image = StatusImage().Match(lineText);
            var code = CodeInBrackets().Match(lineText);
            var statusText = image.Success ? image.Value : code.Success ? code.Value : null;
            yield return new LineAddition(new TextSpan(lineStart, end), match, insert, statusText);
        }
    }

    // The sections whose lists are not lists of taxa: from a heading such as "References" or
    // "External links" to the next heading of the same level or higher.
    private static List<TextSpan> SkippedSections(string masked) {
        var headings = Heading().Matches(masked).Select(m => (m.Index, Level: m.Groups["eq"].Length, Title: m.Groups["title"].Value.Trim())).ToList();
        var spans = new List<TextSpan>();
        for (var i = 0; i < headings.Count; i++) {
            if (!SkippedSectionTitles.Contains(headings[i].Title)) {
                continue;
            }
            var next = headings.Skip(i + 1).FirstOrDefault(h => h.Level <= headings[i].Level);
            spans.Add(new TextSpan(headings[i].Index, next.Title is null ? masked.Length : next.Index));
        }
        return spans;
    }

    private static readonly HashSet<string> SkippedSectionTitles = new(StringComparer.OrdinalIgnoreCase) {
        "References", "External links", "Further reading", "See also", "Bibliography", "Sources", "Notes", "Footnotes",
        "Citations", "Literature", "Literature cited", "Works cited", "Notes and references",
        // A list of synonyms would get the status of the taxon after every old name.
        "Synonyms", "Synonymy",
    };

    // Templates that only lay out the list inside them in columns or on one line: their list lines
    // are lines of the article's list. Other templates' lines (navboxes, taxoboxes) are not.
    private static readonly HashSet<string> ListWrappers = [
        "columns-list", "col-list", "collist", "column-list", "div col", "div col list", "plainlist", "plain list",
        "flatlist", "flat list", "unbulleted list", "multicol",
    ];

    private bool IsKnownName(string name) => StatusTaxonResolver.NameVariants(name).Any(v =>
        _lookup.InReleaseTaxaWithName(v, StatusNameKind.Scientific).Count > 0 || _lookup.InReleaseTaxaWithName(v, StatusNameKind.Synonym).Count > 0);

    private static bool HasTemplateStartIn(List<int> starts, int from, int to) {
        var i = starts.BinarySearch(from);
        if (i < 0) {
            i = ~i;
        }
        return i < starts.Count && starts[i] < to;
    }

    // The end of the first place the name is written, with the markup around it (italics around a
    // link) and an authority in brackets after it ("''Panthera tigris'' (Linnaeus, 1758)").
    private static int InsertAfterName(WikitextScanner s, List<(string Name, TextSpan Span)> occurrences, int lineEnd) {
        var first = occurrences.MinBy(o => o.Span.Start).Span;
        var start = first.Start;
        var end = first.End;
        // Italics or bold around a link: "''[[Basking shark|Cetorhinus]]''".
        var lineStart = s.Text.LastIndexOf('\n', Math.Max(0, start - 1)) + 1;
        var around = Italic().Matches(s.Masked[lineStart..lineEnd])
            .Select(m => new TextSpan(lineStart + m.Index, lineStart + m.Index + m.Length)).ToList();
        bool grown;
        do {
            grown = false;
            foreach (var span in occurrences.Select(o => o.Span).Concat(around)) {
                if (span.Start < end && span.End > start && (span.Start < start || span.End > end)) {
                    start = Math.Min(start, span.Start);
                    end = Math.Max(end, span.End);
                    grown = true;
                }
            }
        } while (grown);
        // A name in brackets after the common name: "[[Tiger]] (''P. tigris'')".
        var open = start - 1;
        while (open >= 0 && s.Masked[open] == ' ') {
            open--;
        }
        var close = end;
        while (close < lineEnd && s.Masked[close] == ' ') {
            close++;
        }
        if (open >= 0 && s.Masked[open] == '(' && close < lineEnd && s.Masked[close] == ')') {
            end = close + 1;
        }
        // The authority after the name: "(Linnaeus, 1758)", {{small|Kosterm.}} or <small>Mack.</small>.
        var authority = Authority().Match(s.Masked[end..lineEnd]);
        if (authority.Success) {
            return end + authority.Length;
        }
        var gap = end;
        while (gap < lineEnd && s.Masked[gap] is ' ' or '\t') {
            gap++;
        }
        if (s.TemplatesWithin(new TextSpan(gap, lineEnd)).FirstOrDefault(t => t.Span.Start == gap && t.Name == "small") is { } small && small.Span.End <= lineEnd) {
            return small.Span.End;
        }
        var smallTag = SmallTag().Match(s.Masked[gap..lineEnd]);
        return smallTag.Success ? gap + smallTag.Length : end;
    }

    private StatusFinding AddToLine(WikitextScanner s, LineAddition addition, List<Edit> edits) {
        var line = s.LineOf(addition.Span.Start);
        var before = s.Original(addition.Span).TrimEnd('\r');
        var taxon = addition.Match.Taxon!;
        var notes = new List<StatusNote>();
        if (addition.Match.HowFound is { } howFound) {
            notes.Add(howFound);
        }
        StatusFinding Fail(StatusNoteKind kind, string? detail = null) =>
            new(StatusItemKind.ListLineAdded, line, StatusOutcome.NotUpdated, before, null, taxon, [.. notes, new StatusNote(kind, detail)]);
        if (addition.StatusText is { } statusText) {
            return Fail(StatusNoteKind.StatusInText, statusText.Trim());
        }
        if (taxon.LatestGlobal is not { } latest) {
            return Fail(StatusNoteKind.NoGlobalAssessment);
        }
        if (!IucnCategories.HasStatusTemplateCode(latest)) {
            return Fail(StatusNoteKind.NoCode, latest.Category);
        }
        edits.Add(new Edit(addition.Insert, addition.Insert, " " + NewStatusTemplate(taxon, latest)));
        var after = Apply(s.Text, edits, addition.Span).TrimEnd('\r');
        return new StatusFinding(StatusItemKind.ListLineAdded, line, StatusOutcome.Updated, before, after, taxon, notes);
    }

    // {{IUCN status|EN}}, with the ids and the year when the reader asks for them (EX and EW have no year).
    private string NewStatusTemplate(StatusTaxon taxon, AssessmentRow latest) {
        var code = IucnStatusTemplate.ToTemplateCode(latest.Category, latest.PossiblyExtinct, latest.PossiblyExtinctInTheWild);
        var ids = _options.AddIds ? $"|{taxon.TaxonId}/{latest.AssessmentId}|1" : string.Empty;
        var year = _options.AddYear && code is not ("EX" or "EW") && latest.YearPublished is { } y
            ? "|year=" + y.ToString(CultureInfo.InvariantCulture)
            : string.Empty;
        return $"{{{{IUCN status|{code}{ids}{year}}}}}";
    }

    // ---------------------------------------------------------------- tables

    // Tables with no status column (no header names the status, and no data cell holds a status code
    // or {{IUCN status}}), not nested in or holding another table, with at least MinTableRows data rows,
    // at least half of which name one taxon. At most _maxItems rows are looked up.
    private IEnumerable<TableAddition> TablesWithoutStatus(WikitextScanner s, IReadOnlyList<WikiTable> tables) {
        var looked = 0;
        foreach (var table in tables) {
            if (looked >= _maxItems) {
                yield break;
            }
            if (tables.Any(other => other != table && (Inside(other.Span, table.Span) || Inside(table.Span, other.Span)))) {
                continue;
            }
            var data = table.Rows.Where(r => !r.IsHeaderRow).ToList();
            if (data.Count < MinTableRows || HasStatus(s, table)) {
                continue;
            }
            var rows = new List<(TableRow Row, StatusTaxonResolver.NameMatch Match)>();
            var nameColumns = new Dictionary<int, int>();
            var matched = 0;
            foreach (var row in data) {
                looked++;
                var cells = row.Cells.Concat(row.Spanning).ToList();
                var names = cells.SelectMany(c => NamesIn(s, c.Content)).Distinct().ToList();
                var match = _resolver.Resolve(names, s, null, notEvaluated: false, ArticleTitles(s, cells.Select(c => c.Content)));
                rows.Add((row, match));
                if (match.Taxon is null) {
                    continue;
                }
                matched++;
                // The cell with the scientific name, or with the link that found the taxon.
                var linked = match.HowFound is { Kind: StatusNoteKind.MatchedByArticle, Detail: { } title } ? title : null;
                if (row.Cells.FirstOrDefault(c => linked is null
                        ? NamesIn(s, c.Content).Any()
                        : ArticleLinks(s, c.Content).Any(l => string.Equals(l.Title, linked, StringComparison.OrdinalIgnoreCase))) is { } nameCell) {
                    var column = nameCell.Column + nameCell.Colspan - 1;
                    nameColumns[column] = nameColumns.GetValueOrDefault(column) + 1;
                }
            }
            if (matched * 2 < data.Count) {
                continue;
            }
            var last = table.Rows.Max(r => r.Cells.Count == 0 ? 0 : r.Cells[^1].Column + r.Cells[^1].Colspan - 1);
            var after = nameColumns.Count > 0 ? nameColumns.MaxBy(kv => kv.Value).Key : last;
            yield return new TableAddition(table, rows, after, LayoutProblem(s, table));
        }
    }

    private static bool Inside(TextSpan inner, TextSpan outer) => inner.Start > outer.Start && inner.End <= outer.End;

    private static bool HasStatus(WikitextScanner s, WikiTable table) {
        foreach (var row in table.Rows) {
            foreach (var cell in row.Cells) {
                if (cell.Content.Length > MaxStatusCellLength) {
                    continue;
                }
                if (cell.IsHeader && row.IsHeaderRow && IsStatusHeader(s.Masked[cell.Content.Start..cell.Content.End])) {
                    return true;
                }
                if (row.IsHeaderRow) {
                    continue;
                }
                var core = s.Core(cell.Content);
                if ((core.Length <= MaxCodeLength && BareCode(s.Masked[core.Start..core.End]) is not null)
                    || s.TemplatesWithin(cell.Content).Any(t => t.Name == "iucn status")) {
                    return true;
                }
            }
        }
        return false;
    }

    // The first row that keeps a column from being added, as a ColumnLayout note: the column is added
    // only to a table with one header row, first, and rows with the same number of cells, none
    // spanning several rows or columns. Null when the column can be added.
    private static StatusNote? LayoutProblem(WikitextScanner s, WikiTable table) {
        StatusNote Problem(TableRow row, string cause) =>
            new(StatusNoteKind.ColumnLayout, cause, s.LineOf(row.Cells.Count > 0 ? row.Cells[0].Whole.Start : table.Span.Start));
        if (table.Rows.Count == 0 || !table.Rows[0].IsHeaderRow) {
            return new StatusNote(StatusNoteKind.ColumnLayout, LayoutNoHeader, s.LineOf(table.Span.Start));
        }
        var cells = table.Rows[0].Cells.Count;
        for (var i = 0; i < table.Rows.Count; i++) {
            var row = table.Rows[i];
            if (row.Spanning.Count > 0 || row.Cells.Any(c => c.Colspan > 1 || c.Rowspan > 1)) {
                return Problem(row, LayoutSpan);
            }
            if (i > 0 && row.IsHeaderRow) {
                return Problem(row, LayoutSecondHeader);
            }
            if (row.Cells.Count != cells) {
                return Problem(row, $"{LayoutCellCount}:{row.Cells.Count}:{cells}");
            }
        }
        return null;
    }

    /// The causes in a ColumnLayout note's Detail. LayoutCellCount is followed by ":cells:header cells".
    public const string LayoutNoHeader = "no-header";
    public const string LayoutSpan = "span";
    public const string LayoutSecondHeader = "second-header";
    public const string LayoutCellCount = "cells";

    private (List<StatusFinding> Findings, List<Edit> Edits) AddColumn(WikitextScanner s, TableAddition addition) {
        var table = addition.Table;
        var header = table.Rows[0];
        var headerSpan = new TextSpan(s.Text.LastIndexOf('\n', Math.Max(0, header.Cells[0].Whole.Start - 1)) + 1, header.Cells[^1].Whole.End);
        var headerLine = s.LineOf(headerSpan.Start);
        var headerBefore = s.Original(headerSpan).TrimEnd('\r');
        if (addition.Layout is { } layout) {
            return ([new StatusFinding(StatusItemKind.TableColumnAdded, headerLine, StatusOutcome.NotUpdated, headerBefore, null, null,
                [layout])], []);
        }
        var c = addition.Column;
        var edits = new List<Edit>();
        var findings = new List<StatusFinding>();
        var headerEdit = NewCell(s, header, c, StatusColumnHeader);
        var added = 0;
        foreach (var (row, match) in addition.Rows) {
            var cell = row.Cells[c];
            var line = s.LineOf(cell.Whole.Start);
            var before = s.Original(s.Core(cell.Content));
            var notes = new List<StatusNote>();
            if (match.HowFound is { } howFound) {
                notes.Add(howFound);
            }
            var taxon = match.Taxon;
            StatusNote? failure = taxon is null ? match.Failure
                : taxon.LatestGlobal is not { } global ? new StatusNote(StatusNoteKind.NoGlobalAssessment)
                : !IucnCategories.HasStatusTemplateCode(global) ? new StatusNote(StatusNoteKind.NoCode, global.Category)
                : null;
            if (failure is not null) {
                var empty = NewCell(s, row, c, string.Empty);
                edits.Add(empty);
                findings.Add(new StatusFinding(StatusItemKind.TableRowAdded, line, StatusOutcome.NotUpdated, before, null, taxon,
                    [.. notes, failure, new StatusNote(StatusNoteKind.EmptyCellAdded)]));
                continue;
            }
            var edit = NewCell(s, row, c, NewStatusTemplate(taxon!, taxon!.LatestGlobal!));
            edits.Add(edit);
            added++;
            var span = s.Core(cell.Content);
            var after = Apply(s.Text, [edit], new TextSpan(span.Start, Math.Max(span.End, edit.End))).TrimEnd('\r', '\n');
            findings.Add(new StatusFinding(StatusItemKind.TableRowAdded, line, StatusOutcome.Updated, before, after, taxon, notes));
        }
        var headerAfter = Apply(s.Text, [headerEdit], headerSpan).TrimEnd('\r');
        findings.Insert(0, new StatusFinding(StatusItemKind.TableColumnAdded, headerLine, StatusOutcome.Updated, headerBefore, headerAfter, null,
            [new StatusNote(StatusNoteKind.ColumnAdded, $"{added}/{addition.Rows.Count}", c + 1)]));
        edits.Add(headerEdit);
        return (findings, edits);
    }

    // A new cell after cell column of the row, written the way the row writes its cells: on the same
    // line after "||" (or "!!" in a header row) when the row's cells share a line, else on a line of
    // its own starting with "|" (or "!").
    private static Edit NewCell(WikitextScanner s, TableRow row, int column, string content) {
        var cell = row.Cells[column];
        var header = row.IsHeaderRow;
        var at = s.Core(cell.Whole).End;
        bool inline;
        if (column + 1 < row.Cells.Count) {
            var next = row.Cells[column + 1];
            inline = !s.Text.AsSpan(cell.Whole.End, Math.Max(0, next.Whole.Start - cell.Whole.End)).Contains('\n');
        } else {
            inline = cell.Whole.Start >= 2 && s.Masked[(cell.Whole.Start - 2)..cell.Whole.Start] is "||" or "!!";
        }
        if (inline) {
            // "a||b" gets "a || new ||b": a space before the next separator when the row has none.
            var space = at < s.Text.Length && s.Text[at] is '|' or '!' ? " " : string.Empty;
            return new Edit(at, at, $" {(header ? "!!" : "||")} {content}".TrimEnd() + space);
        }
        var newline = s.Text.IndexOf('\n', at);
        var eol = newline > 0 && s.Text[newline - 1] == '\r' ? "\r\n" : "\n";
        return new Edit(at, at, $"{eol}{(header ? "!" : "|")} {content}".TrimEnd(' '));
    }

    // A line that names a rank before the taxon: "* Family [[Basking shark|Cetorhinidae]]",
    // "** Genus ''[[Lamna]]''", "*** '''Genus ''Sorex'''''".
    [GeneratedRegex(@"^[*#:; ]*(?:'{2,5})?\s*(?:Superfamily|Family|Subfamily|Tribe|Subtribe|Genus|Subgenus|Order|Suborder|Infraorder|Class|Subclass|Section|Clade)\b")]
    private static partial Regex RankLine();

    // A species group named after one species: "''S. vagrans'' complex", "''S. cinereus'' group".
    [GeneratedRegex(@"^\s*(?:species\s+)?(?:complex|group)\b", RegexOptions.IgnoreCase)]
    private static partial Regex GroupAfterName();

    // A link labelled with the name of a group: one capitalised word that is in italics (a genus,
    // "''[[Basking shark|Cetorhinus]]''") or has the ending of a family, subfamily, tribe or order name.
    // A one-word English name ("[[Wildcat|Wildcats]]") is neither.
    private static bool IsGroupLink(string link, bool inItalics) {
        var m = OneWordLabel().Match(link);
        return m.Success && (inItalics || m.Groups["italic"].Success || GroupEnding().IsMatch(m.Groups["word"].Value));
    }

    [GeneratedRegex(@"^\[\[[^|\]]*\|\s*(?<italic>'{2,5})?(?<word>\p{Lu}\p{Ll}+)(?:'{2,5})?\s*\]\]$")]
    private static partial Regex OneWordLabel();

    [GeneratedRegex(@"(?:idae|inae|ini|oidea|iformes|aceae|oideae|eae|ales)$")]
    private static partial Regex GroupEnding();

    // [[File:Status iucn3.1 EN.svg]] and the like.
    [GeneratedRegex(@"status[ _]iucn[^|\]]*", RegexOptions.IgnoreCase)]
    private static partial Regex StatusImage();

    // A category code in brackets, as some lists write it: "(EN)", "(CR)".
    [GeneratedRegex(@"\(\s*(?:EX|EW|CR|EN|VU|NT|LC|DD|NE|LR/(?:cd|nt|lc))\s*\)")]
    private static partial Regex CodeInBrackets();

    [GeneratedRegex(@"^<small>[^<\n]*</small>", RegexOptions.IgnoreCase)]
    private static partial Regex SmallTag();

    [GeneratedRegex(@"\[(?:https?:)?//[^\s\]]+[^\]\n]*\]", RegexOptions.IgnoreCase)]
    private static partial Regex ExternalLink();

    [GeneratedRegex(@"^(?<eq>={2,6})(?<title>[^=\n]+)\k<eq>[ \t\r]*$", RegexOptions.Multiline)]
    private static partial Regex Heading();

    // An authority in brackets straight after a name: " (Linnaeus, 1758)".
    [GeneratedRegex(@"^\s*\([^()\n]*\d{4}[^()\n]*\)")]
    private static partial Regex Authority();
}
