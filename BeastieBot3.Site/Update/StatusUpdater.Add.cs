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

    /// A list line with no status whose names name one taxon. Insert: where the template goes after
    /// the name; EndInsert: where it goes at the end of the line (StatusUpdateOptions.StatusAtLineEnd).
    /// StatusText: a status the line gives some other way ("(EN)", a status image), or null.
    private sealed record LineAddition(TextSpan Span, StatusTaxonResolver.NameMatch Match, int Insert, string? StatusText, int EndInsert);

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
            _bareMembers.Add(Member(line.Match.Taxon!, s.LineOf(line.Span.Start), line.Match.HowFound, null) with { Source = ListMemberSource.ListLine });
            if (_options.AddToListLines) {
                candidates.Add(new LineAddCandidate(line));
            } else if (line.StatusText is null && line.Match.Taxon?.LatestGlobal is { } latest && IucnCategories.HasStatusTemplateCode(latest)) {
                lines++;
            }
        }
        var tableCount = 0;
        foreach (var table in TablesWithoutStatus(s, tables)) {
            foreach (var (row, match) in table.Rows.Where(r => r.Match.Taxon is not null)) {
                _bareMembers.Add(Member(match.Taxon!, s.LineOf(row.Cells[0].Whole.Start), match.HowFound, null) with { Source = ListMemberSource.TableRow });
            }
            if (_options.AddStatusColumns) {
                candidates.Add(new TableAddCandidate(table));
            } else {
                tableCount++;
            }
        }
        return (lines, tableCount);
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
        var nextSection = 0;
        for (var start = 0; start < masked.Length && looked < _maxItems;) {
            var newline = masked.IndexOf('\n', start);
            var end = newline < 0 ? masked.Length : newline;
            var lineStart = start;
            start = end + 1;
            // "{{columns-list|colwidth=30em|*[[Black crested gibbon]]": the list starts on the
            // template's own line.
            if (masked[lineStart] == '{' && WrapperStart().Match(masked, lineStart, end - lineStart) is { Success: true } wrapper
                && ListWrappers.Contains(WikitextScanner.NormalizeName(wrapper.Groups["name"].Value))) {
                lineStart = wrapper.Index + wrapper.Length;
            }
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
            while (nextSection < skippedSections.Count && skippedSections[nextSection].End <= lineStart) {
                nextSection++;
            }
            if (HasTemplateStartIn(statusTemplateStarts, lineStart, end)
                || skippedSections.Skip(nextSection).TakeWhile(span => span.Start <= lineStart).Any(span => span.Contains(lineStart))
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
            // The links only for a line with no scientific name: "''Panthera zdanskyi'', a relative of
            // the [[Tiger]]" is not about the tiger.
            var match = _resolver.Resolve(nameCount == 1 ? [occurrences[0].Name] : [], s, null, notEvaluated: false,
                nameCount == 0 ? [.. links.Select(l => l.Title).Distinct(StringComparer.OrdinalIgnoreCase)] : null);
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
            yield return new LineAddition(new TextSpan(lineStart, end), match, insert, statusText, Math.Max(insert, LineEndInsert(s, lineStart, end)));
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
    internal static readonly HashSet<string> ListWrappers = [
        "columns-list", "col-list", "collist", "column-list", "div col", "div col list", "plainlist", "plain list",
        "flatlist", "flat list", "unbulleted list", "multicol",
    ];

    // Where a status goes at the end of a list line: before the references and footnotes at its end,
    // before a reference or template that runs on to the next lines, and before the "}}" of a list
    // layout template that ends on the line ("* ''Panthera tigris''}}").
    private static int LineEndInsert(WikitextScanner s, int lineStart, int end) {
        var masked = s.Masked;
        var trailing = TrailingReferences().Match(masked[lineStart..end]);
        var at = trailing.Success ? lineStart + trailing.Index : lineStart + masked[lineStart..end].TrimEnd().Length;
        if (s.OuterTemplateAt(lineStart) is { } wrapper && ListWrappers.Contains(wrapper.Name)
            && wrapper.Span.End <= end && wrapper.Span.End - 2 >= lineStart) {
            at = Math.Min(at, wrapper.Span.End - 2);
        }
        if (s.ContainerAt(at, ListWrappers) is { } open && open.Start >= lineStart) {
            at = open.Start;
        }
        while (at > lineStart && masked[at - 1] is ' ' or '\t') {
            at--;
        }
        return at;
    }

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
        if (smallTag.Success) {
            return gap + smallTag.Length;
        }
        // An authority as plain text, as genus articles write it: "''Alseodaphne albifrons'' Kosterm. –",
        // "''Acer x'' (C.K.Allen) Kosterm.", "''Bulinus hightoni'' Brown & Wright, 1978", up to a dash,
        // a comma, a bracket, a reference or the end of the line.
        var plain = PlainAuthority().Match(s.Masked[end..lineEnd]);
        return plain.Success ? end + plain.Groups["authority"].Index + plain.Groups["authority"].Length : end;
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
            return Fail(NoAssessmentKind);
        }
        if (!IucnCategories.HasStatusTemplateCode(latest)) {
            return Fail(StatusNoteKind.NoCode, latest.Category);
        }
        var at = _options.StatusAtLineEnd ? addition.EndInsert : addition.Insert;
        edits.Add(new Edit(at, at, " " + NewStatusTemplate(taxon, latest) + NewReference(s, taxon, latest)));
        var after = Apply(s.Text, edits, addition.Span).TrimEnd('\r');
        return new StatusFinding(StatusItemKind.ListLineAdded, line, StatusOutcome.Updated, before, after, taxon, notes);
    }

    // The reference after an added status (StatusUpdateOptions.AddReferences): the named reference the
    // text defines for the taxon's latest assessment, else the citation of it in a <ref>; empty when
    // not asked for or the site has no citation.
    private string NewReference(WikitextScanner s, StatusTaxon taxon, AssessmentRow latest) {
        if (!_options.AddReferences) {
            return string.Empty;
        }
        if (_resolver.References.NameForAssessment(latest.AssessmentId) is { } name) {
            return $"<ref name=\"{name}\"/>";
        }
        return ReadParts(latest.CitationJson) is { } parts ? $"<ref>{ReplacementCitation(latest, parts)}</ref>" : string.Empty;
    }

    // {{IUCN status|EN}}, with the ids and the year when the reader asks for them (EX and EW have no year).
    private string NewStatusTemplate(StatusTaxon taxon, AssessmentRow latest) =>
        NewStatusTemplate(IucnStatusTemplate.ToTemplateCode(latest.Category, latest.PossiblyExtinct, latest.PossiblyExtinctInTheWild),
            taxon.TaxonId, latest.AssessmentId, latest.YearPublished, _options.AddIds, _options.AddYear);

    /// A new {{IUCN status|EN}}, with "|taxonId/assessmentId|1" and "|year=" when asked for (EX and EW
    /// have no year). Used for statuses added to lines and columns and for missing taxa put in.
    internal static string NewStatusTemplate(string code, long taxonId, long? assessmentId, int? year, bool addIds, bool addYear) {
        var ids = addIds && assessmentId is { } a ? $"|{taxonId}/{a}|1" : string.Empty;
        var yearText = addYear && code is not ("EX" or "EW") && year is { } y ? "|year=" + y.ToString(CultureInfo.InvariantCulture) : string.Empty;
        return $"{{{{IUCN status|{code}{ids}{yearText}}}}}";
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

    // The start of a list layout template, up to the first list marker on its line.
    [GeneratedRegex(@"\G\{\{(?<name>[^|{}\n]+)\|(?:[^|{}\n]*\|)*?(?=[*#])")]
    private static partial Regex WrapperStart();

    // References, footnotes and spaces at the end of a line.
    [GeneratedRegex(@"(?:\s*(?:<ref[^>]*/>|<ref[^>]*>.*?</ref\s*>|\{\{\s*(?:efn|sfn|r|refn)\b[^{}]*\}\}))+\s*$", RegexOptions.IgnoreCase)]
    private static partial Regex TrailingReferences();

    // [[File:Status iucn3.1 EN.svg]] and the like.
    [GeneratedRegex(@"status[ _]iucn[^|\]]*", RegexOptions.IgnoreCase)]
    private static partial Regex StatusImage();

    // A category code in brackets, as some lists write it: "(EN)", "(CR)".
    [GeneratedRegex(@"\(\s*(?:EX|EW|CR|EN|VU|NT|LC|DD|NE|LR/(?:cd|nt|lc))\s*\)")]
    private static partial Regex CodeInBrackets();

    // Author names (capitalised, often abbreviated with a full stop), joined by "&", "and", "ex", "et al.",
    // with particles such as "de" or "von" and a year, perhaps in brackets.
    [GeneratedRegex(@"^\s+(?<authority>\(?(?:\p{Lu}[\p{L}'’-]*\.?(?:\p{Lu}[\p{L}'’-]*\.?)*)(?:(?:\s*[,&]\s*|\s+)(?:&|and|ex|et\s+al\.|in|de|du|da|von|van|der|f\.|\p{Lu}[\p{L}'’-]*\.?(?:\p{Lu}[\p{L}'’-]*\.?)*|\d{4}\)?|\))){0,8}\)?(?:\s*\p{Lu}[\p{L}'’-]*\.?(?:\p{Lu}[\p{L}'’-]*\.?)*)*)(?=\s*(?:[–—-]\s|,|\(|<ref|$))")]
    private static partial Regex PlainAuthority();

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
