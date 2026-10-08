using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;

namespace BeastieBot3.Site.Update;

/// {{IUCN statuses}}: the box of species counts by IUCN category that lists of species have
/// ("{{IUCN statuses|ex=3|ew=0|cr=5|en=7|vu=17|nt=14|lc=174|dd=58|ne=45}}"). Its counts are made
/// again from the updated text, as editors make them: one for each {{Species table/row}}, by its
/// iucn-status (NE or none counts as ne); or, in a text with no species table rows, one for each
/// {{IUCN status}}, by its code. Of the 95 articles that used the template in October 2026, 88 list
/// their species with {{Species table/row}}, and in 79 of them every count was the count of rows.
/// CR(PE) and CR(PEW) count as cr, LR/nt and LR/cd as nt, LR/lc as lc.
public static partial class IucnStatusesSummary {
    public const string TemplateName = "iucn statuses";
    public const string SpeciesTableRow = "species table/row";
    public const string StatusTemplate = "iucn status";

    /// The count parameters, in the template's order.
    public static readonly IReadOnlyList<string> Keys = ["ex", "ew", "cr", "en", "vu", "nt", "lc", "dd", "ne"];

    /// What the text gives to count. FromRows: counted from {{Species table/row}}, else from
    /// {{IUCN status}}. Items: the rows or templates counted. Uncounted: those with a code that has no
    /// count parameter ("NA").
    public sealed record Counts(IReadOnlyDictionary<string, int> ByKey, int Items, bool FromRows, int Uncounted);

    public static Counts Count(WikitextScanner scanner) {
        var byKey = Keys.ToDictionary(k => k, _ => 0);
        var rows = scanner.Templates.Where(t => t.Name == SpeciesTableRow).ToList();
        var uncounted = 0;
        if (rows.Count > 0) {
            foreach (var row in rows) {
                var value = row.Named("iucn-status") is { } p ? scanner.Original(p.Value) : null;
                if (KeyFor(value, missing: "ne") is { } key) {
                    byKey[key]++;
                } else {
                    uncounted++;
                }
            }
            return new Counts(byKey, rows.Count - uncounted, true, uncounted);
        }
        var items = 0;
        foreach (var template in scanner.Templates.Where(t => t.Name == StatusTemplate)) {
            var code = template.Positional(1) is { } p ? scanner.Original(p.Value) : null;
            if (KeyFor(code, missing: null) is { } key) {
                byKey[key]++;
                items++;
            } else {
                uncounted++;
            }
        }
        return new Counts(byKey, items, false, uncounted);
    }

    /// The count parameter for a status code; `missing` for a blank code; null for a code with no
    /// count parameter.
    public static string? KeyFor(string? code, string? missing) {
        var text = code is null ? string.Empty : CodeEnd().Replace(code, string.Empty);
        text = text.Replace(" ", string.Empty, StringComparison.Ordinal).Trim().ToUpperInvariant();
        return text switch {
            "" => missing,
            "EX" => "ex",
            "EW" => "ew",
            "CR" or "CR(PE)" or "CR(PEW)" or "PE" or "PEW" => "cr",
            "EN" => "en",
            "VU" => "vu",
            "NT" or "LR/NT" or "LR/CD" or "CD" => "nt",
            "LC" or "LR/LC" => "lc",
            "DD" => "dd",
            "NE" => "ne",
            _ => null,
        };
    }

    /// The text from the first reference, comment or template on: "LC<ref name=x/>" is LC.
    [GeneratedRegex(@"[<{].*$", RegexOptions.Singleline)]
    private static partial Regex CodeEnd();

    /// Brings the {{IUCN statuses}} template in the result's text up to date with the text's counts,
    /// and adds one when add is true and the text has species table rows but no such template. The
    /// finding is put among the result's findings by its line in input (the text before any change).
    /// SummaryMissing is set when a template could be added and add is false.
    public static StatusUpdateResult Apply(string input, StatusUpdateResult result, bool add) {
        var scanner = new WikitextScanner(result.Text);
        var counts = Count(scanner);
        var templates = scanner.Templates.Where(t => t.Name == TemplateName).ToList();
        if (templates.Count == 0) {
            if (!counts.FromRows || counts.Items == 0) {
                return result;
            }
            if (!add) {
                return result with { SummaryMissing = true };
            }
            return Add(input, result, scanner, counts);
        }

        var findings = new List<StatusFinding>();
        var text = result.Text;
        if (templates.Count > 1) {
            foreach (var template in templates) {
                var original = scanner.Original(template.Span);
                findings.Add(new StatusFinding(StatusItemKind.StatusSummary, LineIn(input, original), StatusOutcome.NotUpdated, original, null,
                    null, [new StatusNote(StatusNoteKind.SummaryTwoOrMore)]));
            }
        } else {
            var template = templates[0];
            var original = scanner.Original(template.Span);
            var line = LineIn(input, original);
            if (counts.Items == 0) {
                findings.Add(new StatusFinding(StatusItemKind.StatusSummary, line, StatusOutcome.NotUpdated, original, null, null,
                    [new StatusNote(StatusNoteKind.SummaryNothingToCount)]));
            } else {
                var (updated, changes) = Rewrite(scanner, template, counts);
                var notes = new List<StatusNote> { new(StatusNoteKind.SummaryCountedFrom, counts.FromRows ? "rows" : "templates", counts.Items) };
                if (counts.Uncounted > 0) {
                    notes.Add(new StatusNote(StatusNoteKind.SummaryUncounted, null, counts.Uncounted));
                }
                if (changes.Count == 0) {
                    findings.Add(new StatusFinding(StatusItemKind.StatusSummary, line, StatusOutcome.Current, original, null, null, notes));
                } else {
                    notes.Insert(0, new StatusNote(StatusNoteKind.SummaryChanged, string.Join(", ", changes)));
                    findings.Add(new StatusFinding(StatusItemKind.StatusSummary, line, StatusOutcome.Updated, original, updated, null, notes));
                    text = text[..template.Span.Start] + updated + text[template.Span.End..];
                }
            }
        }
        return result with { Text = text, Findings = Merge(result.Findings, findings) };
    }

    // The template with each count parameter set to its count: a parameter it has gets the new
    // number in place of the old (its spacing kept); a count it lacks is added at the end unless it
    // is 0, or is dd or ne and the box hides them (suppress-others=y). Changes: "en 6 → 7".
    private static (string Text, List<string> Changes) Rewrite(WikitextScanner scanner, WikiTemplate template, Counts counts) {
        var original = scanner.Original(template.Span);
        var start = template.Span.Start;
        var edits = new List<(int Start, int End, string Text)>();
        var changes = new List<string>();
        var suppressed = template.Named("suppress-others") is { } s && scanner.Original(s.Value).Trim() == "y";
        var appended = new StringBuilder();
        foreach (var key in Keys) {
            var count = counts.ByKey[key];
            if (template.Named(key) is { } p) {
                var value = scanner.Original(p.Value);
                var trimmed = value.Trim();
                if (int.TryParse(trimmed, NumberStyles.None, CultureInfo.InvariantCulture, out var old) && old == count) {
                    continue;
                }
                var lead = value.Length - value.TrimStart().Length;
                edits.Add((p.Value.Start + lead, p.Value.Start + lead + trimmed.Length, count.ToString(CultureInfo.InvariantCulture)));
                changes.Add($"{key} {(trimmed.Length == 0 ? "0" : trimmed)} → {count}");
            } else if (count > 0 && !(suppressed && key is "dd" or "ne")) {
                appended.Append(CultureInfo.InvariantCulture, $"|{key}={count}");
                changes.Add($"{key} 0 → {count}");
            }
        }
        var text = new StringBuilder(original);
        if (appended.Length > 0) {
            // Before the closing "}}", after any spacing or newline that ends the last parameter.
            var close = original.Length - 2;
            while (close > 0 && char.IsWhiteSpace(original[close - 1])) {
                close--;
            }
            text.Insert(close, appended.ToString());
        }
        foreach (var (editStart, editEnd, value) in edits.OrderByDescending(e => e.Start)) {
            text.Remove(editStart - start, editEnd - editStart).Insert(editStart - start, value);
        }
        return (text.ToString(), changes);
    }

    // A new template: on the line after a "Conventions" heading (where the lists that have one put it),
    // else after the heading above the first species table row, else on the line before that row's
    // outermost template.
    private static StatusUpdateResult Add(string input, StatusUpdateResult result, WikitextScanner scanner, Counts counts) {
        var box = "{{IUCN statuses|" + string.Join("|", Keys.Select(k => $"{k}={counts.ByKey[k]}")) + "}}";
        var text = result.Text;
        var firstRow = scanner.Templates.First(t => t.Name == SpeciesTableRow).Span.Start;
        var outer = scanner.OuterTemplateAt(firstRow)?.Span.Start ?? firstRow;
        int at;
        string? heading;
        int line;
        if (ConventionsHeading().Match(text) is { Success: true } conventions) {
            (at, heading, line) = (LineEndAfter(text, conventions.Index), conventions.Groups["title"].Value.Trim(), LineIn(input, conventions.Value) + 1);
        } else if (AnyHeading().Matches(text[..outer]).LastOrDefault() is { } above) {
            (at, heading, line) = (LineEndAfter(text, above.Index), above.Groups["title"].Value.Trim(), LineIn(input, above.Value) + 1);
        } else {
            at = scanner.LineStart(scanner.LineOf(outer));
            heading = null;
            line = LineIn(input, text[at..LineEndAfter(text, at)].TrimEnd('\n'));
        }
        var updated = text[..at] + box + "\n" + text[at..];
        var notes = new List<StatusNote> {
            new(StatusNoteKind.SummaryAdded, heading),
            new(StatusNoteKind.SummaryCountedFrom, "rows", counts.Items),
        };
        if (counts.Uncounted > 0) {
            notes.Add(new StatusNote(StatusNoteKind.SummaryUncounted, null, counts.Uncounted));
        }
        var finding = new StatusFinding(StatusItemKind.StatusSummary, line, StatusOutcome.Updated, string.Empty, box, null, notes);
        return result with { Text = updated, Findings = Merge(result.Findings, [finding]) };
    }

    // The position after the end of the line that the position is on (after its "\n").
    private static int LineEndAfter(string text, int position) {
        var end = text.IndexOf('\n', position);
        return end < 0 ? text.Length : end + 1;
    }

    // The 1-based line of the first place the snippet is in the input; 1 when it is not there.
    private static int LineIn(string input, string snippet) {
        var at = snippet.Length == 0 ? -1 : input.IndexOf(snippet, StringComparison.Ordinal);
        return at < 0 ? 1 : input.AsSpan(0, at).Count('\n') + 1;
    }

    private static IReadOnlyList<StatusFinding> Merge(IReadOnlyList<StatusFinding> findings, IReadOnlyList<StatusFinding> added) =>
        added.Count == 0 ? findings : [.. findings.Concat(added).OrderBy(f => f.Line)];

    [GeneratedRegex(@"^(={2,6})\s*(?<title>Conventions)\s*\1\s*$", RegexOptions.Multiline | RegexOptions.IgnoreCase)]
    private static partial Regex ConventionsHeading();

    [GeneratedRegex(@"^(={2,6})(?<title>[^=\n].*?)\1\s*$", RegexOptions.Multiline)]
    private static partial Regex AnyHeading();
}
