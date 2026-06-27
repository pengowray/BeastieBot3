using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using BeastieBot3.Audit.Model;

// Builds the aggregate summary tables shared by the two synonym formatting reports: a per-issue
// count table (optionally with a "% of all synonyms" column) and the HTML-presence table that
// makes the consistency case. Counts come from the per-synonym issue lists the producer collected,
// so the kinds can add up to more than the distinct synonym total.

namespace BeastieBot3.Audit.Producers;

internal static class SynonymSummary {
    // One row per candidate issue (kept even at zero so it is clear the check ran), then a distinct
    // synonym total. perSynonym is the issue list of each synonym actually listed in the report.
    public static AuditSummaryTable ByIssueType(
        string title, IReadOnlyList<SynonymIssue> candidates,
        IReadOnlyList<IReadOnlyList<SynonymIssue>> perSynonym, string note) {

        var rows = candidates
            .Select(issue => new[] {
                SynonymIssues.Label(issue),
                perSynonym.Count(list => list.Contains(issue)).ToString("N0", CultureInfo.InvariantCulture),
            } as IReadOnlyList<string>)
            .ToList();
        rows.Add(new[] { "Total (distinct synonyms)", perSynonym.Count.ToString("N0", CultureInfo.InvariantCulture) });

        return new AuditSummaryTable {
            Title = title, Note = note,
            Headers = new[] { "Issue", "Synonyms" }, Rows = rows, NumericColumns = new[] { 1 },
        };
    }

    // As above plus a "% of all synonyms" column measured against every synonym examined.
    public static AuditSummaryTable ByIssueTypeWithPercent(
        string title, IReadOnlyList<SynonymIssue> candidates,
        IReadOnlyList<IReadOnlyList<SynonymIssue>> perSynonym, long totalSynonyms, string note) {

        var rows = candidates
            .Select(issue => {
                var count = perSynonym.Count(list => list.Contains(issue));
                return new[] {
                    SynonymIssues.Label(issue),
                    count.ToString("N0", CultureInfo.InvariantCulture),
                    Percent(count, totalSynonyms),
                } as IReadOnlyList<string>;
            })
            .ToList();
        rows.Add(new[] {
            "Total (distinct synonyms)",
            perSynonym.Count.ToString("N0", CultureInfo.InvariantCulture),
            Percent(perSynonym.Count, totalSynonyms),
        });

        return new AuditSummaryTable {
            Title = title, Note = note,
            Headers = new[] { "Issue", "Synonyms", "% of all synonyms" }, Rows = rows, NumericColumns = new[] { 1, 2 },
        };
    }

    // The with/without-HTML breakdown plus a release-agnostic sentence on whether HTML is
    // consistent. htmlCount is the number of synonyms carrying markup or a stray entity.
    public static AuditSummaryTable HtmlPresence(long totalSynonyms, long htmlCount) {
        var plain = totalSynonyms - htmlCount;
        var rows = new List<IReadOnlyList<string>> {
            new[] { "Contains HTML markup or entities", htmlCount.ToString("N0", CultureInfo.InvariantCulture), Percent(htmlCount, totalSynonyms) },
            new[] { "Plain text (no HTML)", plain.ToString("N0", CultureInfo.InvariantCulture), Percent(plain, totalSynonyms) },
        };
        return new AuditSummaryTable {
            Title = "HTML in synonym names",
            Note = HtmlCase(totalSynonyms, htmlCount),
            Headers = new[] { "Synonyms", "Count", "% of all synonyms" }, Rows = rows, NumericColumns = new[] { 1, 2 },
        };
    }

    private static string HtmlCase(long total, long html) {
        if (total == 0) {
            return "No synonyms were available to examine.";
        }
        if (html == 0) {
            return $"None of the {total:N0} synonym names examined contain HTML. The field is uniformly plain text, so there is no inconsistency to resolve.";
        }
        var pct = 100.0 * html / total;
        var pctText = Percent(html, total);
        if (pct < 5) {
            return $"Only {html:N0} of {total:N0} synonym names ({pctText}) contain HTML; the field is overwhelmingly plain text. " +
                   "That makes the HTML-bearing names inconsistent outliers, most likely pasted from a rendered page, and good candidates to normalise to plain text.";
        }
        return $"{html:N0} of {total:N0} synonym names ({pctText}) contain HTML while the rest are plain text. " +
               "The field mixes the two conventions, so HTML use is inconsistent across the synonym set.";
    }

    private static string Percent(long count, long total) =>
        total == 0 ? "0%" : (100.0 * count / total).ToString("0.###", CultureInfo.InvariantCulture) + "%";
}
