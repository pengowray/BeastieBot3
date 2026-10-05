using System.Text.RegularExpressions;
using BeastieBot3.Site.Display;

namespace BeastieBot3.Site.Update;

/// A line for the Wikipedia edit summary of an edit made with the status updater's result. It names
/// each taxon whose category changed, with the old and new codes, while that fits in MaxListLength;
/// otherwise it counts the changes by new category. Items that changed without a category change
/// (ids, year, status_system, status_ref, direction) and replaced citations are counted after it.
/// Items that were not changed are left out. Null when nothing changed.
public static partial class EditSummary {
    /// Room left for the reader's own words: MediaWiki keeps the first 500 characters of a summary.
    public const int MaxListLength = 300;

    public sealed record CategoryChange(string Name, string From, string To);

    public static string? For(StatusUpdateResult result, string? version) {
        var changes = new List<CategoryChange>();
        var otherItems = 0;
        var citations = 0;
        foreach (var finding in result.Findings.Where(f => f.Outcome == StatusOutcome.Updated)) {
            if (finding.Kind == StatusItemKind.Citation) {
                citations++;
                continue;
            }
            var from = CodeIn(finding.Before);
            var to = finding.After is null ? null : CodeIn(finding.After);
            if (from is not null && to is not null && !SameCode(from, to) && finding.Taxon is { } taxon) {
                if (!changes.Any(c => c.Name == taxon.ScientificName && SameCode(c.To, to))) {
                    changes.Add(new CategoryChange(taxon.ScientificName, from, to));
                }
            } else {
                otherItems++;
            }
        }
        return UpdateText.EditSummary(version, changes, otherItems, citations);
    }

    /// The status code in an item's text: {{IUCN status|EN|...}}, "| status = EN",
    /// "| iucn-status = EN", or a table cell that is only a code. Null when there is none.
    internal static string? CodeIn(string text) {
        if (TemplateCode().Match(text) is { Success: true } template) {
            return template.Groups["code"].Value.Trim();
        }
        if (ParameterCode().Match(text) is { Success: true } parameter) {
            return parameter.Groups["code"].Value.Trim();
        }
        var bare = text.Trim();
        return StatusUpdater.BareCode(bare) is not null ? bare : null;
    }

    private static bool SameCode(string a, string b) =>
        string.Equals(a.Replace(" ", "", StringComparison.Ordinal), b.Replace(" ", "", StringComparison.Ordinal), StringComparison.OrdinalIgnoreCase);

    [GeneratedRegex(@"\{\{\s*IUCN[ _]status\s*\|\s*(?<code>[^|}]+)", RegexOptions.IgnoreCase)]
    private static partial Regex TemplateCode();

    [GeneratedRegex(@"\|\s*(?:iucn-)?status\s*=\s*(?<code>[^|\n}<]+)", RegexOptions.IgnoreCase)]
    private static partial Regex ParameterCode();
}
