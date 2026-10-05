using System.Globalization;
using BeastieBot3.Site.Update;

namespace BeastieBot3.Site.Display;

// The edit summary line of the status update page's result (Update/EditSummary.cs decides what
// goes in it).

public static partial class UpdateText {
    /// "IUCN Red List 2026-1: Panthera tigris VU→EN, Ursus maritimus EN→VU; 3 other IUCN statuses updated
    /// (ids, years, references or trends); 2 IUCN citations updated (assisted by Beastie Bot Species Status)".
    /// With more changes than fit in EditSummary.MaxListLength: "42 IUCN statuses changed (12 to EN,
    /// 20 to VU, 10 to LC)". Null when nothing changed.
    public static string? EditSummary(string? version, IReadOnlyList<Update.EditSummary.CategoryChange> changes, int otherItems, int citations) {
        if (changes.Count == 0 && otherItems == 0 && citations == 0) {
            return null;
        }
        var parts = new List<string>();
        if (changes.Count > 0) {
            var list = string.Join(", ", changes.Select(c => $"{c.Name} {c.From}→{c.To}"));
            if (list.Length > Update.EditSummary.MaxListLength) {
                var byCategory = changes.GroupBy(c => c.To, StringComparer.OrdinalIgnoreCase)
                    .OrderBy(g => SeverityOrder(g.Key))
                    .Select(g => $"{Count(g.Count())} to {g.Key}");
                list = $"{Count(changes.Count)} IUCN statuses changed ({string.Join(", ", byCategory)})";
            }
            parts.Add(list);
        }
        if (otherItems > 0) {
            var other = changes.Count > 0 ? " other" : "";
            var unchanged = changes.Count > 0 ? "" : "; categories unchanged";
            parts.Add(otherItems == 1
                ? $"1{other} IUCN status updated (ids, year, reference or trend{unchanged})"
                : $"{Count(otherItems)}{other} IUCN statuses updated (ids, years, references or trends{unchanged})");
        }
        if (citations > 0) {
            parts.Add(citations == 1 ? "1 IUCN citation updated" : $"{Count(citations)} IUCN citations updated");
        }
        var prefix = version is null ? "IUCN Red List" : $"IUCN Red List {version}";
        return $"{prefix}: {string.Join("; ", parts)} ({EditSummaryCredit})";
    }

    // Most threatened first; codes not listed come last.
    private static int SeverityOrder(string code) {
        var i = Array.FindIndex(SeverityCodes, c => string.Equals(c, code.Replace(" ", ""), StringComparison.OrdinalIgnoreCase));
        return i < 0 ? SeverityCodes.Length : i;
    }

    private static readonly string[] SeverityCodes =
        ["EX", "EW", "CR(PE)", "CR(PEW)", "CR", "EN", "VU", "LR/cd", "NT", "LR/nt", "LC", "LR/lc", "DD", "NE"];
}
