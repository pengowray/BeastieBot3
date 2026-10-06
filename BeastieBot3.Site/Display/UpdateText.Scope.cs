using BeastieBot3.Site.Data;
using BeastieBot3.Site.Lists;
using BeastieBot3.Site.Update;

namespace BeastieBot3.Site.Display;

// The status update page's comparison of the listed taxa with their group (Update/ListScope.cs).
// PROVISIONAL wording, to be redrafted and signed off.

public static partial class UpdateText {
    public static string ScopeHeading(GroupRow group) => $"Compared with {GroupList.HeadingText(group)}";

    public static string ScopeSummary(ListScopeResult r) {
        var categories = r.Categories is null ? "" : $" in {CategoryList(r.Categories)}";
        return $"The text lists {Count(r.Species)} of the {Count(r.SpeciesInScope)} species{categories} that IUCN has in {GroupList.HeadingText(r.Scope)}.";
    }

    public static string ScopeCategories(IReadOnlySet<string> categories, bool fromCodes) => fromCodes
        ? $"The statuses in the text are all {CategoryList(categories)}, so only {CategoryList(categories)} taxa are compared."
        : $"The text gives no statuses, and nearly all its species are {CategoryList(categories)} now, so only {CategoryList(categories)} taxa are compared.";

    public static string ScopeInfra(ListScopeResult r) => r.InfraChecked
        ? $"Subspecies and varieties are compared too: the text lists {Count(r.Infra)} of {Count(r.InfraInScope)}."
        : "Subspecies and varieties are not compared.";

    public const string ScopeChooseLabel = "Compare with";
    public const string ScopeChooseButton = "Compare";

    public const string ScopePartial = "The text lists too few of these taxa to be a list of the whole group, so the missing taxa are not listed.";
    public const string ScopeListAnywayButton = "List the missing taxa anyway";

    public static string MissingHeading(int n) => n == 1 ? "1 taxon missing from the text" : $"{Count(n)} taxa missing from the text";
    public const string MissingNone = "No taxa missing.";
    public static string MissingCapped(int shown, int total) => $"The first {Count(shown)} of {Count(total)} are listed.";
    public const string MissingLinesLabel = "List lines for the missing taxa (read only)";
    public const string CopyMissingAccessible = "Copy list lines for the missing taxa";
    public const string MissingLinesHelp = "Each line has {{IUCN status}} with the ids and year. Put each line in its place in the list.";

    public static string OutsideHeading(int n) => n == 1 ? "1 listed taxon outside the group" : $"{Count(n)} listed taxa outside the group";
    public static string OtherCategoryHeading(int n) => n == 1 ? "1 listed taxon now in another category" : $"{Count(n)} listed taxa now in another category";
    public static string DuplicatesHeading(int n) => n == 1 ? "1 taxon listed under two names" : $"{Count(n)} taxa listed under two or more names";

    public const string ColumnLines = "Lines";
    public const string ColumnInText = "In the text";
    public const string ColumnLatest = "Latest";
    public const string ColumnNames = "Names in the text";
    public const string ColumnCategory = "Category";

    public static string LineNumbers(IReadOnlyList<int> lines) => string.Join(", ", lines.Select(l => Count(l)));

    private static string CategoryList(IReadOnlySet<string> categories) {
        var ordered = categories.OrderBy(c => Array.IndexOf(new[] { "EX", "EW", "CR", "EN", "VU", "NT", "LC", "DD" }, c)).ToList();
        return ordered.Count switch {
            1 => ordered[0],
            2 => $"{ordered[0]} or {ordered[1]}",
            _ => string.Join(", ", ordered.Take(ordered.Count - 1)) + " or " + ordered[^1],
        };
    }
}
