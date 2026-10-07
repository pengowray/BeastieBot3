using System.Text.RegularExpressions;

namespace BeastieBot3.Shared.Wikitext;

/// A choice of the IUCN categories a list is of, for comparing a list with its group (the status
/// update page's "Categories" choice, and `wikipedia report-species-lists`). Codes are {{IUCN status}}
/// codes: "CR" takes in CR(PE) and CR(PEW), and "CR(PE)" alone means only possibly extinct taxa.
/// Empty Codes: every category.
public sealed record ListCategoryChoice(string Key, string Label, IReadOnlySet<string> Codes) {
    public bool All => Codes.Count == 0;
}

public static class ListCategories {
    public static readonly ListCategoryChoice All = new("all", "All categories", new HashSet<string>());
    public static readonly ListCategoryChoice Threatened = new("threatened", "Threatened (CR, EN, VU)", new HashSet<string> { "CR", "EN", "VU" });
    // The "Recently extinct" lists (rules/list-presets.yml, preset ex): Extinct, and Critically
    // Endangered (Possibly Extinct) in a section of its own.
    public static readonly ListCategoryChoice Extinct = new("extinct", "Extinct and possibly extinct (EX, CR(PE), CR(PEW))",
        new HashSet<string> { "EX", "CR(PE)", "CR(PEW)" });

    /// The choices in the order the status update page offers them.
    public static readonly IReadOnlyList<ListCategoryChoice> Choices = [
        All,
        Threatened,
        Extinct,
        new("EW", "Extinct in the Wild (EW)", new HashSet<string> { "EW" }),
        new("CR", "Critically Endangered (CR, CR(PE), CR(PEW))", new HashSet<string> { "CR" }),
        new("EN", "Endangered (EN)", new HashSet<string> { "EN" }),
        new("VU", "Vulnerable (VU)", new HashSet<string> { "VU" }),
        new("NT", "Near Threatened (NT)", new HashSet<string> { "NT" }),
        new("LC", "Least Concern (LC)", new HashSet<string> { "LC" }),
        new("DD", "Data Deficient (DD)", new HashSet<string> { "DD" }),
    ];

    public static ListCategoryChoice? Find(string? key) =>
        string.IsNullOrWhiteSpace(key) ? null : Choices.FirstOrDefault(c => string.Equals(c.Key, key.Trim(), StringComparison.OrdinalIgnoreCase));

    /// The categories a page's title says the list is of, as the list presets title them ("List of
    /// critically endangered amphibians", "List of recently extinct mammals", "List of threatened
    /// birds of Brazil"); null when the title names no category. "Endangered" alone is EN, as in
    /// the per-category lists.
    public static ListCategoryChoice? FromTitle(string? title) {
        var t = (title ?? string.Empty).ToLowerInvariant().Replace('-', ' ');
        if (t.Contains("extinct in the wild", StringComparison.Ordinal)) {
            return Find("EW");
        }
        if (Word("extinct").IsMatch(t)) {
            return Extinct;
        }
        if (t.Contains("critically endangered", StringComparison.Ordinal)) {
            return Find("CR");
        }
        if (t.Contains("near threatened", StringComparison.Ordinal)) {
            return Find("NT");
        }
        if (Word("threatened").IsMatch(t)) {
            return Threatened;
        }
        if (Word("endangered").IsMatch(t)) {
            return Find("EN");
        }
        if (Word("vulnerable").IsMatch(t)) {
            return Find("VU");
        }
        if (t.Contains("least concern", StringComparison.Ordinal)) {
            return Find("LC");
        }
        if (t.Contains("data deficient", StringComparison.Ordinal)) {
            return Find("DD");
        }
        return null;
    }

    private static Regex Word(string word) => new($@"\b{word}\b", RegexOptions.CultureInvariant);
}
