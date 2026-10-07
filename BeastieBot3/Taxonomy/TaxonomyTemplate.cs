using System.Text.RegularExpressions;

// English Wikipedia's taxonomy templates (Template:Taxonomy/Felis), which automatic taxoboxes and
// speciesboxes climb to show a taxon's classification: each gives the taxon's rank (in Latin:
// familia, ordo, ...), its parent (the next template up) and the article it links.

namespace BeastieBot3.Taxonomy;

/// One taxonomy template. Name: the part after "Template:Taxonomy/" ("Felis", "Panthera (genus)");
/// Rank: in English ("family"), null for no rank; Display: the name shown (from link=). SameAs: a
/// template ("Mammalia/skip" says "same as=Mammalia") whose rank and name this one takes, with its own parent.
internal sealed record TaxonomyTemplate(string Name, string? Rank, string? Parent, string Display, string? SameAs = null);

internal static partial class TaxonomyTemplates {
    public const string Prefix = "Template:Taxonomy/";

    public static string Title(string name) => Prefix + name.Trim();

    /// The template's fields, or null when the text is not a taxonomy template.
    public static TaxonomyTemplate? Parse(string name, string wikitext) {
        var rank = Field(wikitext, "rank");
        var parent = Field(wikitext, "parent");
        var sameAs = CleanName(Field(wikitext, "same as"));
        if (rank is null && parent is null && sameAs is null) {
            return null;
        }
        var link = Field(wikitext, "link");
        var display = link is null ? name : (link.Contains('|') ? link[(link.IndexOf('|') + 1)..] : link);
        display = Regex.Replace(display, @"'{2,}|<[^>]+>|\[\[|\]\]", "").Trim();
        if (display.Length == 0) {
            display = name;
        }
        return new TaxonomyTemplate(name, EnglishRank(rank), CleanName(parent), display, sameAs);
    }

    // A template name as a parent gives it: "Felidae", not "Felidae|..." or a template call.
    internal static string? CleanName(string? value) {
        if (value is null) {
            return null;
        }
        var name = value.Split('|')[0].Trim();
        return name.Length == 0 || name.IndexOfAny(['{', '}', '[', ']', '<', '>', '#']) >= 0 ? null : name;
    }

    // "|rank=familia" on a line of its own.
    private static string? Field(string text, string name) =>
        Regex.Match(text, $@"^\s*\|\s*{name}\s*=\s*(?<v>[^\n]*?)\s*$", RegexOptions.Multiline) is { Success: true } m && m.Groups["v"].Value.Length > 0
            ? m.Groups["v"].Value.Replace("<!--", "").Trim()
            : null;

    // The Latin ranks the templates use, in English: "subfamilia" -> "subfamily".
    private static readonly Dictionary<string, string> Latin = new(StringComparer.OrdinalIgnoreCase) {
        ["regnum"] = "kingdom", ["phylum"] = "phylum", ["divisio"] = "division", ["classis"] = "class", ["cohors"] = "cohort",
        ["ordo"] = "order", ["familia"] = "family", ["tribus"] = "tribe", ["genus"] = "genus", ["sectio"] = "section",
        ["series"] = "series", ["species"] = "species", ["varietas"] = "variety", ["forma"] = "form", ["domain"] = "domain",
        ["dominium"] = "domain", ["legio"] = "legion", ["clade"] = "clade", ["cladus"] = "clade", ["unranked"] = "unranked",
        ["grandordo"] = "grandorder", ["mirordo"] = "mirorder", ["magnordo"] = "magnorder", ["alliance"] = "alliance",
    };

    public static string? EnglishRank(string? rank) {
        if (string.IsNullOrWhiteSpace(rank)) {
            return null;
        }
        // "grandordo-mb": the part after the hyphen is a display variant.
        var r = rank.Trim().ToLowerInvariant().Split('-')[0];
        if (Latin.TryGetValue(r, out var english)) {
            return english is "unranked" ? null : english;
        }
        foreach (var prefix in new[] { "super", "sub", "infra", "parv", "epi", "nan" }) {
            if (r.StartsWith(prefix, StringComparison.Ordinal) && Latin.TryGetValue(r[prefix.Length..], out var baseRank)) {
                return prefix + baseRank;
            }
        }
        return r;
    }

    /// The template a taxobox starts its classification from: an automatic taxobox's taxon, else a
    /// speciesbox's parent (a subgenus), else its genus.
    public static string? StartOf(IReadOnlyDictionary<string, string> fields) => CleanName(
        fields.TryGetValue("taxon", out var taxon) && taxon.Trim().Length > 0 && !taxon.Trim().Contains(' ') ? taxon.Trim()
        : fields.TryGetValue("parent", out var parent) && parent.Trim().Length > 0 ? parent.Trim()
        : fields.TryGetValue("genus", out var genus) && genus.Trim().Length > 0 ? genus.Trim()
        : fields.TryGetValue("taxon", out var speciesTaxon) && speciesTaxon.Trim().Split(' ') is [var g, ..] ? g
        : null);
}
