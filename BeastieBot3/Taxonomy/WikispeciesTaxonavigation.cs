using System.Text;
using System.Text.RegularExpressions;

// Wikispecies classifications. A taxon page's Taxonavigation section calls the template of the taxon
// above it and names the taxon on a line of its own:
//     =={{int:Taxonavigation}}==
//     {{Panthera (Leo)}}
//     Species: ''[[Panthera leo]]''
// Each taxonavigation template (Template:Panthera) calls the template above it in the same way and
// names one taxon, sometimes more than one, each on a "Rank: name" line with a Latin rank:
//     {{Pantherinae}}
//     Genus: ''[[Panthera]]'' <br>
// {{Taxonav|Feloidea}} also calls Template:Feloidea. The "summary" block of a template
// ({{#ifeq:{{{1}}}|summary|...}}), comments, <noinclude> parts and DISPLAYTITLE are not read.

namespace BeastieBot3.Taxonomy;

/// One "Rank: name" line. Rank: in English ("family"), null for no rank. Name: the taxon's
/// Wikispecies page title, from the line's link or {{fbr|...}}-style template.
internal sealed record WikispeciesStep(string? Rank, string Name);

/// Parent: the template the text calls first (without "Template:"), null when none. Steps: top down.
internal sealed record WikispeciesTaxonav(string? Parent, IReadOnlyList<WikispeciesStep> Steps);

internal static partial class WikispeciesTaxonavigation {
    public const string TemplatePrefix = "Template:";

    public static string TemplateTitle(string name) => TemplatePrefix + name.Trim();

    /// The Wikispecies page title for an IUCN scientific name: "Panthera leo persica" for IUCN's
    /// "Panthera leo ssp. persica"; plant "subsp." and "var." stay in the title.
    public static string TitleFor(string iucnScientificName) =>
        SpacesRegex().Replace(iucnScientificName.Replace(" ssp. ", " ", StringComparison.Ordinal), " ").Trim();

    /// A taxonavigation template's parent and steps; null when the text names no parent and no step.
    public static WikispeciesTaxonav? ParseTemplate(string wikitext) {
        var result = Read(Clean(wikitext), stopAt: null);
        return result.Parent is null && result.Steps.Count == 0 ? null : result;
    }

    /// The Taxonavigation section of a taxon page: the template it calls and its steps down to the
    /// page's own taxon (pageTitle; the steps after it list lower taxa). Null when the page has no
    /// Taxonavigation section.
    public static WikispeciesTaxonav? ParsePage(string wikitext, string pageTitle) {
        var lines = wikitext.Split('\n');
        var start = Array.FindIndex(lines, l => TaxonavHeadingRegex().IsMatch(l));
        if (start < 0) {
            return null;
        }
        var section = new StringBuilder();
        for (var i = start + 1; i < lines.Length && !lines[i].TrimStart().StartsWith("==", StringComparison.Ordinal); i++) {
            section.Append(lines[i]).Append('\n');
        }
        var result = Read(Clean(section.ToString()), stopAt: pageTitle);
        return result.Parent is null && result.Steps.Count == 0 ? null : result;
    }

    private static WikispeciesTaxonav Read(string text, string? stopAt) {
        string? parent = null;
        var steps = new List<WikispeciesStep>();
        foreach (var raw in text.Split('\n')) {
            var line = raw.Replace("‎", "").Replace("‏", "").Trim();
            line = BreakRegex().Replace(line, "").Trim();
            if (line.Length == 0) {
                continue;
            }
            if (steps.Count == 0 && parent is null && ParentCall(line) is { } call) {
                parent = call;
                continue;
            }
            if (RankLineRegex().Match(line) is not { Success: true } m) {
                continue;
            }
            if (NameOf(m.Groups["rest"].Value) is not { } name) {
                continue;
            }
            steps.Add(new WikispeciesStep(TaxonomyTemplates.EnglishRank(m.Groups["rank"].Value), name));
            if (stopAt is not null && SameTitle(name, stopAt)) {
                break;
            }
        }
        return new WikispeciesTaxonav(parent, steps);
    }

    // "{{Pantherinae}}" or "{{Taxonav|Feloidea}}" alone on a line: the template above.
    private static string? ParentCall(string line) {
        if (ParentCallRegex().Match(line) is not { Success: true } m) {
            return null;
        }
        var name = m.Groups["name"].Value.Trim();
        var arg = m.Groups["arg"].Success ? m.Groups["arg"].Value.Trim() : null;
        if (name.Equals("Taxonav", StringComparison.OrdinalIgnoreCase)) {
            return arg is { Length: > 0 } ? Capitalize(arg) : null;
        }
        // Magic words, parser functions and page furniture are not taxonavigation templates.
        if (arg is not null || name.Contains(':') || name.StartsWith('#') || NotParents.Contains(name)) {
            return null;
        }
        return Capitalize(name);
    }

    private static readonly HashSet<string> NotParents = new(StringComparer.OrdinalIgnoreCase) {
        "image", "images", "clear", "-", "!", "!!", "taxonbar", "reflist", "PAGENAME", "BASEPAGENAME", "nowrap",
    };

    // The taxon a line names: one link, one {{fbr|...}}-style template, or plain text. A line naming
    // several taxa ("Subspecies: {{ssp|...}}, {{ssp|...}}") names none.
    private static string? NameOf(string rest) {
        var links = LinkRegex().Matches(rest);
        var brs = BrTemplateRegex().Matches(rest);
        var templates = rest.Split("{{").Length - 1;
        if (links.Count + brs.Count > 1 || (links.Count + brs.Count == 0 && templates > 0)) {
            return null;
        }
        string name;
        if (brs.Count == 1) {
            if (templates > 1) {
                return null;
            }
            name = brs[0].Groups["name"].Value;
        } else if (links.Count == 1) {
            name = links[0].Groups["target"].Value;
        } else {
            name = rest.Replace("''", "").Trim().TrimStart('†', '?').Trim();
            if (!PlainNameRegex().IsMatch(name)) {
                return null;
            }
        }
        name = SpacesRegex().Replace(name.Replace('_', ' '), " ").Trim();
        return name.Length == 0 || name.IndexOfAny(['{', '}', '[', ']', '|', '<', '>', '#', ':']) >= 0 ? null : Capitalize(name);
    }

    private static bool SameTitle(string a, string b) =>
        string.Equals(Capitalize(a.Replace('_', ' ').Trim()), Capitalize(b.Replace('_', ' ').Trim()), StringComparison.Ordinal);

    private static string Capitalize(string s) => s.Length == 0 ? s : char.ToUpperInvariant(s[0]) + s[1..];

    // Removes what is not part of the classification: comments, <noinclude> parts, parser functions
    // and magic words ({{#ifeq:...}}, {{DISPLAYTITLE:...}}), <section> and <includeonly> tags.
    internal static string Clean(string text) {
        text = CommentRegex().Replace(text, "");
        text = NoincludeRegex().Replace(text, "");
        var at = text.IndexOf("<noinclude>", StringComparison.OrdinalIgnoreCase);
        if (at >= 0) {
            text = text[..at];
        }
        text = RemoveParserFunctions(text);
        text = TagRegex().Replace(text, "");
        return text.Replace("__NOTOC__", "").Replace("__NOEDITSECTION__", "");
    }

    // {{#...}} and {{NAME:...}} calls, with everything nested inside them.
    private static string RemoveParserFunctions(string text) {
        var sb = new StringBuilder(text.Length);
        var i = 0;
        while (i < text.Length) {
            if (text.AsSpan(i).StartsWith("{{") && IsFunctionStart(text, i + 2)) {
                var depth = 0;
                var j = i;
                while (j < text.Length) {
                    if (text.AsSpan(j).StartsWith("{{")) {
                        depth++;
                        j += 2;
                    } else if (text.AsSpan(j).StartsWith("}}")) {
                        depth--;
                        j += 2;
                        if (depth == 0) {
                            break;
                        }
                    } else {
                        j++;
                    }
                }
                i = j;
                continue;
            }
            sb.Append(text[i]);
            i++;
        }
        return sb.ToString();
    }

    private static bool IsFunctionStart(string text, int at) {
        if (at < text.Length && text[at] == '#') {
            return true;
        }
        var end = text.IndexOfAny(['|', '}', ':', '\n'], at);
        return end > at && text[end] == ':' && text[at..end].Trim() is { Length: > 0 } word && word.All(c => char.IsUpper(c) || c == '_');
    }

    [GeneratedRegex(@"^\s*==\s*(\{\{\s*int:Taxonavigation\s*\}\}|Taxonavigation)\s*==\s*$", RegexOptions.IgnoreCase)]
    private static partial Regex TaxonavHeadingRegex();

    [GeneratedRegex(@"^\{\{\s*(?<name>[^{}|]+?)\s*(\|\s*(?<arg>[^{}|]*?)\s*(\|[^{}]*)?)?\}\}$")]
    private static partial Regex ParentCallRegex();

    [GeneratedRegex(@"^[†?\s]*(?<rank>[A-Z][a-z]+)\s*:\s*(?<rest>.+)$")]
    private static partial Regex RankLineRegex();

    [GeneratedRegex(@"\[\[\s*(?<target>[^\]|]+?)\s*(\|[^\]]*)?\]\]")]
    private static partial Regex LinkRegex();

    [GeneratedRegex(@"\{\{\s*[a-z]{1,4}br\s*\|\s*(?<name>[^}|]+?)\s*(\|[^}]*)?\}\}")]
    private static partial Regex BrTemplateRegex();

    [GeneratedRegex(@"^[A-Z][a-z-]+( [a-z-]+| subsp\.| var\.| subg\.| sect\.| ×){0,4}$")]
    private static partial Regex PlainNameRegex();

    [GeneratedRegex(@"<br\s*/?>", RegexOptions.IgnoreCase)]
    private static partial Regex BreakRegex();

    [GeneratedRegex(@"<!--.*?(-->|$)", RegexOptions.Singleline)]
    private static partial Regex CommentRegex();

    [GeneratedRegex(@"<noinclude>.*?</noinclude>", RegexOptions.Singleline | RegexOptions.IgnoreCase)]
    private static partial Regex NoincludeRegex();

    [GeneratedRegex(@"</?(section|includeonly|onlyinclude)\b[^>]*>", RegexOptions.IgnoreCase)]
    private static partial Regex TagRegex();

    [GeneratedRegex(@"\s+")]
    private static partial Regex SpacesRegex();
}
