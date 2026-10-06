using System.Text.Json;
using System.Text.RegularExpressions;
using BeastieBot3.Iucn.Citations;
using BeastieBot3.Shared.SiteData;

// The credits of an assessment for the site's taxon page: the payload's credits[] array, one group
// per credit_type_name, with value[]'s entries (full names, usually with an affiliation) as IUCN wrote
// them. Rules:
//   - Whitespace in an entry is collapsed to single spaces and trimmed ("Pranav  Chanchani"); nothing
//     else in a name or affiliation is changed.
//   - Email addresses are left out: an entry that is only an email address is dropped, and an address
//     inside an entry is removed with its brackets ("Nicola Biggs (n.biggs@kew.org)" gives
//     "Nicola Biggs"). About 0.75% of entries have one. "@" on its own ("was @ Whitaker Consultants")
//     is kept.
//   - Entries that are not strings (null) are skipped, and an entry listed twice in a type is kept once.
//   - A few payloads repeat a credit type; the blocks are merged into one group.
//   - value[] is in no fixed order (in 2026-1 it differs from the order of the "full" string in two
//     of every three blocks with two or more names), while IUCN's pages and the citation use the order
//     of "full". The entries are put in that order when each entry's surname (its last word, after
//     notes in brackets and a Jr./Sr./II/III/IV) is found as a whole word exactly once in "full", at
//     different places; otherwise value[]'s order is kept.
//   - A type whose value[] is empty, or holds only email addresses, is shown as its "full" string
//     (HTML tags such as <i>et al.</i> removed, entities decoded).
//   - Groups are in CreditTypes.Order; a type not in it comes after, in payload order.

namespace BeastieBot3.SiteBuild;

/// One credit group: the entries of value[], or (IsFullOnly) the one "full" string.
internal sealed record CreditGroup(string Type, IReadOnlyList<string> Names, bool IsFullOnly);

internal static partial class AssessmentCreditsReader {
    public static IReadOnlyList<CreditGroup> Read(JsonElement root) {
        if (root.ValueKind != JsonValueKind.Object
            || !root.TryGetProperty("credits", out var credits) || credits.ValueKind != JsonValueKind.Array) {
            return [];
        }
        var byType = new Dictionary<string, (int First, List<string> Names, HashSet<string> Seen, List<string> Fulls)>(StringComparer.Ordinal);
        foreach (var credit in credits.EnumerateArray()) {
            if (credit.ValueKind != JsonValueKind.Object) continue;
            var type = ReadString(credit, "credit_type_name")?.Trim();
            if (string.IsNullOrEmpty(type)) continue;
            if (!byType.TryGetValue(type, out var group)) {
                group = (byType.Count, new List<string>(), new HashSet<string>(StringComparer.Ordinal), new List<string>());
                byType[type] = group;
            }
            var full = IucnCitationPartsParser.CleanText(ReadString(credit, "full"));
            if (full.Length > 0) group.Fulls.Add(full);
            if (!credit.TryGetProperty("value", out var value) || value.ValueKind != JsonValueKind.Array) continue;
            foreach (var entry in value.EnumerateArray()) {
                if (entry.ValueKind != JsonValueKind.String) continue;
                if (CleanEntry(entry.GetString()) is { } cleaned && group.Seen.Add(cleaned)) {
                    group.Names.Add(cleaned);
                }
            }
        }

        var result = new List<(int Rank, int First, CreditGroup Group)>();
        foreach (var (type, group) in byType) {
            CreditGroup? made = null;
            if (group.Names.Count > 0) {
                var names = group.Fulls.Count == 1 ? OrderByFull(group.Names, group.Fulls[0]) : group.Names;
                made = new CreditGroup(type, names, IsFullOnly: false);
            } else if (group.Fulls.Count > 0) {
                made = new CreditGroup(type, [group.Fulls[0]], IsFullOnly: true);
            }
            if (made is not null) result.Add((CreditTypes.Rank(type), group.First, made));
        }
        return result.OrderBy(r => r.Rank).ThenBy(r => r.First).Select(r => r.Group).ToList();
    }

    /// The entry with whitespace collapsed and email addresses removed; null when nothing is left.
    internal static string? CleanEntry(string? raw) {
        if (string.IsNullOrWhiteSpace(raw)) return null;
        var text = Whitespace().Replace(raw, " ").Trim();
        if (text.Contains('@')) {
            text = BracketedEmail().Replace(text, string.Empty);
            text = Email().Replace(text, string.Empty);
            // What an address leaves behind in brackets it shared: "(a@b.org / IUCN)" gives "(IUCN)".
            text = EmptyBrackets().Replace(text, string.Empty);
            text = OpeningSeparator().Replace(text, "(");
            text = ClosingSeparator().Replace(text, ")");
            text = Whitespace().Replace(text, " ").Trim().TrimEnd(',', ';', '/').TrimEnd();
        }
        return text.Length == 0 ? null : text;
    }

    /// The names in the order their surnames appear in full, when each surname is found there once;
    /// else the names unchanged.
    internal static IReadOnlyList<string> OrderByFull(IReadOnlyList<string> names, string full) {
        if (names.Count < 2) return names;
        var foldedFull = AssessorGivenNames.FoldName(full);
        var positions = new List<int>(names.Count);
        foreach (var name in names) {
            if (AssessorGivenNames.Clean(name) is not { } cleaned) return names;
            var words = NameSuffix().Replace(cleaned.Name, string.Empty).Trim().Split(' ', StringSplitOptions.RemoveEmptyEntries);
            if (words.Length == 0) return names;
            var position = SingleWholeWord(foldedFull, AssessorGivenNames.FoldName(words[^1]));
            if (position < 0 || positions.Contains(position)) return names;
            positions.Add(position);
        }
        return names.Select((name, i) => (name, positions[i])).OrderBy(p => p.Item2).Select(p => p.name).ToList();
    }

    // The place of word in text as a whole word when it is there exactly once; -1 otherwise.
    private static int SingleWholeWord(string text, string word) {
        if (word.Length == 0) return -1;
        var found = -1;
        for (var at = text.IndexOf(word, StringComparison.Ordinal); at >= 0; at = text.IndexOf(word, at + 1, StringComparison.Ordinal)) {
            var startOk = at == 0 || !char.IsLetter(text[at - 1]);
            var end = at + word.Length;
            var endOk = end == text.Length || !char.IsLetter(text[end]);
            if (!startOk || !endOk) continue;
            if (found >= 0) return -1;
            found = at;
        }
        return found;
    }

    private static string? ReadString(JsonElement element, string property) =>
        element.TryGetProperty(property, out var value) && value.ValueKind == JsonValueKind.String ? value.GetString() : null;

    [GeneratedRegex(@"\s+")]
    private static partial Regex Whitespace();

    // An address and the brackets around it: "Nicola Biggs (n.biggs@kew.org)", "Uganda (jkalema@cns.mak.ac.ug) / IUCN".
    [GeneratedRegex(@"\s*\(\s*[^\s@()]+@[^\s@()]+\.[^\s@()]+\s*\)")]
    private static partial Regex BracketedEmail();

    [GeneratedRegex(@"[^\s@()]+@[^\s@()]+\.[^\s@()]+")]
    private static partial Regex Email();

    [GeneratedRegex(@"\s*\(\s*[/,;]?\s*\)")]
    private static partial Regex EmptyBrackets();

    [GeneratedRegex(@"\(\s*[/,;]\s*")]
    private static partial Regex OpeningSeparator();

    [GeneratedRegex(@"\s*[/,;]\s*\)")]
    private static partial Regex ClosingSeparator();

    [GeneratedRegex(@"\s*,?\s+(?:Jr|Jnr|Sr|Snr|II|III|IV)\.?$")]
    private static partial Regex NameSuffix();
}
