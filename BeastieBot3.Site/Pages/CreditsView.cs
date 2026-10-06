using BeastieBot3.Shared.SiteData;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Display;

namespace BeastieBot3.Site.Pages;

/// One credit group on the taxon page: IUCN's heading, and the names, or (IsFullOnly) the one
/// citation-form line IUCN gives instead.
public sealed record CreditGroupView(string Label, IReadOnlyList<string> Names, bool IsFullOnly);

/// The credits section of the selected assessment. DistinctNames counts a name credited under two
/// headings once, comparing the names without their bracketed affiliations; null when a group is
/// only a citation-form line.
public sealed record CreditsView(IReadOnlyList<CreditGroupView> Groups, int? DistinctNames) {
    public string Summary => SiteText.CreditsSummary(DistinctNames);

    /// Null when the assessment has no credits.
    public static CreditsView? Build(AssessmentCredits credits) {
        var groups = new List<CreditGroupView>();
        foreach (var group in credits.Groups) {
            var ids = group.Full is { } full ? [full] : group.Names;
            var names = ids.Select(id => credits.Names.GetValueOrDefault(id)).OfType<string>().ToList();
            if (names.Count > 0) {
                groups.Add(new CreditGroupView(SiteText.CreditTypeLabel(group.Type), names, group.IsFullOnly));
            }
        }
        if (groups.Count == 0) {
            return null;
        }
        int? distinct = groups.Any(g => g.IsFullOnly)
            ? null
            : groups.SelectMany(g => g.Names).Select(NameKey).Distinct(StringComparer.Ordinal).Count();
        return new CreditsView(groups, distinct);
    }

    // "Krystal Tolley (IUCN SSC Chameleon Specialist Group)" and "Krystal Tolley (SANBI)" are one name.
    internal static string NameKey(string entry) {
        var text = entry.Trim();
        while (text.EndsWith(')')) {
            var depth = 0;
            var open = -1;
            for (var i = text.Length - 1; i >= 0; i--) {
                if (text[i] == ')') depth++;
                else if (text[i] == '(' && --depth == 0) {
                    open = i;
                    break;
                }
            }
            if (open <= 0) break;
            text = text[..open].TrimEnd();
        }
        return SiteNameKey.Fold(text);
    }
}
