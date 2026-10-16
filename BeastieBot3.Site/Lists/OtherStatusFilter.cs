using BeastieBot3.Shared.SiteData;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Display;

// The statuses of the chosen other list that the list maker's "Only taxa on this list" and "Both" can
// leave out (ListLineOptions.OtherSkip): one checkbox per status that the group's taxa have in the
// list, with how many lines it has. A taxon stays when any of its statuses in the list is ticked, and
// its line then shows all of them.

namespace BeastieBot3.Site.Lists;

/// One status to tick. Key: what the form sends and the address keeps (the status as the list writes
/// it; NatureServe's rounded global rank for its global ranks, so G2G3 and G2 are one choice). Lines:
/// how many lines of the list, with the other options as they are, have a taxon with the status.
public sealed record OtherStatusFilterChoice(string Key, int Lines, bool Ticked);

public static class OtherStatusFilter {
    /// The status as the filter groups it.
    public static string Key(string system, ChosenStatus status) =>
        system == OtherStatusSystems.NatureServeGlobal && status.StatusCode is { Length: > 0 } code ? code : status.Status;

    /// Whether a taxon with these statuses stays: it has a status that is not left out.
    public static bool Keeps(string system, IReadOnlyList<ChosenStatus>? statuses, IReadOnlySet<string> skip) =>
        statuses is not null && statuses.Any(s => !skip.Contains(Key(system, s)));

    /// The statuses the listed taxa have, in the order of IUCN's categories where a status starts
    /// with one ("Vulnerable (breeding)" after "Vulnerable"), then the others by name.
    public static IReadOnlyList<OtherStatusFilterChoice> Choices(string system, IEnumerable<long> listedTaxa,
        IReadOnlyDictionary<long, IReadOnlyList<ChosenStatus>> statuses, IReadOnlySet<string> skip) {
        var lines = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (var taxonId in listedTaxa) {
            if (!statuses.TryGetValue(taxonId, out var taxonStatuses)) {
                continue;
            }
            foreach (var key in taxonStatuses.Select(s => Key(system, s)).Distinct(StringComparer.Ordinal)) {
                lines[key] = lines.GetValueOrDefault(key) + 1;
            }
        }
        return lines
            .OrderBy(p => CategoryRank(p.Key))
            .ThenBy(p => p.Key, StringComparer.OrdinalIgnoreCase)
            .Select(p => new OtherStatusFilterChoice(p.Key, p.Value, !skip.Contains(p.Key)))
            .ToList();
    }

    // IUCN's categories as national red lists write them, most threatened first. A status that starts
    // with none of them goes after them all.
    private static readonly string[] Categories = [
        "Extinct", "Extinct in the Wild", "Regionally Extinct", "Critically Endangered", "Endangered", "Vulnerable",
        "Near Threatened", "Least Concern", "Data Deficient", "Not Applicable", "Not Evaluated",
    ];

    private static int CategoryRank(string status) {
        // The longest match, so "Extinct in the Wild" is not taken for "Extinct".
        var best = -1;
        for (var i = 0; i < Categories.Length; i++) {
            var category = Categories[i];
            if (status.StartsWith(category, StringComparison.OrdinalIgnoreCase)
                && (status.Length == category.Length || !char.IsLetter(status[category.Length]))
                && (best < 0 || category.Length > Categories[best].Length)) {
                best = i;
            }
        }
        return best < 0 ? Categories.Length : best;
    }
}
