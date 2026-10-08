namespace BeastieBot3.Site.Display;

/// One step of a classification, top down. Rank as the source names it (null: no rank); Url: the
/// step's page in its source, when there is one.
public sealed record LadderStep(string? Rank, string Name, string? Url = null);

/// One classification beside the others: its title, a link to the taxon in that source, its steps.
/// Backbone: IUCN's classification or the Catalogue of Life's (or this site's, built from both),
/// whose groups are shown at first; a group only the other sources have is a hidden row.
/// Help: what the column shows, as hover text on its heading.
public sealed record LadderColumn(string Title, string? Url, IReadOnlyList<LadderStep> Steps, bool Backbone = false, string? Help = null);

/// A row of the comparison: one group, in every column that has it. RankLabel: the main rank
/// (kingdom to species), else the rank a backbone column gives, else the rank most of its cells
/// give ("no rank" for none). AboveCut: a row above the order row (above family or genus when no
/// column has an order), hidden until the reader asks for it: the lower ranks are the ones that
/// differ between sources.
public sealed record ComparisonRow(string RankLabel, bool IsMain, bool AboveCut, IReadOnlyList<ComparisonCell> Cells);

/// Step: the column's group in this row, or null. Differs: a main rank whose name is not the first
/// column's. OtherRank: the step's rank is not the row's, so the cell shows it.
public sealed record ComparisonCell(LadderStep? Step, bool Differs, bool OtherRank);

/// Lines classifications up row by row: a main rank (kingdom to species) is one row in every column,
/// and any other group is one row for every column that has a group of that name, in the order the
/// columns give.
public static class ClassificationComparison {
    /// The main ranks, top down; "division" is the botanical name of phylum.
    public static readonly string[] MainRanks = ["kingdom", "phylum", "class", "order", "family", "genus", "species"];

    public const string NoRank = "no rank";

    /// The rows above the first of these main ranks that the columns have are hidden at first.
    public static readonly string[] CutRanks = ["order", "family", "genus"];

    private static string? MainRankOf(string? rank) => rank?.Trim().ToLowerInvariant() switch {
        "division" => "phylum",
        var r when r is not null && MainRanks.Contains(r) => r,
        _ => null,
    };

    public static IReadOnlyList<ComparisonRow> Build(IReadOnlyList<LadderColumn> columns) {
        // Each step's row key: "m:<rank>" for a column's first step at a main rank, else its name.
        var keyed = columns.Select(column => {
            var seen = new HashSet<string>(StringComparer.Ordinal);
            return column.Steps.Select(step => {
                var key = MainRankOf(step.Rank) is { } main && seen.Add("m:" + main) ? "m:" + main : "n:" + NameKey(step.Name);
                for (var i = 2; key.StartsWith("n:", StringComparison.Ordinal) && !seen.Add(key); i++) {
                    key = $"n:{NameKey(step.Name)}#{i}";
                }
                return (Key: key, Step: step);
            }).ToList();
        }).ToList();

        var order = MergeOrder(keyed.Select(c => c.Select(s => s.Key).ToList()).ToList());
        var cut = CutRanks.Select(r => order.IndexOf("m:" + r)).FirstOrDefault(i => i >= 0, 0);
        var rows = new List<ComparisonRow>();
        for (var index = 0; index < order.Count; index++) {
            var key = order[index];
            var steps = keyed.Select(c => c.FirstOrDefault(s => s.Key == key).Step).ToList();
            var isMain = key.StartsWith("m:", StringComparison.Ordinal);
            // The rank a backbone column gives, else the rank most columns give.
            var label = isMain ? key[2..]
                : steps.Where((s, i) => s is not null && columns[i].Backbone).Select(s => Rank(s!.Rank)).FirstOrDefault()
                    ?? steps.OfType<LadderStep>().GroupBy(s => Rank(s.Rank)).OrderByDescending(g => g.Count()).First().Key;
            var first = isMain ? steps.FirstOrDefault(s => s is not null)?.Name : null;
            rows.Add(new ComparisonRow(label, isMain, index < cut, [.. steps.Select(s => new ComparisonCell(s,
                isMain && s is not null && first is not null && !string.Equals(Clean(s.Name), Clean(first), StringComparison.OrdinalIgnoreCase),
                s is not null && !isMain && Rank(s.Rank) != label))]));
        }
        return rows;
    }

    private static string Rank(string? rank) => string.IsNullOrWhiteSpace(rank) ? NoRank : rank.Trim().ToLowerInvariant();

    // Names compare as Clean does, so "Panthera leo persica (Meyer, 1826)" is "Panthera leo persica".
    private static string NameKey(string name) => Clean(name);

    /// One order for the row keys of all columns that keeps each column's own order where the
    /// columns agree. When they disagree (a loop: one source puts A above B, another B above A), the
    /// key that is highest on average comes first.
    internal static List<string> MergeOrder(IReadOnlyList<List<string>> columns) {
        var after = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);
        var before = new Dictionary<string, int>(StringComparer.Ordinal);
        var place = new Dictionary<string, List<double>>(StringComparer.Ordinal);
        foreach (var keys in columns) {
            for (var i = 0; i < keys.Count; i++) {
                after.TryAdd(keys[i], []);
                before.TryAdd(keys[i], 0);
                (place.TryGetValue(keys[i], out var p) ? p : place[keys[i]] = []).Add(keys.Count == 1 ? 0 : (double)i / (keys.Count - 1));
                if (i > 0 && after[keys[i - 1]].Add(keys[i])) {
                    before[keys[i]]++;
                }
            }
        }
        double Average(string key) => place[key].Average();
        var order = new List<string>();
        var left = new HashSet<string>(after.Keys, StringComparer.Ordinal);
        while (left.Count > 0) {
            var next = left.Where(k => before[k] == 0).OrderBy(Average).ThenBy(k => k, StringComparer.Ordinal).FirstOrDefault()
                ?? left.OrderBy(Average).ThenBy(k => k, StringComparer.Ordinal).First();
            order.Add(next);
            left.Remove(next);
            foreach (var k in after[next]) {
                before[k]--;
            }
        }
        return order;
    }

    // "Panthera leo (Linnaeus, 1758)" and "PANTHERA LEO" compare as "panthera leo": the first word
    // and the lower-case words after it (epithets), without an authority.
    internal static string Clean(string name) {
        var words = name.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (words.Length == 0) {
            return "";
        }
        var kept = new List<string> { words[0] };
        var allUpper = name == name.ToUpperInvariant();
        kept.AddRange(words.Skip(1).TakeWhile(w => allUpper ? char.IsLetter(w[0]) : char.IsLower(w[0])));
        return string.Join(' ', kept).ToLowerInvariant();
    }
}
