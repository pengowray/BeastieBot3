namespace BeastieBot3.Site.Display;

/// One step of a classification, top down. Rank as the source names it (null: no rank); Url: the
/// step's page in its source, when there is one.
public sealed record LadderStep(string? Rank, string Name, string? Url = null);

/// One classification beside the others: its title, a link to the taxon in that source, its steps.
public sealed record LadderColumn(string Title, string? Url, IReadOnlyList<LadderStep> Steps);

/// A row of the comparison: a main rank (kingdom to species), or the steps above kingdom.
/// Cells: per column, the step at the main rank (null when the column has none) and the steps
/// between it and the next main rank. Differs: per column, whether the main step's name is not the
/// first column's.
public sealed record ComparisonRow(string RankLabel, IReadOnlyList<ComparisonCell> Cells);

public sealed record ComparisonCell(LadderStep? Main, IReadOnlyList<LadderStep> Between, bool Differs);

/// Lines classifications up on the main ranks, so the groups each source has between them (CoL's
/// subfamilies, Wikidata's clades) show under the rank above them.
public static class ClassificationComparison {
    /// The main ranks, top down; "division" is the botanical name of phylum.
    public static readonly string[] MainRanks = ["kingdom", "phylum", "class", "order", "family", "genus", "species"];

    public const string AboveKingdom = "above kingdom";

    private static string? Main(string? rank) => rank?.Trim().ToLowerInvariant() switch {
        "division" => "phylum",
        var r when r is not null && MainRanks.Contains(r) => r,
        _ => null,
    };

    public static IReadOnlyList<ComparisonRow> Build(IReadOnlyList<LadderColumn> columns) {
        // Per column: bucket label -> (main step, steps after it).
        var buckets = columns.Select(column => {
            var map = new Dictionary<string, (LadderStep? Main, List<LadderStep> Between)>(StringComparer.Ordinal);
            var current = AboveKingdom;
            map[current] = (null, []);
            foreach (var step in column.Steps) {
                if (Main(step.Rank) is { } rank && !map.ContainsKey(rank)) {
                    current = rank;
                    map[current] = (step, []);
                } else {
                    map[current].Between.Add(step);
                }
            }
            return map;
        }).ToList();
        var rows = new List<ComparisonRow>();
        foreach (var label in new[] { AboveKingdom }.Concat(MainRanks)) {
            var cells = buckets.Select(b => b.TryGetValue(label, out var cell) ? cell : (null, [])).ToList();
            if (cells.All(c => c.Main is null && c.Between.Count == 0)) {
                continue;
            }
            var first = cells[0].Main?.Name;
            rows.Add(new ComparisonRow(label, [.. cells.Select((c, i) => new ComparisonCell(c.Main, c.Between,
                i > 0 && first is not null && c.Main is not null && !string.Equals(Clean(c.Main.Name), Clean(first), StringComparison.OrdinalIgnoreCase)))]));
        }
        return rows;
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
