namespace BeastieBot3.Site.Update;

/// The population trend templates the species tables on English Wikipedia write (List of felids,
/// List of vespertilionines), for IUCN's population trend. The status update page writes them into
/// |direction= of {{Species table/row}}, and the group page's species tables write them too.
public static class PopulationTrendTemplate {
    public const string Decreasing = "{{decrease|Population declining}}";
    public const string Stable = "{{steady|Population steady}}";
    public const string Increasing = "{{increase|Population increasing}}";
    public const string Unknown = "{{population change unknown}}";

    /// The template for IUCN's trend ("Decreasing", "Stable", "Increasing", "Unknown"); the
    /// unknown template when IUCN gives none.
    public static string For(string? trend) => trend?.Trim().ToLowerInvariant() switch {
        "decreasing" => Decreasing,
        "stable" => Stable,
        "increasing" => Increasing,
        _ => Unknown,
    };
}
