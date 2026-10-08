namespace BeastieBot3.Shared.Wikitext;

/// Which year a {{cite iucn}} of a Green Status assessment gives. IUCN's own citation gives the year
/// the assessment was published, which IUCN's API does not give (October 2026); it can be later
/// than the year assessed (the Iberian lynx: assessed 31 October 2023, cited as 2024).
public enum GreenStatusYearRule {
    /// The year of the assessment date.
    Assessed,
    /// The year published when the site knows it (the year of the Red List release it first
    /// appeared in); else the later of the year assessed and the year the Red List assessment on the
    /// same page was published.
    Published,
}

public static class GreenStatusYear {
    /// knownPublishedYear: the year of the release the assessment first appeared in, or null.
    /// redListYear: the year the Red List assessment on the same page was published, or null.
    public static int For(GreenStatusYearRule rule, int assessedYear, int? knownPublishedYear, int? redListYear) => rule switch {
        GreenStatusYearRule.Published => knownPublishedYear ?? Math.Max(assessedYear, redListYear ?? assessedYear),
        _ => assessedYear,
    };
}
