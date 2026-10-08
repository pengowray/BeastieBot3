namespace BeastieBot3.SiteBuild;

/// How a status list row was matched to its taxon.
internal enum StatusListMatchKind {
    /// The row's own scientific name is the taxon's name.
    Name,
    /// One of the other names the list gives the row (NatureServe's synonyms) is the taxon's name.
    OtherName,
    /// The row's own scientific name is one of the taxon's IUCN synonyms.
    IucnSynonym,
}

/// A status list row and the taxon it was matched to.
internal sealed record StatusListMatch<T>(SiteTaxon Taxon, T Row, StatusListMatchKind Kind);

/// Matches the rows of a status list that gives a taxon at most one row (NatureServe, NZTCS, SALVE)
/// to the site's taxa. ECOS gives a taxon a row per population and has its own loop.
internal static class StatusListMatcher {
    /// Gives each taxon at most one row and each row at most one taxon. Rows are taken in the order
    /// given, so the rows to prefer must come first.
    ///
    /// First, every row is looked up by its own name in the row's kingdom (any kingdom when
    /// kingdomOf gives null). An earlier row keeps its taxon: a later row whose name finds the same
    /// taxon is dropped, and is not looked up again. Then each row whose name found no taxon is
    /// looked up by its other names, in their order, taking the first taxon that no row has yet;
    /// when none gives one, by its name among the taxa's IUCN synonyms, when that taxon has no row
    /// yet. A match by name therefore beats a match by another name or an IUCN synonym in any row.
    ///
    /// Returns the matches in the order they were made.
    public static List<StatusListMatch<T>> OnePerTaxon<T>(IEnumerable<T> rows, StatusListNameIndex index,
        Func<T, string?> kingdomOf, Func<T, string> nameOf, Func<T, IEnumerable<string>?>? otherNamesOf = null) {
        var matches = new List<StatusListMatch<T>>();
        var taken = new HashSet<long>();
        var notFound = new List<T>();
        foreach (var row in rows) {
            if (index.Find(kingdomOf(row), nameOf(row)) is { } taxon) {
                if (taken.Add(taxon.TaxonId)) {
                    matches.Add(new StatusListMatch<T>(taxon, row, StatusListMatchKind.Name));
                }
            } else {
                notFound.Add(row);
            }
        }
        foreach (var row in notFound) {
            var kingdom = kingdomOf(row);
            var kind = StatusListMatchKind.OtherName;
            var taxon = otherNamesOf?.Invoke(row)?.Select(n => index.Find(kingdom, n))
                .FirstOrDefault(t => t is not null && !taken.Contains(t.TaxonId));
            if (taxon is null) {
                kind = StatusListMatchKind.IucnSynonym;
                taxon = index.FindByIucnSynonym(kingdom, nameOf(row));
            }
            if (taxon is not null && taken.Add(taxon.TaxonId)) {
                matches.Add(new StatusListMatch<T>(taxon, row, kind));
            }
        }
        return matches;
    }
}
