using BeastieBot3.Iucn;

// The tree of groups as `site build-db` builds it, for commands that need the groups without
// building the site database (`wikipedia fetch-group-titles`), and the placement reading that
// `site build-db` uses.

namespace BeastieBot3.SiteBuild;

internal static class SiteGroupTree {
    /// <summary>
    /// The CoL groups of the placement built from this IUCN database and the current rules. When the
    /// placement file only has one built from an older copy of the file, an older CoL file or older
    /// rules, that one is used and <paramref name="warning"/> says so. <paramref name="state"/> is
    /// "current" or "out-of-date", or null when there is no placement to use.
    /// </summary>
    public static SitePlacement ReadPlacement(string iucnDatabase, string? colDatabase, string? placementPath,
        IucnNotAssignedRules notAssigned, out string? state, out string? warning, CancellationToken cancellationToken) {
        state = null;
        warning = null;
        if (placementPath is null || !File.Exists(placementPath) || colDatabase is null) {
            return SitePlacement.Empty;
        }
        var status = Col.TaxonPlacementStore.Status(iucnDatabase, colDatabase, notAssigned);
        string sourceKey;
        if (status.IsCurrent) {
            sourceKey = status.SourceKey;
            state = "current";
        } else if (status.Source is { } earlier) {
            sourceKey = earlier.SourceKey;
            state = "out-of-date";
            warning = $"The Catalogue of Life placement is out of date ({status.State}), so the Catalogue of Life groups may not match this IUCN release. To update it, run col build-placement.";
        } else {
            warning = $"The Catalogue of Life placement file has no placement for this IUCN database ({status.State}), so the tree has IUCN's ranks only. To add the Catalogue of Life groups, run col build-placement.";
            return SitePlacement.Empty;
        }
        return SiteLinkReaders.ReadPlacementPaths(placementPath, sourceKey, cancellationToken);
    }

    /// The tree of groups of the taxa in the IUCN CSV export, with the placement's CoL groups.
    public static SiteTaxonTree Load(string iucnDatabase, string? colDatabase, string? placementPath,
        IucnNotAssignedRules notAssigned, out string? warning, CancellationToken cancellationToken) {
        List<SiteTaxon> taxa;
        using (var csv = SiteIucnCsvReader.OpenReadOnly(iucnDatabase)) {
            taxa = SiteIucnCsvReader.ReadTaxa(csv, limit: null, cancellationToken);
        }
        var placement = ReadPlacement(iucnDatabase, colDatabase, placementPath, notAssigned, out _, out warning, cancellationToken);
        return SiteTaxonTree.Build(taxa, placement, notAssigned);
    }
}
