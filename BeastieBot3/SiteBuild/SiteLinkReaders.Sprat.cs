using System.Globalization;
using BeastieBot3.Shared.SiteData;
using BeastieBot3.Sprat;
using BeastieBot3.Infrastructure;

// SPRAT for `site build-db`: the EPBC Act listings of each taxon and its populations, and their
// Australian state and territory statuses.

namespace BeastieBot3.SiteBuild;

internal static partial class SiteLinkReaders {
    // ------------------------------------------------------------ SPRAT

    /// The SPRAT columns of each state and territory list: the status, and the name the list uses.
    /// The listed-name columns have no header of their own in the report ("Listed Name" after each
    /// status), so the importer numbers them in report order.
    internal static readonly IReadOnlyList<(string System, string StatusColumn, string ListedNameColumn)> SpratStateColumns = [
        (OtherStatusSystems.AustralianCapitalTerritory, SpratColumns.ActStatus, "Listed_Name"),
        (OtherStatusSystems.NewSouthWales, SpratColumns.NswStatus, "Listed_Name_2"),
        (OtherStatusSystems.NorthernTerritory, SpratColumns.NtStatus, "Listed_Name_3"),
        (OtherStatusSystems.Queensland, SpratColumns.QldStatus, "Listed_Name_4"),
        (OtherStatusSystems.SouthAustralia, SpratColumns.SaStatus, "Listed_Name_5"),
        (OtherStatusSystems.Tasmania, SpratColumns.TasStatus, "Listed_Name_6"),
        (OtherStatusSystems.Victoria, SpratColumns.VicStatus, "Listed_Name_7"),
        (OtherStatusSystems.WesternAustralia, SpratColumns.WaStatus, "Listed_Name_8"),
    ];

    /// Fills each taxon's EpbcListings and OtherStatuses (the EPBC Act listing and the state and
    /// territory statuses of each SPRAT profile it gets); returns the report file the SPRAT database
    /// was imported from.
    /// A name shared by several taxa goes to the taxon in the release with the lowest id, else the
    /// lowest id of the others. A database with no sprat_species table, or one without the SPRAT id,
    /// scientific name or EPBC status column, gives no listings and a warning. A missing IUCN listed
    /// names or EPBC listed name column reads as empty, with a warning.
    public static string? ReadSprat(string path, IReadOnlyDictionary<long, SiteTaxon> taxa, SiteBuildStats stats,
        CancellationToken cancellationToken) {
        using var connection = OpenReadOnly(path);
        var columns = SpratTableColumns.Read(connection);
        if (columns is null) {
            stats.Warnings.Add($"The SPRAT database {path} has no {SpratColumns.Table} table, so no SPRAT profiles or EPBC listings were used.");
            return null;
        }
        var missingRequired = new[] { SpratColumns.SpratTaxonId, SpratColumns.ScientificName, SpratColumns.EpbcStatus }
            .Where(c => !columns.Has(c)).ToList();
        if (missingRequired.Count > 0) {
            stats.Warnings.Add($"The SPRAT database {path} has no {string.Join(", ", missingRequired)} column in {SpratColumns.Table}, "
                + "so no SPRAT profiles or EPBC listings were used.");
            return null;
        }
        if (!columns.Has(SpratColumns.IucnListedName)) {
            stats.Warnings.Add($"The SPRAT database {path} has no {SpratColumns.IucnListedName} column, "
                + "so SPRAT profiles were matched by their scientific name only.");
        }
        if (!columns.Has(SpratColumns.EpbcListedName)) {
            stats.Warnings.Add($"The SPRAT database {path} has no {SpratColumns.EpbcListedName} column, "
                + "so each EPBC listing gives SPRAT's scientific name as the listed name.");
        }

        var byName = new Dictionary<string, SiteTaxon>(StringComparer.Ordinal);
        foreach (var taxon in taxa.Values.OrderBy(t => t.InRelease ? 0 : 1).ThenBy(t => t.TaxonId)) {
            byName.TryAdd(taxon.ScientificName, taxon);
        }

        // The profile for the whole taxon: an exact scientific name match beats a sense in brackets,
        // which beats a listed-name match; then a profile with an EPBC listing; then the lowest SPRAT id.
        var best = new Dictionary<long, (int Rank, EpbcListing Listing)>();
        var profileStatuses = new Dictionary<long, SpratStatuses>();
        var populations = new Dictionary<long, List<EpbcListing>>();
        using (var command = connection.CreateCommand()) {
            var stateColumns = string.Concat(SpratStateColumns.Select(c =>
                $", {columns.Select(c.StatusColumn)}, {columns.Select(c.ListedNameColumn)}"));
            command.CommandText = $"""
                SELECT {columns.Select(SpratColumns.SpratTaxonId)}, {columns.Select(SpratColumns.ScientificName)},
                       {columns.Select(SpratColumns.EpbcStatus)}, {columns.Select(SpratColumns.IucnListedName)},
                       {columns.Select(SpratColumns.EpbcListedName)}, {columns.Select(SpratColumns.EpbcDateEffective)}
                       {stateColumns}
                FROM {SpratTableColumns.Quote(SpratColumns.Table)}
                """;
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                cancellationToken.ThrowIfCancellationRequested();
                if (reader.IsDBNull(0) || !long.TryParse(reader.GetString(0).Trim(), NumberStyles.None, CultureInfo.InvariantCulture, out var spratId)) {
                    continue;
                }
                var epbc = SiteBuildRules.EpbcCode(reader.IsDBNull(2) ? null : reader.GetString(2));
                var scientificName = SiteBuildRules.NullIfBlank(reader.IsDBNull(1) ? null : reader.GetString(1));
                var listedName = SiteBuildRules.NullIfBlank(reader.IsDBNull(4) ? null : reader.GetString(4)) ?? scientificName;
                profileStatuses[spratId] = ReadSpratStatuses(reader, epbc);

                // A population of a taxon: the taxon's name with the population in brackets.
                SiteTaxon? populationOf = null;
                if (scientificName is not null) {
                    for (var at = scientificName.IndexOf(" (", StringComparison.Ordinal); at > 0;
                         at = scientificName.IndexOf(" (", at + 1, StringComparison.Ordinal)) {
                        if (!byName.TryGetValue(scientificName[..at], out var taxon)) {
                            continue;
                        }
                        var match = SiteBuildRules.ClassifySpratName(scientificName, taxon.ScientificName);
                        if (match.Kind == SpratNameKind.Population) {
                            // The population as the listed name gives it, when that has the same form.
                            var listed = listedName is null ? match : SiteBuildRules.ClassifySpratName(listedName, taxon.ScientificName);
                            var population = listed.Kind == SpratNameKind.Population ? listed.Population! : match.Population!;
                            if (!populations.TryGetValue(taxon.TaxonId, out var list)) {
                                populations[taxon.TaxonId] = list = new List<EpbcListing>();
                            }
                            list.Add(new EpbcListing(spratId, listedName ?? scientificName, epbc, EpbcAppliesTo.Population, population));
                            populationOf = taxon;
                        } else if (match.Kind == SpratNameKind.Taxon) {
                            Consider(taxon, 2);
                        } else if (match.Kind == SpratNameKind.NotPopulation) {
                            stats.SpratBracketsNotPopulation++;
                        }
                        break;
                    }
                }

                // The profile of the whole taxon: by its scientific name, then by each IUCN name it lists.
                var names = new List<(string Name, bool Exact)>();
                if (scientificName is not null) {
                    names.Add((scientificName, true));
                }
                if (!reader.IsDBNull(3)) {
                    names.AddRange(reader.GetString(3).Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                        .Select(n => (n, false)));
                }
                foreach (var (name, exact) in names) {
                    if (!byName.TryGetValue(name, out var taxon) || taxon == populationOf) {
                        continue;
                    }
                    Consider(taxon, exact ? 0 : 4);
                    break;
                }

                void Consider(SiteTaxon taxon, int baseRank) {
                    var rank = baseRank + (epbc is null ? 1 : 0);
                    if (!best.TryGetValue(taxon.TaxonId, out var current) || rank < current.Rank
                        || (rank == current.Rank && spratId < current.Listing.SpratTaxonId)) {
                        best[taxon.TaxonId] = (rank, new EpbcListing(spratId, listedName ?? scientificName ?? string.Empty, epbc,
                            EpbcAppliesTo.Taxon, null));
                    }
                }
            }
        }
        foreach (var (taxonId, (_, listing)) in best) {
            taxa[taxonId].EpbcListings.Add(listing);
            AddOtherStatuses(taxa[taxonId], listing);
            stats.SpratMatched++;
            if (listing.Status is not null) {
                stats.EpbcStatuses++;
            }
        }
        foreach (var (taxonId, list) in populations) {
            var taxon = taxa[taxonId];
            foreach (var listing in list.OrderBy(l => l.Population, StringComparer.OrdinalIgnoreCase).ThenBy(l => l.SpratTaxonId)) {
                if (taxon.EpbcListings.Any(l => l.SpratTaxonId == listing.SpratTaxonId)) {
                    continue;
                }
                taxon.EpbcListings.Add(listing);
                AddOtherStatuses(taxon, listing);
                stats.SpratPopulationProfiles++;
                if (listing.Status is not null) {
                    stats.EpbcPopulationListings++;
                }
            }
        }

        // The EPBC Act listing and the state and territory statuses of each profile a taxon got.
        void AddOtherStatuses(SiteTaxon taxon, EpbcListing listing) {
            if (!profileStatuses.TryGetValue(listing.SpratTaxonId, out var statuses)) {
                return;
            }
            var sourceId = listing.SpratTaxonId.ToString(CultureInfo.InvariantCulture);
            var url = OtherStatusSources.SpratUrl(listing.SpratTaxonId);
            if (OtherStatusSystems.EpbcLabel(listing.Status) is { } epbcLabel) {
                taxon.OtherStatuses.Add(new OtherStatus(OtherStatusSystems.Epbc, epbcLabel, null,
                    SiteBuildRules.OtherListedName(listing.ListedName, taxon.ScientificName),
                    listing.Population, OtherStatusSources.Sprat, sourceId, url, statuses.EpbcListedOn));
            }
            foreach (var (system, status, stateListedName) in statuses.States) {
                taxon.OtherStatuses.Add(new OtherStatus(system, status, null,
                    SiteBuildRules.OtherListedName(stateListedName ?? listing.ListedName, taxon.ScientificName),
                    listing.Population, OtherStatusSources.Sprat, sourceId, url, null));
                stats.StateStatuses++;
            }
        }

        // `sprat import` records the report file in import_metadata.
        if (DelimitedTableImporter.GetTableColumns(connection, "import_metadata")?.Contains("filename") != true) {
            return null;
        }
        using var file = connection.CreateCommand();
        file.CommandText = "SELECT filename FROM import_metadata ORDER BY id DESC LIMIT 1";
        return file.ExecuteScalar() is string fileName ? Path.GetFileName(fileName.Trim()) : null;
    }

    /// The date the EPBC listing took effect and the state and territory statuses of one SPRAT row.
    private sealed record SpratStatuses(string? EpbcListedOn, List<(string System, string Status, string? ListedName)> States);

    // Columns 5 on of the query in ReadSprat: the EPBC date, then a status and a listed name for
    // each list in SpratStateColumns.
    private static SpratStatuses ReadSpratStatuses(Microsoft.Data.Sqlite.SqliteDataReader reader, string? epbc) {
        var listedOn = epbc is null ? null : SiteBuildRules.SpratDate(reader.IsDBNull(5) ? null : reader.GetString(5));
        var states = new List<(string, string, string?)>();
        for (var i = 0; i < SpratStateColumns.Count; i++) {
            var status = SiteBuildRules.ListStatusText(reader.IsDBNull(6 + 2 * i) ? null : reader.GetString(6 + 2 * i));
            if (status is null) {
                continue;
            }
            var stateName = SiteBuildRules.NullIfBlank(reader.IsDBNull(7 + 2 * i) ? null : reader.GetString(7 + 2 * i));
            states.Add((SpratStateColumns[i].System, status, stateName));
        }
        return new SpratStatuses(listedOn, states);
    }
}
