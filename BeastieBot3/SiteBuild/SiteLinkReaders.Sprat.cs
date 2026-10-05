using System.Globalization;
using BeastieBot3.Sprat;
using BeastieBot3.Infrastructure;

// SPRAT for `site build-db`: the EPBC Act listings of each taxon and its populations.

namespace BeastieBot3.SiteBuild;

internal static partial class SiteLinkReaders {
    // ------------------------------------------------------------ SPRAT

    /// Fills each taxon's EpbcListings; returns the report file the SPRAT database was imported from.
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
        var populations = new Dictionary<long, List<EpbcListing>>();
        using (var command = connection.CreateCommand()) {
            command.CommandText = $"""
                SELECT {columns.Select(SpratColumns.SpratTaxonId)}, {columns.Select(SpratColumns.ScientificName)},
                       {columns.Select(SpratColumns.EpbcStatus)}, {columns.Select(SpratColumns.IucnListedName)},
                       {columns.Select(SpratColumns.EpbcListedName)}
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
                stats.SpratPopulationProfiles++;
                if (listing.Status is not null) {
                    stats.EpbcPopulationListings++;
                }
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
}
