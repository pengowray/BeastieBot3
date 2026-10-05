using BeastieBot3.Shared.SiteData;
using Microsoft.Data.Sqlite;

// The Catalogue of Life for `site build-db`: each taxon's CoL id from the placement file, the CoL
// groups between IUCN ranks, and the authorship of CoL synonyms.

namespace BeastieBot3.SiteBuild;

internal static partial class SiteLinkReaders {
    // ------------------------------------------------------------ Catalogue of Life

    /// Adds the Catalogue of Life's authorship to the CoL synonyms of each taxon with a col_id: the
    /// synonym rows whose parentID is the taxon's CoL id, matched by name.
    public static void ReadColSynonymAuthorities(string path, IReadOnlyDictionary<long, SiteTaxon> taxa, SiteBuildStats stats,
        CancellationToken cancellationToken) {
        using var connection = OpenReadOnly(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT scientificName, authorship FROM nameusage
            WHERE parentID = @id AND status IN ('synonym', 'ambiguous synonym') AND authorship IS NOT NULL AND authorship <> ''
            """;
        var idParameter = command.Parameters.Add("@id", SqliteType.Text);
        foreach (var taxon in taxa.Values) {
            cancellationToken.ThrowIfCancellationRequested();
            if (taxon.ColId is not { } colId || taxon.ColSynonyms.Count == 0) {
                continue;
            }
            idParameter.Value = colId;
            var authorities = new Dictionary<string, string>(StringComparer.Ordinal);
            using (var reader = command.ExecuteReader()) {
                while (reader.Read()) {
                    authorities.TryAdd(SiteNameKey.Fold(reader.GetString(0)), reader.GetString(1));
                }
            }
            for (var i = 0; i < taxon.ColSynonyms.Count; i++) {
                var synonym = taxon.ColSynonyms[i];
                if (synonym.Authority is null && authorities.TryGetValue(SiteNameKey.Fold(synonym.Name), out var authority)) {
                    taxon.ColSynonyms[i] = synonym with { Authority = authority };
                    stats.ColSynonymAuthorities++;
                }
            }
        }
    }

    /// Sets col_id from the placement file and returns the CoL release it was built from ("COL26.7 XR").
    public static string? ReadColPlacement(string path, IReadOnlyDictionary<long, SiteTaxon> taxa, SiteBuildStats stats,
        CancellationToken cancellationToken) {
        using var connection = OpenReadOnly(path);
        var bySpecies = new Dictionary<(string Kingdom, string Genus, string Species), string>();
        using (var command = connection.CreateCommand()) {
            command.CommandText = """
                SELECT kingdom, genus, species, accepted_id
                FROM species_match
                WHERE match_kind IN ('Accepted', 'Synonym', 'ProvisionallyAccepted') AND accepted_id IS NOT NULL AND accepted_id <> ''
                """;
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                cancellationToken.ThrowIfCancellationRequested();
                bySpecies[(reader.GetString(0), reader.GetString(1), reader.GetString(2))] = reader.GetString(3);
            }
        }
        foreach (var taxon in taxa.Values) {
            if (taxon.Kind != SiteTaxonKind.Species || taxon.Kingdom is null || taxon.Genus is null || taxon.SpeciesEpithet is null) {
                continue;
            }
            if (bySpecies.TryGetValue((taxon.Kingdom, taxon.Genus, taxon.SpeciesEpithet), out var colId)) {
                taxon.ColId = colId;
                stats.ColIdsFromPlacement++;
            }
        }

        using var source = connection.CreateCommand();
        source.CommandText = "SELECT col_path FROM placement_source ORDER BY built_at DESC LIMIT 1";
        return SiteBuildRules.ColReleaseFromPath(source.ExecuteScalar() as string);
    }

    /// The CoL groups between IUCN ranks from the placement file's placement for one IUCN database
    /// (source_key), with their CoL ids.
    public static SitePlacement ReadPlacementPaths(string path, string sourceKey, CancellationToken cancellationToken) {
        using var connection = OpenReadOnly(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT span, anchor_key, name, col_rank, show_rank, col_id
            FROM placement WHERE source_key = @key
            ORDER BY span, anchor_key, seq
            """;
        command.Parameters.AddWithValue("@key", sourceKey);
        var paths = new Dictionary<(Taxonomy.PlacementSpan, string), IReadOnlyList<SitePlacementNode>>();
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            cancellationToken.ThrowIfCancellationRequested();
            if (!Enum.TryParse<Taxonomy.PlacementSpan>(reader.GetString(0), out var span)) {
                continue;
            }
            var key = (span, reader.GetString(1));
            if (!paths.TryGetValue(key, out var list)) {
                paths[key] = list = new List<SitePlacementNode>();
            }
            ((List<SitePlacementNode>)list).Add(new SitePlacementNode(reader.GetString(2), reader.GetString(3), reader.GetInt32(4) != 0,
                reader.IsDBNull(5) ? null : SiteBuildRules.NullIfBlank(reader.GetString(5))));
        }
        return new SitePlacement(paths);
    }

    public static void ApplyColCrossReferences(IReadOnlyDictionary<long, SiteTaxon> taxa, IReadOnlyDictionary<long, string> crossReferences,
        SiteBuildStats stats) {
        foreach (var (taxonId, colId) in crossReferences) {
            if (taxa.TryGetValue(taxonId, out var taxon) && taxon.ColId is null && taxon.Kind != SiteTaxonKind.Subpopulation) {
                taxon.ColId = colId;
                stats.ColIdsFromCrossReference++;
            }
        }
    }
}
