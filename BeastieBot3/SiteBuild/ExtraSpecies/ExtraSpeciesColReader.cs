using Microsoft.Data.Sqlite;

// Reads the Catalogue of Life database for the extra species: the accepted species of each IUCN
// genus (and, with ExtraPlacement.Family, of each IUCN family), species-rank synonyms, and CoL's
// accepted names of IUCN taxa that CoL knows under another name.
//
// The CoL database is about 13 GB, so it is read from the IUCN side: one indexed query per IUCN
// genus or family (INDEXED BY, since the file has no ANALYZE statistics and `rank` is indexed too),
// never a scan of the 2.5 million accepted species.

namespace BeastieBot3.SiteBuild.ExtraSpecies;

internal static class ExtraSpeciesColReader {
    private const string AcceptedSpecies = "rank = 'species' AND status IN ('accepted', 'provisionally accepted')";

    /// The accepted species of the given genera (name as IUCN writes it, "Panthera") and families
    /// (title case, "Felidae"), each species once. Fossil species (extinct = 'true') are counted and
    /// left out.
    public static List<ColSpeciesRow> ReadSpecies(string path, IEnumerable<string> genera, IEnumerable<string> families,
        ExtraSpeciesStats stats, CancellationToken ct) {
        using var connection = SiteLinkReaders.OpenReadOnly(path);
        var rows = new Dictionary<string, ColSpeciesRow>(StringComparer.Ordinal);
        void Read(string column, string index, IEnumerable<string> values) {
            using var command = connection.CreateCommand();
            command.CommandText = $"""
                SELECT ID, genericName, specificEpithet, authorship, family, kingdom, extinct
                FROM nameusage INDEXED BY {index}
                WHERE {column} = @value AND {AcceptedSpecies}
                """;
            var value = command.Parameters.Add("@value", SqliteType.Text);
            foreach (var v in values) {
                ct.ThrowIfCancellationRequested();
                value.Value = v;
                using var reader = command.ExecuteReader();
                while (reader.Read()) {
                    var id = Text(reader, 0);
                    var genus = Text(reader, 1);
                    var epithet = Text(reader, 2);
                    var kingdom = Text(reader, 5);
                    if (id is null || genus is null || epithet is null || kingdom is null || rows.ContainsKey(id)) {
                        continue;
                    }
                    stats.ColRead++;
                    if (string.Equals(Text(reader, 6), "true", StringComparison.OrdinalIgnoreCase)) {
                        stats.ColExtinct++;
                        continue;
                    }
                    if (ExtraSpeciesNameRules.SplitBinomial(genus + " " + epithet) is null) {
                        continue;
                    }
                    rows[id] = new ColSpeciesRow(id, genus, epithet, Text(reader, 3), Text(reader, 4), kingdom.ToUpperInvariant());
                }
            }
        }
        Read("genus", "idx_nameusage_genus", genera);
        Read("family", "idx_nameusage_family", families);
        return rows.Values.ToList();
    }

    /// Species-rank synonyms whose name is in names, or whose id is in ids: name to the ids of the
    /// usages they are synonyms of, and synonym id to that usage. One pass over the synonyms (about
    /// 2.4 million rows).
    public static (Dictionary<string, List<string>> ByName, Dictionary<string, string> ById) ReadSynonyms(string path,
        IReadOnlySet<string> names, IReadOnlySet<string> ids, ExtraSpeciesStats stats, CancellationToken ct) {
        using var connection = SiteLinkReaders.OpenReadOnly(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT ID, parentID, genericName, specificEpithet FROM nameusage INDEXED BY idx_nameusage_rank
            WHERE rank = 'species' AND status IN ('synonym', 'ambiguous synonym')
            """;
        command.CommandTimeout = 0;
        var byName = new Dictionary<string, List<string>>(StringComparer.Ordinal);
        var byId = new Dictionary<string, string>(StringComparer.Ordinal);
        using var reader = command.ExecuteReader();
        var n = 0;
        while (reader.Read()) {
            if (++n % 100_000 == 0) {
                ct.ThrowIfCancellationRequested();
            }
            var id = Text(reader, 0);
            var parent = Text(reader, 1);
            if (id is null || parent is null) {
                continue;
            }
            stats.ColSynonymsRead++;
            if (ids.Contains(id)) {
                byId[id] = parent;
            }
            if (Text(reader, 2) is { } genus && Text(reader, 3) is { } epithet) {
                var name = genus + " " + epithet;
                if (names.Contains(name)) {
                    if (!byName.TryGetValue(name, out var list)) {
                        byName[name] = list = new List<string>();
                    }
                    list.Add(parent);
                }
            }
        }
        // A synonym of a subspecies or variety stands for the species above it.
        var parents = byName.Values.SelectMany(l => l).Concat(byId.Values).ToHashSet(StringComparer.Ordinal);
        var up = SpeciesOfInfraspecific(connection, parents, ct);
        foreach (var list in byName.Values) {
            for (var i = 0; i < list.Count; i++) {
                list[i] = up.GetValueOrDefault(list[i], list[i]);
            }
        }
        foreach (var id in byId.Keys.ToList()) {
            byId[id] = up.GetValueOrDefault(byId[id], byId[id]);
        }
        return (byName, byId);
    }

    // Of the given usage ids, those of subspecies, varieties and forms, with the id of the usage they
    // are under (their species).
    private static Dictionary<string, string> SpeciesOfInfraspecific(SqliteConnection connection, IEnumerable<string> ids, CancellationToken ct) {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT parentID, rank FROM nameusage INDEXED BY idx_nameusage_ID WHERE ID = @id LIMIT 1";
        var id = command.Parameters.Add("@id", SqliteType.Text);
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var value in ids) {
            ct.ThrowIfCancellationRequested();
            id.Value = value;
            using var reader = command.ExecuteReader();
            if (reader.Read() && Text(reader, 0) is { } parent && Text(reader, 1) is "subspecies" or "variety" or "form" or "infraspecific name") {
                result[value] = parent;
            }
        }
        return result;
    }

    /// CoL's accepted names for the given usage ids ("Panthera leo"), for IUCN taxa that the
    /// placement file matched to CoL through a synonym.
    public static Dictionary<string, string> ReadNames(string path, IEnumerable<string> ids, CancellationToken ct) {
        using var connection = SiteLinkReaders.OpenReadOnly(path);
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT genericName, specificEpithet FROM nameusage INDEXED BY idx_nameusage_ID WHERE ID = @id LIMIT 1";
        var idParameter = command.Parameters.Add("@id", SqliteType.Text);
        var names = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var id in ids) {
            ct.ThrowIfCancellationRequested();
            idParameter.Value = id;
            using var reader = command.ExecuteReader();
            if (reader.Read() && Text(reader, 0) is { } genus && Text(reader, 1) is { } epithet) {
                names[id] = genus + " " + epithet;
            }
        }
        return names;
    }

    /// The (kingdom, genus, species) of the IUCN species that the placement file matched to CoL
    /// through a synonym, with the accepted id.
    public static List<(string Kingdom, string Genus, string Species, string AcceptedId)> ReadSynonymMatches(string placementPath) {
        using var connection = SiteLinkReaders.OpenReadOnly(placementPath);
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT kingdom, genus, species, accepted_id FROM species_match
            WHERE match_kind = 'Synonym' AND accepted_id IS NOT NULL AND accepted_id <> ''
            """;
        var list = new List<(string, string, string, string)>();
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            list.Add((reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetString(3)));
        }
        return list;
    }

    private static string? Text(SqliteDataReader reader, int i) =>
        reader.IsDBNull(i) ? null : reader.GetString(i) is { Length: > 0 } s ? s.Trim() : null;
}
