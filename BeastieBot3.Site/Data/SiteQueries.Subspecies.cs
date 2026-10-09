using BeastieBot3.Shared.SiteData;
using static BeastieBot3.Site.Data.ReaderValues;

namespace BeastieBot3.Site.Data;

/// A subspecies or variety of a species as one source lists it. Source "iucn": a taxon in the release
/// whose parent is the species (SourceId its taxon id); "col" and "wikidata": an infraspecific_name row
/// (SourceId a CoL ID or a QID). Rank is InfraspecificNames.Subspecies or Variety.
public sealed record InfraspecificNameRow(string Source, string SourceId, string Rank, string Name, string? Authority);

public sealed partial class SiteQueries {
    /// The subspecies and varieties of a species from IUCN (its subspecies and varieties in the
    /// release), the Catalogue of Life and Wikidata, unmerged.
    public IReadOnlyList<InfraspecificNameRow> GetInfraspecificNames(long speciesTaxonId) {
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT 'iucn', CAST(t.taxon_id AS TEXT), t.kind, t.scientific_name, t.authority
            FROM taxon t
            WHERE t.parent_taxon_id = @id AND t.kind IN ('subspecies', 'variety') AND t.in_release = 1
            UNION ALL
            SELECT n.source, n.source_id, n.rank, n.name, n.authority
            FROM infraspecific_name n
            WHERE n.taxon_id = @id
            """;
        command.Parameters.AddWithValue("@id", speciesTaxonId);
        using var reader = command.ExecuteReader();
        var rows = new List<InfraspecificNameRow>();
        while (reader.Read()) {
            rows.Add(new InfraspecificNameRow(reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetString(3), Text(reader, 4)));
        }
        return rows;
    }
}
