using System.Globalization;
using BeastieBot3.Infrastructure;
using BeastieBot3.Shared.SiteData;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.SiteBuild;

internal static partial class SiteLinkReaders {
    // CITES: each species, subspecies and variety of the Checklist of CITES Species (`statuses
    // cites-import`) goes to the taxon with its name in its kingdom, else the one taxon one of its
    // Checklist synonyms finds, else the one taxon whose IUCN synonyms include it; CITES-accepted names
    // first. Each of its current listings is a row: Appendix I, II or III (with the Party that listed
    // it), the populations its note names, and the date it took effect. A listing inherited from a
    // genus, family or order says so (listed_under), and the higher taxon's note, which is about the
    // higher taxon's other members, is left out. A site species the Checklist does not name is covered
    // by the listing of its IUCN genus, else family, else order, when the Checklist has that taxon with a
    // listing of its own whose note names no exceptions.
    private static void ReadCites(SqliteConnection connection, StatusListNameIndex index, IEnumerable<SiteTaxon> siteTaxa,
        SiteBuildStats stats, CancellationToken cancellationToken) {
        var taxa = new Dictionary<long, CitesTaxon>();
        using (var command = connection.CreateCommand()) {
            command.CommandText = """
                SELECT taxon_concept_id, full_name, taxon_rank, cites_accepted, kingdom, url
                FROM cites_taxon
                ORDER BY cites_accepted DESC, taxon_concept_id
                """;
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                cancellationToken.ThrowIfCancellationRequested();
                taxa[reader.GetInt64(0)] = new CitesTaxon(reader.GetInt64(0), reader.GetString(1), reader.GetString(2),
                    reader.GetInt64(3) == 1, Text(reader, 4), reader.GetString(5), new List<CitesListingRow>(), new List<string>());
            }
        }
        using (var command = connection.CreateCommand()) {
            command.CommandText = """
                SELECT taxon_concept_id, appendix, party_name, effective_on, short_note, inherited_rank, inherited_name
                FROM cites_listing
                ORDER BY taxon_concept_id, appendix, listing_change_id
                """;
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                if (taxa.TryGetValue(reader.GetInt64(0), out var taxon)) {
                    taxon.Listings.Add(new CitesListingRow(reader.GetString(1), Text(reader, 2), Text(reader, 3), Text(reader, 4),
                        Text(reader, 5), Text(reader, 6)));
                }
            }
        }
        using (var command = connection.CreateCommand()) {
            command.CommandText = "SELECT taxon_concept_id, name FROM cites_synonym";
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                if (taxa.TryGetValue(reader.GetInt64(0), out var taxon)) {
                    taxon.Synonyms.Add(reader.GetString(1));
                }
            }
        }
        (stats.CitesFetched, stats.CitesCitation) = CitesSource(connection);

        var lowerTaxa = taxa.Values.Where(t => t.Rank is "SPECIES" or "SUBSPECIES" or "VARIETY" && t.Listings.Count > 0).ToList();
        stats.CitesTaxa = lowerTaxa.Count;
        var matches = StatusListMatcher.OnePerTaxon(lowerTaxa, index, t => StatusListNameIndex.Kingdom(t.Kingdom), t => t.Name, t => t.Synonyms);
        var covered = new HashSet<long>();
        foreach (var (taxon, cites, _) in matches) {
            AddCitesRows(taxon, cites, cites.Listings, null, stats);
            covered.Add(taxon.TaxonId);
        }
        stats.CitesMatched = matches.Count;

        // Higher taxa with a listing of their own and no exceptions in its note, by rank, kingdom and name.
        var higher = new Dictionary<(string Rank, string? Kingdom, string Name), CitesTaxon?>();
        foreach (var taxon in taxa.Values.Where(t => t.Rank is "GENUS" or "FAMILY" or "ORDER")) {
            var own = taxon.Listings.Where(l => l.InheritedName is null).ToList();
            if (own.Count == 0 || own.Any(l => l.ShortNote is not null)) {
                continue;
            }
            var key = (taxon.Rank, StatusListNameIndex.Kingdom(taxon.Kingdom), taxon.Name.ToUpperInvariant());
            higher[key] = higher.ContainsKey(key) ? null : taxon;
        }
        foreach (var taxon in siteTaxa) {
            if (taxon.Kind != SiteTaxonKind.Species || covered.Contains(taxon.TaxonId)) {
                continue;
            }
            var kingdom = taxon.Kingdom?.Trim().ToUpperInvariant();
            foreach (var (rank, name) in new[] { ("GENUS", taxon.Genus), ("FAMILY", taxon.Family), ("ORDER", taxon.OrderName) }) {
                if (name is null || higher.GetValueOrDefault((rank, kingdom, name.Trim().ToUpperInvariant())) is not { } listed) {
                    continue;
                }
                AddCitesRows(taxon, listed, listed.Listings.Where(l => l.InheritedName is null).ToList(),
                    $"{rank.ToLowerInvariant()} {listed.Name}", stats);
                stats.CitesByHigherTaxon++;
                break;
            }
        }
    }

    // One row per listing. listedUnder: the higher taxon whose own listing covers a species the
    // Checklist does not name; otherwise a listing's inherited rank and name give it.
    private static void AddCitesRows(SiteTaxon taxon, CitesTaxon cites, IReadOnlyList<CitesListingRow> listings, string? listedUnder,
        SiteBuildStats stats) {
        var listedName = listedUnder is null ? SiteBuildRules.OtherListedName(cites.Name, taxon.ScientificName) : null;
        foreach (var listing in listings) {
            var under = listedUnder
                ?? (listing.InheritedName is { } inheritedName && listing.InheritedRank is { } inheritedRank
                    ? $"{inheritedRank.ToLowerInvariant()} {inheritedName}"
                    : null);
            var status = "Appendix " + listing.Appendix + (listing.Appendix == "III" && listing.PartyName is { } party ? $" ({party})" : "");
            // A listing's own note names the populations or parts it covers; an inherited listing's note
            // is about the higher taxon's other members.
            var population = under is null && listing.ShortNote is { } note ? SiteBuildRules.NullIfBlank(StatusLists.NztcsApi.PlainText(note)) : null;
            taxon.OtherStatuses.Add(new OtherStatus(OtherStatusSystems.Cites, status, listing.Appendix, listedName, population,
                OtherStatusSources.Cites, cites.Id.ToString(CultureInfo.InvariantCulture), cites.Url, listing.EffectiveOn,
                ListedUnder: under));
            stats.CitesRows++;
        }
    }

    // The download date and the citation the Checklist asks for.
    private static (string? Fetched, string? Citation) CitesSource(SqliteConnection connection) {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT fetched_at, citation FROM status_source WHERE source = 'cites'";
        using var reader = command.ExecuteReader();
        if (!reader.Read()) {
            return (null, null);
        }
        var fetched = Text(reader, 0) is { } at && StoredUtc.Parse(at) is { } utc ? utc.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) : null;
        return (fetched, Text(reader, 1));
    }

    private sealed record CitesTaxon(long Id, string Name, string Rank, bool Accepted, string? Kingdom, string Url,
        List<CitesListingRow> Listings, List<string> Synonyms);

    private sealed record CitesListingRow(string Appendix, string? PartyName, string? EffectiveOn, string? ShortNote,
        string? InheritedRank, string? InheritedName);
}
