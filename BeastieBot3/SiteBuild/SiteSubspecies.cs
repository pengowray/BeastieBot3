using System.Globalization;
using BeastieBot3.Shared.SiteData;
using Microsoft.Data.Sqlite;

// The subspecies and varieties that the Catalogue of Life, Wikidata, the Mammal Diversity Database and
// the Reptile Database list under each IUCN species in the release, for the species page's list of
// subspecies and varieties (infraspecific_name). The site adds IUCN's own subspecies and varieties
// from the taxon table and merges the rows by name. The two checklists are read by ReadChecklists
// (SiteSubspecies.Checklists.cs).
//   col       accepted and provisionally accepted name usages of rank subspecies or variety whose
//             parentID is the species' col_id: one indexed query per species (nameusage has an
//             index on parentID; about 210,000 such usages in COL26.7 XR), with CoL's authorship.
//   wikidata  items of `wikidata sweep-taxa`'s table with rank subspecies (Q68947) or variety
//             (Q767728), read once by rank (about 300,000 rows) and placed under each species whose
//             item is among their parent taxa (P171). Left out: items that are an instance of
//             synonym, fossil taxon, unavailable combination or original combination, and items that
//             another item names as a taxon synonym (P1420). When two subspecies or variety items name
//             each other as a synonym (Panthera leo leo names P. l. persica and the persica item names
//             P. l. leo), neither can be taken as the other's synonym, so both are kept.
// Names that InfraspecificNames.Split cannot read as genus, species and one more epithet are left out
// and counted.

namespace BeastieBot3.SiteBuild;

internal sealed record SiteInfraspecificName(long TaxonId, string Source, string SourceId, string Rank, string Name, string? Authority);

/// What reading the subspecies and varieties found, for the build summary.
internal sealed class SubspeciesCounts {
    /// IUCN's subspecies and varieties in the release whose parent is a species in the release.
    public int IucnRows;
    public int ColSpeciesRead;
    public int ColRows;
    public int ColUnreadable;
    public int WikidataItemsRead;
    public int WikidataRows;
    public int WikidataUnreadable;
    public int WikidataLeftOutByInstance;
    public int WikidataLeftOutAsSynonym;
    /// Items kept although another item names them as a taxon synonym, because each item that does is
    /// named as a synonym by them in turn.
    public int WikidataKeptAsMutualSynonym;
    /// Species with at least one row from any source, and those with rows from two or more sources.
    public int SpeciesWithList;
    public int SpeciesWithSeveralSources;
    public readonly HashSet<long> ColSpecies = new();
    public readonly HashSet<long> WikidataSpecies = new();
    public readonly ChecklistSubspeciesCounts Mdd = new();
    public readonly ChecklistSubspeciesCounts ReptileDb = new();
}

/// What reading one checklist's subspecies (checklists store) found.
internal sealed class ChecklistSubspeciesCounts {
    /// The source's species with subspecies, and those an IUCN species in the release takes them from.
    public int SourceSpecies;
    public int SourceSpeciesMatched;
    /// The subspecies of the matched species: rows stored, fossil subspecies left out, and names that
    /// InfraspecificNames.Split cannot read (or of a species with no record id) left out.
    public int Rows;
    public int LeftOutFossil;
    public int Unreadable;
    /// IUCN species with one or more rows.
    public readonly HashSet<long> Species = new();
}

internal static partial class SiteSubspecies {
    public const string Col = "col";
    public const string Wikidata = "wikidata";
    public const long SubspeciesRankQid = 68947;
    public const long VarietyRankQid = 767728;

    /// Instance of (P31) values that make an item something other than a subspecies or variety to
    /// list: synonym, fossil taxon, unavailable combination, original combination. Extinct taxon
    /// (Q98961713) is kept: an extinct subspecies such as the Cape lion is still one of the species'
    /// subspecies.
    internal static readonly IReadOnlySet<long> LeftOutInstances = new HashSet<long> { 1040689, 23038290, 17487588, 14594740 };

    public const string Insert = """
        INSERT OR IGNORE INTO infraspecific_name (taxon_id, source, source_id, rank, name, authority)
        VALUES (@t, @s, @i, @r, @n, @a)
        """;
    public static readonly string[] InsertParameters = ["@t", "@s", "@i", "@r", "@n", "@a"];

    public static object?[] InsertValues(SiteInfraspecificName row) =>
        [row.TaxonId, row.Source, row.SourceId, row.Rank, row.Name, row.Authority];

    /// The species the site lists subspecies for: species in the release.
    public static bool IsListed(SiteTaxon taxon) => taxon.InRelease && taxon.Kind == SiteTaxonKind.Species;

    public static List<SiteInfraspecificName> ReadCol(string path, IEnumerable<SiteTaxon> taxa, SubspeciesCounts counts, CancellationToken ct) {
        using var connection = SiteLinkReaders.OpenReadOnly(path);
        // The CoL database has no ANALYZE statistics and rank is indexed too, so name the parentID
        // index when the file has it.
        var indexed = HasIndex(connection, "idx_nameusage_parentID") ? "INDEXED BY idx_nameusage_parentID" : string.Empty;
        using var command = connection.CreateCommand();
        command.CommandText = $"""
            SELECT ID, rank, scientificName, authorship FROM nameusage {indexed}
            WHERE parentID = @id AND rank IN ('subspecies', 'variety') AND status IN ('accepted', 'provisionally accepted')
            """;
        var id = command.Parameters.Add("@id", SqliteType.Text);
        var rows = new List<SiteInfraspecificName>();
        foreach (var taxon in taxa) {
            ct.ThrowIfCancellationRequested();
            if (!IsListed(taxon) || taxon.ColId is not { } colId) {
                continue;
            }
            counts.ColSpeciesRead++;
            id.Value = colId;
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                var name = reader.IsDBNull(2) ? null : reader.GetString(2).Trim();
                if (reader.IsDBNull(0) || name is null || InfraspecificNames.Split(name) is null) {
                    counts.ColUnreadable++;
                    continue;
                }
                var rank = reader.GetString(1) == "variety" ? InfraspecificNames.Variety : InfraspecificNames.Subspecies;
                rows.Add(new SiteInfraspecificName(taxon.TaxonId, Col, reader.GetString(0), rank, name,
                    reader.IsDBNull(3) ? null : SiteBuildRules.NullIfBlank(reader.GetString(3))));
                counts.ColRows++;
                counts.ColSpecies.Add(taxon.TaxonId);
            }
        }
        return rows;
    }

    /// Null (with a warning) when the Wikidata cache has no taxon sweep.
    public static List<SiteInfraspecificName>? ReadWikidata(string path, IEnumerable<SiteTaxon> taxa, SubspeciesCounts counts,
        out string? warning, CancellationToken ct) {
        warning = null;
        using var connection = SiteLinkReaders.OpenReadOnly(path);
        var columns = Columns(connection, "wikidata_taxon_sweep");
        if (columns.Count == 0) {
            warning = "The Wikidata cache has no taxon sweep, so the species pages list no subspecies or varieties from Wikidata. To add them, run wikidata sweep-taxa.";
            return null;
        }
        if (!columns.Contains("instance_of") || !columns.Contains("synonym_of")) {
            warning = "The Wikidata taxon sweep is from before instance of (P31) and taxon synonym (P1420) were stored, so the subspecies and varieties from Wikidata include synonyms and fossil taxa. Run wikidata sweep-taxa --restart.";
        }

        // The species by the number of their Wikidata item.
        var speciesByItem = new Dictionary<long, List<long>>();
        foreach (var taxon in taxa) {
            if (IsListed(taxon) && taxon.WikidataQid is { } qid && qid.Length > 1 && qid[0] == 'Q'
                && long.TryParse(qid.AsSpan(1), NumberStyles.None, CultureInfo.InvariantCulture, out var number)) {
                if (!speciesByItem.TryGetValue(number, out var list)) {
                    speciesByItem[number] = list = new List<long>();
                }
                list.Add(taxon.TaxonId);
            }
        }

        using var command = connection.CreateCommand();
        command.CommandText = $"""
            SELECT qid, taxon_name, rank_qid, parent_qids,
                   {(columns.Contains("instance_of") ? "instance_of" : "NULL")}, {(columns.Contains("synonym_of") ? "synonym_of" : "NULL")}
            FROM wikidata_taxon_sweep WHERE rank_qid IN ({SubspeciesRankQid}, {VarietyRankQid})
            """;
        command.CommandTimeout = 0;
        // Read first and decided after, because whether an item is kept as a mutual synonym depends on
        // the other item's row.
        var found = new List<(long Qid, string Rank, string Name, List<long> Species, HashSet<long> SynonymOf)>();
        var synonymOfByItem = new Dictionary<long, HashSet<long>>();
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            if (++counts.WikidataItemsRead % 50_000 == 0) {
                ct.ThrowIfCancellationRequested();
            }
            var species = Numbers(reader.IsDBNull(3) ? null : reader.GetString(3))
                .Distinct()
                .Where(speciesByItem.ContainsKey)
                .SelectMany(p => speciesByItem[p])
                .Distinct()
                .ToList();
            if (species.Count == 0) {
                continue;
            }
            if (Numbers(reader.IsDBNull(4) ? null : reader.GetString(4)).Any(LeftOutInstances.Contains)) {
                counts.WikidataLeftOutByInstance++;
                continue;
            }
            var name = reader.GetString(1).Trim();
            if (InfraspecificNames.Split(name) is null) {
                counts.WikidataUnreadable++;
                continue;
            }
            var qid = reader.GetInt64(0);
            var synonymOf = Numbers(reader.IsDBNull(5) ? null : reader.GetString(5)).ToHashSet();
            synonymOfByItem[qid] = synonymOf;
            found.Add((qid, reader.GetInt64(2) == VarietyRankQid ? InfraspecificNames.Variety : InfraspecificNames.Subspecies, name, species, synonymOf));
        }

        var rows = new List<SiteInfraspecificName>();
        foreach (var (qid, rank, name, species, synonymOf) in found) {
            if (synonymOf.Count > 0) {
                // Kept only when every item that names it as a synonym is named as a synonym by it.
                if (!synonymOf.All(other => synonymOfByItem.TryGetValue(other, out var back) && back.Contains(qid))) {
                    counts.WikidataLeftOutAsSynonym++;
                    continue;
                }
                counts.WikidataKeptAsMutualSynonym++;
            }
            var sourceId = "Q" + qid.ToString(CultureInfo.InvariantCulture);
            foreach (var taxonId in species) {
                rows.Add(new SiteInfraspecificName(taxonId, Wikidata, sourceId, rank, name, null));
                counts.WikidataRows++;
                counts.WikidataSpecies.Add(taxonId);
            }
        }
        return rows;
    }

    /// Counts IUCN's subspecies and varieties under the listed species and the species that have a
    /// list from any source. Call after the other readers.
    public static void CountLists(IEnumerable<SiteTaxon> taxa, IReadOnlyDictionary<long, SiteTaxon> byId, SubspeciesCounts counts) {
        var iucnSpecies = new HashSet<long>();
        foreach (var taxon in taxa) {
            if (taxon.InRelease && InfraspecificNames.RankOfKind(taxon.Kind) is not null
                && taxon.ParentTaxonId is { } parent && byId.TryGetValue(parent, out var species) && IsListed(species)) {
                counts.IucnRows++;
                iucnSpecies.Add(parent);
            }
        }
        HashSet<long>[] sources = [iucnSpecies, counts.ColSpecies, counts.WikidataSpecies, counts.Mdd.Species, counts.ReptileDb.Species];
        var all = new HashSet<long>();
        foreach (var source in sources) {
            all.UnionWith(source);
        }
        counts.SpeciesWithList = all.Count;
        counts.SpeciesWithSeveralSources = all.Count(id => sources.Count(source => source.Contains(id)) >= 2);
    }

    private static bool HasIndex(SqliteConnection connection, string name) {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = @name";
        command.Parameters.AddWithValue("@name", name);
        return Convert.ToInt64(command.ExecuteScalar(), CultureInfo.InvariantCulture) > 0;
    }

    private static HashSet<string> Columns(SqliteConnection connection, string table) {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT name FROM pragma_table_info(@table)";
        command.Parameters.AddWithValue("@table", table);
        var columns = new HashSet<string>(StringComparer.Ordinal);
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            columns.Add(reader.GetString(0));
        }
        return columns;
    }

    // "140 2118614" -> [140, 2118614]
    private static IEnumerable<long> Numbers(string? text) {
        if (string.IsNullOrWhiteSpace(text)) {
            yield break;
        }
        foreach (var word in text.Split(' ', StringSplitOptions.RemoveEmptyEntries)) {
            if (long.TryParse(word, NumberStyles.None, CultureInfo.InvariantCulture, out var n)) {
                yield return n;
            }
        }
    }
}
