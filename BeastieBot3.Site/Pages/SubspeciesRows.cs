using BeastieBot3.Shared.SiteData;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Display;

namespace BeastieBot3.Site.Pages;

/// One record of a source: the text of its link (the record's id), its address, and the name as the
/// record writes it when that is not the row's name ("Panthera leo melanochaitus" in a row named
/// "Panthera leo melanochaita"), else null.
public sealed record SubspeciesRecord(string Id, string Url, string? OtherName = null);

/// A source that lists a subspecies or variety, with its records (usually one; two when the source has
/// two records with the name, as Wikidata sometimes has) and, when its authority differs from the one
/// shown, its own authority.
public sealed record SubspeciesSource(string Source, string Label, IReadOnlyList<SubspeciesRecord> Records, string? OtherAuthority);

/// One subspecies or variety on the species page: the name as the site writes it ("Panthera leo
/// melanochaita" for an animal, "Abies alba var. acutifolia" for a plant) in the spelling of the first
/// source in SourceOrder, its rank, the authority of the first source that gives one, and the sources
/// in SourceOrder.
public sealed record SubspeciesRow(string Name, string Rank, string? Authority, IReadOnlyList<SubspeciesSource> Sources);

/// The species page's list of subspecies and varieties from IUCN, Wikidata, the Catalogue of Life, the
/// Mammal Diversity Database and the Reptile Database:
/// the names with most sources first, then by name, as in the table of names in other languages.
public sealed record SubspeciesList(IReadOnlyList<SubspeciesRow> Rows) {
    public bool HasSubspecies => Rows.Any(r => r.Rank == InfraspecificNames.Subspecies);
    public bool HasVarieties => Rows.Any(r => r.Rank == InfraspecificNames.Variety);

    /// True when a row's only source is Wikidata, whose subspecies items include many names that
    /// current classifications treat as synonyms.
    public bool HasWikidataOnlyRows => Rows.Any(r => r.Sources.Count == 1 && r.Sources[0].Source == "wikidata");
}

public static class SubspeciesRows {
    /// The list for a species in the release; null for any other taxon, and when no source lists a
    /// subspecies or variety of it.
    public static SubspeciesList? Load(SiteQueries queries, TaxonRow taxon) =>
        taxon.Kind == TaxonKinds.Species && taxon.InRelease ? Build(queries.GetInfraspecificNames(taxon.TaxonId), taxon.Kingdom) : null;

    /// One row per name (InfraspecificNames.MergeKey: the rank and the stem of the infraspecific
    /// epithet, so spellings with other Latin endings or another species part are one row; subspecies
    /// and varieties kept apart), the names with most sources first, then by name. kingdom: the
    /// species' kingdom, which decides the rank marker shown (none for an animal subspecies). Null when
    /// there are no rows.
    public static SubspeciesList? Build(IEnumerable<InfraspecificNameRow> rows, string? kingdom) {
        var list = rows
            .GroupBy(r => InfraspecificNames.MergeKey(r.Rank, r.Name) ?? FallbackKey(r))
            .Select(g => {
                var ordered = g.OrderBy(r => SourceOrder(r.Source)).ThenBy(r => r.SourceId, StringComparer.Ordinal).ToList();
                var first = ordered[0];
                var shown = DisplayName(first.Name, first.Rank, kingdom);
                string? OtherName(InfraspecificNameRow r) =>
                    DisplayName(r.Name, r.Rank, kingdom) is var name && SiteNameKey.Fold(name) != SiteNameKey.Fold(shown) ? name : null;
                var authority = ordered.Select(r => r.Authority).FirstOrDefault(a => !string.IsNullOrWhiteSpace(a));
                var sources = ordered
                    .GroupBy(r => r.Source)
                    .Select(s => {
                        var own = s.Select(r => r.Authority).FirstOrDefault(a => !string.IsNullOrWhiteSpace(a));
                        var other = own is not null && authority is not null && !SameAuthority(own, authority) ? own : null;
                        var records = s.Select(r => new SubspeciesRecord(r.SourceId, RecordUrl(r.Source, r.SourceId), OtherName(r)))
                            .DistinctBy(r => r.Id).ToList();
                        return new SubspeciesSource(s.Key, SiteText.SourceLabel(s.Key), records, other);
                    })
                    .ToList();
                return new SubspeciesRow(shown, first.Rank, authority, sources);
            })
            .OrderByDescending(r => r.Sources.Count)
            .ThenBy(r => r.Name, StringComparer.OrdinalIgnoreCase)
            .ThenBy(r => r.Name, StringComparer.Ordinal)
            .ToList();
        return list.Count == 0 ? null : new SubspeciesList(list);
    }

    /// The name as the site writes it: genus, species, the rank marker for the kingdom
    /// (SpeciesListLine.InfraspecificRankMarker: none for an animal subspecies, "subsp." for any other,
    /// "var." for a variety) and the infraspecific epithet. A name InfraspecificNames.Split cannot read
    /// is kept as the source writes it.
    public static string DisplayName(string name, string rank, string? kingdom) {
        if (InfraspecificNames.Split(name) is not { } parts) {
            return name.Trim();
        }
        var marker = SpeciesListLine.InfraspecificRankMarker(rank == InfraspecificNames.Variety ? "var." : "subsp.", kingdom);
        return marker is null
            ? $"{parts.Genus} {parts.Species} {parts.Infra}"
            : $"{parts.Genus} {parts.Species} {marker} {parts.Infra}";
    }

    /// The record's address: this site's page of an IUCN taxon, the Catalogue of Life record, the
    /// Wikidata item, or the species page that lists the subspecies on the Mammal Diversity Database
    /// (source id the MDD id) or the Reptile Database (source id the query of the species page).
    public static string RecordUrl(string source, string sourceId) => source switch {
        "iucn" => "/species/" + sourceId,
        "col" => SiteFormat.CatalogueOfLifeUrl(sourceId),
        "wikidata" => SiteFormat.WikidataUrl(sourceId),
        "mdd" => SiteText.MddUrl + "taxon/" + Uri.EscapeDataString(sourceId) + "/",
        "reptiledb" => "https://reptile-database.reptarium.cz/species?" + sourceId,
        _ => string.Empty,
    };

    // The order of the names tables (TaxonNames): IUCN, Wikidata, the Catalogue of Life; then the
    // Mammal Diversity Database and the Reptile Database.
    private static int SourceOrder(string source) => source switch {
        "iucn" => 0,
        "wikidata" => 1,
        "col" => 2,
        "mdd" => 3,
        "reptiledb" => 4,
        _ => 5,
    };

    // The key of a name InfraspecificNames.Split cannot read: the rank and the stem of the name's last
    // word, as MergeKey gives for a name it reads.
    private static string FallbackKey(InfraspecificNameRow row) =>
        row.Rank + ":" + LatinNameVariant.Stem(SiteNameKey.Fold(row.Name).Split(' ', StringSplitOptions.RemoveEmptyEntries).LastOrDefault() ?? "");

    // Authorities that differ only in spacing, case or accents are the same.
    private static bool SameAuthority(string a, string b) => SiteNameKey.Fold(a) == SiteNameKey.Fold(b);
}
