// Writes SiteExtraSpeciesBuild's rows: extra_species, extra_overlap, higher_taxon_extra and
// taxon_source_name (SiteDbSchema).

namespace BeastieBot3.SiteBuild.ExtraSpecies;

internal static class ExtraSpeciesWriter {
    public static void Write(SiteDbWriter writer, SiteExtraSpeciesBuild build) {
        writer.InsertRows("""
            INSERT INTO extra_species (extra_id, sources, scientific_name, wikidata_name, col_id, wikidata_qid, common_name_en,
                enwiki_title, node_id, sort_pos)
            VALUES (@extra_id, @sources, @scientific_name, @wikidata_name, @col_id, @wikidata_qid, @common_name_en,
                @enwiki_title, @node_id, @sort_pos)
            """,
            ["@extra_id", "@sources", "@scientific_name", "@wikidata_name", "@col_id", "@wikidata_qid", "@common_name_en",
                "@enwiki_title", "@node_id", "@sort_pos"],
            build.Entries.Select(e => new object?[] {
                e.ExtraId, (e.ColId is not null ? 1 : 0) | (e.Qid is not null ? 2 : 0), e.Name, e.WikidataName, e.ColId, e.Qid,
                e.CommonNameEn, e.EnwikiTitle, e.Node.NodeId, e.SortPos,
            }));
        writer.InsertRows("""
            INSERT INTO extra_overlap (extra_id, taxon_id, other_extra_id, reason, likely)
            VALUES (@extra_id, @taxon_id, @other_extra_id, @reason, @likely)
            """,
            ["@extra_id", "@taxon_id", "@other_extra_id", "@reason", "@likely"],
            build.Overlaps.OrderBy(o => o.Entry.ExtraId).Select(o => new object?[] {
                o.Entry.ExtraId, o.TaxonId, o.Other?.ExtraId, o.Reason, o.Likely ? 1 : 0,
            }));
        writer.InsertRows("""
            INSERT INTO higher_taxon_extra (node_id, last_node_id, col_count, wikidata_count, both_count)
            VALUES (@node_id, @last_node_id, @col_count, @wikidata_count, @both_count)
            """,
            ["@node_id", "@last_node_id", "@col_count", "@wikidata_count", "@both_count"],
            build.NodeCounts.OrderBy(p => p.Key).Select(p => new object?[] {
                p.Key, p.Value.LastNodeId, p.Value.Col, p.Value.Wikidata, p.Value.Both,
            }));
        writer.InsertRows("""
            INSERT OR IGNORE INTO taxon_source_name (taxon_id, source, scientific_name) VALUES (@taxon_id, @source, @scientific_name)
            """,
            ["@taxon_id", "@source", "@scientific_name"],
            build.SourceNames.Select(s => new object?[] { s.TaxonId, s.Source, s.ScientificName }));
    }
}
