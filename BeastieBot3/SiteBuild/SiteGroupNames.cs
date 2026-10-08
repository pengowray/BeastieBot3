using BeastieBot3.Col;
using BeastieBot3.CommonNames;
using BeastieBot3.Taxonomy;
using BeastieBot3.WikipediaLists;
using Microsoft.Data.Sqlite;

// Names and links of the groups in the site's tree (higher_taxon), for `site build-db`:
//
//   common_name_en  the name the Wikipedia list headings use for the group (HeadingFormatter):
//                   taxon-rules.yml, then rules-list.txt, then the common names store, then the
//                   title of the article the scientific name redirects to (Araneae -> Spider).
//   enwiki_title    a wikilink from the rules, else the group's name when English Wikipedia has a
//                   page or redirect with it that is not about another kingdom, else, when the name
//                   is a disambiguation page, the name with a bracketed word for the kingdom.
//   col_id          for a CoL group, the id the placement file gives; for an IUCN group, the accepted
//                   CoL name usage with the same name, rank and kingdom (for a genus with two such
//                   usages, the one in the same family).
//   higher_taxon_name  CoL's English vernacular names for that CoL id, with the caps rules applied
//                   as for a group name ("Typical Big Cats" is "typical big cats", "Old World
//                   Monkeys" is "Old World monkeys"), each name listed once.
// Every source is opened read-only, and each is optional.

namespace BeastieBot3.SiteBuild;

internal static class SiteGroupNames {
    /// <param name="capsRules">The common names store's caps rules, for CoL's names; empty leaves
    /// them as CoL writes them.</param>
    public static void Resolve(IReadOnlyList<SiteTreeNode> nodes, HeadingFormatter? headings, EnwikiTitleCheck? titles,
        string? colDatabase, IReadOnlyDictionary<string, string> capsRules, SiteBuildStats stats, CancellationToken cancellationToken) {
        foreach (var node in nodes) {
            cancellationToken.ThrowIfCancellationRequested();
            if (headings is not null) {
                var (name, source) = headings.ResolveCommonName(node.Name, node.Kingdom);
                if (!string.IsNullOrWhiteSpace(name)) {
                    node.CommonNameEn = name.Trim();
                    node.CommonNameSource = source;
                    stats.GroupCommonNames++;
                }
            }
            node.EnwikiTitle = ArticleFor(node, headings, titles);
            if (node.EnwikiTitle is not null) {
                stats.GroupArticles++;
            }
        }

        if (colDatabase is null || !File.Exists(colDatabase)) {
            return;
        }
        using var col = ColLineageMatcher.OpenReadOnly(colDatabase);
        using var findUsage = col.CreateCommand();
        // The unary + keeps SQLite on the scientificName index: without statistics it picks the
        // rank index, and every lookup then reads every usage of that rank.
        findUsage.CommandText = """
            SELECT ID, kingdom, family FROM nameusage
            WHERE scientificName = @name AND +rank = @rank AND +status = 'accepted'
            """;
        var nameParameter = findUsage.Parameters.Add("@name", SqliteType.Text);
        var rankParameter = findUsage.Parameters.Add("@rank", SqliteType.Text);
        using var findNames = col.CreateCommand();
        findNames.CommandText = "SELECT name FROM vernacularname WHERE taxonID = @id AND language = 'eng' AND name IS NOT NULL";
        var idParameter = findNames.Parameters.Add("@id", SqliteType.Text);

        foreach (var node in nodes) {
            cancellationToken.ThrowIfCancellationRequested();
            if (node.ColId is null && node.Source != SiteTreeSource.Col) {
                nameParameter.Value = node.Name;
                rankParameter.Value = node.Rank;
                var matches = new List<(string Id, string? Kingdom, string? Family)>();
                using (var reader = findUsage.ExecuteReader()) {
                    while (reader.Read()) {
                        matches.Add((reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1),
                            reader.IsDBNull(2) ? null : reader.GetString(2)));
                    }
                }
                node.ColId = ChooseUsage(node, matches);
            }
            if (node.ColId is null) {
                continue;
            }
            stats.GroupColIds++;
            idParameter.Value = node.ColId;
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            if (node.CommonNameEn is not null) {
                seen.Add(node.CommonNameEn);
            }
            using var names = findNames.ExecuteReader();
            while (names.Read()) {
                var name = names.GetString(0).Trim();
                if (capsRules.Count > 0) {
                    name = CommonNameNormalizer.ApplyGroupCapitalization(name, capsRules).Trim();
                }
                if (name.Length > 0 && seen.Add(name)) {
                    node.ColNames.Add(name);
                }
            }
            if (node.ColNames.Count > 0) {
                stats.GroupsWithColNames++;
            }
        }
    }

    // The accepted usage in the node's kingdom; for a genus with several, the one whose family is
    // the node's family. Null when none, or when it stays ambiguous.
    internal static string? ChooseUsage(SiteTreeNode node, IReadOnlyList<(string Id, string? Kingdom, string? Family)> matches) {
        var inKingdom = matches.Where(m => string.Equals(m.Kingdom, node.Kingdom, StringComparison.OrdinalIgnoreCase)).ToList();
        if (inKingdom.Count == 1) {
            return inKingdom[0].Id;
        }
        if (inKingdom.Count > 1 && node.Rank == "genus") {
            var family = Ancestor(node, "family")?.Name;
            var inFamily = inKingdom.Where(m => string.Equals(m.Family, family, StringComparison.OrdinalIgnoreCase)).ToList();
            if (inFamily.Count == 1) {
                return inFamily[0].Id;
            }
        }
        return null;
    }

    private static string? ArticleFor(SiteTreeNode node, HeadingFormatter? headings, EnwikiTitleCheck? titles) {
        if (headings?.ResolveWikilink(node.Name, node.Kingdom) is { } fromRules && !string.IsNullOrWhiteSpace(fromRules)) {
            return fromRules.Trim();
        }
        if (titles is null) {
            return null;
        }
        if (titles.Exists(node.Name) && !titles.IsAboutAnotherKingdom(node.Name, node.Kingdom)) {
            return node.Name;
        }
        return titles.IsDisambiguation(node.Name) ? titles.QualifiedTitle(node.Name, node.Kingdom) : null;
    }

    private static SiteTreeNode? Ancestor(SiteTreeNode node, string rank) {
        for (var at = node.Parent; at is not null; at = at.Parent) {
            if (at.Rank == rank) {
                return at;
            }
        }
        return null;
    }
}
