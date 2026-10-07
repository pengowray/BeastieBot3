using Microsoft.Data.Sqlite;

// The classification of each taxon in the Catalogue of Life and in Wikidata, for the species page's
// comparison of ranks (ladder_node): the taxon's own node and every node above it, each with its
// parent, rank and name.
//   col       from the CoL database's nameusage (ID, parentID, rank, scientificName), from each
//             taxon's col_id up.
//   wikidata  from the Wikidata taxon sweep (wikidata_taxon_sweep: qid, parent_qids, rank_qid,
//             taxon_name), from each taxon's wikidata_qid up. An item with two or more parent taxa
//             (P171) is followed through the first; rank items are named from
//             rules/wikidata-taxon-ranks.csv.
//   wikipedia from the Wikipedia cache: a node "article:<title>" for the taxon's English Wikipedia
//             article (its taxobox's name and rank), whose parent is the taxonomy template its
//             taxobox starts from (Template:Taxonomy/Felis, id "Felis"), then each template's parent
//             (`wikipedia fetch-taxonomy-templates` downloads them).
//   wikispecies from the Wikispecies cache (`wikispecies fetch`): a node "page:<IUCN scientific
//             name>" for the taxon on its Wikispecies page (the page of that name, through any
//             redirect), with "#<n>" for the taxa above it on the same page, then the taxonavigation
//             templates the page climbs: a template's last taxon has the template's name as its id,
//             the ones above it "<template>#<n>". A template with no taxon line is passed through.
// Only the nodes above IUCN's taxa are kept, so the tables stay small.

namespace BeastieBot3.SiteBuild;

internal sealed record LadderNode(string Source, string Id, string? ParentId, string? Rank, string Name);

internal static class SiteLadders {
    public const string Col = "col";
    public const string Wikidata = "wikidata";
    public const string Wikipedia = "wikipedia";
    public const string ArticlePrefix = "article:";
    public const string Wikispecies = "wikispecies";
    public const string PagePrefix = "page:";

    public static List<LadderNode> ReadWikispecies(string wikispeciesCache, IEnumerable<string> scientificNames, CancellationToken ct) {
        using var cache = global::BeastieBot3.Wikipedia.WikipediaCacheStore.OpenReadOnly(wikispeciesCache);
        if (cache is null) {
            return [];
        }
        var nodes = new List<LadderNode>();
        // Template name -> the id of its last taxon (or of the nearest template above that has one).
        var tops = new Dictionary<string, string?>(StringComparer.Ordinal);
        string? TopOf(string name, int depth) {
            if (tops.TryGetValue(name, out var known)) {
                return known;
            }
            tops[name] = null; // a loop of templates ends here
            if (depth >= MaxDepth || cache.ReadArticleText(Taxonomy.WikispeciesTaxonavigation.TemplateTitle(name)) is not { } page) {
                return null;
            }
            // A redirect: the template it leads to, under that template's name.
            var key = page.Title.StartsWith(Taxonomy.WikispeciesTaxonavigation.TemplatePrefix, StringComparison.Ordinal)
                ? page.Title[Taxonomy.WikispeciesTaxonavigation.TemplatePrefix.Length..]
                : name;
            if (key != name) {
                return tops[name] = TopOf(key, depth + 1);
            }
            if (Taxonomy.WikispeciesTaxonavigation.ParseTemplate(page.Wikitext, name) is not { } template) {
                return null;
            }
            var parent = template.Parent is { } p ? TopOf(p, depth + 1) : null;
            return tops[name] = AddChain(nodes, name, template.Steps, parent);
        }
        foreach (var scientificName in scientificNames.Distinct(StringComparer.Ordinal)) {
            ct.ThrowIfCancellationRequested();
            var title = Taxonomy.WikispeciesTaxonavigation.TitleFor(scientificName);
            if (cache.ReadArticleText(title) is not { } page
                || Taxonomy.WikispeciesTaxonavigation.ParsePage(page.Wikitext, page.Title) is not { Steps.Count: > 0 } taxonav
                // The last step must be the page's own taxon, or the ladder would end above it.
                || !string.Equals(taxonav.Steps[^1].Name, page.Title, StringComparison.Ordinal)) {
                continue;
            }
            var parent = taxonav.Parent is { } p ? TopOf(p, 0) : null;
            AddChain(nodes, PagePrefix + scientificName, taxonav.Steps, parent);
        }
        return nodes;
    }

    // One node per step, each the parent of the next; the last has the id given, the others "<id>#<n>".
    private static string? AddChain(List<LadderNode> nodes, string id, IReadOnlyList<Taxonomy.WikispeciesStep> steps, string? parent) {
        for (var i = 0; i < steps.Count; i++) {
            var stepId = i == steps.Count - 1 ? id : $"{id}#{i}";
            nodes.Add(new LadderNode(Wikispecies, stepId, parent, steps[i].Rank, steps[i].Name));
            parent = stepId;
        }
        return parent;
    }

    public static List<LadderNode> ReadWikipedia(string wikipediaCache, IEnumerable<string> articleTitles, CancellationToken ct) {
        using var cache = global::BeastieBot3.Wikipedia.WikipediaCacheStore.OpenReadOnly(wikipediaCache);
        if (cache is null) {
            return [];
        }
        var templates = new Dictionary<string, Taxonomy.TaxonomyTemplate?>(StringComparer.Ordinal);
        Taxonomy.TaxonomyTemplate? Template(string name) {
            if (!templates.TryGetValue(name, out var t)) {
                var page = cache.ReadArticleText(Taxonomy.TaxonomyTemplates.Title(name));
                t = page is null ? null : Taxonomy.TaxonomyTemplates.Parse(name, page.Wikitext);
                // "Mammalia/skip" (same as=Mammalia): Mammalia's rank and name, its own parent.
                if (t?.SameAs is { } sameAs && sameAs != name && Template(sameAs) is { } same) {
                    t = t with { Rank = same.Rank, Display = same.Display };
                } else if (t?.SameAs is { } other) {
                    t = t with { Display = other };
                }
                templates[name] = t;
            }
            return t;
        }
        var nodes = new List<LadderNode>();
        var added = new HashSet<string>(StringComparer.Ordinal);
        foreach (var title in articleTitles.Distinct(StringComparer.Ordinal)) {
            ct.ThrowIfCancellationRequested();
            if (cache.ResolveDownloadedArticle(title, readTaxobox: false) is not { } article
                || cache.GetTaxoboxFields(article.PageRowId) is not { } fields
                || Taxonomy.TaxonomyTemplates.StartOf(fields) is not { } start || Template(start) is null) {
                continue;
            }
            var genus = Plain(fields.GetValueOrDefault("genus"));
            var species = Plain(fields.GetValueOrDefault("species"));
            var name = genus is { Length: > 0 } && species is { Length: > 0 } ? $"{genus} {species}"
                : Plain(fields.GetValueOrDefault("taxon")) is { Length: > 0 } t ? t : article.Title;
            var subspecies = Plain(fields.GetValueOrDefault("subspecies"));
            var rank = subspecies is { Length: > 0 } ? "subspecies" : species is { Length: > 0 } || name.Contains(' ') ? "species" : null;
            if (subspecies is { Length: > 0 }) {
                name = $"{name} {subspecies}";
            }
            nodes.Add(new LadderNode(Wikipedia, ArticlePrefix + title, start, rank, name));
            for (var (at, depth) = (start, 0); at is not null && depth < MaxDepth && added.Add(at); depth++) {
                if (Template(at) is not { } template) {
                    break;
                }
                nodes.Add(new LadderNode(Wikipedia, at, template.Parent, template.Rank, template.Display));
                at = template.Parent;
            }
        }
        return nodes;
    }

    // The most steps up from a taxon: deeper chains are loops in the data.
    private const int MaxDepth = 80;

    public static List<LadderNode> ReadCol(string colDatabase, IEnumerable<string> colIds, CancellationToken ct) {
        using var connection = Open(colDatabase);
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT parentID, rank, scientificName FROM nameusage WHERE ID = @id LIMIT 1";
        var id = command.Parameters.Add("@id", SqliteType.Text);
        return Walk(Col, colIds, ct, nodeId => {
            id.Value = nodeId;
            using var reader = command.ExecuteReader();
            return reader.Read()
                ? (reader.IsDBNull(0) || reader.GetString(0).Length == 0 ? null : reader.GetString(0),
                    reader.IsDBNull(1) ? null : reader.GetString(1), reader.IsDBNull(2) ? nodeId : reader.GetString(2))
                : null;
        });
    }

    public static List<LadderNode> ReadWikidata(string wikidataCache, IEnumerable<string> qids, IReadOnlyDictionary<string, string> rankNames,
        CancellationToken ct) {
        using var connection = Open(wikidataCache);
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT parent_qids, rank_qid, taxon_name FROM wikidata_taxon_sweep WHERE qid = @qid";
        var qid = command.Parameters.Add("@qid", SqliteType.Integer);
        return Walk(Wikidata, qids, ct, nodeId => {
            if (!long.TryParse(nodeId.TrimStart('Q'), out var number)) {
                return null;
            }
            qid.Value = number;
            using var reader = command.ExecuteReader();
            if (!reader.Read()) {
                return null;
            }
            var parents = reader.IsDBNull(0) ? "" : Convert.ToString(reader.GetValue(0), System.Globalization.CultureInfo.InvariantCulture) ?? "";
            var parent = parents.Split(' ', StringSplitOptions.RemoveEmptyEntries).FirstOrDefault();
            var rank = reader.IsDBNull(1) ? null : Convert.ToString(reader.GetValue(1), System.Globalization.CultureInfo.InvariantCulture);
            return (parent is null ? null : "Q" + parent,
                rank is { Length: > 0 } r ? rankNames.GetValueOrDefault("Q" + r) : null,
                reader.IsDBNull(2) ? nodeId : reader.GetString(2));
        });
    }

    // A taxobox value as a name: no references, templates, comments, links or italics.
    private static string? Plain(string? value) {
        if (value is null) {
            return null;
        }
        var text = System.Text.RegularExpressions.Regex.Replace(value, @"<ref[^>]*/>|<ref[^>]*>.*?</ref>|<!--.*?-->|\{\{[^{}]*\}\}", "",
            System.Text.RegularExpressions.RegexOptions.Singleline);
        text = System.Text.RegularExpressions.Regex.Replace(text, @"'{2,}|\[\[|\]\]|<[^>]+>", "");
        return text.Split('<')[0].Trim();
    }

    /// rules/wikidata-taxon-ranks.csv: rank item id and its English name.
    public static Dictionary<string, string> ReadRankNames(string? path) {
        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        if (path is null || !File.Exists(path)) {
            return map;
        }
        foreach (var line in File.ReadLines(path).Skip(1)) {
            var f = line.Split(',');
            if (f.Length >= 2 && f[1].Length > 0) {
                map[f[0]] = f[1];
            }
        }
        return map;
    }

    private static List<LadderNode> Walk(string source, IEnumerable<string> starts, CancellationToken ct,
        Func<string, (string? Parent, string? Rank, string Name)?> read) {
        var nodes = new Dictionary<string, LadderNode>(StringComparer.Ordinal);
        foreach (var start in starts) {
            ct.ThrowIfCancellationRequested();
            var at = start;
            for (var depth = 0; at is not null && depth < MaxDepth && !nodes.ContainsKey(at); depth++) {
                if (read(at) is not { } node) {
                    break;
                }
                nodes[at] = new LadderNode(source, at, node.Parent, node.Rank, node.Name);
                at = node.Parent;
            }
        }
        return [.. nodes.Values];
    }

    private static SqliteConnection Open(string path) {
        var connection = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = path, Mode = SqliteOpenMode.ReadOnly, Pooling = false }.ConnectionString);
        connection.Open();
        return connection;
    }
}
