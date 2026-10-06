using BeastieBot3.Shared.SiteData;
using BeastieBot3.Wikipedia;

// The names English Wikipedia gives a group (higher_taxon_name rows with source 'wikipedia'), for
// `site build-db`: the title of the group's article and the titles of the redirects to it, which
// `wikipedia fetch-group-titles` downloads. "Fruit bat" redirects to "Megabat", whose taxobox taxon
// is Pteropodidae, so family Pteropodidae gets "Megabat" and "Fruit bat".
//
// The article counts as the group's only when it is about the group:
//   - it is not a disambiguation page, and WikiPageKingdom finds nothing in it about a taxon in
//     another kingdom;
//   - its taxobox taxon is the group's name; or, when it has no taxobox name, the group's title
//     is a redirect to it and no taxon in the site is matched to it. An article whose taxobox
//     names another taxon is never the group's: genus Orycteropus redirects to "Aardvark", whose
//     taxobox taxon is Orycteropus afer.
// A title is left out when it is the group's own name, the scientific name of another group or of
// a taxon in the site, a redirect to a section, has brackets, digits or a colon, is possessive or
// all capitals, names the study of the group ("-ology", "-ologist"), or looks like a misspelling
// of the article title or the group's name.

namespace BeastieBot3.SiteBuild;

internal static class SiteGroupWikipediaNames {
    /// <summary>
    /// Sets <see cref="SiteTreeNode.WikipediaNames"/> on each node whose article (its enwiki_title,
    /// else its name) is downloaded and about the group.
    /// </summary>
    public static void Resolve(IReadOnlyList<SiteTreeNode> nodes, WikipediaCacheStore cache, IReadOnlySet<string> scientificNameKeys,
        IReadOnlySet<string> taxonArticleTitles, SiteBuildStats stats, CancellationToken cancellationToken) {
        foreach (var node in nodes) {
            cancellationToken.ThrowIfCancellationRequested();
            var title = node.EnwikiTitle ?? node.Name;
            if (cache.ResolveDownloadedArticle(title) is not { } article) {
                continue;
            }
            var evidence = cache.GetPageKingdomEvidence(article.PageRowId);
            var conflict = evidence is not null && WikiPageKingdom.Conflict(node.Kingdom, evidence, otherTitle: title) is not null;
            var ownedByTaxon = taxonArticleTitles.Contains(article.NormalizedTitle);
            if (!IsAboutGroup(node.Name, article, conflict, ownedByTaxon)) {
                continue;
            }
            stats.GroupWikipediaArticles++;
            var redirects = cache.ReadIncomingRedirects(article.Title);
            if (redirects is null) {
                stats.GroupWikipediaArticlesWithoutRedirects++;
            }
            var names = Names(node.Name, article.Title, redirects ?? [], key => scientificNameKeys.Contains(key));
            if (names.Count == 0) {
                continue;
            }
            node.WikipediaNames.AddRange(names);
            stats.GroupsWithWikipediaNames++;
            stats.GroupWikipediaNames += names.Count;
            if (node.CommonNameEn is null && names.Any(n => !CommonNames.CommonNameNormalizer.LooksLikeScientificName(n, null, null))) {
                stats.GroupsGainingEnglishNameCandidate++;
            }
        }
    }

    /// Whether <paramref name="article"/> is about the group named <paramref name="groupName"/> (see the file comment).
    internal static bool IsAboutGroup(string groupName, WikiGroupArticle article, bool kingdomConflict, bool ownedByTaxon) {
        if (article.IsDisambiguation || kingdomConflict) {
            return false;
        }
        if (!string.IsNullOrWhiteSpace(article.TaxoboxName)) {
            return string.Equals(CleanTaxoboxName(article.TaxoboxName), groupName.Trim(), StringComparison.OrdinalIgnoreCase);
        }
        // With no taxobox to say what the page is about, a page that a taxon's name leads to (a
        // species whose name redirects to its genus page is matched to that page) stays the taxon's.
        return article.Redirected && !ownedByTaxon;
    }

    // "†Pteropodidae", "''Pteropus''" -> the bare name.
    private static string CleanTaxoboxName(string name) =>
        string.Join(' ', name.Replace("''", string.Empty).Trim().TrimStart('†', '?').Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));

    /// <summary>
    /// The names to store for a group: the article title (when it is not the group's name) and the
    /// redirects to it, without the titles described in the file comment. <paramref name="isOtherName"/>
    /// tells whether a folded name (SiteNameKey.Fold) is the scientific name of another group or taxon.
    /// </summary>
    internal static IReadOnlyList<string> Names(string groupName, string articleTitle, IReadOnlyList<WikipediaIncomingRedirect> redirects,
        Func<string, bool> isOtherName) {
        var ownKey = SiteNameKey.Fold(groupName);
        var anchors = new[] { Letters(articleTitle), Letters(groupName) };
        var seen = new HashSet<string>(StringComparer.Ordinal) { ownKey };
        var names = new List<string>();
        void Consider(string title, bool isArticle) {
            var name = title.Trim();
            var key = SiteNameKey.Fold(name);
            if (key.Length == 0 || seen.Contains(key) || !LooksUsable(name) || isOtherName(key)) {
                return;
            }
            if (!isArticle && anchors.Any(anchor => LooksLikeMisspelling(Letters(name), anchor))) {
                return;
            }
            seen.Add(key);
            names.Add(name);
        }
        Consider(articleTitle, isArticle: true);
        foreach (var redirect in redirects.OrderBy(r => r.Title, StringComparer.Ordinal)) {
            if (redirect.Fragment is null) {
                Consider(redirect.Title, isArticle: false);
            }
        }
        return names;
    }

    // No brackets (a disambiguated title), digits, colons or slashes; not possessive; not all capitals.
    internal static bool LooksUsable(string name) {
        if (name.Length < 2 || name.IndexOfAny(['(', ')', ':', '/', '"', '[', ']', '&', ',']) >= 0 || name.Any(char.IsDigit)) {
            return false;
        }
        if (name.Contains("'s", StringComparison.OrdinalIgnoreCase) || name.Contains("’s", StringComparison.OrdinalIgnoreCase)
            || name.EndsWith("s'", StringComparison.OrdinalIgnoreCase)) {
            return false;
        }
        // The study of the group and the people who study it ("Chiropterology", "Chiropterologist").
        if (name.EndsWith("ology", StringComparison.OrdinalIgnoreCase) || name.EndsWith("ologist", StringComparison.OrdinalIgnoreCase)
            || name.EndsWith("ologists", StringComparison.OrdinalIgnoreCase)) {
            return false;
        }
        var letters = name.Where(char.IsLetter).ToList();
        return letters.Count > 1 && !letters.All(char.IsUpper);
    }

    // Close to an anchor (one edit for a short name, two for a long one) without one being the start
    // of the other: "Chiroptra" is a misspelling of "Chiroptera"; "Megabats" and "Chiropteran" are not.
    internal static bool LooksLikeMisspelling(string candidate, string anchor) {
        if (candidate.Length == 0 || anchor.Length == 0 || candidate == anchor) {
            return false;
        }
        if (candidate.StartsWith(anchor, StringComparison.Ordinal) || anchor.StartsWith(candidate, StringComparison.Ordinal)) {
            return false;
        }
        var shorter = Math.Min(candidate.Length, anchor.Length);
        var allowed = shorter >= 8 ? 2 : shorter >= 5 ? 1 : 0;
        return allowed > 0 && Math.Abs(candidate.Length - anchor.Length) <= allowed && Distance(candidate, anchor) <= allowed;
    }

    private static string Letters(string text) => new(text.Where(char.IsLetter).Select(char.ToLowerInvariant).ToArray());

    // Optimal string alignment distance (Damerau-Levenshtein without repeated edits of a substring).
    private static int Distance(string a, string b) {
        var d = new int[a.Length + 1, b.Length + 1];
        for (var i = 0; i <= a.Length; i++) d[i, 0] = i;
        for (var j = 0; j <= b.Length; j++) d[0, j] = j;
        for (var i = 1; i <= a.Length; i++) {
            for (var j = 1; j <= b.Length; j++) {
                var cost = a[i - 1] == b[j - 1] ? 0 : 1;
                d[i, j] = Math.Min(Math.Min(d[i - 1, j] + 1, d[i, j - 1] + 1), d[i - 1, j - 1] + cost);
                if (i > 1 && j > 1 && a[i - 1] == b[j - 2] && a[i - 2] == b[j - 1]) {
                    d[i, j] = Math.Min(d[i, j], d[i - 2, j - 2] + 1);
                }
            }
        }
        return d[a.Length, b.Length];
    }
}
