using System.Text.RegularExpressions;

// The IUCN status in the taxobox of each taxon's English Wikipedia article, for the species page,
// which compares it with the latest assessment (enwiki_taxobox_status). Read from the Wikipedia
// cache's copy of the article, and only when the taxobox is about the taxon: its genus and species
// (and subspecies or variety), or its taxon, are the taxon's scientific name.

namespace BeastieBot3.SiteBuild;

internal sealed record SiteTaxoboxStatus(long TaxonId, string? Status, string? StatusSystem, long? RefAssessmentId, long? RevisionId, string Downloaded);

internal static partial class SiteTaxoboxStatusReader {
    public static List<SiteTaxoboxStatus> Read(string wikipediaCache, IEnumerable<SiteTaxon> taxa, CancellationToken ct) {
        var rows = new List<SiteTaxoboxStatus>();
        using var cache = global::BeastieBot3.Wikipedia.WikipediaCacheStore.OpenReadOnly(wikipediaCache);
        if (cache is null) {
            return rows;
        }
        foreach (var taxon in taxa) {
            ct.ThrowIfCancellationRequested();
            if (taxon.EnwikiTitle is not { } title
                || cache.ResolveDownloadedArticle(title, readTaxobox: false) is not { } article
                || cache.GetTaxoboxFields(article.PageRowId) is not { } fields
                || Subject(fields) is not { } subject
                || !SameTaxon(subject, taxon.ScientificName)
                || cache.ReadPageText(article.PageRowId) is not { DownloadedAt: { } downloaded } page) {
                continue;
            }
            var (status, system) = Status(fields);
            var refId = status is null ? null : RefAssessmentId(fields.GetValueOrDefault("status_ref"), page.Wikitext);
            rows.Add(new SiteTaxoboxStatus(taxon.TaxonId, status, system, refId, page.RevisionId, downloaded.ToString("yyyy-MM-dd")));
        }
        return rows;
    }

    /// The taxon a taxobox is about: "Genus species" (with its subspecies or variety), else its taxon.
    internal static string? Subject(IReadOnlyDictionary<string, string> fields) {
        var genus = SiteLadders.Plain(fields.GetValueOrDefault("genus"));
        var species = SiteLadders.Plain(fields.GetValueOrDefault("species"));
        if (genus is { Length: > 0 } && species is { Length: > 0 }) {
            var infra = SiteLadders.Plain(fields.GetValueOrDefault("subspecies")) is { Length: > 0 } ssp ? ssp
                : SiteLadders.Plain(fields.GetValueOrDefault("variety")) is { Length: > 0 } var ? var : null;
            return infra is null ? $"{genus} {species}" : $"{genus} {species} {infra}";
        }
        return SiteLadders.Plain(fields.GetValueOrDefault("taxon")) is { Length: > 0 } taxon ? taxon : null;
    }

    /// Whether a taxobox's subject is the taxon: the same words, leaving out IUCN's rank markers.
    internal static bool SameTaxon(string subject, string scientificName) =>
        string.Equals(Words(subject), Words(scientificName), StringComparison.OrdinalIgnoreCase);

    private static string Words(string name) => string.Join(' ', name.Split(' ', StringSplitOptions.RemoveEmptyEntries)
        .Where(w => w is not ("ssp." or "subsp." or "var." or "×")));

    /// The taxobox's status and status_system when the system is IUCN's (IUCN3.1 or IUCN2.3);
    /// (null, null) when it has no IUCN status.
    internal static (string? Status, string? System) Status(IReadOnlyDictionary<string, string> fields) {
        var system = SiteLadders.Plain(fields.GetValueOrDefault("status_system"))?.Replace(" ", "").ToUpperInvariant();
        var status = SiteLadders.Plain(fields.GetValueOrDefault("status"))?.Trim();
        return system is "IUCN3.1" or "IUCN2.3" && status is { Length: > 0 } ? (status, system) : (null, null);
    }

    /// The assessment id the status reference cites ("e.T15951A259030422", a DOI or a Red List URL),
    /// looking up a reused named reference (<ref name=IUCN/>) in the article; null when it names none.
    internal static long? RefAssessmentId(string? statusRef, string wikitext) {
        if (string.IsNullOrWhiteSpace(statusRef)) {
            return null;
        }
        if (AssessmentId(statusRef) is { } id) {
            return id;
        }
        if (ReusedRefRegex().Match(statusRef) is { Success: true } reused) {
            var name = Regex.Escape(reused.Groups["name"].Value.Trim());
            var definition = Regex.Match(wikitext, $@"<ref\s+name\s*=\s*[""']?{name}[""']?\s*>(?<body>.*?)</ref>",
                RegexOptions.Singleline | RegexOptions.IgnoreCase);
            return definition.Success ? AssessmentId(definition.Groups["body"].Value) : null;
        }
        return null;
    }

    private static long? AssessmentId(string text) {
        var m = TaxonAssessmentRegex().Match(text);
        if (!m.Success) {
            m = RedListUrlRegex().Match(text);
        }
        return m.Success && long.TryParse(m.Groups["a"].Value, out var id) ? id : null;
    }

    [GeneratedRegex(@"\bT(?<t>\d+)A(?<a>\d+)\b")]
    private static partial Regex TaxonAssessmentRegex();

    [GeneratedRegex(@"iucnredlist\.org/(?:species|details)/(?<t>\d+)/(?<a>\d+)", RegexOptions.IgnoreCase)]
    private static partial Regex RedListUrlRegex();

    [GeneratedRegex(@"<ref\s+name\s*=\s*[""']?(?<name>[^""'/>]+?)[""']?\s*/>", RegexOptions.IgnoreCase)]
    private static partial Regex ReusedRefRegex();
}
