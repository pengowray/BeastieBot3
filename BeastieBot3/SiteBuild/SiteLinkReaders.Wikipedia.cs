using System.Globalization;
using System.Text.Json;
using BeastieBot3.CommonNames;
using BeastieBot3.Shared.SiteData;
using BeastieBot3.Taxonomy;

// English Wikipedia for `site build-db`: each taxon's article title, and the names in its taxobox.

namespace BeastieBot3.SiteBuild;

internal static partial class SiteLinkReaders {
    // ------------------------------------------------------------ English Wikipedia

    public static void ReadWikipedia(string path, IReadOnlyDictionary<long, SiteTaxon> taxa, SiteBuildStats stats,
        CancellationToken cancellationToken) {
        using var connection = OpenReadOnly(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT taxon_identifier, redirect_final_title, normalized_title
            FROM taxon_wiki_matches
            WHERE taxon_source = 'iucn' AND match_status = 'matched'
            """;
        command.CommandTimeout = 0;
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            cancellationToken.ThrowIfCancellationRequested();
            if (!long.TryParse(reader.GetString(0), NumberStyles.None, CultureInfo.InvariantCulture, out var taxonId)
                || !taxa.TryGetValue(taxonId, out var taxon)) {
                continue;
            }
            var title = SiteBuildRules.NullIfBlank(reader.IsDBNull(1) ? null : reader.GetString(1))
                ?? SiteBuildRules.NullIfBlank(reader.IsDBNull(2) ? null : reader.GetString(2));
            if (title is not null) {
                taxon.EnwikiTitle = title;
                stats.EnwikiTitles++;
            }
        }
    }


    /// Synonyms from the taxobox of each taxon's English Wikipedia article: the taxobox's own
    /// scientific name with its authority (Wikipedia can use another name than IUCN: "Nycticeinops
    /// crassulus" for IUCN's Pipistrellus crassulus), and the names in its synonyms parameter
    /// (TaxoboxSynonymsParser). A page about a genus or a higher taxon gives none. When the page is
    /// matched to several taxa, only those whose scientific name is the taxobox's take its names, and
    /// when none is, none does: a split species' page lists the other parts' names as synonyms.
    public static void ReadWikipediaTaxoboxSynonyms(string path, IReadOnlyDictionary<long, SiteTaxon> taxa, SiteBuildStats stats,
        CancellationToken cancellationToken) {
        using var connection = OpenReadOnly(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT m.page_row_id, m.taxon_identifier, t.data_json
            FROM taxon_wiki_matches m
            JOIN wiki_taxobox_data t ON t.page_row_id = m.page_row_id
            WHERE m.taxon_source = 'iucn' AND m.match_status = 'matched'
            ORDER BY m.page_row_id
            """;
        command.CommandTimeout = 0;
        using var reader = command.ExecuteReader();
        long? page = null;
        string? json = null;
        var pageTaxa = new List<SiteTaxon>();
        while (reader.Read()) {
            cancellationToken.ThrowIfCancellationRequested();
            var rowPage = reader.GetInt64(0);
            if (rowPage != page) {
                AddTaxoboxSynonyms(pageTaxa, json, stats);
                page = rowPage;
                json = reader.IsDBNull(2) ? null : reader.GetString(2);
                pageTaxa.Clear();
            }
            if (long.TryParse(reader.GetString(1), NumberStyles.None, CultureInfo.InvariantCulture, out var taxonId)
                && taxa.TryGetValue(taxonId, out var taxon)) {
                pageTaxa.Add(taxon);
            }
        }
        AddTaxoboxSynonyms(pageTaxa, json, stats);
    }

    // "Nanger granti ssp. granti" and "Nanger granti granti" have the same key.
    private static string NameKeyWithoutRank(string name) =>
        SiteNameKey.Fold(string.Join(' ', ScientificNameCheck.WithoutRankMarkers(name.Split(' ', StringSplitOptions.RemoveEmptyEntries))));

    internal static void AddTaxoboxSynonyms(List<SiteTaxon> pageTaxa, string? json, SiteBuildStats stats) {
        if (pageTaxa.Count == 0 || string.IsNullOrWhiteSpace(json)) {
            return;
        }
        Dictionary<string, string>? fields;
        try {
            fields = JsonSerializer.Deserialize<Dictionary<string, string>>(json);
        } catch (JsonException) {
            return;
        }
        if (fields is null || WikipediaPageMatch.IsGenusOrHigherPage(fields)) {
            return;
        }
        var subject = WikipediaPageMatch.SubjectName(fields);
        var receivers = pageTaxa;
        if (pageTaxa.Count > 1) {
            var subjectKey = subject is null ? null : NameKeyWithoutRank(subject);
            receivers = pageTaxa.Where(t => subjectKey is not null
                && NameKeyWithoutRank(t.ScientificName) == subjectKey).ToList();
            if (receivers.Count == 0) {
                stats.WikipediaTaxoboxPagesShared++;
                return;
            }
        }
        var synonyms = new List<SiteSynonym>();
        if (subject is not null) {
            // The author of the name that became the subject: a trinomial's, else a binomial's.
            var parameters = subject.Split(' ').Length > 2
                ? new[] { "trinomial_authority", "authority" }
                : new[] { "binomial_authority", "authority" };
            var authority = parameters
                .Select(p => fields.TryGetValue(p, out var value) ? TaxoboxSynonymsParser.CleanAuthority(value) : null)
                .FirstOrDefault(a => a is not null);
            synonyms.Add(new SiteSynonym(subject, authority));
        }
        if (fields.TryGetValue("synonyms", out var synonymsText)) {
            synonyms.AddRange(TaxoboxSynonymsParser.Parse(synonymsText).Select(s => new SiteSynonym(s.Name, s.Authority)));
        }
        foreach (var taxon in receivers) {
            taxon.WikipediaSynonyms.AddRange(synonyms);
            stats.WikipediaTaxoboxSynonyms += synonyms.Count;
        }
    }
}
