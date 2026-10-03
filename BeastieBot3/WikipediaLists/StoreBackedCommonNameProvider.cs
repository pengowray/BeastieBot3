using System;
using System.Collections.Generic;
using System.IO;
using BeastieBot3;
using BeastieBot3.CommonNames;
using BeastieBot3.Taxonomy;
using BeastieBot3.Wikipedia;
using Microsoft.Data.Sqlite;

// CommonNameStore-backed provider for Wikipedia list generation: finds a taxon's names in the
// store and has CommonNameChooser pick and capitalise the best one (source priority, ambiguous
// names skipped, caps.txt rules). Also gives the Wikipedia article each taxon links to (from the
// store, checked against the Wikipedia cache) and redirect targets from the Wikipedia cache.

namespace BeastieBot3.WikipediaLists;

/// <summary>
/// Common name provider backed by the CommonNameStore.
/// Uses pre-aggregated common names with source priority and ambiguity detection.
/// </summary>
internal sealed class StoreBackedCommonNameProvider : IDisposable {
    private readonly CommonNameStore _store;
    private readonly bool _ownsStore;
    private readonly WikipediaCacheStore? _wikiCache;
    private readonly bool _ownsWikiCache;
    private readonly bool _allowAmbiguous;
    // Whether English Wikipedia has a page or a redirect with a title; null without a Wikipedia cache.
    private readonly Func<string, bool>? _titleExists;
    private readonly EnwikiTitleCheck? _ownedTitleCheck;

    // Per-run memoization. Generation resolves the same taxon's id/name/article several times per
    // record (Style B/C sort + line formatting + parent-species link) and again for the same taxon
    // across multiple lists; caching makes each unique lookup happen once. The provider is created
    // once per generate-lists invocation, so this spans the whole run. Single-threaded — no locking.
    private readonly Dictionary<long, long?> _storeTaxonIdCache = new();
    private readonly Dictionary<long, string?> _commonNameCache = new();
    private readonly Dictionary<long, string?> _wikiArticleCache = new();
    private readonly Dictionary<string, string?> _articleByScientificCache = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Creates a provider that owns and will dispose the store and the Wikipedia cache, which it
    /// opens read-only.
    /// </summary>
    public StoreBackedCommonNameProvider(string commonNameDbPath, string? wikipediaCachePath = null, bool allowAmbiguous = false) {
        _store = CommonNameStore.OpenReadOnly(commonNameDbPath);
        _ownsStore = true;
        _allowAmbiguous = allowAmbiguous;
        Chooser = CommonNameChooser.ForStore(_store, allowAmbiguous: allowAmbiguous);

        _wikiCache = string.IsNullOrWhiteSpace(wikipediaCachePath) ? null : WikipediaCacheStore.OpenReadOnly(wikipediaCachePath);
        _ownsWikiCache = _wikiCache is not null;
        _ownedTitleCheck = _wikiCache is null ? null : EnwikiTitleCheck.OpenReadOnly(wikipediaCachePath!);
        _titleExists = _ownedTitleCheck is null ? null : _ownedTitleCheck.Exists;
    }

    /// <summary>
    /// Creates a provider using an existing store (caller retains ownership).
    /// <paramref name="titleExists"/> says whether English Wikipedia has a page or a redirect with a
    /// title; without it, the provider asks <paramref name="wikiCache"/> for pages it has
    /// downloaded, and without either it cannot tell (see <see cref="GetWikipediaArticleTitle"/>).
    /// </summary>
    public StoreBackedCommonNameProvider(CommonNameStore store, WikipediaCacheStore? wikiCache = null, bool allowAmbiguous = false,
        Func<string, bool>? titleExists = null) {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _ownsStore = false;
        _allowAmbiguous = allowAmbiguous;
        Chooser = CommonNameChooser.ForStore(_store, allowAmbiguous: allowAmbiguous);
        _wikiCache = wikiCache;
        _ownsWikiCache = false;
        _titleExists = titleExists ?? (wikiCache is null ? null : title => EnwikiTitleCheck.IsDownloaded(wikiCache, title));
    }

    /// <summary>
    /// How many English common names the store gives to more than one taxon; each is skipped for
    /// every taxon but the one that keeps it (<see cref="AmbiguousNames"/>). 0 when the provider was
    /// created with allowAmbiguous. The store computes the verdicts on first use and caches them,
    /// so reading this before generation costs nothing extra.
    /// </summary>
    public int AmbiguousNameCount => _allowAmbiguous ? 0 : _store.GetAmbiguousNames("en").Count;

    /// <summary>
    /// The chooser this provider picks store names with; it has no rules-list.txt
    /// (<see cref="CommonNameChooser.WithRules"/> adds one).
    /// </summary>
    public CommonNameChooser Chooser { get; }

    /// <summary>
    /// Get the best common name for a species record.
    /// Returns null if no suitable name found.
    /// </summary>
    public string? GetBestCommonName(IucnSpeciesRecord record) {
        if (record is null) {
            return null;
        }
        if (_commonNameCache.TryGetValue(record.TaxonId, out var cached)) {
            return cached;
        }

        // Look up by taxon_id (which maps to IUCN sis_id as primary_source_id)
        var taxonId = FindTaxonId(record);
        var binomial = !string.IsNullOrWhiteSpace(record.GenusName) && !string.IsNullOrWhiteSpace(record.SpeciesName)
            ? $"{record.GenusName} {record.SpeciesName}"
            : record.ScientificNameTaxonomy;
        var resolved = taxonId.HasValue ? BestStoreName(taxonId.Value, binomial) : null;

        _commonNameCache[record.TaxonId] = resolved;
        return resolved;
    }

    /// <summary>
    /// Get the best common name for a taxon by scientific name (supports higher taxa).
    /// </summary>
    public string? GetBestCommonNameByScientificName(string scientificName, string? kingdom = null) {
        if (string.IsNullOrWhiteSpace(scientificName)) {
            return null;
        }

        var taxonId = FindTaxonIdByScientificName(scientificName, kingdom);
        return taxonId.HasValue ? BestStoreName(taxonId.Value, scientificName) : null;
    }

    // The store's best English name for a store taxon, repaired and capitalised; null when it has
    // none that is usable and not ambiguous for it. scientificName lets the chooser skip a name
    // that is the scientific name with a subgenus.
    private string? BestStoreName(long storeTaxonId, string? scientificName) =>
        Chooser.FromStore(storeTaxonId, CommonNameStore.ToCandidates(_store.GetCommonNamesForTaxon(storeTaxonId, "en")),
            scientificName)?.DisplayName;

    /// <summary>
    /// The Wikipedia article a species record links to, or null:
    /// <list type="number">
    /// <item>the page its wikipedia_title or wikipedia_taxobox name came from
    /// (<see cref="CommonNameStore.GetWikipediaNamePage"/>);</item>
    /// <item>else, when the taxon is matched to a page (<see cref="CommonNameStore.GetMatchedWikipediaPage"/>),
    /// its own scientific name if English Wikipedia has a page or a redirect with that name, so a
    /// redirect is linked as it is: [[Leucoraja wallacei]], a redirect to the species list of the
    /// genus article, and not [[Leucoraja]];</item>
    /// <item>else the matched page: "Crenimugil buchanani" for Moolgarda buchanani, whose own name
    /// has no page. Without a Wikipedia cache the provider cannot tell, and links the matched page.</item>
    /// </list>
    /// A subspecies or variety is checked by its name without a rank marker, then by its name as
    /// IUCN writes it.
    /// </summary>
    public string? GetWikipediaArticleTitle(IucnSpeciesRecord record) {
        if (record is null) {
            return null;
        }
        if (_wikiArticleCache.TryGetValue(record.TaxonId, out var cached)) {
            return cached;
        }

        var taxonId = FindTaxonId(record);
        var resolved = taxonId.HasValue ? ArticleTitle(taxonId.Value, OwnNames(record)) : null;
        _wikiArticleCache[record.TaxonId] = resolved;
        return resolved;
    }

    /// <summary>
    /// The Wikipedia article for a taxon found by its scientific name (supports higher taxa), chosen
    /// as in <see cref="GetWikipediaArticleTitle(IucnSpeciesRecord)"/> with
    /// <paramref name="scientificName"/> as the taxon's own name.
    /// </summary>
    public string? GetWikipediaArticleTitleByScientificName(string scientificName, string? kingdom = null) {
        if (string.IsNullOrWhiteSpace(scientificName)) {
            return null;
        }
        var cacheKey = $"{scientificName}{kingdom}";
        if (_articleByScientificCache.TryGetValue(cacheKey, out var cached)) {
            return cached;
        }

        var taxonId = FindTaxonIdByScientificName(scientificName, kingdom);
        var resolved = taxonId.HasValue ? ArticleTitle(taxonId.Value, [scientificName]) : null;
        _articleByScientificCache[cacheKey] = resolved;
        return resolved;
    }

    // Steps 1 to 3 of GetWikipediaArticleTitle for a store taxon whose own names, as the lists
    // would link them, are ownNames.
    private string? ArticleTitle(long storeTaxonId, IEnumerable<string?> ownNames) {
        if (_store.GetWikipediaNamePage(storeTaxonId, "en") is { } namePage) {
            return namePage;
        }
        var matched = _store.GetMatchedWikipediaPage(storeTaxonId);
        if (matched is null || _titleExists is null) {
            return matched;
        }
        foreach (var own in ownNames) {
            if (!string.IsNullOrWhiteSpace(own) && _titleExists(own)) {
                return own;
            }
        }
        return matched;
    }

    // The names a record's line links when it has no article title: the scientific name the
    // lists show (SpeciesLineFormatter.ResolveScientificName), and for a subspecies or variety
    // first the name without a rank marker ("Panthera leo persica").
    private static IEnumerable<string?> OwnNames(IucnSpeciesRecord record) {
        if (!string.IsNullOrWhiteSpace(record.InfraName)) {
            yield return ScientificNameHelper.BuildFromParts(record.GenusName, record.SpeciesName, record.InfraName);
        }
        yield return SpeciesLineFormatter.ResolveScientificName(record);
    }

    /// <summary>
    /// Get the redirect target title for a scientific name using the Wikipedia cache.
    /// Useful for higher taxa where the scientific name redirects to a common-name article.
    /// </summary>
    public string? GetWikipediaRedirectTitleByScientificName(string scientificName) {
        if (_wikiCache is null || string.IsNullOrWhiteSpace(scientificName)) {
            return null;
        }

        var normalized = WikipediaTitleHelper.Normalize(scientificName);
        if (string.IsNullOrWhiteSpace(normalized)) {
            return null;
        }

        var summary = _wikiCache.GetPageByNormalizedTitle(normalized);
        if (summary is null) {
            return null;
        }

        if (!summary.IsRedirect || string.IsNullOrWhiteSpace(summary.RedirectTarget)) {
            return null;
        }

        return summary.RedirectTarget;
    }

    private long? FindTaxonId(IucnSpeciesRecord record) {
        if (_storeTaxonIdCache.TryGetValue(record.TaxonId, out var cached)) {
            return cached;
        }
        var resolved = FindTaxonIdUncached(record);
        _storeTaxonIdCache[record.TaxonId] = resolved;
        return resolved;
    }

    private long? FindTaxonIdUncached(IucnSpeciesRecord record) {
        // Try by IUCN taxon_id first (most reliable)
        var taxonId = _store.FindTaxonBySourceId("iucn", record.TaxonId.ToString());
        if (taxonId.HasValue) {
            return taxonId;
        }

        // Fall back to scientific name lookup
        var scientificName = record.ScientificNameTaxonomy 
            ?? record.ScientificNameAssessments 
            ?? ScientificNameHelper.BuildFromParts(record.GenusName, record.SpeciesName, record.InfraName);
        
        if (!string.IsNullOrWhiteSpace(scientificName)) {
            return _store.FindTaxonByScientificName(scientificName);
        }

        return null;
    }

    private long? FindTaxonIdByScientificName(string scientificName, string? kingdom) {
        // Prefer kingdom-filtered lookup when provided
        var taxonId = _store.FindTaxonByScientificName(scientificName, kingdom);
        if (taxonId.HasValue) {
            return taxonId;
        }

        // Fall back to kingdom-agnostic lookup
        return _store.FindTaxonByScientificName(scientificName);
    }

    public void Dispose() {
        _ownedTitleCheck?.Dispose();
        if (_ownsWikiCache) {
            _wikiCache?.Dispose();
        }
        if (_ownsStore) {
            _store.Dispose();
        }
    }
}

/// <summary>
/// Whether English Wikipedia has a page or a redirect with a title, as far as the Wikipedia cache
/// knows: the title is in the imported list of every article title (enwiki_dump_titles, from
/// `wikipedia titles-dump`), or the cache has downloaded it as a page or a redirect. Any other title
/// counts as having no page, including one the cache knows nothing about. Answers are kept for
/// the run.
/// </summary>
internal sealed class EnwikiTitleCheck : IDisposable {
    private readonly SqliteConnection _connection;
    private readonly bool _ownsConnection;
    private readonly bool _hasTitleList;
    private readonly Dictionary<string, bool> _answers = new(StringComparer.Ordinal);

    /// <summary>A check over <paramref name="connection"/>, an open connection to a Wikipedia cache.</summary>
    internal EnwikiTitleCheck(SqliteConnection connection, bool ownsConnection) {
        _connection = connection;
        _ownsConnection = ownsConnection;
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'enwiki_dump_titles'";
        _hasTitleList = command.ExecuteScalar() is not null;
    }

    /// <summary>
    /// A check that opens the Wikipedia cache at <paramref name="databasePath"/> read-only; null when
    /// the file does not exist or cannot be opened.
    /// </summary>
    public static EnwikiTitleCheck? OpenReadOnly(string databasePath) {
        if (!File.Exists(databasePath)) {
            return null;
        }
        var connection = new SqliteConnection(new SqliteConnectionStringBuilder {
            DataSource = databasePath,
            Mode = SqliteOpenMode.ReadOnly,
        }.ConnectionString);
        try {
            connection.Open();
            return new EnwikiTitleCheck(connection, ownsConnection: true);
        } catch (SqliteException) {
            connection.Dispose();
            return null;
        }
    }

    /// <summary>Whether English Wikipedia has a page or a redirect titled <paramref name="title"/>.</summary>
    public bool Exists(string title) {
        var normalized = WikipediaTitleHelper.Normalize(title);
        if (normalized.Length == 0) {
            return false;
        }
        if (!_answers.TryGetValue(normalized, out var exists)) {
            _answers[normalized] = exists = IsDownloaded(normalized) || IsInTitleList(normalized);
        }
        return exists;
    }

    /// <summary>
    /// Whether <paramref name="cache"/> has downloaded <paramref name="title"/> as a page or a
    /// redirect. The test for a provider given only the cache, which cannot read the title list.
    /// </summary>
    public static bool IsDownloaded(WikipediaCacheStore cache, string title) {
        var normalized = WikipediaTitleHelper.Normalize(title);
        return normalized.Length > 0
            && cache.GetPageByNormalizedTitle(normalized)?.DownloadStatus == WikiPageDownloadStatus.Cached;
    }

    private bool IsDownloaded(string normalizedTitle) {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT 1 FROM wiki_pages WHERE normalized_title = @title AND download_status = @cached";
        command.Parameters.AddWithValue("@title", normalizedTitle);
        command.Parameters.AddWithValue("@cached", WikiPageDownloadStatus.Cached);
        return command.ExecuteScalar() is not null;
    }

    private bool IsInTitleList(string normalizedTitle) {
        if (!_hasTitleList) {
            return false;
        }
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT 1 FROM enwiki_dump_titles WHERE title = @title";
        command.Parameters.AddWithValue("@title", normalizedTitle);
        return command.ExecuteScalar() is not null;
    }

    public void Dispose() {
        if (_ownsConnection) {
            _connection.Dispose();
        }
    }
}
