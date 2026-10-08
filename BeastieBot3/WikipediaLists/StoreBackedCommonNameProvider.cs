using System;
using System.Collections.Generic;
using System.Linq;
using BeastieBot3;
using BeastieBot3.CommonNames;
using BeastieBot3.Taxonomy;
using BeastieBot3.Wikipedia;

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
    // Whether English Wikipedia has a page or a redirect with a title, not counting a disambiguation
    // page; null without a Wikipedia cache.
    private readonly Func<string, bool>? _titleExists;
    // Disambiguation pages, pages about another kingdom and titles with a kingdom word; null without
    // a Wikipedia cache, and for a provider given only titleExists.
    private readonly EnwikiTitleCheck? _titleCheck;

    // Per-run memoization. Generation resolves the same taxon's id/name/article several times per
    // record (Style B/C sort + line formatting + parent-species link) and again for the same taxon
    // across multiple lists; caching makes each unique lookup happen once. The provider is created
    // once per generate-lists invocation, so this spans the whole run. Single-threaded — no locking.
    private readonly Dictionary<long, long?> _storeTaxonIdCache = new();
    private readonly Dictionary<long, string?> _commonNameCache = new();
    private readonly Dictionary<long, string?> _wikiArticleCache = new();
    private readonly Dictionary<string, string?> _articleByScientificCache = new(StringComparer.OrdinalIgnoreCase);
    // The words of the store's English names, read on the first redirect title that needs them.
    private NameWordSets? _nameWords;

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
        _titleCheck = _wikiCache is null ? null : new EnwikiTitleCheck(_wikiCache, ownsCache: false);
        _titleExists = _titleCheck is null ? null : _titleCheck.Exists;
    }

    /// <summary>
    /// Creates a provider using an existing store (caller retains ownership).
    /// <paramref name="titleCheck"/> answers what the lists need to know about English Wikipedia
    /// titles. <paramref name="titleExists"/> says only whether English Wikipedia has a page or a
    /// redirect with a title. Without either, the provider asks <paramref name="wikiCache"/> about the
    /// pages it has downloaded, and without any of them it cannot tell (see
    /// <see cref="GetWikipediaArticleTitle"/>).
    /// </summary>
    public StoreBackedCommonNameProvider(CommonNameStore store, WikipediaCacheStore? wikiCache = null, bool allowAmbiguous = false,
        Func<string, bool>? titleExists = null, EnwikiTitleCheck? titleCheck = null) {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _ownsStore = false;
        _allowAmbiguous = allowAmbiguous;
        Chooser = CommonNameChooser.ForStore(_store, allowAmbiguous: allowAmbiguous);
        _wikiCache = wikiCache;
        _ownsWikiCache = false;
        _titleCheck = titleCheck
            ?? (titleExists is null && wikiCache is not null ? new EnwikiTitleCheck(wikiCache, ownsCache: false, useTitleList: false) : null);
        _titleExists = titleExists ?? (_titleCheck is null ? null : _titleCheck.Exists);
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
    /// has no page. Without a Wikipedia cache the provider cannot tell, and links the matched page;</item>
    /// <item>else, when the own name is a disambiguation page, the own name with the bracketed word
    /// for the taxon's kingdom, if English Wikipedia has that title: "Ficus variegata (plant)".</item>
    /// </list>
    /// An own name that is a disambiguation page or a redirect to one is never linked, and no page is
    /// linked that is about a taxon in another kingdom (<see cref="WikiPageKingdom"/>): the plant Ficus
    /// variegata is matched to "Ficus variegata (gastropod)", and "Ficus variegata" is a
    /// disambiguation page. A subspecies or variety is checked by its name without a rank marker,
    /// then by its name as IUCN writes it.
    /// </summary>
    public string? GetWikipediaArticleTitle(IucnSpeciesRecord record) {
        if (record is null) {
            return null;
        }
        if (_wikiArticleCache.TryGetValue(record.TaxonId, out var cached)) {
            return cached;
        }

        var taxonId = FindTaxonId(record);
        var resolved = taxonId.HasValue ? ArticleTitle(taxonId.Value, OwnNames(record), record.KingdomName) : null;
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
        var resolved = taxonId.HasValue ? ArticleTitle(taxonId.Value, [scientificName], kingdom) : null;
        _articleByScientificCache[cacheKey] = resolved;
        return resolved;
    }

    // Steps 1 to 4 of GetWikipediaArticleTitle for a store taxon in kingdom (null when not known)
    // whose own names, as the lists would link them, are ownNames.
    private string? ArticleTitle(long storeTaxonId, IReadOnlyList<string?> ownNames, string? kingdom) {
        if (_store.GetWikipediaNamePage(storeTaxonId, "en") is { } namePage && !AboutAnotherKingdom(namePage, kingdom)) {
            return namePage;
        }
        var matched = _store.GetMatchedWikipediaPage(storeTaxonId);
        if (_titleExists is null) {
            return matched;
        }
        var ownNameIsDisambiguation = false;
        foreach (var own in ownNames) {
            if (string.IsNullOrWhiteSpace(own)) {
                continue;
            }
            if (_titleCheck?.IsDisambiguation(own) == true) {
                ownNameIsDisambiguation = true;
            }
            else if (matched is not null && _titleExists(own) && !AboutAnotherKingdom(own, kingdom)) {
                return own;
            }
        }
        if (matched is not null && !AboutAnotherKingdom(matched, kingdom)) {
            return matched;
        }
        if (ownNameIsDisambiguation && _titleCheck is not null) {
            foreach (var own in ownNames) {
                if (!string.IsNullOrWhiteSpace(own) && _titleCheck.QualifiedTitle(own, kingdom) is { } qualified) {
                    return qualified;
                }
            }
        }
        return null;
    }

    private bool AboutAnotherKingdom(string title, string? kingdom) =>
        _titleCheck?.IsAboutAnotherKingdom(title, kingdom) == true;

    // The names a record's line links when it has no article title: the scientific name the
    // lists show (SpeciesLineFormatter.ResolveScientificName), and for a subspecies or variety
    // first the name without a rank marker ("Panthera leo persica").
    private static IReadOnlyList<string?> OwnNames(IucnSpeciesRecord record) {
        var names = new List<string?>(2);
        if (!string.IsNullOrWhiteSpace(record.InfraName)) {
            names.Add(ScientificNameHelper.BuildFromParts(record.GenusName, record.SpeciesName, record.InfraName));
        }
        names.Add(SpeciesLineFormatter.ResolveScientificName(record));
        return names;
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

        // A downloaded redirect target gives no name when it has no taxobox (genus Thera redirects to
        // "Santorini", genus Athene to "Athena"), or when its taxobox is another taxon and that taxon
        // is a species of this genus (genus Ashbyia redirects to "Gibberbird", the article of its one
        // species, Ashbyia lovensis). A title that is a scientific name gives none either
        // (IsScientificTitle). Other targets keep the name as before: Araneae -> "Spider" (taxobox
        // Araneae), Cetartiodactyla -> "Even-toed ungulate" (taxobox Artiodactyla), a monotypic
        // family -> its species' article (Pedionomidae -> "Plains-wanderer"), and a target that is
        // not downloaded.
        var article = _wikiCache.ResolveDownloadedArticle(normalized);
        if (article is not null) {
            if (article.TaxoboxName is not { } taxobox) {
                return null;
            }
            if (!article.TaxoboxIs(scientificName) && taxobox.StartsWith(scientificName.Trim() + " ", StringComparison.OrdinalIgnoreCase)) {
                return null;
            }
        }
        if (IsScientificTitle(scientificName, summary.RedirectTarget, article, () => _nameWords ??= _store.LoadNameWordSets())) {
            return null;
        }

        return summary.RedirectTarget;
    }

    // One-word names with these endings are scientific names: family, subfamily, superfamily,
    // orders (-iformes, -ida, botanical -ales), and the botanical family and subfamily.
    private static readonly string[] RankEndings = ["idae", "inae", "oidea", "oideae", "aceae", "iformes", "ida", "ales"];

    // A group whose name has one of these endings ranks above genus: the above, tribes (-ini, -eae)
    // and botanical orders (-ales).
    private static readonly string[] AboveGenusEndings = ["idae", "inae", "ini", "oidea", "eae", "ales", "iformes"];

    /// <summary>
    /// Whether a redirect target's title, without its bracketed word, is a scientific name rather
    /// than the group's English name:
    /// <list type="bullet">
    /// <item>the group's own name ("Contia (snake)" for genus Contia);</item>
    /// <item>one word with the ending of a family, subfamily, superfamily or order
    /// ("Pseudomyrmecinae" for tribe Pseudomyrmecini, "Stylephoridae" for order Stylephoriformes,
    /// "Sepiida" for superfamily Sepioidea, "Amylocorticiales" for family Amylocorticiaceae);</item>
    /// <item>the taxon in the article's taxobox, or the genus of the species in it: "Paspalum" for
    /// genus Thrasya, "Drepana (moth)" for genus Watsonalla, "Komarekiona" for family
    /// Komarekionidae, whose article's taxobox is Komarekiona eatoni. For a group above genus such
    /// a title is still an English name when English common names use it as a word
    /// (<see cref="NameWordSets.IsEnglishRatherThanScientific"/>): tribe Gorillini, "Gorilla";
    /// subfamily Polyborinae, "Caracara (subfamily)". For a genus it never is: "Paspalum" is used
    /// as a word in English names, but genus Thrasya is not Paspalum.</item>
    /// </list>
    /// </summary>
    internal static bool IsScientificTitle(string groupName, string redirectTarget, WikiGroupArticle? article, Func<NameWordSets> nameWords) {
        var group = groupName.Trim();
        var title = CommonNameNormalizer.RemoveDisambiguationSuffix(redirectTarget).Trim();
        if (title.Equals(group, StringComparison.OrdinalIgnoreCase)) {
            return true;
        }
        if (!title.Contains(' ') && HasEnding(title, RankEndings)) {
            return true;
        }
        if (article is null || !(article.TaxoboxIs(title) || article.TaxoboxIsSpeciesOf(title))) {
            return false;
        }
        return !(HasEnding(group, AboveGenusEndings) && nameWords().IsEnglishRatherThanScientific(title.ToLowerInvariant()));
    }

    private static bool HasEnding(string name, string[] endings) =>
        endings.Any(e => name.EndsWith(e, StringComparison.OrdinalIgnoreCase));

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
        if (_ownsWikiCache) {
            _wikiCache?.Dispose();
        }
        if (_ownsStore) {
            _store.Dispose();
        }
    }
}
