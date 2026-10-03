using System;
using System.Collections.Generic;
using System.Linq;
using BeastieBot3.Wikipedia;

namespace BeastieBot3.WikipediaLists;

/// <summary>
/// What the Wikipedia cache knows about English Wikipedia titles, for choosing the article a list
/// line links:
/// <list type="bullet">
/// <item>whether English Wikipedia has a page or a redirect with a title (<see cref="Exists"/>): the
/// title is in the list of every article title (enwiki_dump_titles, from `wikipedia titles-dump`),
/// or the cache has downloaded it. A downloaded disambiguation page, or a downloaded redirect to
/// one, does not count. Any other title counts as having no page, including one the cache knows
/// nothing about;</item>
/// <item>whether a downloaded page is about a taxon in another kingdom (<see cref="IsAboutAnotherKingdom"/>);</item>
/// <item>the title of a name with the bracketed word for its kingdom, such as "Ficus variegata
/// (plant)" (<see cref="QualifiedTitle"/>).</item>
/// </list>
/// Answers are kept for the run.
/// </summary>
internal sealed class EnwikiTitleCheck : IDisposable {
    private readonly WikipediaCacheStore _cache;
    private readonly bool _ownsCache;
    private readonly bool _useTitleList;
    private readonly Dictionary<string, bool> _exists = new(StringComparer.Ordinal);
    private readonly Dictionary<string, bool> _disambiguation = new(StringComparer.Ordinal);
    private readonly Dictionary<(string Title, string Kingdom), bool> _anotherKingdom = new();

    /// <summary>
    /// A check over <paramref name="cache"/>. Without <paramref name="useTitleList"/> it reads only the
    /// pages the cache has downloaded, not the list of every article title.
    /// </summary>
    internal EnwikiTitleCheck(WikipediaCacheStore cache, bool ownsCache, bool useTitleList = true) {
        _cache = cache ?? throw new ArgumentNullException(nameof(cache));
        _ownsCache = ownsCache;
        _useTitleList = useTitleList;
    }

    /// <summary>
    /// A check that opens the Wikipedia cache at <paramref name="databasePath"/> read-only; null when
    /// the file does not exist or cannot be opened.
    /// </summary>
    public static EnwikiTitleCheck? OpenReadOnly(string databasePath) =>
        WikipediaCacheStore.OpenReadOnly(databasePath) is { } cache ? new EnwikiTitleCheck(cache, ownsCache: true) : null;

    /// <summary>
    /// Whether English Wikipedia has a page or a redirect titled <paramref name="title"/> that is not a
    /// disambiguation page or a redirect to one, as far as the cache knows.
    /// </summary>
    public bool Exists(string title) {
        var normalized = WikipediaTitleHelper.Normalize(title);
        if (normalized.Length == 0) {
            return false;
        }
        if (!_exists.TryGetValue(normalized, out var exists)) {
            var page = _cache.GetPageByNormalizedTitle(normalized);
            exists = page?.DownloadStatus == WikiPageDownloadStatus.Cached
                ? !IsDisambiguation(normalized)
                : _useTitleList && _cache.IsInTitleList(normalized);
            _exists[normalized] = exists;
        }
        return exists;
    }

    /// <summary>
    /// Whether the cache has downloaded <paramref name="title"/> as a disambiguation page, or as a
    /// redirect to a downloaded disambiguation page. False for a title the cache has not downloaded.
    /// </summary>
    public bool IsDisambiguation(string title) {
        var normalized = WikipediaTitleHelper.Normalize(title);
        if (normalized.Length == 0) {
            return false;
        }
        if (!_disambiguation.TryGetValue(normalized, out var disambiguation)) {
            var page = Downloaded(normalized);
            if (page is { IsRedirect: true, RedirectTarget: { } target }) {
                page = Downloaded(WikipediaTitleHelper.Normalize(target)) ?? page;
            }
            disambiguation = page?.IsDisambiguation == true;
            _disambiguation[normalized] = disambiguation;
        }
        return disambiguation;
    }

    /// <summary>
    /// Whether the page titled <paramref name="title"/> is about a taxon in a kingdom other than
    /// <paramref name="kingdom"/> (an IUCN kingdom name, such as PLANTAE), by <see cref="WikiPageKingdom"/>:
    /// for a downloaded page or a redirect to one, by the target page's taxobox, title and categories
    /// and by the redirect's title; for a title the cache has not downloaded, by its bracketed word
    /// alone ("Ficus variegata (gastropod)"). False when <paramref name="kingdom"/> is not given.
    /// </summary>
    public bool IsAboutAnotherKingdom(string title, string? kingdom) {
        var normalized = WikipediaTitleHelper.Normalize(title);
        if (normalized.Length == 0 || string.IsNullOrWhiteSpace(kingdom)) {
            return false;
        }
        var key = (normalized, kingdom.Trim().ToUpperInvariant());
        if (!_anotherKingdom.TryGetValue(key, out var another)) {
            var page = Downloaded(normalized);
            if (page is { IsRedirect: true, RedirectTarget: { } target }) {
                page = Downloaded(WikipediaTitleHelper.Normalize(target)) ?? page;
            }
            var evidence = (page is null ? null : _cache.GetPageKingdomEvidence(page.PageRowId))
                ?? new WikiPageKingdomEvidence(normalized, null, null, null, []);
            another = WikiPageKingdom.Conflict(key.Item2, evidence, otherTitle: normalized) is not null;
            _anotherKingdom[key] = another;
        }
        return another;
    }

    /// <summary>
    /// <paramref name="name"/> with the bracketed word for a group in <paramref name="kingdom"/>, such as
    /// "Ficus variegata (plant)" for a plant, when English Wikipedia has that title (<see cref="Exists"/>)
    /// and it is not about another kingdom; null when there is none. When there are several, the
    /// first in <see cref="WikiPageKingdom.QualifiedTitlesFor"/> order: "(plant)" before "(tree)".
    /// </summary>
    public string? QualifiedTitle(string name, string? kingdom) {
        var normalized = WikipediaTitleHelper.Normalize(name);
        if (normalized.Length == 0 || string.IsNullOrWhiteSpace(kingdom)) {
            return null;
        }
        var titles = _cache.FindTitlesWithQualifier(normalized, _useTitleList);
        return WikiPageKingdom.QualifiedTitlesFor(normalized, kingdom, titles)
            .FirstOrDefault(title => Exists(title) && !IsAboutAnotherKingdom(title, kingdom));
    }

    private WikiPageSummary? Downloaded(string normalizedTitle) =>
        _cache.GetPageByNormalizedTitle(normalizedTitle) is { DownloadStatus: WikiPageDownloadStatus.Cached } page ? page : null;

    public void Dispose() {
        if (_ownsCache) {
            _cache.Dispose();
        }
    }
}
