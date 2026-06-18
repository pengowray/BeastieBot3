using System;
using BeastieBot3.CommonNames;
using BeastieBot3.WikipediaLists.Legacy;
using static BeastieBot3.WikipediaLists.ProseFormat;

namespace BeastieBot3.WikipediaLists;

/// <summary>
/// The text + optional links for one section heading: the scientific-name heading line, an optional
/// {{main}} link, the "Members of X are called Y" common-name sentence, and an optional descriptive
/// blurb. Produced by <see cref="HeadingFormatter"/> and consumed by the tree renderer.
/// </summary>
internal readonly record struct HeadingInfo(string Text, string? MainLink, string? CommonNameSentence = null, string? Description = null);

// Builds taxonomy-section headings from a raw taxon name (or a virtual group), resolving the common
// name + wikilink from YAML rules, legacy rules, and the store-backed provider. Extracted from
// WikipediaListGenerator (R2 carve-up); the tree renderer calls in here per node. Holds the same rule
// sources + common-name provider the generator was constructed with. Import with `using static` so the
// pure helpers (IsOtherOrUnknownHeading, FormatVirtualGroupHeading) stay terse at the call sites.
internal sealed class HeadingFormatter {
    private readonly LegacyTaxaRuleList _legacyRules;
    private readonly TaxonRulesService? _taxonRules;
    private readonly StoreBackedCommonNameProvider? _storeBackedProvider;

    public HeadingFormatter(
        LegacyTaxaRuleList legacyRules,
        TaxonRulesService? taxonRules,
        StoreBackedCommonNameProvider? storeBackedProvider) {
        _legacyRules = legacyRules ?? throw new ArgumentNullException(nameof(legacyRules));
        _taxonRules = taxonRules;
        _storeBackedProvider = storeBackedProvider;
    }

    public HeadingInfo FormatHeading(string? raw, string? rank = null, string? kingdom = null) {
        if (string.IsNullOrWhiteSpace(raw)) {
            return new HeadingInfo("Unassigned", null);
        }

        if (IsOtherOrUnknownHeading(raw)) {
            return new HeadingInfo(raw.Trim(), null);
        }

        // Apply title case to the raw taxon name for display
        var displayName = ToTitleCase(raw);

        // --- Heading text is always the scientific name with rank label ---
        var headingText = FormatHeadingText(displayName, rank, showRankLabel: true, isScientificName: true);

        // --- Resolve common name from all sources (for sentence, not heading) ---
        string? commonName = null;
        var yamlRule = _taxonRules?.GetRule(raw);
        var legacyRules = _legacyRules.Get(raw);

        // Priority: YAML CommonPlural > YAML CommonName > Legacy CommonPlural > Legacy CommonName
        if (!string.IsNullOrWhiteSpace(yamlRule?.CommonPlural))
            commonName = yamlRule.CommonPlural;
        else if (!string.IsNullOrWhiteSpace(yamlRule?.CommonName))
            commonName = yamlRule.CommonName;
        else if (!string.IsNullOrWhiteSpace(legacyRules?.CommonPlural))
            commonName = legacyRules.CommonPlural;
        else if (!string.IsNullOrWhiteSpace(legacyRules?.CommonName))
            commonName = legacyRules.CommonName;
        else if (_storeBackedProvider is not null) {
            // Store-backed common names for higher taxa
            var storeName = _storeBackedProvider.GetBestCommonNameByScientificName(raw, kingdom);
            if (!string.IsNullOrWhiteSpace(storeName)) {
                commonName = storeName;
            } else {
                // Fallback: Wikipedia redirect target (e.g., Araneae -> Spider)
                var redirectTitle = _storeBackedProvider.GetWikipediaRedirectTitleByScientificName(raw);
                if (!string.IsNullOrWhiteSpace(redirectTitle) && !redirectTitle.Equals(raw, StringComparison.OrdinalIgnoreCase)) {
                    var cleaned = CommonNameNormalizer.RemoveDisambiguationSuffix(redirectTitle);
                    if (!CommonNameNormalizer.LooksLikeScientificName(cleaned, null, null)) {
                        commonName = cleaned;
                    }
                }
            }
        }

        // --- Resolve wikilink target for the sentence ---
        string? wikilinkTarget = null;
        if (!string.IsNullOrWhiteSpace(yamlRule?.Wikilink))
            wikilinkTarget = yamlRule.Wikilink;
        else if (!string.IsNullOrWhiteSpace(legacyRules?.Wikilink))
            wikilinkTarget = legacyRules.Wikilink;
        else {
            var yamlMainArticle = _taxonRules?.GetMainArticle(raw);
            if (!string.IsNullOrWhiteSpace(yamlMainArticle))
                wikilinkTarget = yamlMainArticle;
            else if (_storeBackedProvider is not null)
                wikilinkTarget = _storeBackedProvider.GetWikipediaArticleTitleByScientificName(raw, kingdom);
        }

        // --- Build common name sentence ---
        var sentence = BuildCommonNameSentence(displayName, rank, commonName, wikilinkTarget);

        // --- Revived comprises/blurb grey-text line (legacy TaxonHeaderBlurb.GrayText) ---
        var description = FormatTaxonDescription(yamlRule);

        return new HeadingInfo(headingText, null, sentence, description);
    }

    public HeadingInfo FormatVirtualGroupHeading(VirtualGroup group) {
        // Prefer the group's own configured common plural/name; otherwise fall back to a common plural
        // from the rule files (e.g. rules-list.txt "Diplopoda plural millipedes") so auto-discovered
        // class groups read "Millipedes" rather than the raw scientific "Diplopoda".
        var displayName = !string.IsNullOrWhiteSpace(group.CommonPlural)
            ? Uppercase(group.CommonPlural)
            : !string.IsNullOrWhiteSpace(group.CommonName)
                ? Uppercase(group.CommonName)
                : ResolveHigherTaxonCommonName(group.Name) ?? group.Name;

        return new HeadingInfo(displayName!, group.MainArticle);
    }

    /// <summary>
    /// Resolves a common (plural) name for a higher taxon (e.g. an invertebrate class) from the rule
    /// sources, capitalized for use as a section heading, or null if none is configured. Used for both
    /// virtual-group headings and the orphan-class headings in the parent lists.
    /// </summary>
    public string? ResolveHigherTaxonCommonName(string? scientificName) {
        if (string.IsNullOrWhiteSpace(scientificName)) {
            return null;
        }
        var yamlRule = _taxonRules?.GetRule(scientificName);
        if (!string.IsNullOrWhiteSpace(yamlRule?.CommonPlural)) return Uppercase(yamlRule.CommonPlural);
        if (!string.IsNullOrWhiteSpace(yamlRule?.CommonName)) return Uppercase(yamlRule.CommonName);
        var legacy = _legacyRules.Get(scientificName);
        if (!string.IsNullOrWhiteSpace(legacy?.CommonPlural)) return Uppercase(legacy.CommonPlural);
        if (!string.IsNullOrWhiteSpace(legacy?.CommonName)) return Uppercase(legacy.CommonName);
        return null;
    }

    /// <summary>
    /// Build the optional descriptive line under a heading from a taxon rule's <c>blurb</c>/<c>comprises</c>.
    /// <c>blurb</c> is emitted as authored (already a sentence, e.g. "Includes tree frogs and allies");
    /// <c>comprises</c> becomes an italic "Comprises X." line. Returns null when neither is authored.
    /// </summary>
    private static string? FormatTaxonDescription(TaxonRule? rule) {
        if (rule is null) return null;
        if (!string.IsNullOrWhiteSpace(rule.Blurb)) {
            return rule.Blurb.Trim();
        }
        if (!string.IsNullOrWhiteSpace(rule.Comprises)) {
            return $"''Comprises {rule.Comprises.Trim()}.''";
        }
        return null;
    }

    /// <summary>
    /// Formats heading text, optionally adding rank label prefix.
    /// </summary>
    private static string FormatHeadingText(string displayName, string? rank, bool showRankLabel, bool isScientificName) {
        if (!showRankLabel || string.IsNullOrWhiteSpace(rank) || !isScientificName) {
            return displayName;
        }

        // Capitalize the rank for display (e.g., "family" -> "Family")
        var capitalizedRank = char.ToUpperInvariant(rank[0]) + rank.Substring(1).ToLowerInvariant();
        return $"{capitalizedRank} {displayName}";
    }

    /// <summary>
    /// Builds a descriptive sentence showing the common name for a taxon.
    /// Example: "Members of the [[Sminthidae]] family are called birch mice."
    /// </summary>
    private static string? BuildCommonNameSentence(
        string scientificName, string? rank, string? commonNameOrPlural, string? wikilinkOverride) {
        if (string.IsNullOrWhiteSpace(commonNameOrPlural)) {
            return null;
        }

        // Build wikilink expression
        string wikilink;
        if (!string.IsNullOrWhiteSpace(wikilinkOverride) &&
            !wikilinkOverride.Equals(scientificName, StringComparison.OrdinalIgnoreCase)) {
            wikilink = $"[[{wikilinkOverride}|{scientificName}]]";
        } else {
            wikilink = $"[[{scientificName}]]";
        }

        // Build sentence with or without rank
        if (!string.IsNullOrWhiteSpace(rank)) {
            var lowerRank = rank.ToLowerInvariant();
            return $"Members of the {wikilink} {lowerRank} are called {commonNameOrPlural}.";
        }

        return $"Members of {wikilink} are called {commonNameOrPlural}.";
    }

    public static bool IsOtherOrUnknownHeading(string raw) {
        var trimmed = raw.Trim();
        return trimmed.StartsWith("Other ", StringComparison.OrdinalIgnoreCase)
            || trimmed.StartsWith("Unknown ", StringComparison.OrdinalIgnoreCase)
            || trimmed.Equals("Other", StringComparison.OrdinalIgnoreCase)
            || trimmed.Equals("Unknown", StringComparison.OrdinalIgnoreCase);
    }
}
