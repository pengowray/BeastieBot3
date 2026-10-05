using System;
using System.Text;
using BeastieBot3.CommonNames;
using BeastieBot3.Iucn;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Taxonomy;
using BeastieBot3.WikipediaLists.Legacy;
using static BeastieBot3.WikipediaLists.ProseFormat;
using static BeastieBot3.WikipediaLists.RecordClassification;

namespace BeastieBot3.WikipediaLists;

// Renders a single species/infraspecific record to one wikitext bullet line, in whichever listing
// style the display preferences select (Style A scientific-focus, Style B common-focus, Style C
// common-only). Owns the name resolution the line needs: common name, Wikipedia article title,
// the parent species' article and the scientific name. The line itself is rendered by
// SpeciesListLine in BeastieBot3.Shared, which the public site uses too; ToEntry builds its input.
// Extracted from WikipediaListGenerator (R2 carve-up); the generator builds the taxonomy tree and
// calls in here per leaf record. Holds the same common-name providers the generator was constructed with.
internal sealed class SpeciesLineFormatter {
    private readonly LegacyTaxaRuleList _legacyRules;
    private readonly CommonNameProvider? _commonNameProvider;
    private readonly StoreBackedCommonNameProvider? _storeBackedProvider;

    public SpeciesLineFormatter(
        LegacyTaxaRuleList legacyRules,
        StoreBackedCommonNameProvider? storeBackedProvider,
        CommonNameProvider? commonNameProvider) {
        _legacyRules = legacyRules ?? throw new ArgumentNullException(nameof(legacyRules));
        _storeBackedProvider = storeBackedProvider;
        _commonNameProvider = commonNameProvider;
    }

    public string FormatSpeciesLine(IucnSpeciesRecord record, DisplayPreferences display, string? listStatusContext, OtherBucketContext? otherContext = null) {
        var builder = new StringBuilder(SpeciesListLine.Format(ToEntry(record), ToLineOptions(display, listStatusContext)));
        AppendAnnotations(builder, record, otherContext);
        return builder.ToString();
    }

    public string FormatSubspeciesLine(IucnSpeciesRecord record, DisplayPreferences display, string? statusContext, OtherBucketContext? otherContext = null) {
        // Indented subspecies line
        var line = FormatSpeciesLine(record, display, statusContext, otherContext);
        // Add extra indentation (** instead of *)
        if (line.StartsWith("* ")) {
            return "*" + line;
        }
        return line;
    }

    public string FormatInfraspecificLine(IucnSpeciesRecord record, DisplayPreferences display, string? listStatusContext, OtherBucketContext? otherContext = null) {
        var builder = new StringBuilder(SpeciesListLine.FormatInfraspecificUnderSpecies(ToEntry(record), ToLineOptions(display, listStatusContext)));
        AppendAnnotations(builder, record, otherContext);
        return builder.ToString();
    }

    /// <summary>
    /// The values SpeciesListLine renders a line from: the record's names, with the common name,
    /// the article title and (for a subspecies or variety with no article) the parent species'
    /// article resolved as the lists resolve them.
    /// </summary>
    internal SpeciesListEntry ToEntry(IucnSpeciesRecord record) {
        var articleTitle = ResolveArticleTitle(record);
        var parentArticle = !string.IsNullOrWhiteSpace(record.InfraName) && string.IsNullOrWhiteSpace(articleTitle)
            ? ResolveParentSpeciesArticle(record)
            : null;
        return new SpeciesListEntry {
            ScientificName = ResolveScientificName(record),
            Genus = record.GenusName,
            SpeciesEpithet = record.SpeciesName,
            InfraType = record.InfraType,
            InfraName = record.InfraName,
            Kingdom = record.KingdomName,
            SubpopulationName = record.SubpopulationName,
            RegionalScopeLabel = GetRegionalScopeLabel(record),
            CommonName = ResolveCommonName(record),
            ArticleTitle = articleTitle,
            ParentSpeciesArticleTitle = parentArticle,
            StatusCode = IucnRedlistStatus.Describe(record.StatusCode).Code,
            PossiblyExtinct = IsFlagTrue(record.PossiblyExtinct),
            PossiblyExtinctInTheWild = IsFlagTrue(record.PossiblyExtinctInTheWild),
            TaxonId = record.TaxonId,
            AssessmentId = record.AssessmentId,
            YearPublished = record.YearPublished,
        };
    }

    internal static SpeciesListLineOptions ToLineOptions(DisplayPreferences display, string? statusContext) => new() {
        Style = display.ListingStyle switch {
            ListingStyle.ScientificNameFocus => SpeciesListStyle.ScientificNameFirst,
            ListingStyle.CommonNameOnly => SpeciesListStyle.CommonNameOnly,
            _ => SpeciesListStyle.CommonNameFirst,
        },
        IncludeStatusTemplate = display.IncludeStatusTemplate,
        ItalicizeScientific = display.ItalicizeScientific,
        StatusContext = statusContext,
    };

    // The CSV's "true"/"false" text: exactly "true" in any case, untrimmed, as IucnRedlistStatus reads it.
    private static bool IsFlagTrue(string? flag) => string.Equals(flag, "true", StringComparison.OrdinalIgnoreCase);

    // After the shared line: the rank of an "Other" bucket item (e.g. Family, Subfamily, Tribe), then
    // a source-supplied annotation (the SPRAT multi-system status string), appended verbatim with an
    // em-dash separator; null for IUCN-sourced records, so the line is unchanged.
    private static void AppendAnnotations(StringBuilder builder, IucnSpeciesRecord record, OtherBucketContext? otherContext) {
        if (otherContext is { IsInOtherBucket: true }) {
            var rankValue = otherContext.GetRankValue(record);
            if (!string.IsNullOrWhiteSpace(rankValue)) {
                var displayValue = ToTitleCase(rankValue);
                var shouldLink = otherContext.ShouldLinkValue(displayValue);
                if (shouldLink) {
                    builder.Append($" ({otherContext.RankLabel}: [[{displayValue}]])");
                } else {
                    builder.Append($" ({otherContext.RankLabel}: {displayValue})");
                }
            }
        }

        if (!string.IsNullOrWhiteSpace(record.StatusAnnotation)) {
            builder.Append(" — ");
            builder.Append(record.StatusAnnotation);
        }
    }

    /// <summary>
    /// Resolves the Wikipedia article for an infraspecific record's parent species (Genus species),
    /// or null when no article is known — used so a subspecies/variety with no article of its own can
    /// still bluelink to its species page instead of redlinking a trinomial.
    /// </summary>
    internal string? ResolveParentSpeciesArticle(IucnSpeciesRecord record) {
        if (_storeBackedProvider is null) {
            return null;
        }
        var genus = record.GenusName?.Trim();
        var species = record.SpeciesName?.Trim();
        if (string.IsNullOrWhiteSpace(genus) || string.IsNullOrWhiteSpace(species)) {
            return null;
        }
        var article = _storeBackedProvider.GetWikipediaArticleTitleByScientificName($"{genus} {species}", record.KingdomName);
        return string.IsNullOrWhiteSpace(article) ? null : article;
    }

    /// <summary>
    /// The Wikipedia article title the lists link for a record, or null when none is known.
    /// </summary>
    internal string? ResolveArticleTitle(IucnSpeciesRecord record) {
        // A curated per-taxon wikilink override (rules-list.txt "<sci> wikilink <Article>") wins over
        // every data-derived title — it's the manual correction for cases the sources resolve wrongly
        // (e.g. the Canis familiaris/Dingo taxobox split, where the hub has no usable article title).
        var taxaRules = _legacyRules.Get(record.ScientificNameTaxonomy ?? record.ScientificNameAssessments ?? string.Empty);
        if (!string.IsNullOrWhiteSpace(taxaRules?.Wikilink)) {
            return taxaRules!.Wikilink;
        }

        // A title resolved by the source itself (e.g. a SPRAT taxon's hub-resolved article title).
        if (!string.IsNullOrWhiteSpace(record.ArticleTitleOverride)) {
            return record.ArticleTitleOverride;
        }

        // Try store-backed provider first (has Wikipedia source data)
        if (_storeBackedProvider is not null) {
            return _storeBackedProvider.GetWikipediaArticleTitle(record);
        }

        return null;
    }

    /// <summary>
    /// The vernacular name that would be shown for this record (after the junk/duplicate filter),
    /// or null when none is usable. Used by the renderer to sort common-name-focused lists by the
    /// label the reader actually sees.
    /// </summary>
    public string? ResolveDisplayCommonName(IucnSpeciesRecord record) => ResolveCommonName(record);

    // The common name chooser for this formatter's rules-list.txt and store (CommonNameChooser
    // applies rules-list.txt, the store's best name and the unusable check); built on first use.
    private CommonNameChooser? _commonNameChooser;
    private CommonNameChooser NameChooser =>
        _commonNameChooser ??= (_storeBackedProvider?.Chooser ?? CommonNameChooser.RulesOnly(null)).WithRules(_legacyRules);

    private string? ResolveCommonName(IucnSpeciesRecord record) {
        var subject = new CommonNameSubject(
            RulesKey: record.ScientificNameTaxonomy ?? record.ScientificNameAssessments,
            ScientificName: ResolveScientificName(record),
            Genus: record.GenusName,
            SpeciesEpithet: record.SpeciesName);
        var provider = _storeBackedProvider;
        var choice = NameChooser.Choose(subject, provider is null ? null : () => provider.GetBestCommonName(record));
        if (choice.Found) {
            // Null when the name is not usable: the list falls back to scientific-name styling.
            return choice.Name;
        }

        // Neither rules-list.txt nor the store has a name. A name carried on the record itself
        // (SPRAT's vernacular for an Australia list), else the legacy provider (--use-legacy-names).
        var fallback = !string.IsNullOrWhiteSpace(record.CommonNameOverride)
            ? record.CommonNameOverride
            : _commonNameProvider?.GetBestCommonName(record.ToTaxonomyRow(), entityIds: null);
        return IsUnusableCommonName(fallback, record) ? null : fallback;
    }

    // Returns true when the resolved "common name" is not actually a usable vernacular: a working
    // placeholder, an authority/homonym string, or simply the scientific name repeated.
    private static bool IsUnusableCommonName(string? candidate, IucnSpeciesRecord record) =>
        CommonNameChooser.IsUnusable(candidate, ResolveScientificName(record), record.GenusName, record.SpeciesName);

    public static string? ResolveScientificName(IucnSpeciesRecord record) {
        // A cleaned CoL spelling (set only for a formatting-equivalent slip in the IUCN name) is the
        // name of record for display, links, sorting, and exclusion matching.
        if (!string.IsNullOrWhiteSpace(record.ScientificNameOverride)) {
            return record.ScientificNameOverride;
        }

        if (!string.IsNullOrWhiteSpace(record.ScientificNameTaxonomy)) {
            return record.ScientificNameTaxonomy;
        }

        if (!string.IsNullOrWhiteSpace(record.ScientificNameAssessments)) {
            return record.ScientificNameAssessments;
        }

        var withRank = ScientificNameHelper.BuildWithRankLabel(record.GenusName, record.SpeciesName, record.InfraType, record.InfraName);
        if (!string.IsNullOrWhiteSpace(withRank)) {
            return withRank;
        }

        return ScientificNameHelper.BuildFromParts(record.GenusName, record.SpeciesName, record.InfraName);
    }
}
