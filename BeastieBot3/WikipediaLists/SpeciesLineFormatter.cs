using System;
using System.Text;
using System.Text.RegularExpressions;
using BeastieBot3.CommonNames;
using BeastieBot3.Iucn;
using BeastieBot3.Taxonomy;
using BeastieBot3.WikipediaLists.Legacy;
using static BeastieBot3.WikipediaLists.ProseFormat;
using static BeastieBot3.WikipediaLists.RecordClassification;

namespace BeastieBot3.WikipediaLists;

// Renders a single species/infraspecific record to one wikitext bullet line, in whichever listing
// style the display preferences select (Style A scientific-focus, Style B common-focus, Style C
// common-only). Owns the name resolution it needs — common name, Wikipedia article title, scientific
// name formatting, link-target selection, and the {{IUCN status}} template. Extracted from
// WikipediaListGenerator (R2 carve-up); the generator builds the taxonomy tree and calls in here per
// leaf record. Holds the same common-name providers the generator was constructed with.
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
        var descriptor = IucnRedlistStatus.Describe(record.StatusCode);
        var builder = new StringBuilder();
        builder.Append("* ");

        builder.Append(BuildNameFragment(record, display));

        // Add special indicator for PE/PEW if not redundant with list context
        var specialLabel = GetSpecialStatusLabel(record.StatusCode, listStatusContext);
        if (!string.IsNullOrWhiteSpace(specialLabel)) {
            builder.Append(" (");
            builder.Append(specialLabel);
            builder.Append(')');
        }

        // Append subpopulation name if present
        var scopeLabel = GetRegionalScopeLabel(record);
        if (!string.IsNullOrWhiteSpace(record.SubpopulationName) || !string.IsNullOrWhiteSpace(scopeLabel)) {
            builder.Append(" (");
            if (!string.IsNullOrWhiteSpace(record.SubpopulationName)) {
                builder.Append(record.SubpopulationName);
            }

            if (!string.IsNullOrWhiteSpace(scopeLabel)) {
                if (!string.IsNullOrWhiteSpace(record.SubpopulationName)) {
                    builder.Append("; ");
                }
                builder.Append("scope: ");
                builder.Append(scopeLabel);
            }

            builder.Append(')');
        }

        // Add IUCN status template at end: {{IUCN status|XX|taxonId/assessmentId|1|year=YYYY}}
        if (display.IncludeStatusTemplate) {
            builder.Append(' ');
            builder.Append(BuildIucnStatusTemplate(record, descriptor));
        }

        // Add rank annotation for "Other" bucket items (e.g., Family, Subfamily, Tribe)
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
        var descriptor = IucnRedlistStatus.Describe(record.StatusCode);
        var builder = new StringBuilder();
        builder.Append("* ");

        var commonName = ResolveCommonName(record);
        var articleTitle = ResolveWikipediaArticle(record);
        var infraLink = BuildInfraspecificLink(record, articleTitle, abbreviateGenus: true);
        if (!string.IsNullOrWhiteSpace(infraLink)) {
            builder.Append(infraLink);
            if (!string.IsNullOrWhiteSpace(commonName)) {
                builder.Append(", ");
                builder.Append(commonName);
            }
        } else {
            builder.Append(BuildNameFragment(record, display));
        }

        var specialLabel = GetSpecialStatusLabel(record.StatusCode, listStatusContext);
        if (!string.IsNullOrWhiteSpace(specialLabel)) {
            builder.Append(" (");
            builder.Append(specialLabel);
            builder.Append(')');
        }

        var scopeLabel = GetRegionalScopeLabel(record);
        if (!string.IsNullOrWhiteSpace(record.SubpopulationName) || !string.IsNullOrWhiteSpace(scopeLabel)) {
            builder.Append(" (");
            if (!string.IsNullOrWhiteSpace(record.SubpopulationName)) {
                builder.Append(record.SubpopulationName);
            }

            if (!string.IsNullOrWhiteSpace(scopeLabel)) {
                if (!string.IsNullOrWhiteSpace(record.SubpopulationName)) {
                    builder.Append("; ");
                }
                builder.Append("scope: ");
                builder.Append(scopeLabel);
            }

            builder.Append(')');
        }

        if (display.IncludeStatusTemplate) {
            builder.Append(' ');
            builder.Append(BuildIucnStatusTemplate(record, descriptor));
        }

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

        return builder.ToString();
    }

    private string BuildNameFragment(IucnSpeciesRecord record, DisplayPreferences display) {
        var commonName = ResolveCommonName(record);
        var articleTitle = ResolveWikipediaArticle(record);
        var rawScientific = ResolveScientificName(record);
        var formattedScientific = FormatScientificNameForDisplay(record, display.ItalicizeScientific);

        // For infraspecific taxa, use properly formatted name for link targets.
        // This ensures animal subspecies omit "ssp." and plants include "subsp."/"var.".
        var linkScientific = !string.IsNullOrWhiteSpace(record.InfraName)
            ? BuildScientificNameForLink(record)
            : rawScientific;

        return display.ListingStyle switch {
            ListingStyle.ScientificNameFocus => BuildScientificNameFocusFragment(commonName, articleTitle, linkScientific, formattedScientific, record),
            ListingStyle.CommonNameOnly => BuildCommonNameOnlyFragment(commonName, articleTitle, linkScientific, formattedScientific, record),
            _ => BuildCommonNameFocusFragment(commonName, articleTitle, linkScientific, formattedScientific, record),  // Default: CommonNameFocus
        };
    }

    /// <summary>
    /// Style A: Scientific name focus. Shows scientific name first, common name after comma.
    /// Examples:
    /// - ''[[Pinus radiata]]'', Monterey pine
    /// - ''[[Scientific name]]''
    /// - ''[[Wikilink|Scientific name]]'', Common name
    /// </summary>
    private string BuildScientificNameFocusFragment(string? commonName, string? articleTitle, string? rawScientific, string formattedScientific, IucnSpeciesRecord record) {
        // For infraspecific taxa with var./subsp., use special formatting
        var hasInfrarank = !string.IsNullOrWhiteSpace(record.InfraType) && !string.IsNullOrWhiteSpace(record.InfraName);
        var infraLink = hasInfrarank ? BuildInfraspecificLink(record, articleTitle) : null;

        if (!string.IsNullOrWhiteSpace(infraLink)) {
            if (!string.IsNullOrWhiteSpace(commonName)) {
                return $"{infraLink}, {commonName}";
            }
            return infraLink;
        }

        // Standard species formatting
        var linkTarget = ResolveLinkTarget(record, articleTitle, rawScientific);

        if (string.IsNullOrWhiteSpace(linkTarget)) {
            return formattedScientific;
        }

        // Use ''[[X]]'' format when link target matches scientific name
        if (string.Equals(linkTarget, rawScientific, StringComparison.OrdinalIgnoreCase)) {
            var linkedScientific = $"''[[{rawScientific}]]''";
            if (!string.IsNullOrWhiteSpace(commonName)) {
                return $"{linkedScientific}, {commonName}";
            }
            return linkedScientific;
        }

        // Article uses common name as title, so use [[Wikilink|Scientific name]]
        var linkedWithPipe = $"[[{linkTarget}|{formattedScientific}]]";
        if (!string.IsNullOrWhiteSpace(commonName)) {
            return $"{linkedWithPipe}, {commonName}";
        }
        return linkedWithPipe;
    }

    /// <summary>
    /// Style B: Common name focus (default). Shows common name first, scientific name in parentheses.
    /// Scientific name must always be explicitly visible — never hidden inside a link.
    /// Examples:
    /// - [[Common name]] (''Scientific name'')
    /// - [[Wikilink|Common name]] (''Scientific name'')
    /// - [[Scientific name|Article title]] (''Scientific name'')  (fallback when no common name but article exists with different title)
    /// - ''[[Scientific name]]'' (fallback when no common name and no distinct article)
    /// </summary>
    private string BuildCommonNameFocusFragment(string? commonName, string? articleTitle, string? rawScientific, string formattedScientific, IucnSpeciesRecord record) {
        if (string.IsNullOrWhiteSpace(commonName)) {
            // For infraspecific taxa, use specialized formatting with proper rank markers
            var hasInfrarank = !string.IsNullOrWhiteSpace(record.InfraType) && !string.IsNullOrWhiteSpace(record.InfraName);
            if (hasInfrarank) {
                var infraLink = BuildInfraspecificLink(record, articleTitle);
                if (!string.IsNullOrWhiteSpace(infraLink)) {
                    return infraLink;
                }
            }

            // Fallback: no common name resolved
            var linkTarget = ResolveLinkTarget(record, articleTitle, rawScientific);
            if (!string.IsNullOrWhiteSpace(linkTarget)) {
                if (string.Equals(linkTarget, rawScientific, StringComparison.OrdinalIgnoreCase)) {
                    return $"''[[{linkTarget}]]''";
                }

                if (!string.IsNullOrWhiteSpace(rawScientific)) {
                    return $"[[{rawScientific}|{linkTarget}]] ({formattedScientific})";
                }
                return $"[[{linkTarget}]] ({formattedScientific})";
            }
            return formattedScientific;
        }

        // We have a common name
        var commonLinkTarget = ResolveLinkTargetForCommonName(articleTitle, rawScientific, commonName);

        if (string.IsNullOrWhiteSpace(commonLinkTarget)) {
            // No link target available, just use common name
            return $"[[{commonName}]] ({formattedScientific})";
        }

        // Build the link
        string linkedCommonName;
        if (string.Equals(commonLinkTarget, commonName, StringComparison.Ordinal)) {
            linkedCommonName = $"[[{commonName}]]";
        } else {
            linkedCommonName = $"[[{commonLinkTarget}|{commonName}]]";
        }

        return $"{linkedCommonName} ({formattedScientific})";
    }

    /// <summary>
    /// Style C: Common name only. Shows only common name (falls back to scientific if unavailable).
    /// Examples:
    /// - [[Common name]]
    /// - [[Wikilink|Common name]]
    /// - ''[[Scientific name]]'' (fallback when no common name)
    /// </summary>
    private string BuildCommonNameOnlyFragment(string? commonName, string? articleTitle, string? rawScientific, string formattedScientific, IucnSpeciesRecord record) {
        if (string.IsNullOrWhiteSpace(commonName)) {
            // For infraspecific taxa, use specialized formatting with proper rank markers
            var hasInfrarank = !string.IsNullOrWhiteSpace(record.InfraType) && !string.IsNullOrWhiteSpace(record.InfraName);
            if (hasInfrarank) {
                var infraLink = BuildInfraspecificLink(record, articleTitle);
                if (!string.IsNullOrWhiteSpace(infraLink)) {
                    return infraLink;
                }
            }

            // Fallback to scientific name
            var linkTarget = ResolveLinkTarget(record, articleTitle, rawScientific);
            if (!string.IsNullOrWhiteSpace(linkTarget)) {
                if (string.Equals(linkTarget, rawScientific, StringComparison.OrdinalIgnoreCase)) {
                    return $"''[[{linkTarget}]]''";
                }
                return $"[[{linkTarget}|{formattedScientific}]]";
            }
            return formattedScientific;
        }

        // We have a common name - show only common name
        var commonLinkTarget = ResolveLinkTargetForCommonName(articleTitle, rawScientific, commonName);

        if (string.IsNullOrWhiteSpace(commonLinkTarget)) {
            return $"[[{commonName}]]";
        }

        if (string.Equals(commonLinkTarget, commonName, StringComparison.Ordinal)) {
            return $"[[{commonName}]]";
        }

        return $"[[{commonLinkTarget}|{commonName}]]";
    }

    /// <summary>
    /// Builds a properly formatted link for subspecies/varieties with correct italicization.
    /// For infraspecific taxa, we need [[link|''Genus species'' subsp. ''subspecies'']] format.
    /// For animals, the rank marker is hidden.
    /// </summary>
    private string? BuildInfraspecificLink(IucnSpeciesRecord record, string? articleTitle, bool abbreviateGenus = false) {
        if (string.IsNullOrWhiteSpace(record.InfraName)) {
            return null;
        }

        var displayText = BuildInfraspecificDisplayText(record, abbreviateGenus);
        var fullScientific = BuildScientificNameForLink(record);
        if (string.IsNullOrWhiteSpace(displayText) || string.IsNullOrWhiteSpace(fullScientific)) {
            return null;
        }

        if (!string.IsNullOrWhiteSpace(articleTitle)) {
            return $"[[{articleTitle}|{displayText}]]";
        }

        // Subspecies/variety articles rarely exist. Rather than redlink a bare trinomial, link the
        // formatted name to the PARENT SPECIES article when one is known (e.g. an Antarctic blue
        // whale subspecies → the blue whale article); otherwise fall back to plain (italic) text.
        var parentArticle = ResolveParentSpeciesArticle(record);
        if (!string.IsNullOrWhiteSpace(parentArticle)) {
            return $"[[{parentArticle}|{displayText}]]";
        }

        return displayText;
    }

    /// <summary>
    /// Resolves the Wikipedia article for an infraspecific record's parent species (Genus species),
    /// or null when no article is known — used so a subspecies/variety with no article of its own can
    /// still bluelink to its species page instead of redlinking a trinomial.
    /// </summary>
    private string? ResolveParentSpeciesArticle(IucnSpeciesRecord record) {
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
    /// Format a scientific name for display (with italics if requested).
    /// </summary>
    private static string FormatScientificNameForDisplay(IucnSpeciesRecord record, bool italicize) {
        if (!string.IsNullOrWhiteSpace(record.InfraName)) {
            var formatted = BuildInfraspecificDisplayText(record, abbreviateGenus: false, stripItalics: !italicize);
            if (!string.IsNullOrWhiteSpace(formatted)) {
                return formatted;
            }
        }

        var scientific = BuildScientificNameForDisplay(record);
        if (string.IsNullOrWhiteSpace(scientific)) {
            return record.GenusName ?? "";
        }

        return italicize ? $"''{scientific}''" : scientific;
    }

    private static string? BuildScientificNameForDisplay(IucnSpeciesRecord record) {
        if (!string.IsNullOrWhiteSpace(record.InfraName)) {
            return BuildInfraspecificDisplayText(record, abbreviateGenus: false, stripItalics: true);
        }

        return ResolveScientificName(record);
    }

    private static string BuildScientificNameForLink(IucnSpeciesRecord record) {
        var genus = record.GenusName?.Trim();
        var species = record.SpeciesName?.Trim();
        if (string.IsNullOrWhiteSpace(genus) || string.IsNullOrWhiteSpace(species)) {
            return ResolveScientificName(record) ?? string.Empty;
        }

        if (string.IsNullOrWhiteSpace(record.InfraName)) {
            return $"{genus} {species}";
        }

        var rankMarker = ResolveInfraspecificRankMarker(record);
        if (!string.IsNullOrWhiteSpace(rankMarker)) {
            return $"{genus} {species} {rankMarker} {record.InfraName?.Trim()}".Replace("  ", " ");
        }

        return $"{genus} {species} {record.InfraName?.Trim()}".Replace("  ", " ");
    }

    private static string? BuildInfraspecificDisplayText(
        IucnSpeciesRecord record,
        bool abbreviateGenus,
        bool stripItalics = false) {
        var genus = record.GenusName?.Trim();
        var species = record.SpeciesName?.Trim();
        var infraName = record.InfraName?.Trim();
        if (string.IsNullOrWhiteSpace(genus) || string.IsNullOrWhiteSpace(species) || string.IsNullOrWhiteSpace(infraName)) {
            return null;
        }

        if (abbreviateGenus) {
            genus = genus.Length > 0 ? $"{genus[0]}." : genus;
        }

        var rankMarker = ResolveInfraspecificRankMarker(record);
        if (!string.IsNullOrWhiteSpace(rankMarker)) {
            var head = stripItalics ? $"{genus} {species}" : $"''{genus} {species}''";
            var tail = stripItalics ? infraName : $"''{infraName}''";
            return $"{head} {rankMarker} {tail}";
        }

        return stripItalics
            ? $"{genus} {species} {infraName}"
            : $"''{genus} {species} {infraName}''";
    }

    private static string? ResolveInfraspecificRankMarker(IucnSpeciesRecord record) {
        var infraType = record.InfraType?.Trim().ToLowerInvariant() ?? string.Empty;
        var kingdom = record.KingdomName?.ToUpperInvariant() ?? string.Empty;

        if (infraType.Contains("var")) {
            return "var.";
        }

        if (infraType.Contains("subsp") || infraType.Contains("ssp")) {
            return kingdom == "ANIMALIA" ? null : "subsp.";
        }

        // Botanical "form" rank (IUCN/CoL spell it "forma"/"form"/"f."). Map to the
        // canonical marker rather than fabricating "forma." in the fall-through below.
        if (infraType.StartsWith("form") || infraType == "f." || infraType == "f") {
            return "f.";
        }

        if (!string.IsNullOrWhiteSpace(infraType)) {
            return infraType.EndsWith(".") ? infraType : infraType + ".";
        }

        return null;
    }

    private static bool RequiresRankMarker(IucnSpeciesRecord record) {
        return !string.IsNullOrWhiteSpace(ResolveInfraspecificRankMarker(record));
    }

    /// <summary>
    /// Resolve the Wikipedia article title for a record.
    /// </summary>
    private string? ResolveWikipediaArticle(IucnSpeciesRecord record) {
        // Try store-backed provider first (has Wikipedia source data)
        if (_storeBackedProvider is not null) {
            return _storeBackedProvider.GetWikipediaArticleTitle(record);
        }

        return null;
    }

    private string ResolveLinkTarget(IucnSpeciesRecord record, string? articleTitle, string? rawScientific) {
        if (!string.IsNullOrWhiteSpace(articleTitle)) {
            return articleTitle;
        }

        if (!string.IsNullOrWhiteSpace(rawScientific)) {
            return rawScientific;
        }

        var built = BuildScientificNameForLink(record);
        if (!string.IsNullOrWhiteSpace(built)) {
            return built;
        }

        return record.GenusName ?? record.SpeciesName ?? string.Empty;
    }

    private static string ResolveLinkTargetForCommonName(string? articleTitle, string? rawScientific, string commonName) {
        if (!string.IsNullOrWhiteSpace(articleTitle)) {
            return articleTitle;
        }

        if (!string.IsNullOrWhiteSpace(rawScientific)) {
            return rawScientific;
        }

        return commonName;
    }

    /// <summary>
    /// The vernacular name that would be shown for this record (after the junk/duplicate filter),
    /// or null when none is usable. Used by the renderer to sort common-name-focused lists by the
    /// label the reader actually sees.
    /// </summary>
    public string? ResolveDisplayCommonName(IucnSpeciesRecord record) => ResolveCommonName(record);

    private string? ResolveCommonName(IucnSpeciesRecord record) {
        var candidate = ResolveCommonNameCandidate(record);
        // Drop candidates that are really the scientific name repeated, or carry working/authority
        // strings ("sp. nov.", "(Author) 1993", " non ") — fall back to scientific-name styling instead.
        return IsUnusableCommonName(candidate, record) ? null : candidate;
    }

    private string? ResolveCommonNameCandidate(IucnSpeciesRecord record) {
        // First check legacy rules (highest priority - manual overrides)
        var taxaRules = _legacyRules.Get(record.ScientificNameTaxonomy ?? record.ScientificNameAssessments ?? string.Empty);
        if (!string.IsNullOrWhiteSpace(taxaRules?.CommonName)) {
            return Uppercase(taxaRules!.CommonName);
        }

        // Try the new store-backed provider if available
        if (_storeBackedProvider is not null) {
            return _storeBackedProvider.GetBestCommonName(record);
        }

        // Fall back to legacy provider
        if (_commonNameProvider is null) {
            return null;
        }

        var row = record.ToTaxonomyRow();
        return _commonNameProvider.GetBestCommonName(row, entityIds: null);
    }

    // A 4-digit year (1600–2099) betrays a botanical/zoological authority citation rather than a
    // vernacular name; common names effectively never contain one.
    private static readonly Regex AuthorityYearPattern = new(@"\b(1[6-9]\d{2}|20\d{2})\b", RegexOptions.Compiled);

    // Returns true when the resolved "common name" is not actually a usable vernacular: a working
    // placeholder, an authority/homonym string, or simply the scientific name repeated.
    private static bool IsUnusableCommonName(string? candidate, IucnSpeciesRecord record) {
        if (string.IsNullOrWhiteSpace(candidate)) {
            return true;
        }

        var name = candidate.Trim();

        if (name.Contains("sp. nov", StringComparison.OrdinalIgnoreCase)) return true;
        if (name.Contains(" spp.", StringComparison.OrdinalIgnoreCase)) return true;
        if (name.Contains(" non ", StringComparison.Ordinal)) return true;
        if (AuthorityYearPattern.IsMatch(name)) return true;

        // A "common name" that is really just the scientific name repeated.
        var scientific = ResolveScientificName(record);
        if (!string.IsNullOrWhiteSpace(scientific) &&
            string.Equals(name, scientific, StringComparison.OrdinalIgnoreCase)) {
            return true;
        }

        var binomial = BuildBinomial(record);
        if (!string.IsNullOrWhiteSpace(binomial) &&
            string.Equals(FirstTwoTokens(name), binomial, StringComparison.OrdinalIgnoreCase)) {
            return true;
        }

        return false;
    }

    private static string? BuildBinomial(IucnSpeciesRecord record) {
        var genus = record.GenusName?.Trim();
        var species = record.SpeciesName?.Trim();
        if (string.IsNullOrWhiteSpace(genus) || string.IsNullOrWhiteSpace(species)) {
            return null;
        }
        return $"{genus} {species}";
    }

    private static string FirstTwoTokens(string name) {
        var parts = name.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries);
        return parts.Length <= 2 ? string.Join(' ', parts) : parts[0] + " " + parts[1];
    }

    public static string? ResolveScientificName(IucnSpeciesRecord record) {
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

    private static string BuildIucnStatusTemplate(IucnSpeciesRecord record, RedlistStatusDescriptor descriptor) {
        // Build the status code, accounting for PE/PEW flags from database
        var statusCode = GetWikipediaStatusCode(descriptor.Code, record.PossiblyExtinct, record.PossiblyExtinctInTheWild);
        var builder = new StringBuilder();
        builder.Append("{{IUCN status|");
        builder.Append(statusCode);
        builder.Append('|');
        builder.Append(record.TaxonId);
        builder.Append('/');
        builder.Append(record.AssessmentId);
        builder.Append("|1"); // 1 = make link visible

        // Add year for non-extinct statuses
        if (!IsExtinctStatus(descriptor.Code) && !string.IsNullOrWhiteSpace(record.YearPublished)) {
            builder.Append("|year=");
            builder.Append(record.YearPublished);
        }

        builder.Append("}}");
        return builder.ToString();
    }

    /// <summary>
    /// Maps IUCN status codes to Wikipedia template codes.
    /// Uses PE/PEW database flags for CR species to produce CR(PE) or CR(PEW).
    /// Maps legacy LR/* codes to their modern equivalents, except LR/cd which has no exact equivalent.
    /// </summary>
    private static string GetWikipediaStatusCode(string code, string? possiblyExtinct, string? possiblyExtinctInTheWild) {
        var normalized = code.ToUpperInvariant();

        // For CR species, check PE/PEW flags from database
        if (normalized == "CR" || normalized == "CRITICALLY ENDANGERED") {
            if (string.Equals(possiblyExtinct, "true", StringComparison.OrdinalIgnoreCase)) {
                return "CR(PE)";
            }
            if (string.Equals(possiblyExtinctInTheWild, "true", StringComparison.OrdinalIgnoreCase)) {
                return "CR(PEW)";
            }
            return "CR";
        }

        // Map legacy/alternative codes
        // Note: LR/cd is a valid Wikipedia template code, don't map it to NT
        return normalized switch {
            "CR(PE)" or "PE" => "CR(PE)",
            "CR(PEW)" or "PEW" => "CR(PEW)",
            "LR/CD" or "CD" => "LR/cd",
            "LR/NT" => "LR/nt", //"NT",
            "LR/LC" => "LR/lc", //"LC"",
            _ => normalized
        };
    }

    private static bool IsExtinctStatus(string code) => code.ToUpperInvariant() switch {
        "EX" or "EW" => true,
        _ => false
    };

    private static string? GetSpecialStatusLabel(string statusCode, string? listStatusContext) {
        // Don't add redundant labels when the list is specifically for that status
        var code = statusCode.ToUpperInvariant();
        var context = listStatusContext?.ToUpperInvariant() ?? string.Empty;

        // PE/PEW always need indicator except on dedicated PE lists
        if (code is "CR(PE)" or "PE") {
            // If context contains CR(PE) or PE, suppress the label
            if (context.Contains("CR(PE)") || (context.Contains("PE") && !context.Contains("PEW"))) return null;
            return "possibly extinct"; // non-breaking space
        }

        if (code is "CR(PEW)" or "PEW") {
            if (context.Contains("CR(PEW)") || context.Contains("PEW")) return null;
            return "possibly extinct in the wild";
        }

        // EW indicator only needed if not on an EW-specific list
        if (code == "EW") {
            if (context.Contains("EW")) return null;
            return "extinct in the wild";
        }

        return null;
    }
}
