using System.Text;

namespace BeastieBot3.Shared.Wikitext;

// One bullet line of a generated Wikipedia list: the taxon's names and link, the possibly extinct
// or extinct in the wild label, the subpopulation and regional scope, and the {{IUCN status}}
// template. The CLI's SpeciesLineFormatter resolves the names (common name, article titles) and
// calls in here, so the public site can render the same line from the same values.

/// The listing styles of the generated lists (the CLI's ListingStyle A, B and C).
public enum SpeciesListStyle {
    /// Style A: scientific name first, common name after a comma. Plants and invertebrates.
    ScientificNameFirst,
    /// Style B (default): common name first, scientific name in brackets.
    CommonNameFirst,
    /// Style C: common name only, the scientific name when there is no common name.
    CommonNameOnly,
}

/// The values a list line is made from. Names and titles are already resolved.
public sealed record SpeciesListEntry {
    /// The scientific name the lists show and link (SpeciesLineFormatter.ResolveScientificName).
    public string? ScientificName { get; init; }
    public string? Genus { get; init; }
    public string? SpeciesEpithet { get; init; }
    /// IUCN's infraType text ("ssp.", "subsp.", "var.").
    public string? InfraType { get; init; }
    public string? InfraName { get; init; }
    /// IUCN spelling, any case ("ANIMALIA", "Plantae").
    public string? Kingdom { get; init; }
    public string? SubpopulationName { get; init; }
    /// The regional scopes of a regional assessment ("Europe, Mediterranean"); null for a global one.
    public string? RegionalScopeLabel { get; init; }
    /// The common name to show; null when there is none or it is not usable.
    public string? CommonName { get; init; }
    /// The Wikipedia article the line links; null when none is known.
    public string? ArticleTitle { get; init; }
    /// The article of the parent species, linked from a subspecies or variety with no article of its own.
    public string? ParentSpeciesArticleTitle { get; init; }
    /// The {{IUCN status}} code: "CR", "CR(PE)", "CR(PEW)", "EW", "LR/nt" and so on.
    public string StatusCode { get; init; } = "";
    /// With StatusCode "CR", makes the template code CR(PE).
    public bool PossiblyExtinct { get; init; }
    /// With StatusCode "CR", makes the template code CR(PEW).
    public bool PossiblyExtinctInTheWild { get; init; }
    public long TaxonId { get; init; }
    public long AssessmentId { get; init; }
    /// The assessment's year; left out of the template for EX and EW.
    public string? YearPublished { get; init; }
}

public sealed record SpeciesListLineOptions {
    public SpeciesListStyle Style { get; init; } = SpeciesListStyle.CommonNameFirst;
    public bool IncludeStatusTemplate { get; init; } = true;
    public bool ItalicizeScientific { get; init; } = true;
    /// The status of the list or section ("CR(PE)", "EW"). The line leaves out a possibly extinct,
    /// possibly extinct in the wild or extinct in the wild label that the context already states.
    public string? StatusContext { get; init; }
}

public static class SpeciesListLine {
    /// "* " + name fragment + status label + subpopulation and scope + {{IUCN status}}.
    /// The CLI appends its "Other" bucket rank and SPRAT status annotation after this.
    public static string Format(SpeciesListEntry entry, SpeciesListLineOptions options) {
        var builder = new StringBuilder();
        builder.Append("* ");
        builder.Append(NameFragment(entry, options));
        AppendTail(builder, entry, options);
        return builder.ToString();
    }

    /// The line for a subspecies or variety listed under its species: the name has an abbreviated
    /// genus ("''G. species'' subsp. ''name''"), then the common name after a comma.
    public static string FormatInfraspecificUnderSpecies(SpeciesListEntry entry, SpeciesListLineOptions options) {
        var builder = new StringBuilder();
        builder.Append("* ");

        var infraLink = BuildInfraspecificLink(entry, abbreviateGenus: true);
        if (!string.IsNullOrWhiteSpace(infraLink)) {
            builder.Append(infraLink);
            if (!string.IsNullOrWhiteSpace(entry.CommonName)) {
                builder.Append(", ");
                builder.Append(entry.CommonName);
            }
        } else {
            builder.Append(NameFragment(entry, options));
        }

        AppendTail(builder, entry, options);
        return builder.ToString();
    }

    /// The names and link of the line, in the style the options select.
    public static string NameFragment(SpeciesListEntry entry, SpeciesListLineOptions options) {
        var commonName = entry.CommonName;
        var articleTitle = entry.ArticleTitle;
        var formattedScientific = FormatScientificNameForDisplay(entry, options.ItalicizeScientific);

        // For infraspecific taxa, use properly formatted name for link targets.
        // This ensures animal subspecies omit "ssp." and plants include "subsp."/"var.".
        var linkScientific = !string.IsNullOrWhiteSpace(entry.InfraName)
            ? BuildScientificNameForLink(entry)
            : entry.ScientificName;

        return options.Style switch {
            SpeciesListStyle.ScientificNameFirst => BuildScientificNameFocusFragment(commonName, articleTitle, linkScientific, formattedScientific, entry),
            SpeciesListStyle.CommonNameOnly => BuildCommonNameOnlyFragment(commonName, articleTitle, linkScientific, formattedScientific, entry),
            _ => BuildCommonNameFocusFragment(commonName, articleTitle, linkScientific, formattedScientific, entry),
        };
    }

    /// The label for a possibly extinct, possibly extinct in the wild or extinct in the wild
    /// status, or null when the status has none or the status context already states it.
    public static string? SpecialStatusLabel(string statusCode, string? statusContext) {
        var code = statusCode.ToUpperInvariant();
        var context = statusContext?.ToUpperInvariant() ?? string.Empty;

        if (code is "CR(PE)" or "PE") {
            if (context.Contains("CR(PE)") || (context.Contains("PE") && !context.Contains("PEW"))) return null;
            return "possibly extinct";
        }

        if (code is "CR(PEW)" or "PEW") {
            if (context.Contains("CR(PEW)") || context.Contains("PEW")) return null;
            return "possibly extinct in the wild";
        }

        if (code == "EW") {
            if (context.Contains("EW")) return null;
            return "extinct in the wild";
        }

        return null;
    }

    /// {{IUCN status|CODE|taxonId/assessmentId|1|year=YYYY}} for the entry.
    public static string StatusTemplate(SpeciesListEntry entry) =>
        IucnStatusTemplate.Render(entry.StatusCode, entry.PossiblyExtinct, entry.PossiblyExtinctInTheWild,
            entry.TaxonId, entry.AssessmentId, entry.YearPublished);

    // The status label, the subpopulation and scope, and the status template.
    private static void AppendTail(StringBuilder builder, SpeciesListEntry entry, SpeciesListLineOptions options) {
        var specialLabel = SpecialStatusLabel(entry.StatusCode, options.StatusContext);
        if (!string.IsNullOrWhiteSpace(specialLabel)) {
            builder.Append(" (");
            builder.Append(specialLabel);
            builder.Append(')');
        }

        var subpopulation = entry.SubpopulationName;
        var scopeLabel = entry.RegionalScopeLabel;
        if (!string.IsNullOrWhiteSpace(subpopulation) || !string.IsNullOrWhiteSpace(scopeLabel)) {
            builder.Append(" (");
            if (!string.IsNullOrWhiteSpace(subpopulation)) {
                builder.Append(subpopulation);
            }

            if (!string.IsNullOrWhiteSpace(scopeLabel)) {
                if (!string.IsNullOrWhiteSpace(subpopulation)) {
                    builder.Append("; ");
                }
                builder.Append("scope: ");
                builder.Append(scopeLabel);
            }

            builder.Append(')');
        }

        if (options.IncludeStatusTemplate) {
            builder.Append(' ');
            builder.Append(StatusTemplate(entry));
        }
    }

    /// <summary>
    /// Style A: Scientific name focus. Shows scientific name first, common name after comma.
    /// Examples:
    /// - ''[[Pinus radiata]]'', Monterey pine
    /// - ''[[Scientific name]]''
    /// - ''[[Wikilink|Scientific name]]'', Common name
    /// </summary>
    private static string BuildScientificNameFocusFragment(string? commonName, string? articleTitle, string? rawScientific, string formattedScientific, SpeciesListEntry entry) {
        // For infraspecific taxa with var./subsp., use special formatting
        var hasInfrarank = !string.IsNullOrWhiteSpace(entry.InfraType) && !string.IsNullOrWhiteSpace(entry.InfraName);
        var infraLink = hasInfrarank ? BuildInfraspecificLink(entry) : null;

        if (!string.IsNullOrWhiteSpace(infraLink)) {
            if (!string.IsNullOrWhiteSpace(commonName)) {
                return $"{infraLink}, {commonName}";
            }
            return infraLink;
        }

        // Standard species formatting
        var linkTarget = ResolveLinkTarget(entry, articleTitle, rawScientific);

        if (string.IsNullOrWhiteSpace(linkTarget)) {
            // No linkable target (e.g. an undescribed "sp. nov." placeholder) — show plain italic,
            // keeping the common name if there is one.
            return !string.IsNullOrWhiteSpace(commonName) ? $"{formattedScientific}, {commonName}" : formattedScientific;
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
    /// - [[Article title|''Scientific name'']] (no common name; the article has another title)
    /// - ''[[Scientific name]]'' (no common name; no article, or the article has the scientific name as its title)
    /// With no common name the line is the same as in Style C: the scientific name, linked to the
    /// article. The article title is only a link target, never shown, because it is often another
    /// scientific name ("Crenimugil buchanani" for Moolgarda buchanani) or a genus.
    /// </summary>
    private static string BuildCommonNameFocusFragment(string? commonName, string? articleTitle, string? rawScientific, string formattedScientific, SpeciesListEntry entry) {
        if (string.IsNullOrWhiteSpace(commonName)) {
            return BuildScientificNameOnlyFragment(articleTitle, rawScientific, formattedScientific, entry);
        }

        var commonLinkTarget = ResolveLinkTargetForCommonName(articleTitle, rawScientific, commonName);

        if (string.IsNullOrWhiteSpace(commonLinkTarget)) {
            return $"[[{commonName}]] ({formattedScientific})";
        }

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
    private static string BuildCommonNameOnlyFragment(string? commonName, string? articleTitle, string? rawScientific, string formattedScientific, SpeciesListEntry entry) {
        if (string.IsNullOrWhiteSpace(commonName)) {
            return BuildScientificNameOnlyFragment(articleTitle, rawScientific, formattedScientific, entry);
        }

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
    /// The name fragment of Styles B and C for a taxon with no common name: the scientific name,
    /// linked to the article when there is one. A subspecies or variety gets its rank marker
    /// (<see cref="BuildInfraspecificLink"/>). Examples:
    /// - ''[[Scientific name]]'' (the link target is the scientific name)
    /// - [[Article title|''Scientific name'']]
    /// - ''Scientific name'' (nothing to link, such as an undescribed "sp. nov." name)
    /// </summary>
    private static string BuildScientificNameOnlyFragment(string? articleTitle, string? rawScientific, string formattedScientific, SpeciesListEntry entry) {
        var hasInfrarank = !string.IsNullOrWhiteSpace(entry.InfraType) && !string.IsNullOrWhiteSpace(entry.InfraName);
        if (hasInfrarank) {
            var infraLink = BuildInfraspecificLink(entry);
            if (!string.IsNullOrWhiteSpace(infraLink)) {
                return infraLink;
            }
        }

        var linkTarget = ResolveLinkTarget(entry, articleTitle, rawScientific);
        if (string.IsNullOrWhiteSpace(linkTarget)) {
            return formattedScientific;
        }
        // Ordinal: a title that differs from the scientific name only in case is still a different
        // text, and shown as the link text it would put the article title on the line.
        if (string.Equals(linkTarget, rawScientific, StringComparison.Ordinal)) {
            return $"''[[{linkTarget}]]''";
        }
        return $"[[{linkTarget}|{formattedScientific}]]";
    }

    /// <summary>
    /// Builds a properly formatted link for subspecies/varieties with correct italicization.
    /// For infraspecific taxa, we need [[link|''Genus species'' subsp. ''subspecies'']] format.
    /// For animals, the rank marker is hidden.
    /// </summary>
    private static string? BuildInfraspecificLink(SpeciesListEntry entry, bool abbreviateGenus = false) {
        if (string.IsNullOrWhiteSpace(entry.InfraName)) {
            return null;
        }

        var displayText = BuildInfraspecificDisplayText(entry, abbreviateGenus);
        var fullScientific = BuildScientificNameForLink(entry);
        if (string.IsNullOrWhiteSpace(displayText) || string.IsNullOrWhiteSpace(fullScientific)) {
            return null;
        }

        if (!string.IsNullOrWhiteSpace(entry.ArticleTitle)) {
            return $"[[{entry.ArticleTitle}|{displayText}]]";
        }

        // Subspecies/variety articles rarely exist. Rather than redlink a bare trinomial, link the
        // formatted name to the PARENT SPECIES article when one is known (e.g. an Antarctic blue
        // whale subspecies → the blue whale article); otherwise fall back to plain (italic) text.
        if (!string.IsNullOrWhiteSpace(entry.ParentSpeciesArticleTitle)) {
            return $"[[{entry.ParentSpeciesArticleTitle}|{displayText}]]";
        }

        return displayText;
    }

    /// <summary>
    /// Format a scientific name for display (with italics if requested).
    /// </summary>
    private static string FormatScientificNameForDisplay(SpeciesListEntry entry, bool italicize) {
        if (!string.IsNullOrWhiteSpace(entry.InfraName)) {
            var formatted = BuildInfraspecificDisplayText(entry, abbreviateGenus: false, stripItalics: !italicize);
            if (!string.IsNullOrWhiteSpace(formatted)) {
                return formatted;
            }
        }

        var scientific = BuildScientificNameForDisplay(entry);
        if (string.IsNullOrWhiteSpace(scientific)) {
            return entry.Genus ?? "";
        }

        return italicize ? ItalicizeSafe(scientific) : scientific;
    }

    // Italicize a display string, guarding the MediaWiki quirk where ''X'' with X ending in an
    // apostrophe (e.g. an undescribed epithet "sp. nov. 'loguerciae'") yields a stray ''' that renders
    // as bold. Falls back to explicit <i></i> only in that case.
    private static string ItalicizeSafe(string text) =>
        text.EndsWith("'", StringComparison.Ordinal) ? $"<i>{text}</i>" : $"''{text}''";

    private static string? BuildScientificNameForDisplay(SpeciesListEntry entry) {
        if (!string.IsNullOrWhiteSpace(entry.InfraName)) {
            return BuildInfraspecificDisplayText(entry, abbreviateGenus: false, stripItalics: true);
        }

        return entry.ScientificName;
    }

    private static string BuildScientificNameForLink(SpeciesListEntry entry) {
        var genus = entry.Genus?.Trim();
        var species = entry.SpeciesEpithet?.Trim();
        if (string.IsNullOrWhiteSpace(genus) || string.IsNullOrWhiteSpace(species)) {
            return entry.ScientificName ?? string.Empty;
        }

        if (string.IsNullOrWhiteSpace(entry.InfraName)) {
            return $"{genus} {species}";
        }

        var rankMarker = ResolveInfraspecificRankMarker(entry);
        if (!string.IsNullOrWhiteSpace(rankMarker)) {
            return $"{genus} {species} {rankMarker} {entry.InfraName?.Trim()}".Replace("  ", " ");
        }

        return $"{genus} {species} {entry.InfraName?.Trim()}".Replace("  ", " ");
    }

    private static string? BuildInfraspecificDisplayText(
        SpeciesListEntry entry,
        bool abbreviateGenus,
        bool stripItalics = false) {
        var genus = entry.Genus?.Trim();
        var species = entry.SpeciesEpithet?.Trim();
        var infraName = entry.InfraName?.Trim();
        if (string.IsNullOrWhiteSpace(genus) || string.IsNullOrWhiteSpace(species) || string.IsNullOrWhiteSpace(infraName)) {
            return null;
        }

        if (abbreviateGenus) {
            genus = genus.Length > 0 ? $"{genus[0]}." : genus;
        }

        var rankMarker = ResolveInfraspecificRankMarker(entry);
        if (!string.IsNullOrWhiteSpace(rankMarker)) {
            var head = stripItalics ? $"{genus} {species}" : $"''{genus} {species}''";
            var tail = stripItalics ? infraName : $"''{infraName}''";
            return $"{head} {rankMarker} {tail}";
        }

        return stripItalics
            ? $"{genus} {species} {infraName}"
            : $"''{genus} {species} {infraName}''";
    }

    /// The rank marker shown in an infraspecific name: "var.", "subsp." (none for animals), "f.",
    /// or IUCN's own infraType text with a full stop; null for a species.
    public static string? InfraspecificRankMarker(string? infraType, string? kingdom) {
        var type = infraType?.Trim().ToLowerInvariant() ?? string.Empty;
        var upperKingdom = kingdom?.ToUpperInvariant() ?? string.Empty;

        if (type.Contains("var")) {
            return "var.";
        }

        if (type.Contains("subsp") || type.Contains("ssp")) {
            return upperKingdom == "ANIMALIA" ? null : "subsp.";
        }

        // Botanical "form" rank (IUCN/CoL spell it "forma"/"form"/"f."). Map to the
        // canonical marker rather than fabricating "forma." in the fall-through below.
        if (type.StartsWith("form") || type == "f." || type == "f") {
            return "f.";
        }

        if (!string.IsNullOrWhiteSpace(type)) {
            return type.EndsWith(".") ? type : type + ".";
        }

        return null;
    }

    private static string? ResolveInfraspecificRankMarker(SpeciesListEntry entry) =>
        InfraspecificRankMarker(entry.InfraType, entry.Kingdom);

    // Undescribed-species placeholders ("Genus sp. nov. 'x'") never have a Wikipedia article.
    private static bool IsUndescribedName(string? name) =>
        !string.IsNullOrWhiteSpace(name) && name.Contains("sp. nov", StringComparison.OrdinalIgnoreCase);

    private static string ResolveLinkTarget(SpeciesListEntry entry, string? articleTitle, string? rawScientific) {
        if (!string.IsNullOrWhiteSpace(articleTitle)) {
            return articleTitle;
        }

        // Don't emit a guaranteed redlink for an undescribed placeholder — callers fall back to plain
        // italic text instead of ''[[Genus sp. nov. 'x']]''.
        if (IsUndescribedName(rawScientific) || IsUndescribedName(entry.ScientificName)) {
            return string.Empty;
        }

        if (!string.IsNullOrWhiteSpace(rawScientific)) {
            return rawScientific;
        }

        var built = BuildScientificNameForLink(entry);
        if (!string.IsNullOrWhiteSpace(built)) {
            return built;
        }

        return entry.Genus ?? entry.SpeciesEpithet ?? string.Empty;
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
}
