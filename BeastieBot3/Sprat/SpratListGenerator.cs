using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using BeastieBot3.CommonNames;
using BeastieBot3.WikipediaLists;
using BeastieBot3.WikipediaLists.Legacy;

// Generates the "List of threatened <group> of Australia" wikitext pages from SPRAT. Reuses the
// existing taxonomy-tree renderer (SectionBodyRenderer + SpeciesLineFormatter + HeadingFormatter)
// for the species body — taxonomy grouping, infraspecific handling, and the three listing styles —
// but owns its own SPRAT/EPBC-flavoured intro and footer prose (the IUCN generator's IntroProseBuilder
// is hard-wired to IUCN wording). Records arrive pre-sectioned by primary Australian status with the
// multi-system annotation already attached (see SpratListQueryService); the IUCN {{IUCN status}}
// template is suppressed (IncludeStatusTemplate=false) since SPRAT carries no IUCN assessment id.

namespace BeastieBot3.Sprat;

internal sealed class SpratListGenerator {
    private readonly SpratListQueryService _query;
    private readonly SectionBodyRenderer _renderer;
    // Optional aggregated-names hub: resolves a Wikipedia article title (real bluelinks) and a
    // conventionally-cased vernacular for taxa it knows (mostly the IUCN-assessed ones). When absent,
    // SPRAT's own vernacular is used, sentence-cased via the caps rules.
    private readonly StoreBackedCommonNameProvider? _hub;
    private readonly IReadOnlyDictionary<string, string> _capsRules;

    // Status sections, most-severe first. Mirrors AustralianStatus.QualifyingBySeverity: the three
    // threatened categories, then Near Threatened, then the state "Rare" category.
    private static readonly string[] SectionOrder = { "CR", "EN", "VU", "NT", "Rare" };

    private static readonly IReadOnlyList<GroupingLevelDefinition> Grouping = new[] {
        new GroupingLevelDefinition { Level = "order", Label = "Order", UnknownLabel = "Other orders" },
        new GroupingLevelDefinition {
            Level = "family", Label = "Family", UnknownLabel = "Unassigned families",
            MinItems = 5, MinGroupsForOther = 3, ShowRankLabel = true,
        },
    };

    public SpratListGenerator(
        SpratListQueryService query,
        LegacyTaxaRuleList legacyRules,
        StoreBackedCommonNameProvider? hub = null,
        IReadOnlyDictionary<string, string>? capsRules = null) {
        _query = query ?? throw new ArgumentNullException(nameof(query));
        _hub = hub;
        _capsRules = capsRules ?? new Dictionary<string, string>();
        // The line formatter is intentionally provider-less: SPRAT taxa carry a SPRAT id (not an IUCN
        // sis id), so the hub is consulted by scientific name during Enrich and baked into the record
        // as overrides, rather than via the formatter's id-keyed lookups.
        var lineFormatter = new SpeciesLineFormatter(legacyRules, storeBackedProvider: null, commonNameProvider: null);
        var headingFormatter = new HeadingFormatter(legacyRules, taxonRules: null, storeBackedProvider: null);
        _renderer = new SectionBodyRenderer(colEnricher: null, taxonRules: null, lineFormatter, headingFormatter);
    }

    public SpratListResult Generate(SpratListGroup group, string outputDirectory, int? limit) {
        var records = _query.Query(group.Filter, limit).Select(Enrich).ToList();
        var display = BuildDisplay(group.Style);

        var body = new StringBuilder();
        var total = 0;
        foreach (var code in SectionOrder) {
            var sectionRecords = records.Where(r => string.Equals(r.StatusCode, code, StringComparison.OrdinalIgnoreCase)).ToList();
            if (sectionRecords.Count == 0) {
                continue;
            }
            total += sectionRecords.Count;
            var heading = IucnRedlistStatus.Describe(code).Label ?? code;
            body.AppendLine($"== {heading} ==");
            var (sectionBody, _) = _renderer.BuildSectionBody(sectionRecords, Grouping, display, code, customGroups: null, startHeading: 3, autoSplit: null);
            body.AppendLine(sectionBody);
            body.AppendLine();
        }

        var content = new StringBuilder();
        content.AppendLine(BuildIntro(group, records));
        content.AppendLine();
        content.Append(body);
        content.AppendLine(BuildFooter(group));

        Directory.CreateDirectory(outputDirectory);
        var outputPath = Path.Combine(outputDirectory, group.OutputFile);
        File.WriteAllText(outputPath, content.ToString());
        return new SpratListResult(group, outputPath, total);
    }

    /// <summary>
    /// Resolves a conventionally-cased common name and a Wikipedia article link target for a SPRAT
    /// record, via the aggregated-names hub (by scientific name, since SPRAT carries no IUCN id).
    /// Falls back to a sentence-cased SPRAT vernacular and no link override when the hub is absent or
    /// doesn't know the taxon.
    /// </summary>
    private IucnSpeciesRecord Enrich(IucnSpeciesRecord r) {
        var kingdomUpper = string.IsNullOrWhiteSpace(r.KingdomName) ? null : r.KingdomName.ToUpperInvariant();
        var fullSci = r.ScientificNameTaxonomy;
        var genusSpecies = !string.IsNullOrWhiteSpace(r.GenusName) && !string.IsNullOrWhiteSpace(r.SpeciesName)
            ? $"{r.GenusName} {r.SpeciesName}"
            : null;

        // Common name: for species, prefer the hub's (Wikipedia/IUCN-sourced, conventionally cased)
        // name. NOT for infraspecific taxa — the hub only knows the species, so it would collapse two
        // subspecies to one shared name; keep SPRAT's subspecies-specific vernacular there instead.
        var isInfra = !string.IsNullOrWhiteSpace(r.InfraName);
        string? hubName = null;
        if (_hub is not null && !isInfra && !string.IsNullOrWhiteSpace(fullSci)) {
            hubName = _hub.GetBestCommonNameByScientificName(fullSci, kingdomUpper);
        }
        var common = hubName ?? CaseSpratName(r.CommonNameOverride);

        // Article link target: the taxon's own article, else its parent species' article (so a
        // subspecies/variety bluelinks to the species page rather than redlinking a trinomial).
        string? article = null;
        if (_hub is not null) {
            if (!string.IsNullOrWhiteSpace(fullSci)) {
                article = _hub.GetWikipediaArticleTitleByScientificName(fullSci, kingdomUpper);
            }
            if (article is null && genusSpecies is not null
                && !string.Equals(genusSpecies, fullSci, StringComparison.OrdinalIgnoreCase)) {
                article = _hub.GetWikipediaArticleTitleByScientificName(genusSpecies, kingdomUpper);
            }
        }

        return r with { CommonNameOverride = common, ArticleTitleOverride = article };
    }

    private string? CaseSpratName(string? raw) => CaseVernacular(raw, _capsRules);

    /// <summary>
    /// Sentence-cases a SPRAT vernacular for display (#1), preserving proper nouns. A trailing region
    /// qualifier like " (Kimberley)" — which distinguishes SPRAT subspecies and is NOT a Wikipedia
    /// disambiguation suffix — is split off, kept verbatim, and re-appended, since ApplyCapitalization
    /// would otherwise strip it.
    /// </summary>
    internal static string? CaseVernacular(string? raw, IReadOnlyDictionary<string, string> capsRules) {
        if (string.IsNullOrWhiteSpace(raw)) {
            return null;
        }
        var match = QualifierSuffix.Match(raw);
        if (match.Success) {
            var head = CommonNameNormalizer.ApplyCapitalization(match.Groups[1].Value, capsRules);
            return $"{head} {match.Groups[2].Value.Trim()}";
        }
        return CommonNameNormalizer.ApplyCapitalization(raw, capsRules);
    }

    private static readonly System.Text.RegularExpressions.Regex QualifierSuffix =
        new(@"^(.*\S)\s*(\([^)]*\))\s*$", System.Text.RegularExpressions.RegexOptions.Compiled);

    private static DisplayPreferences BuildDisplay(ListingStyle style) => new() {
        PreferCommonNames = style != ListingStyle.ScientificNameFocus,
        ItalicizeScientific = true,
        IncludeStatusTemplate = false, // SPRAT lines carry the multi-system annotation, not {{IUCN status}}
        IncludeStatusLabel = false,
        GroupSubspecies = true,
        ListingStyle = style,
        InfraspecificDisplayMode = InfraspecificDisplayMode.SeparateSections,
        SeparateInfraspecificSections = true,
        ExcludeRegionalAssessments = true,
    };

    private string BuildIntro(SpratListGroup group, IReadOnlyList<IucnSpeciesRecord> records) {
        var month = DateTimeOffset.UtcNow.ToString("MMMM yyyy", CultureInfo.InvariantCulture);
        var n = records.Count;
        var taxaWord = n == 1 ? "taxon" : "taxa";

        var sb = new StringBuilder();
        sb.AppendLine($"{{{{Use dmy dates|date={month}}}}}");
        sb.AppendLine();
        sb.Append($"This is a list of the threatened {group.TaxaName} of [[Australia]]. ");
        sb.Append($"It covers {group.Adjective} species and infraspecific taxa listed as ");
        sb.Append("[[Critically endangered species|critically endangered]], [[Endangered species|endangered]], [[Vulnerable species|vulnerable]], [[Near-threatened species|near threatened]], or rare ");
        sb.Append("under Australia's national [[Environment Protection and Biodiversity Conservation Act 1999]] (EPBC Act)");
        sb.Append(SpratReference);
        sb.Append(", on the [[IUCN Red List]], or under the threatened-species legislation of any Australian state or territory. ");
        sb.Append("Each entry notes the taxon's status under the EPBC Act, the IUCN Red List, and the state and territory legislation under which it is listed.");
        sb.AppendLine();
        sb.AppendLine();
        sb.Append($"As of {month}, this list includes {NewspaperNumber(n)} {group.Adjective} {taxaWord}.");
        return sb.ToString();
    }

    private static string BuildFooter(SpratListGroup group) {
        var sb = new StringBuilder();
        sb.AppendLine("== See also ==");
        sb.AppendLine("* [[Environment Protection and Biodiversity Conservation Act 1999]]");
        sb.AppendLine("* [[Conservation in Australia]]");
        sb.AppendLine("* [[IUCN Red List]]");
        sb.AppendLine();
        sb.AppendLine("== References ==");
        sb.AppendLine("{{reflist}}");
        sb.AppendLine();
        foreach (var category in BuildCategories(group)) {
            sb.AppendLine(category);
        }
        return sb.ToString().TrimEnd();
    }

    private static IEnumerable<string> BuildCategories(SpratListGroup group) {
        yield return "[[Category:Lists of threatened species]]";
        yield return $"[[Category:{Capitalize(group.TaxaName)} of Australia]]";
        yield return "[[Category:Environmental lists]]";
    }

    private const string SpratReference =
        "<ref name=\"sprat\">{{cite web |title=Species Profile and Threats Database |publisher=Department of Climate Change, Energy, the Environment and Water, Australian Government |url=https://www.environment.gov.au/sprat-public/action/report |website=environment.gov.au}}</ref>";

    private static string Capitalize(string s) =>
        string.IsNullOrEmpty(s) ? s : char.ToUpperInvariant(s[0]) + s[1..];

    // Whole numbers under ten are spelled out in running prose (MOS:NUMERAL); larger numbers grouped.
    private static string NewspaperNumber(int n) => n switch {
        0 => "no",
        1 => "one", 2 => "two", 3 => "three", 4 => "four", 5 => "five",
        6 => "six", 7 => "seven", 8 => "eight", 9 => "nine",
        _ => n.ToString("N0", CultureInfo.InvariantCulture),
    };
}

internal sealed record SpratListResult(SpratListGroup Group, string OutputPath, int TotalEntries);
