using System;
using System.Collections.Generic;
using System.IO;
using BeastieBot3.Configuration;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Web.Endpoints;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

// The Wikidata-side choices the dry run depends on, kept in rules/wikidata/iucn-status.yml so they
// can change as the community discussion settles them, without code changes: which item stands for
// the Red List release, how a new assessment item is typed and labelled, and which rank variants
// the plan records.
//
// An id that does not exist yet (the 2026.1 release item before anyone creates it) is left empty;
// the plan then uses a "CREATE:" placeholder in its place and the flow page shows the step as to do.
//
// The assessment item's defaults (used when the file or a key is missing) come from the shared
// WikidataItemModel, so the dry run and the public site's QuickStatements batches agree.
// `site build-db` reads the same file through LoadFromRules and stores ToItemModel() in the site
// database.

namespace BeastieBot3.WikidataEdits;

internal sealed class WikidataIucnEditConfig {
    /// IUCN release the plan is for, as the IUCN database names it ("2026-1").
    public string Release { get; set; } = "2026-1";

    /// "The IUCN Red List of Threatened Species 2026.1" edition item. Empty until created.
    public string? EditionItem { get; set; }

    /// Q32059, the IUCN Red List as a work.
    public string RedListItem { get; set; } = AssessmentItemConfig.Defaults.PublishedIn;

    public AssessmentItemConfig AssessmentItem { get; set; } = new();

    /// Rank variants recorded for a changed status: "preferred" (new preferred statement, old
    /// ones kept at normal rank) and/or "replace" (value and references overwritten in place).
    public List<string> RankVariants { get; set; } = new() { "preferred", "replace" };

    /// Adds the assessment page URL (which carries the assessment id) to the release reference,
    /// until Wikidata has a property for the assessment id.
    public bool ReferenceAssessmentUrl { get; set; } = true;

    /// Edit summary. Placeholders: {release} {code} {taxon_id} {assessment_id}. Wording not settled.
    public string EditSummary { get; set; } = "IUCN Red List {release}: conservation status {code} (IUCN taxon {taxon_id}, assessment {assessment_id})";

    public string? EditionItemOrNull => string.IsNullOrWhiteSpace(EditionItem) ? null : EditionItem.Trim();

    public string EditionRef => EditionItemOrNull ?? $"CREATE:iucn-red-list-edition:{Release}";

    public IEnumerable<EditVariant> Variants {
        get {
            foreach (var v in RankVariants) {
                if (string.Equals(v, "preferred", StringComparison.OrdinalIgnoreCase)) yield return EditVariant.Preferred;
                else if (string.Equals(v, "replace", StringComparison.OrdinalIgnoreCase)) yield return EditVariant.Replace;
            }
        }
    }

    public static string PathFor(string rulesDir) => Path.Combine(rulesDir, "wikidata", "iucn-status.yml");

    /// The assessment item model the public site's QuickStatements batches follow.
    public WikidataItemModel ToItemModel() => new() {
        InstanceOf = AssessmentItem.InstanceOf,
        PublishedIn = RedListItem,
        Publisher = AssessmentItem.Publisher,
        Language = AssessmentItem.Language,
        TitleLanguage = AssessmentItem.TitleLanguage,
        LabelTemplate = AssessmentItem.Label,
        DescriptionTemplate = AssessmentItem.Description,
    };

    /// Reads iucn-status.yml from the source rules folder, else the copy beside the program; the
    /// defaults when neither has one or it can't be read. loadedFrom is the file read, or null.
    public static WikidataIucnEditConfig LoadFromRules(PathsService paths, out string? loadedFrom) {
        loadedFrom = null;
        try {
            var rules = RulesPaths.Resolve(paths);
            foreach (var dir in new[] { rules.SourceRulesDir, rules.BuildOutputRulesDir }) {
                var path = PathFor(dir);
                if (File.Exists(path)) {
                    loadedFrom = path;
                    return Load(dir);
                }
            }
        } catch {
            // fall through to defaults
        }
        return new WikidataIucnEditConfig();
    }

    public static WikidataIucnEditConfig Load(string rulesDir) {
        var path = PathFor(rulesDir);
        if (!File.Exists(path)) {
            return new WikidataIucnEditConfig();
        }
        var deserializer = new DeserializerBuilder()
            .WithNamingConvention(UnderscoredNamingConvention.Instance)
            .IgnoreUnmatchedProperties()
            .Build();
        return deserializer.Deserialize<WikidataIucnEditConfig>(File.ReadAllText(path)) ?? new WikidataIucnEditConfig();
    }
}

internal sealed class AssessmentItemConfig {
    /// The shared model's defaults, which are the values in iucn-status.yml.
    internal static readonly WikidataItemModel Defaults = new();

    /// P31 for a new assessment item. Existing ones are mostly scholarly articles (Q13442814);
    /// the class is part of the modelling proposal and may change.
    public string InstanceOf { get; set; } = Defaults.InstanceOf;
    /// Placeholders: {name} {year} {taxon_id} {assessment_id}.
    public string Label { get; set; } = Defaults.LabelTemplate;
    public string Description { get; set; } = Defaults.DescriptionTemplate;
    /// Language of the P1476 title (the scientific name).
    public string TitleLanguage { get; set; } = Defaults.TitleLanguage;
    /// P123 publisher: IUCN.
    public string Publisher { get; set; } = Defaults.Publisher;
    /// P407 language of work: English.
    public string Language { get; set; } = Defaults.Language;
    /// Credit types written as P2093 author name strings, in citation order.
    public List<string> AuthorCreditTypes { get; set; } = new() { "assessor" };
}
