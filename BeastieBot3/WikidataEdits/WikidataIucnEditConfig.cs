using System;
using System.Collections.Generic;
using System.IO;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

// The Wikidata-side choices the dry run depends on, kept in rules/wikidata/iucn-status.yml so they
// can change as the community discussion settles them, without code changes: which item stands for
// the Red List release, how a new assessment item is typed and labelled, and which rank variants
// the plan records.
//
// An id that does not exist yet (the 2026.1 release item before anyone creates it) is left empty;
// the plan then uses a "CREATE:" placeholder in its place and the flow page shows the step as to do.

namespace BeastieBot3.WikidataEdits;

internal sealed class WikidataIucnEditConfig {
    /// IUCN release the plan is for, as the IUCN database names it ("2026-1").
    public string Release { get; set; } = "2026-1";

    /// "The IUCN Red List of Threatened Species 2026.1" edition item. Empty until created.
    public string? EditionItem { get; set; }

    /// Q32059, the IUCN Red List as a work.
    public string RedListItem { get; set; } = "Q32059";

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
    /// P31 for a new assessment item. Existing ones are mostly scholarly articles (Q13442814);
    /// the class is part of the modelling proposal and may change.
    public string InstanceOf { get; set; } = "Q13442814";
    /// Placeholders: {name} {year} {taxon_id} {assessment_id}.
    public string Label { get; set; } = "{name}. The IUCN Red List of Threatened Species {year}: e.T{taxon_id}A{assessment_id}";
    public string Description { get; set; } = "IUCN Red List assessment of {name}";
    /// Language of the P1476 title (the scientific name).
    public string TitleLanguage { get; set; } = "en";
    /// P123 publisher: IUCN.
    public string Publisher { get; set; } = "Q48268";
    /// P407 language of work: English.
    public string Language { get; set; } = "Q1860";
    /// Credit types written as P2093 author name strings, in citation order.
    public List<string> AuthorCreditTypes { get; set; } = new() { "assessor" };
}
