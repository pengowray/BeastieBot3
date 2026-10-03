using System.Text.Json;

namespace BeastieBot3.Shared.Wikitext;

// Wikitext and QuickStatements for citing an IUCN assessment through Wikidata: {{cite Q}} when an
// item for the assessment exists, and QuickStatements v1 batches to create that item or add the
// statements it lacks. The item model is the one the Wikidata status dry run uses
// (rules/wikidata/iucn-status.yml, assessment_item), passed in as WikidataItemModel.
// Signatures only; implementations replace the NotImplementedException bodies.

/// The Wikidata assessment item model: which items the statements point to.
public sealed record WikidataItemModel {
    /// P31, e.g. Q13442814 (scholarly article).
    public string InstanceOf { get; init; } = "Q13442814";
    /// P1433, the IUCN Red List (Q32059).
    public string PublishedIn { get; init; } = "Q32059";
    /// P123, IUCN (Q48268).
    public string Publisher { get; init; } = "Q48268";
    /// P407, English (Q1860).
    public string Language { get; init; } = "Q1860";
    /// Language code of the P1476 title, which is the scientific name.
    public string TitleLanguage { get; init; } = "en";
    /// English label, with {name}, {year}, {taxon_id}, {assessment_id} placeholders.
    public string LabelTemplate { get; init; } = "{name}. The IUCN Red List of Threatened Species {year}: e.T{taxon_id}A{assessment_id}";
    /// English description, with the same placeholders.
    public string DescriptionTemplate { get; init; } = "IUCN Red List assessment of {name}";

    public string ToJson() => JsonSerializer.Serialize(this);
    public static WikidataItemModel FromJson(string? json) =>
        string.IsNullOrWhiteSpace(json) ? new WikidataItemModel() : JsonSerializer.Deserialize<WikidataItemModel>(json) ?? new WikidataItemModel();
}

public sealed record CiteQOptions {
    /// |access-date= in "D Month YYYY" form; null leaves it out.
    public DateOnly? AccessDate { get; init; }
    public bool WrapInRef { get; init; }
    public string? RefName { get; init; }
}

public static class WikidataCitation {
    /// {{cite Q|Q123}} with the options.
    public static string CiteQ(string itemQid, CiteQOptions? options = null) =>
        throw new NotImplementedException();

    /// QuickStatements v1 commands (one per line, tab-separated) that create an item for the
    /// assessment: CREATE, then LAST lines for the label, description and statements.
    public static IReadOnlyList<string> CreateItemCommands(IucnCitationParts parts, string? taxonQid, WikidataItemModel model) =>
        throw new NotImplementedException();

    /// QuickStatements v1 commands that add to an existing item the statements it lacks, judged by
    /// the properties it already has. Empty when nothing is missing.
    public static IReadOnlyList<string> AddMissingCommands(IucnCitationParts parts, string itemQid,
        IReadOnlySet<string> presentProperties, string? taxonQid, WikidataItemModel model) =>
        throw new NotImplementedException();

    /// A link that opens QuickStatements with the commands filled in.
    public static string QuickStatementsUrl(IEnumerable<string> commands) =>
        throw new NotImplementedException();
}
