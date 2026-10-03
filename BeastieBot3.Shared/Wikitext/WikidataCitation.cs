using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;

namespace BeastieBot3.Shared.Wikitext;

// Wikitext and QuickStatements for citing an IUCN assessment through Wikidata: {{cite Q}} when an
// item for the assessment exists, and QuickStatements v1 batches to create that item or add the
// statements it lacks. The site never edits Wikidata: the reader runs a batch in QuickStatements
// under their own account.
//
// The item model is the one the Wikidata status dry run uses (rules/wikidata/iucn-status.yml,
// assessment_item; WikidataEdits/AssessmentItemPayloadBuilder.cs), passed in as WikidataItemModel.
// The statements, in the dry run's order:
//
//   P31 instance of      model.InstanceOf (scholarly article)
//   P1476 title          the scientific name as IUCN's citation gives it, monolingual text in
//                        model.TitleLanguage
//   P1433 published in   model.PublishedIn (IUCN Red List)
//   P123 publisher       model.Publisher (IUCN)
//   P921 main subject    the taxon's item, when the caller knows it
//   P407 language        the language the DOI names (.en English, .es Spanish, .fr French,
//                        .pt Portuguese); model.Language when there is no DOI. The dry run always
//                        writes model.Language; it only plans latest global assessments, which are in
//                        English, while the site also offers batches for Spanish and French ones.
//   P953 full work URL   https://www.iucnredlist.org/species/{taxon}/{assessment}
//   P577 published       the year, precision 9 (+YYYY-00-00T00:00:00Z/9)
//   P356 DOI             upper-cased, as Wikidata stores DOIs; only a DOI naming this assessment's
//                        own taxon and assessment ids. An errata version's DOI names the assessment it
//                        corrects, whose item should carry that DOI: P356 has a distinct-values
//                        constraint, and two items with one DOI would be a duplicate to merge.
//   P2093 author name    each author as IUCN's citation prints the name ("Sayer, C.", "BirdLife
//                        International"), never the full given names, with a P1545 series ordinal
//                        qualifier ("1", "2" ...). Names before an "et al." only.
//
// plus the English label and description from model.LabelTemplate and model.DescriptionTemplate.
// No statement gets a reference: the dry run adds none to the item's own statements, since the item
// is the publication itself.
//
// AddMissingCommands judges what an existing item lacks from the properties the site database
// records for it (assessment.wikidata_item_properties), which `site build-db` reads from the Wikidata
// cache's wikidata_iucn_assessment_items table. That table records P31, P1476, P1433, P921, P577,
// P356, P50, P2093, the URLs and the English label, so only those are judged (JudgedProperties). P123,
// P407 and the description are left out of an add batch: the cache can't say whether the item has
// them, and a batch must never add a second value beside one the item already has. The English label
// is recorded as the token "Len" (EnglishLabelToken), the QuickStatements command that sets it.
// The cache's URL column merges P953, P854 and P856, so any of those counts as P953 here.
//
// QuickStatements v1 syntax, as public_html/quickstatements.php (importDataFromV1, parseValueV1) reads
// it: one command per line, columns separated by tabs. Strings are wrapped in double quotes and taken
// verbatim between the first and the last quote, so a quote inside a value needs no escaping (doubling
// quotes is the CSV format's rule, not v1's). Values are trimmed. Monolingual text is en:"...". A
// qualifier follows the statement on the same line (P1545 "2"). A statement whose property and value
// the item already has is not added again; a second author with the very same printed name
// ("Alemu, S." twice: Shambel and Sisay Alemu) is written with "!P2093", which makes a new statement,
// so its ordinal does not end up as a second qualifier on the first one.

/// The Wikidata assessment item model: which items the statements point to. The defaults are the
/// values in rules/wikidata/iucn-status.yml (assessment_item and red_list_item); the dry run's
/// config classes take their defaults from here.
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
    /// |access-date= in "D Month YYYY" form; null leaves it out. Written only when ItemHasUrl.
    public DateOnly? AccessDate { get; init; }

    /// The item has a full work URL: "P953" is in its wikidata_item_properties. {{cite Q}} takes
    /// |url= from P953, and CS1 reports an access date with no URL as an error ("|access-date=
    /// requires |url="), so |access-date= is only written when this is true.
    public bool ItemHasUrl { get; init; }

    public bool WrapInRef { get; init; }

    /// Name for <ref name="...">; ignored unless WrapInRef. Null or blank gives a plain <ref>.
    public string? RefName { get; init; }
}

/// One title (P1476) statement of an item: its monolingual text, language code and rank
/// ("preferred", "normal" or "deprecated"). `wikidata iucn-assessment-items` stores an item's
/// titles in this form, and `site build-db` copies them into assessment.wikidata_item_titles.
public sealed record WikidataTitle(
    [property: JsonPropertyName("text")] string Text,
    [property: JsonPropertyName("lang")] string? Language,
    [property: JsonPropertyName("rank")] string Rank = "normal") {
    [JsonIgnore]
    public bool IsDeprecated => string.Equals(Rank, "deprecated", StringComparison.Ordinal);

    public static string ListToJson(IReadOnlyList<WikidataTitle> titles) => JsonSerializer.Serialize(titles);

    /// Null for a NULL or blank value, or one that cannot be read: the titles are not known.
    public static IReadOnlyList<WikidataTitle>? ListFromJson(string? json) {
        if (string.IsNullOrWhiteSpace(json)) {
            return null;
        }
        try {
            return JsonSerializer.Deserialize<List<WikidataTitle>>(json);
        } catch (JsonException) {
            return null;
        }
    }
}

public enum WikidataItemChangeKind { Title, EnglishLabel }

/// A value FixCommands replaces. The languages are set for a title only.
public sealed record WikidataItemChange(WikidataItemChangeKind Kind, string Old, string New, string? OldLanguage = null, string? NewLanguage = null);

/// What FixCommands changes on an item, and the commands that do it (empty when nothing).
public sealed record WikidataItemFix(IReadOnlyList<WikidataItemChange> Changes, IReadOnlyList<string> Commands) {
    public static WikidataItemFix None { get; } = new([], []);
}

public static partial class WikidataCitation {
    /// The token for "the item has an English label" in a set of present properties
    /// (assessment.wikidata_item_properties). It is the QuickStatements command that sets the label.
    public const string EnglishLabelToken = "Len";

    /// The properties AddMissingCommands judges, in the order it writes them; see the file comment.
    /// P50 (author items) is not written but counts as having authors.
    public static IReadOnlyList<string> JudgedProperties { get; } =
        ["P31", "P1476", "P1433", "P921", "P953", "P577", "P356", "P2093", "P50", EnglishLabelToken];

    /// The longest QuickStatements link the site should offer. QuickStatements reads the commands
    /// from the URL's fragment, which the browser never sends to a server, so only the browser's own
    /// limit applies (Chrome and Firefox allow far more). 8,000 characters is a common safe ceiling
    /// for any browser, and the longest batch in the 2026-1 data (59 authors) gives a link of about 4,500 characters.
    public const int MaxQuickStatementsUrlLength = 8_000;

    /// The base of a QuickStatements link that opens the tool with a v1 batch filled in.
    public const string QuickStatementsBase = "https://quickstatements.toolforge.org/#/v1=";

    /// The Wikidata item for the language a DOI names (its last part: ".en", ".es", ".fr", ".pt").
    public static IReadOnlyDictionary<string, string> DoiLanguageItems { get; } = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) {
        ["en"] = "Q1860",
        ["es"] = "Q1321",
        ["fr"] = "Q150",
        ["pt"] = "Q5146",
    };

    /// {{cite Q|Q123}} with the options, on one line.
    public static string CiteQ(string itemQid, CiteQOptions? options = null) {
        options ??= new CiteQOptions();
        var sb = new StringBuilder("{{cite Q|").Append(itemQid.Trim());
        if (options.ItemHasUrl && options.AccessDate is { } accessed) {
            sb.Append(" |access-date=").Append(accessed.ToString("d MMMM yyyy", CultureInfo.InvariantCulture));
        }
        sb.Append("}}");
        var template = sb.ToString();
        if (!options.WrapInRef) {
            return template;
        }
        var refName = CiteIucnRenderer.SanitizeRefName(options.RefName);
        return refName.Length == 0
            ? $"<ref>{template}</ref>"
            : $"<ref name=\"{refName}\">{template}</ref>";
    }

    /// QuickStatements v1 commands (one per line, tab-separated) that create an item for the
    /// assessment: CREATE, then LAST lines for the label, description and statements.
    public static IReadOnlyList<string> CreateItemCommands(IucnCitationParts parts, string? taxonQid, WikidataItemModel model) {
        var commands = new List<string> { "CREATE" };
        if (Label(parts, model) is { } label) {
            commands.Add(Line("LAST", "Len", Quote(label)));
        }
        if (Description(parts, model) is { } description) {
            commands.Add(Line("LAST", "Den", Quote(description)));
        }
        foreach (var statement in Statements(parts, taxonQid, model, includeUnjudged: true)) {
            commands.Add(statement.ToLine("LAST"));
        }
        return commands;
    }

    /// QuickStatements v1 commands that add to an existing item the statements it lacks, judged by
    /// the properties it already has (see JudgedProperties; "Len" stands for its English label).
    /// Never a property the item already has, and never a removal. Empty when nothing is missing.
    public static IReadOnlyList<string> AddMissingCommands(IucnCitationParts parts, string itemQid,
        IReadOnlySet<string> presentProperties, string? taxonQid, WikidataItemModel model) {
        var item = itemQid.Trim();
        var commands = new List<string>();
        if (!presentProperties.Contains(EnglishLabelToken) && Label(parts, model) is { } label) {
            commands.Add(Line(item, "Len", Quote(label)));
        }
        var hasAuthors = presentProperties.Contains("P2093") || presentProperties.Contains("P50");
        foreach (var statement in Statements(parts, taxonQid, model, includeUnjudged: false)) {
            var present = statement.Property == "P2093" ? hasAuthors : presentProperties.Contains(statement.Property);
            if (!present) {
                commands.Add(statement.ToLine(item));
            }
        }
        return commands;
    }

    /// QuickStatements v1 commands that replace an existing item's title (P1476) and English label
    /// when they differ from the model's: the title is the scientific name in model.TitleLanguage,
    /// the label comes from model.LabelTemplate. Most items made by SourceMD in 2017 and 2018 have
    /// "Name: author list" as both, which {{cite Q}} prints as the title of the work.
    ///
    /// The title is replaced only when the item's titles are known exactly (titles is not null)
    /// and the item has one title that is not deprecated: the commands add the new title, then
    /// remove the old one by its exact text and language ("-Q1\tP1476\ten:\"old\""), which is how
    /// QuickStatements finds a statement to remove. Nothing is changed when any title, of any rank,
    /// already equals the model's, or when the old text cannot be written so that QuickStatements
    /// matches it (a control character, "||", or space at either end). The label is set with
    /// "Len", which replaces the old one; a missing label is AddMissingCommands' job.
    public static WikidataItemFix FixCommands(IucnCitationParts parts, string itemQid, IReadOnlyList<WikidataTitle>? titles,
        string? labelEn, WikidataItemModel model) {
        var item = itemQid.Trim();
        var changes = new List<WikidataItemChange>();
        var commands = new List<string>();

        var newTitle = CleanValue(parts.ScientificName);
        var language = CleanLanguageCode(model.TitleLanguage);
        if (titles is not null && newTitle.Length > 0
            && !titles.Any(t => t.Text == newTitle && t.Language == language)) {
            var live = titles.Where(t => !t.IsDeprecated).ToList();
            if (live is [var old] && CanMatchExactly(old)) {
                commands.Add(Line(item, "P1476", $"{language}:{Quote(newTitle)}"));
                commands.Add(Line("-" + item, "P1476", $"{old.Language}:{Quote(old.Text)}"));
                changes.Add(new WikidataItemChange(WikidataItemChangeKind.Title, old.Text, newTitle, old.Language, language));
            }
        }

        if (labelEn is not null && Label(parts, model) is { } label && labelEn != label) {
            commands.Add(Line(item, "Len", Quote(label)));
            changes.Add(new WikidataItemChange(WikidataItemChangeKind.EnglishLabel, labelEn, label));
        }
        return new WikidataItemFix(changes, commands);
    }

    // QuickStatements trims a value and takes the language as letters, "_" and "-", and a link's
    // commands are split at "||"; an old title that any of that would alter can't be matched.
    private static bool CanMatchExactly(WikidataTitle title) =>
        title.Text.Length > 0
        && title.Text == title.Text.Trim()
        && !title.Text.Any(char.IsControl)
        && !title.Text.Contains("||", StringComparison.Ordinal)
        && title.Language is { Length: > 0 } language
        && QuickStatementsLanguage().IsMatch(language);

    /// A link that opens QuickStatements with the commands filled in. Commands are joined with "||"
    /// and each tab becomes "|", the form Help:QuickStatements documents; the result is
    /// percent-encoded. A command whose values contain "|" keeps its tabs (sent as %09): the tool
    /// splits a line on tabs when it has one and then leaves "|" alone. A newline can't be sent, as
    /// the tool's page reads the fragment with a pattern that stops at one. Check the length against
    /// MaxQuickStatementsUrlLength before offering the link.
    public static string QuickStatementsUrl(IEnumerable<string> commands) {
        var lines = commands
            .Select(c => c.Replace("\r", string.Empty, StringComparison.Ordinal).Replace("\n", " ", StringComparison.Ordinal))
            .Where(c => c.Length > 0)
            .Select(c => c.Contains('|') ? c : c.Replace('\t', '|'));
        return QuickStatementsBase + Uri.EscapeDataString(string.Join("||", lines));
    }

    /// True when a QuickStatements link is short enough to offer (MaxQuickStatementsUrlLength).
    public static bool QuickStatementsUrlFits(string url) => url.Length <= MaxQuickStatementsUrlLength;

    // ------------------------------------------------------------ statements

    private sealed record Statement(string Property, string Value, string? Qualifier = null, string? QualifierValue = null,
        bool NewStatement = false) {
        public string ToLine(string item) {
            var property = NewStatement ? "!" + Property : Property;
            return Qualifier is null ? Line(item, property, Value) : Line(item, property, Value, Qualifier, QualifierValue!);
        }
    }

    // includeUnjudged: P123 and P407, which a create batch writes and an add batch leaves out.
    private static IEnumerable<Statement> Statements(IucnCitationParts parts, string? taxonQid, WikidataItemModel model,
        bool includeUnjudged) {
        var name = CleanValue(parts.ScientificName);
        var doi = OwnDoi(parts, out var doiLanguage);

        if (IsItemId(model.InstanceOf)) yield return new Statement("P31", model.InstanceOf.Trim());
        if (name.Length > 0) yield return new Statement("P1476", $"{CleanLanguageCode(model.TitleLanguage)}:{Quote(name)}");
        if (IsItemId(model.PublishedIn)) yield return new Statement("P1433", model.PublishedIn.Trim());
        if (includeUnjudged && IsItemId(model.Publisher)) yield return new Statement("P123", model.Publisher.Trim());
        if (taxonQid is not null && IsItemId(taxonQid)) yield return new Statement("P921", taxonQid.Trim());
        if (includeUnjudged) {
            var language = doiLanguage is not null && DoiLanguageItems.TryGetValue(doiLanguage, out var fromDoi) ? fromDoi : model.Language;
            if (IsItemId(language)) yield return new Statement("P407", language.Trim());
        }
        yield return new Statement("P953", Quote(parts.Url));
        if (parts.Year is > 0 and < 10_000) {
            yield return new Statement("P577", $"+{parts.Year.ToString("D4", CultureInfo.InvariantCulture)}-00-00T00:00:00Z/9");
        }
        if (doi is not null) yield return new Statement("P356", Quote(doi.ToUpperInvariant()));

        var seen = new HashSet<string>(StringComparer.Ordinal);
        var ordinal = 0;
        foreach (var author in parts.Authors) {
            var display = CleanValue(EtAlInName().Replace(author.Display ?? string.Empty, string.Empty));
            if (display.Length == 0) {
                continue;
            }
            ordinal++;
            yield return new Statement("P2093", Quote(display), "P1545", Quote(ordinal.ToString(CultureInfo.InvariantCulture)),
                NewStatement: !seen.Add(display));
        }
    }

    private static string? Label(IucnCitationParts parts, WikidataItemModel model) => Fill(model.LabelTemplate, parts, 250);

    private static string? Description(IucnCitationParts parts, WikidataItemModel model) => Fill(model.DescriptionTemplate, parts, 250);

    // Wikidata refuses a label or description over 250 characters, so a longer one is left out.
    private static string? Fill(string? template, IucnCitationParts parts, int maxLength) {
        if (string.IsNullOrWhiteSpace(template)) {
            return null;
        }
        var text = CleanValue(template
            .Replace("{name}", parts.ScientificName, StringComparison.Ordinal)
            .Replace("{year}", parts.Year.ToString(CultureInfo.InvariantCulture), StringComparison.Ordinal)
            .Replace("{taxon_id}", parts.TaxonId.ToString(CultureInfo.InvariantCulture), StringComparison.Ordinal)
            .Replace("{assessment_id}", parts.AssessmentId.ToString(CultureInfo.InvariantCulture), StringComparison.Ordinal));
        return text.Length > 0 && text.Length <= maxLength ? text : null;
    }

    // The DOI only when it names this assessment's own ids; see the file comment.
    private static string? OwnDoi(IucnCitationParts parts, out string? language) {
        language = null;
        if (string.IsNullOrWhiteSpace(parts.Doi)) {
            return null;
        }
        var value = DoiPrefix().Replace(parts.Doi.Trim(), string.Empty);
        var match = IucnDoi().Match(value);
        if (!match.Success
            || match.Groups["taxon"].Value != parts.TaxonId.ToString(CultureInfo.InvariantCulture)
            || match.Groups["assessment"].Value != parts.AssessmentId.ToString(CultureInfo.InvariantCulture)) {
            return null;
        }
        language = match.Groups["lang"].Value;
        return value;
    }

    private static string Line(params string[] columns) => string.Join('\t', columns);

    private static string Quote(string value) => "\"" + value + "\"";

    // One line with single spaces: tabs and line breaks separate commands and columns. HTML tags
    // ("<i>et al.</i>") are removed. A run of "|" becomes one, since "||" separates commands in a
    // QuickStatements link.
    private static string CleanValue(string? value) {
        if (string.IsNullOrEmpty(value)) {
            return string.Empty;
        }
        var text = HtmlTag().Replace(value, string.Empty);
        text = System.Net.WebUtility.HtmlDecode(text);
        text = PipeRun().Replace(text, "|");
        return WikitextValue.CollapseWhitespace(new string(text.Where(c => !char.IsControl(c) || char.IsWhiteSpace(c)).ToArray()));
    }

    private static string CleanLanguageCode(string? code) {
        var text = (code ?? string.Empty).Trim().ToLowerInvariant();
        return LanguageCode().IsMatch(text) ? text : "en";
    }

    private static bool IsItemId(string? value) => value is not null && ItemId().IsMatch(value.Trim());

    [GeneratedRegex(@"^Q[1-9][0-9]*$")]
    private static partial Regex ItemId();

    [GeneratedRegex(@"^[a-z]{2,3}(?:-[a-z0-9]+)*$")]
    private static partial Regex LanguageCode();

    // The language part of a monolingual value in quickstatements.php's parseValueV1.
    [GeneratedRegex(@"^[a-zA-Z_-]+$")]
    private static partial Regex QuickStatementsLanguage();

    [GeneratedRegex(@"<[^<>]*>")]
    private static partial Regex HtmlTag();

    [GeneratedRegex(@"\|{2,}")]
    private static partial Regex PipeRun();

    // An "et al." written inside a name ("Jaffré, T. <i>et al.</i>"), as CiteIucnRenderer removes it.
    [GeneratedRegex(@"[;,]?\s*(?:<i>)?\s*\bet\.?\s*al(?:ii|ia|iae)?\.?\s*(?:</i>)?\s*$", RegexOptions.IgnoreCase)]
    private static partial Regex EtAlInName();

    [GeneratedRegex(@"^(?:https?://(?:dx\.)?doi\.org/|doi:\s*)", RegexOptions.IgnoreCase)]
    private static partial Regex DoiPrefix();

    [GeneratedRegex(@"^10\.\d{4,9}/\S+?[Tt](?<taxon>\d+)[Aa](?<assessment>\d+)\.(?<lang>en|es|fr|pt)$", RegexOptions.IgnoreCase)]
    private static partial Regex IucnDoi();
}
