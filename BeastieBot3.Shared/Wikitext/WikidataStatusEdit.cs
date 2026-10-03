using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;

namespace BeastieBot3.Shared.Wikitext;

// QuickStatements v1 commands that bring a taxon item's IUCN conservation status (P141) up to the
// latest global assessment, for the public site. The site never edits Wikidata: the reader runs the
// commands in QuickStatements with their own account. This follows the Wikidata status dry run
// (BeastieBot3/WikidataEdits/IucnStatusEditPlanner.cs, docs/wikidata-iucn-status.md) as far as
// QuickStatements can express it:
//
// - Value: WikidataStatusValues.QidForCode, compared case-sensitively here, so a pre-1994 "nt" is
//   never read as NT. LR/nt and LR/lc give NT and LC; LR/cd has no value and gets no commands.
//   Possibly extinct is a flag on CR, so CR(PE) and CR(PEW) give critically endangered.
// - Reference, one group on the statement: stated in (P248) the assessment's own Wikidata item when
//   the caller has one, IUCN taxon ID (P627), reference URL (P854) of the assessment page and
//   retrieved (P813), the date the assessment was downloaded. The dry run's release reference
//   cites the Red List release item (P248); there is no item for release 2026-1 on Wikidata yet, so
//   that is left out. The assessment item is never created here: the site's {{cite Q}} part offers
//   that batch, and two batches that each create the item would make duplicates. QuickStatements
//   can refer to an item created earlier in a batch (LAST works as a value in its source), but the
//   help page does not document that use.
// - Which statements count: only those with a reference that cites IUCN (CitesIucn: an IUCN taxon
//   ID (P627) in the reference, or stated in (P248) the IUCN Red List, one of its editions or
//   IUCN). Agrees, Differs and Missing are decided from them alone, by the best-ranked ones
//   (preferred, else normal), as the dry run does. A statement with no reference, or with only
//   references to another source (a national red book), is never removed and is listed to the
//   reader as Others.
// - Rank: QuickStatements v1 cannot set a statement's rank (Help:QuickStatements, Limitations).
//   For a changed status there are the dry run's two conventions:
//     Keep:    add the new value and remove nothing; the reader sets the new statement to
//              preferred rank and the old preferred one to normal by hand (RankSteps).
//     Replace: add the new value and remove the best-ranked IUCN statements with another value by
//              statement id (-STATEMENT, so only the statements listed to the reader are removed).
//              Normal-rank statements under a preferred one are history and are never removed.
//   RecommendedChoice is Keep when the item already has a statement at preferred rank (the
//   convention of the 2025.2 updates, which keep history), else Replace. Either plan lists the
//   ranks still to set when the statements left would compete with the new one.
// - QuickStatements finds the statement to reference by its value, taking the last statement of
//   any rank that has it. When a deprecated statement, or more than one statement, has the IUCN
//   value, the reference could land on the wrong one, so no commands are made (Blocked).
// - Already cited: the statement with the IUCN value has a reference with this taxon's IUCN taxon
//   ID (P627), of any date, or one stated in the assessment's own item. No reference is added
//   then: the cache does not record a reference's URL or retrieved date, and a second reference
//   that differs only in its date would be added again after every download.
// - The add line comes before the removals, so a batch that stops early never leaves the item
//   without a status.

public enum StatusEditOutcome {
    /// The item's best-ranked statements that cite IUCN have the IUCN value.
    Agrees,
    /// The item's best-ranked statements that cite IUCN have another value.
    Differs,
    /// No statement that is not deprecated cites IUCN. Others may still have a value.
    Missing,
    /// Wikidata has no P141 value for the IUCN category (LR/cd).
    NoValue,
    /// QuickStatements cannot be told which statement to reference; see StatusEditBlock.
    Blocked,
}

public enum StatusEditBlock {
    None,
    /// A deprecated statement has the IUCN value.
    DeprecatedStatementHasTheValue,
    /// More than one statement has the IUCN value.
    SeveralStatementsHaveTheValue,
}

/// How a changed status is written: Replace removes the other current values; Keep leaves them,
/// and the reader sets the ranks by hand.
public enum StatusEditChoice { Replace, Keep }

/// One P141 statement on the taxon item, as `site build-db` stores it (taxon.wikidata_p141).
/// Value is the status item ("Q219127"); Rank is "preferred", "normal" or "deprecated"; StatedIn
/// lists the stated in (P248) items of its references (the first of each reference); TaxonIds the
/// IUCN taxon IDs (P627) in its references; References how many references it has; CitesIucn
/// whether one of them cites IUCN (an IUCN taxon ID, or stated in the Red List, an edition of it,
/// or IUCN).
public sealed record WikidataStatusStatement(
    [property: JsonPropertyName("id")] string Id,
    [property: JsonPropertyName("value")] string? Value,
    [property: JsonPropertyName("rank")] string Rank,
    [property: JsonPropertyName("statedIn")] IReadOnlyList<string>? StatedIn = null,
    [property: JsonPropertyName("taxonIds")] IReadOnlyList<string>? TaxonIds = null,
    [property: JsonPropertyName("references")] int References = 0,
    [property: JsonPropertyName("citesIucn")] bool CitesIucn = false) {
    [JsonIgnore]
    public bool IsDeprecated => string.Equals(Rank, "deprecated", StringComparison.Ordinal);
    [JsonIgnore]
    public bool IsPreferred => string.Equals(Rank, "preferred", StringComparison.Ordinal);

    /// The items that a reference stated in (P248) can name and still cite IUCN, besides the Red
    /// List's editions: the IUCN Red List (Q32059) and IUCN (Q48268).
    public static IReadOnlyList<string> IucnSourceItems { get; } = ["Q32059", "Q48268"];

    public static string ListToJson(IReadOnlyList<WikidataStatusStatement> statements) => JsonSerializer.Serialize(statements);

    /// Null for a NULL column (the item's statements are not known).
    public static IReadOnlyList<WikidataStatusStatement>? ListFromJson(string? json) =>
        string.IsNullOrWhiteSpace(json) ? null : JsonSerializer.Deserialize<List<WikidataStatusStatement>>(json);
}

/// Another Wikidata item that states the same IUCN taxon ID (P627) as the taxon's item, as
/// `site build-db` stores it (taxon.wikidata_other_items). TaxonIdDeprecated: it states the id only
/// at deprecated rank. Statements: its P141 statements, any rank; null when the Wikidata cache has
/// not downloaded it.
public sealed record WikidataOtherTaxonItem(
    [property: JsonPropertyName("qid")] string Qid,
    [property: JsonPropertyName("taxonIdDeprecated")] bool TaxonIdDeprecated = false,
    [property: JsonPropertyName("p141")] IReadOnlyList<WikidataStatusStatement>? Statements = null) {
    public static string ListToJson(IReadOnlyList<WikidataOtherTaxonItem> items) => JsonSerializer.Serialize(items);

    /// Null for a NULL column (no other item states the id).
    public static IReadOnlyList<WikidataOtherTaxonItem>? ListFromJson(string? json) =>
        string.IsNullOrWhiteSpace(json) ? null : JsonSerializer.Deserialize<List<WikidataOtherTaxonItem>>(json);
}

public sealed record StatusEditRequest {
    /// The taxon's item, which states the taxon's IUCN taxon id (P627).
    public required string TaxonItemQid { get; init; }
    /// Every P141 statement on the item, any rank.
    public required IReadOnlyList<WikidataStatusStatement> Statements { get; init; }
    /// The latest global assessment's category as published: "CR", "LR/nt".
    public required string Category { get; init; }
    public required long TaxonId { get; init; }
    public required long AssessmentId { get; init; }
    /// The assessment's own Wikidata item, cited in the reference; null when there is none.
    public string? AssessmentItemQid { get; init; }
    /// When the assessment was downloaded (P813); null leaves retrieved out.
    public DateOnly? Retrieved { get; init; }
}

/// The reference the commands add: Stated in (P248) only with an assessment item.
public sealed record StatusReference(string? StatedIn, long TaxonId, string Url, DateOnly? Retrieved) {
    /// The QuickStatements source pairs, tab-separated.
    public string ToSources() {
        var parts = new List<string>();
        if (StatedIn is not null) {
            parts.AddRange(["S248", StatedIn]);
        }
        parts.AddRange(["S627", Quote(TaxonId.ToString(CultureInfo.InvariantCulture)), "S854", Quote(Url)]);
        if (Retrieved is { } date) {
            parts.AddRange(["S813", "+" + date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) + "T00:00:00Z/11"]);
        }
        return string.Join('\t', parts);
    }

    private static string Quote(string value) => "\"" + value + "\"";
}

/// A rank the reader sets by hand after the batch. StatementId null: the statement the batch adds.
public sealed record StatusRankStep(string? StatementId, string Value, string Rank);

public sealed record StatusEditPlan {
    public required StatusEditOutcome Outcome { get; init; }
    public StatusEditChoice Choice { get; init; }
    public StatusEditBlock Block { get; init; }
    /// The P141 value for the IUCN category; null for NoValue.
    public string? TargetQid { get; init; }
    /// The item's current statements (not deprecated), preferred first.
    public IReadOnlyList<WikidataStatusStatement> Current { get; init; } = [];
    /// Current statements with no reference that cites IUCN; never removed.
    public IReadOnlyList<WikidataStatusStatement> Others { get; init; } = [];
    /// The commands add a new statement with the IUCN value.
    public bool AddsValue { get; init; }
    /// The commands add the reference: to the new statement, or to the one that has the value.
    public bool AddsReference { get; init; }
    /// The statement with the IUCN value already has a reference with this taxon's IUCN taxon ID,
    /// or stated in the assessment's item, so no reference is added.
    public bool AlreadyCited { get; init; }
    /// Statements the commands remove (Replace only).
    public IReadOnlyList<WikidataStatusStatement> Removes { get; init; } = [];
    /// Ranks to set by hand after the commands run, so the IUCN value is the item's only
    /// best-ranked one (Differs only).
    public IReadOnlyList<StatusRankStep> RankSteps { get; init; } = [];
    public StatusReference? Reference { get; init; }
    /// QuickStatements v1 commands, tab-separated; empty when there is nothing to run.
    public IReadOnlyList<string> Commands { get; init; } = [];
}

public static partial class WikidataStatusEdit {
    /// The category codes this compares, exactly as IUCN publishes them.
    private static readonly HashSet<string> KnownCodes = new(StringComparer.Ordinal) {
        "EX", "EW", "CR", "EN", "VU", "NT", "LC", "DD", "NE", "LR/nt", "LR/lc", "LR/cd",
    };

    /// The P141 value for an IUCN category code as published; null for LR/cd and any code with no
    /// value. Case-sensitive: "nt" is the pre-1994 Not Threatened, not NT.
    public static string? TargetQid(string? category) {
        var code = category?.Trim();
        return code is not null && KnownCodes.Contains(code) ? WikidataStatusValues.QidForCode(code) : null;
    }

    public static string AssessmentUrl(long taxonId, long assessmentId) =>
        $"https://www.iucnredlist.org/species/{taxonId.ToString(CultureInfo.InvariantCulture)}/{assessmentId.ToString(CultureInfo.InvariantCulture)}";

    /// Keep when the item has a statement at preferred rank that is not deprecated (it keeps earlier
    /// statuses as history), else Replace.
    public static StatusEditChoice RecommendedChoice(IReadOnlyList<WikidataStatusStatement> statements) =>
        statements.Any(s => s.IsPreferred) ? StatusEditChoice.Keep : StatusEditChoice.Replace;

    public static StatusEditPlan Plan(StatusEditRequest request, StatusEditChoice choice = StatusEditChoice.Replace) {
        var item = request.TaxonItemQid.Trim();
        if (!ItemId().IsMatch(item)) {
            throw new ArgumentException($"Not an item id: {request.TaxonItemQid}", nameof(request));
        }
        var current = request.Statements.Where(s => !s.IsDeprecated)
            .OrderBy(s => s.IsPreferred ? 0 : 1)
            .ToList();
        var others = current.Where(s => !s.CitesIucn).ToList();
        var target = TargetQid(request.Category);
        if (target is null) {
            return new StatusEditPlan { Outcome = StatusEditOutcome.NoValue, Choice = choice, Current = current, Others = others };
        }

        var withTarget = request.Statements.Where(s => s.Value == target).ToList();
        var block = withTarget.Any(s => s.IsDeprecated) ? StatusEditBlock.DeprecatedStatementHasTheValue
            : withTarget.Count > 1 ? StatusEditBlock.SeveralStatementsHaveTheValue
            : StatusEditBlock.None;
        if (block != StatusEditBlock.None) {
            return new StatusEditPlan {
                Outcome = StatusEditOutcome.Blocked, Choice = choice, Block = block, TargetQid = target, Current = current, Others = others,
            };
        }

        var assessmentItem = request.AssessmentItemQid?.Trim() is { } q && ItemId().IsMatch(q) ? q : null;
        var reference = new StatusReference(assessmentItem, request.TaxonId, AssessmentUrl(request.TaxonId, request.AssessmentId), request.Retrieved);
        var existing = withTarget.SingleOrDefault();
        var taxonId = request.TaxonId.ToString(CultureInfo.InvariantCulture);
        var alreadyCited = existing is not null
            && ((existing.TaxonIds ?? []).Contains(taxonId, StringComparer.Ordinal)
                || (assessmentItem is not null && (existing.StatedIn ?? []).Contains(assessmentItem, StringComparer.Ordinal)));
        var iucn = current.Where(s => s.CitesIucn).ToList();
        var best = iucn.Where(s => s.IsPreferred).ToList() is { Count: > 0 } preferred ? preferred : iucn;

        StatusEditOutcome outcome;
        var removes = new List<WikidataStatusStatement>();
        var rankSteps = new List<StatusRankStep>();
        if (iucn.Count == 0) {
            outcome = StatusEditOutcome.Missing;
        } else if (best.All(s => s.Value == target)) {
            outcome = StatusEditOutcome.Agrees;
        } else {
            outcome = StatusEditOutcome.Differs;
            if (choice == StatusEditChoice.Replace) {
                removes.AddRange(best.Where(s => s.Value != target));
            }
            // The statements left with another value: the new one has to outrank them.
            var competing = current.Where(s => s != existing && s.Value != target && !removes.Contains(s)).ToList();
            var targetPreferred = existing?.IsPreferred == true;
            if (!targetPreferred && competing.Count > 0) {
                rankSteps.Add(new StatusRankStep(existing?.Id, target, "preferred"));
            }
            rankSteps.AddRange(competing.Where(s => s.IsPreferred).Select(s => new StatusRankStep(s.Id, s.Value ?? string.Empty, "normal")));
        }

        var addsValue = existing is null;
        var addsReference = !alreadyCited;
        var commands = new List<string>();
        if (addsValue || addsReference) {
            commands.Add(string.Join('\t', item, "P141", target, reference.ToSources()));
        }
        foreach (var statement in removes) {
            commands.Add(RemoveStatementCommand(statement.Id));
        }
        return new StatusEditPlan {
            Outcome = outcome,
            Choice = choice,
            TargetQid = target,
            Current = current,
            Others = others,
            AddsValue = addsValue,
            AddsReference = addsReference,
            AlreadyCited = alreadyCited,
            Removes = removes,
            RankSteps = rankSteps,
            Reference = reference,
            Commands = commands,
        };
    }

    /// "-STATEMENT<TAB>Q140$..." removes exactly that statement. Throws for a malformed id, which
    /// could make QuickStatements remove something else or fail half way.
    public static string RemoveStatementCommand(string statementId) {
        var id = statementId.Trim();
        if (!StatementId().IsMatch(id)) {
            throw new ArgumentException($"Not a statement id: {statementId}", nameof(statementId));
        }
        return "-STATEMENT\t" + id;
    }

    [GeneratedRegex(@"^Q[1-9][0-9]*$")]
    private static partial Regex ItemId();

    // Wikibase statement ids: the entity id (any case, as older items have "q140$"), "$", a GUID.
    [GeneratedRegex(@"^[Qq][1-9][0-9]*\$[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$")]
    private static partial Regex StatementId();
}
