using System.Text.RegularExpressions;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Display;

namespace BeastieBot3.Site.Pages;

/// The "{{cite Q}} citation from Wikidata" part of a taxon page's wikitext section, for the
/// assessment shown. With an item: the {{cite Q}} box, and QuickStatements commands that add the
/// statements the item lacks (when it lacks any). Without one: QuickStatements commands that create
/// it. The site never edits Wikidata; the reader runs the commands in QuickStatements.
public sealed record WikidataCiteView {
    /// The assessment's Wikidata item ("Q123"), or null when the site's data has none.
    public string? ItemQid { get; init; }

    /// The {{cite Q}} wikitext; null when there is no item or it could not be made.
    public WikitextBox? CiteQ { get; init; }

    /// QuickStatements commands that create the item (no item) or add its missing statements.
    public WikitextBox? Commands { get; init; }

    /// Opens QuickStatements with Commands filled in.
    public string? QuickStatementsUrl { get; init; }

    /// What the add commands add, as "publisher (P123)"; empty for the create commands.
    public IReadOnlyList<string> AddedStatements { get; init; } = [];

    /// The title and label the commands replace (WikidataCitation.FixCommands); empty when none.
    public IReadOnlyList<WikidataItemChange> Changes { get; init; } = [];

    /// The name the item's title and label use, and which title it was read from
    /// (WikidataCitation.TitleNameFor); null when not known or not usable.
    public TitleName? Name { get; init; }

    /// IUCN's citation name, which for an older assessment can be newer than Name.
    public string? CitationName { get; init; }

    /// True when the commands set the item's title or label (create, add or fix).
    public bool CommandsUseName { get; init; }

    /// No item and no create commands: no usable name is known (IUCN's citation has an internal
    /// name such as "Larus glaucoides_old", and Crossref's title is not known).
    public bool NoUsableName { get; init; }

    /// For an errata version that shares the Wikidata item of the assessment it corrects (same DOI):
    /// that assessment's id. The page then offers no commands for the item.
    public long? ItemIsForAssessmentId { get; init; }

    /// Whether this site has a page for ItemIsForAssessmentId.
    public bool ItemAssessmentHasPage { get; init; }

    /// Show the name note: the name from the item's title or Crossref's title differs from IUCN's
    /// citation name (WikidataCitation.SameName), or the commands use IUCN's citation name because
    /// neither title is known.
    public bool ShowNameNote => Name is { } name && CitationName is { } cited
        && (name.Source == TitleNameSource.IucnCitation ? CommandsUseName : !WikidataCitation.SameName(name.Name, cited));

    /// A Wikidata search for an item for the assessment, shown when the site's data has none, so the
    /// reader can check that none was made after the data was downloaded.
    public string? SearchUrl { get; init; }

    public string? ItemUrl => ItemQid is null ? null : SiteFormat.WikidataUrl(ItemQid);
}

/// Why the IUCN status part shows no comparison: the commands are only for the latest global
/// assessment of a species or subspecies whose Wikidata item states its IUCN taxon id (P627), as in
/// the Wikidata status dry run, and not for the links that dry run holds for review (tiers C and D).
public enum WikidataStatusScope {
    /// The comparison is shown, with commands when WikidataStatusEdit can make them.
    Offered,
    /// The IUCN taxon id is on more than one item (the dry run's tier C): every item is compared,
    /// with no commands.
    TaxonIdOnSeveralItems,
    /// The taxon's item states the IUCN taxon id only at deprecated rank (tier D): compared, with no
    /// commands.
    TaxonIdDeprecated,
    /// A variety or a subpopulation.
    NotSpeciesOrSubspecies,
    /// No Wikidata item for the taxon.
    NoItem,
    /// The taxon's item was matched by name and does not state the taxon's IUCN taxon id.
    MatchedByName,
    /// The Wikidata cache had not downloaded the item, so its statements are not known.
    StatementsUnknown,
}

/// The "IUCN conservation status on Wikidata" part of the wikitext section: the taxon item's P141
/// beside the latest global assessment's category, and QuickStatements commands that bring it up
/// to date (WikidataStatusEdit).
public sealed record WikidataStatusView {
    public required WikidataStatusScope Scope { get; init; }
    public required long TaxonId { get; init; }
    public string? TaxonItemQid { get; init; }
    /// "20 August 2026": when the Wikidata cache downloaded the taxon item.
    public string? DownloadedText { get; init; }
    /// The assessment's own Wikidata item, cited in the reference; null when there is none.
    public string? AssessmentItemQid { get; init; }
    /// The taxon item's P141 statements, any rank (Offered and the review scopes).
    public IReadOnlyList<WikidataStatusStatement> Statements { get; init; } = [];
    /// The other items that state the taxon's IUCN taxon id (TaxonIdOnSeveralItems).
    public IReadOnlyList<WikidataOtherTaxonItem> OtherItems { get; init; } = [];
    /// The plan whose commands are shown first: for a changed status, the recommended choice
    /// (WikidataStatusEdit.RecommendedChoice). Null outside Offered, or when WikidataStatusEdit failed.
    public StatusEditPlan? Plan { get; init; }
    /// For a changed status: the plan for the other choice, shown in a details element. Null when
    /// its commands are the same as Plan's.
    public StatusEditPlan? AltPlan { get; init; }
    public WikitextBox? Commands { get; init; }
    public string? QuickStatementsUrl { get; init; }
    public WikitextBox? AltCommands { get; init; }
    public string? AltQuickStatementsUrl { get; init; }

    public string? TaxonItemUrl => TaxonItemQid is null ? null : SiteFormat.WikidataUrl(TaxonItemQid);

    /// The review scopes, which show the comparison table with no commands.
    public bool IsReview => Scope is WikidataStatusScope.TaxonIdOnSeveralItems or WikidataStatusScope.TaxonIdDeprecated;

    /// False when the comparison could not be made (WikidataStatusEdit threw): the page then leaves
    /// the part out rather than show its heading alone.
    public bool HasContent => Scope != WikidataStatusScope.Offered || Plan is not null;
}

/// The model of the _WikidataStatus partial: the view, the latest global assessment it compares,
/// and the Red List version ("2026-1").
public sealed record WikidataStatusPartial(WikidataStatusView View, AssessmentRow Latest, string? Release);

/// The model of the _WikidataStatusPlan partial: one plan, its commands box and link, whether it is
/// the other choice (shown after the first one's reference), and the Red List version.
public sealed record WikidataStatusPlanPartial(StatusEditPlan Plan, WikitextBox? Box, string? QuickStatementsUrl, bool IsAlternative,
    string? Release);

public static partial class WikidataCite {
    public const string CiteQBoxId = "wikitext-cite-q";
    public const string CommandsBoxId = "wikidata-commands";
    public const string StatusCommandsBoxId = "wikidata-status-commands";
    public const string StatusAltCommandsBoxId = "wikidata-status-alt-commands";

    /// The IUCN status part for a taxon's latest global assessment. parts gives the date the
    /// assessment was downloaded (retrieved, P813); null leaves it out. onError is told when
    /// WikidataStatusEdit throws; the part then shows the comparison scope with no commands.
    public static WikidataStatusView BuildStatus(TaxonRow taxon, AssessmentRow latestGlobal, IucnCitationParts? parts,
        Action<string, Exception>? onError = null) {
        var item = ItemId(taxon.WikidataQid);
        var view = new WikidataStatusView { Scope = WikidataStatusScope.Offered, TaxonId = taxon.TaxonId, TaxonItemQid = item };
        if (taxon.Kind is not (TaxonKinds.Species or TaxonKinds.Subspecies)) {
            return view with { Scope = WikidataStatusScope.NotSpeciesOrSubspecies };
        }
        if (item is null) {
            return view with { Scope = WikidataStatusScope.NoItem };
        }
        if (!taxon.WikidataItemStatesTaxonId) {
            return view with { Scope = WikidataStatusScope.MatchedByName };
        }
        IReadOnlyList<WikidataStatusStatement>? statements;
        IReadOnlyList<WikidataOtherTaxonItem>? otherItems;
        try {
            statements = WikidataStatusStatement.ListFromJson(taxon.WikidataP141);
            otherItems = WikidataOtherTaxonItem.ListFromJson(taxon.WikidataOtherItems);
        } catch (System.Text.Json.JsonException e) {
            onError?.Invoke("WikidataStatusStatement.ListFromJson", e);
            statements = null;
            otherItems = null;
        }
        view = view with {
            DownloadedText = taxon.WikidataItemDownloaded is { } day ? SiteFormat.Date(day) : null,
            AssessmentItemQid = ItemId(latestGlobal.WikidataItemQid),
            Statements = statements ?? [],
            OtherItems = otherItems ?? [],
        };
        // The dry run holds these links for review (tiers C and D); the page compares every item.
        if (otherItems is { Count: > 0 }) {
            return view with { Scope = WikidataStatusScope.TaxonIdOnSeveralItems };
        }
        if (taxon.WikidataP627Deprecated) {
            return view with { Scope = WikidataStatusScope.TaxonIdDeprecated };
        }
        if (statements is null) {
            return view with { Scope = WikidataStatusScope.StatementsUnknown };
        }

        var request = new StatusEditRequest {
            TaxonItemQid = item,
            Statements = statements,
            Category = latestGlobal.Category,
            TaxonId = latestGlobal.TaxonId,
            AssessmentId = latestGlobal.AssessmentId,
            AssessmentItemQid = view.AssessmentItemQid,
            Retrieved = parts?.DownloadedAtUtc is { } at ? DateOnly.FromDateTime(at) : null,
        };
        var recommended = WikidataStatusEdit.RecommendedChoice(statements);
        var other = recommended == StatusEditChoice.Replace ? StatusEditChoice.Keep : StatusEditChoice.Replace;
        StatusEditPlan plan, alt;
        try {
            plan = WikidataStatusEdit.Plan(request, recommended);
            alt = WikidataStatusEdit.Plan(request, other);
        } catch (Exception e) {
            onError?.Invoke("WikidataStatusEdit.Plan", e);
            return view;
        }
        // The other choice only when its commands differ: with nothing it may remove (every IUCN
        // statement is under a preferred one), Replace makes the same commands as Keep.
        var showAlt = plan.Outcome == StatusEditOutcome.Differs && !alt.Commands.SequenceEqual(plan.Commands);
        return view with {
            Plan = plan,
            AltPlan = showAlt ? alt : null,
            Commands = plan.Commands.Count == 0 ? null : StatusBox(StatusCommandsBoxId, SiteText.LabelStatusCommands, plan.Commands),
            QuickStatementsUrl = plan.Commands.Count == 0 ? null : FittingUrl(WikidataCitation.QuickStatementsUrl(plan.Commands)),
            AltCommands = !showAlt || alt.Commands.Count == 0 ? null
                : StatusBox(StatusAltCommandsBoxId, SiteText.LabelStatusChoiceCommands(alt.Choice), alt.Commands),
            AltQuickStatementsUrl = !showAlt || alt.Commands.Count == 0 ? null : FittingUrl(WikidataCitation.QuickStatementsUrl(alt.Commands)),
        };
    }

    private static WikitextBox StatusBox(string id, string label, IReadOnlyList<string> commands) =>
        new(id, label, "QuickStatements", string.Join('\n', commands), Rows: Math.Clamp(commands.Count + 1, 2, 6),
            CopyName: SiteText.CopyQuickStatements);

    /// The view for one assessment. parts: its citation parts, or null when its details were not
    /// downloaded (no commands can be made then). model: the item model from the site database, or
    /// null when it could not be read (no commands then either). hasPage: whether this site has a
    /// page for an assessment id of the taxon. onError is told about any renderer that throws; only
    /// the box that renderer makes is left out.
    public static WikidataCiteView Build(AssessmentRow assessment, IucnCitationParts? parts, string? taxonQid, WikidataItemModel? model,
        CiteQOptions citeQOptions, Action<string, Exception>? onError = null, Func<long, bool>? hasPage = null) {
        var itemQid = ItemId(assessment.WikidataItemQid);
        var taxonItem = ItemId(taxonQid);
        var citationName = parts is null ? null : WikidataCitation.NameText(parts.ScientificName) is { Length: > 0 } cleaned ? cleaned : null;

        T? Try<T>(string what, Func<T> make) where T : class {
            try {
                return make();
            } catch (Exception e) {
                onError?.Invoke(what, e);
                return null;
            }
        }

        if (itemQid is not null) {
            // {{cite Q}} takes |access-date= only for an item with a URL (P953); without one CS1
            // reports "access-date without URL".
            var withUrl = citeQOptions with { ItemHasUrl = Properties(assessment.WikidataItemProperties).Contains("P953") };
            var citeQ = Try("CiteQ", () => WikidataCitation.CiteQ(itemQid, withUrl));
            var citeQBox = string.IsNullOrWhiteSpace(citeQ) ? null : new WikitextBox(CiteQBoxId, SiteText.LabelCiteQ, "{{cite Q}}", citeQ, Rows: 2);
            // An errata version sharing the item of the assessment it corrects: every command for
            // the item comes from the assessment its DOI names, on that assessment's page. Commands
            // built from this row would give the item this row's article number, URL and authors,
            // and the two pages would undo each other's label.
            if (assessment.WikidataItemAssessmentId is { } owner && owner != assessment.AssessmentId) {
                return new WikidataCiteView {
                    ItemQid = itemQid,
                    CiteQ = citeQBox,
                    ItemIsForAssessmentId = owner,
                    ItemAssessmentHasPage = hasPage?.Invoke(owner) == true,
                };
            }
            var titles = WikidataTitle.ListFromJson(assessment.WikidataItemTitles);
            IReadOnlyList<string> add = [];
            // With no list of the item's properties, what it lacks is not known, so nothing is
            // offered (rather than every statement).
            if (parts is not null && model is not null && !string.IsNullOrWhiteSpace(assessment.WikidataItemProperties)) {
                var present = Properties(assessment.WikidataItemProperties);
                add = Try("AddMissingCommands", () => WikidataCitation.AddMissingCommands(parts, itemQid, present, taxonItem, model, titles)) ?? [];
            }
            // A title is changed only when its exact text and language are known
            // (wikidata_item_titles); the label whenever it differs from the model's.
            var fix = parts is null || model is null ? WikidataItemFix.None
                : Try("FixCommands", () => WikidataCitation.FixCommands(parts, itemQid, titles, assessment.WikidataItemLabelEn, model))
                    ?? WikidataItemFix.None;
            IReadOnlyList<string>? commands = add.Count + fix.Commands.Count > 0 ? [.. add, .. fix.Commands] : null;
            // From the add commands only: the fix commands name P1476 and Len for a title and label
            // the item already has.
            var added = StatementsAdded(add);
            return new WikidataCiteView {
                ItemQid = itemQid,
                CiteQ = citeQBox,
                Commands = commands is null ? null
                    : CommandsBox(fix.Changes.Count > 0 ? SiteText.LabelUpdateItem : SiteText.LabelAddStatements, commands),
                QuickStatementsUrl = commands is null ? null : FittingUrl(Try("QuickStatementsUrl", () => WikidataCitation.QuickStatementsUrl(commands))),
                AddedStatements = added,
                Changes = fix.Changes,
                Name = parts is null ? null : WikidataCitation.TitleNameFor(parts, titles),
                CitationName = citationName,
                CommandsUseName = fix.Changes.Count > 0 || add.Any(c => c.Split('\t') is [_, "P1476" or "Len", ..]),
            };
        }

        var view = new WikidataCiteView { SearchUrl = parts is null ? null : SearchUrl(parts), CitationName = citationName };
        if (parts is null || model is null) {
            return view;
        }
        var name = WikidataCitation.TitleNameFor(parts);
        var create = Try("CreateItemCommands", () => WikidataCitation.CreateItemCommands(parts, taxonItem, model));
        if (create is not { Count: > 0 }) {
            return view with { NoUsableName = name is null };
        }
        return view with {
            Commands = CommandsBox(SiteText.LabelCreateItem, create),
            QuickStatementsUrl = FittingUrl(Try("QuickStatementsUrl", () => WikidataCitation.QuickStatementsUrl(create))),
            Name = name,
            CommandsUseName = true,
        };
    }

    // A link too long for browsers or the tool is left out; the commands box can still be copied.
    private static string? FittingUrl(string? url) =>
        url is not null && WikidataCitation.QuickStatementsUrlFits(url) ? url : null;

    private static WikitextBox CommandsBox(string label, IReadOnlyList<string> commands) =>
        new(CommandsBoxId, label, "QuickStatements", string.Join('\n', commands), Rows: Math.Min(commands.Count, 12),
            CopyName: SiteText.CopyQuickStatements);

    /// "Q123" from " q123 ", or null when the text is not an item id.
    public static string? ItemId(string? text) {
        if (string.IsNullOrWhiteSpace(text)) {
            return null;
        }
        var id = text.Trim().ToUpperInvariant();
        return ItemIdPattern().IsMatch(id) ? id : null;
    }

    /// The properties listed in wikidata_item_properties ("P31 P356 P2093 Len"). Property ids are
    /// upper-cased; other tokens keep their case, and the set ignores case. Upper-casing everything
    /// turned the label token "Len" into "LEN", so every item's label was offered again as missing.
    public static IReadOnlySet<string> Properties(string? list) =>
        (list ?? string.Empty)
            .Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(p => p.Length > 1 && (p[0] == 'p' || p[0] == 'P') && p.Skip(1).All(char.IsAsciiDigit) ? p.ToUpperInvariant() : p)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

    /// What a QuickStatements v1 batch adds, from the second field of each tab-separated command,
    /// in the order of first appearance: "publisher (P123)", "label (en)". A CREATE line adds nothing.
    public static IReadOnlyList<string> StatementsAdded(IEnumerable<string> commands) {
        var seen = new List<string>();
        // A line that starts with "-" removes something, so it adds nothing.
        foreach (var line in commands.Where(c => !c.StartsWith('-'))) {
            var fields = line.Split('\t');
            if (fields.Length < 2) {
                continue;
            }
            var what = fields[1].Trim();
            string? text = null;
            if (PropertyPattern().IsMatch(what)) {
                text = SiteText.WikidataPropertyLabel(what.ToUpperInvariant());
            } else if (TermPattern().Match(what) is { Success: true } term) {
                text = SiteText.WikidataTermLabel(term.Groups[1].Value[0], term.Groups[2].Value);
            }
            if (text is not null && !seen.Contains(text)) {
                seen.Add(text);
            }
        }
        return seen;
    }

    /// A Wikidata search for an item for the assessment: by DOI when there is one (Wikidata stores
    /// DOIs in capitals), otherwise by the article number that assessment items have in their label.
    public static string SearchUrl(IucnCitationParts parts) {
        var query = string.IsNullOrWhiteSpace(parts.Doi)
            ? $"\"{parts.ArticleNumber}\""
            : "haswbstatement:P356=" + parts.Doi.Trim().ToUpperInvariant();
        return "https://www.wikidata.org/w/index.php?title=Special:Search&search=" + Uri.EscapeDataString(query);
    }

    [GeneratedRegex("^Q[1-9][0-9]{0,11}$")]
    private static partial Regex ItemIdPattern();

    [GeneratedRegex("^[Pp][1-9][0-9]{0,8}$")]
    private static partial Regex PropertyPattern();

    // QuickStatements v1 terms: L (label), D (description), A (alias) or S (sitelink), then a language
    // code or a site id ("Len", "Senwiki").
    [GeneratedRegex("^([LDAS])([a-z][a-z0-9-]*)$")]
    private static partial Regex TermPattern();
}
