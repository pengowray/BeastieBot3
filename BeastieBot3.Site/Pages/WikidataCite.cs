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

    /// A Wikidata search for an item for the assessment, shown when the site's data has none, so the
    /// reader can check that none was made after the data was downloaded.
    public string? SearchUrl { get; init; }

    public string? ItemUrl => ItemQid is null ? null : SiteFormat.WikidataUrl(ItemQid);
}

public static partial class WikidataCite {
    public const string CiteQBoxId = "wikitext-cite-q";
    public const string CommandsBoxId = "wikidata-commands";

    /// The view for one assessment. parts: its citation parts, or null when its details were not
    /// downloaded (no commands can be made then). model: the item model from the site database, or
    /// null when it could not be read (no commands then either). onError is told about any renderer
    /// that throws; only the box that renderer makes is left out.
    public static WikidataCiteView Build(AssessmentRow assessment, IucnCitationParts? parts, string? taxonQid, WikidataItemModel? model,
        CiteQOptions citeQOptions, Action<string, Exception>? onError = null) {
        var itemQid = ItemId(assessment.WikidataItemQid);
        var taxonItem = ItemId(taxonQid);

        T? Try<T>(string what, Func<T> make) where T : class {
            try {
                return make();
            } catch (Exception e) {
                onError?.Invoke(what, e);
                return null;
            }
        }

        if (itemQid is not null) {
            var citeQ = Try("CiteQ", () => WikidataCitation.CiteQ(itemQid, citeQOptions));
            IReadOnlyList<string>? add = null;
            // With no list of the item's properties, what it lacks is not known, so nothing is
            // offered (rather than every statement).
            if (parts is not null && model is not null && !string.IsNullOrWhiteSpace(assessment.WikidataItemProperties)) {
                var present = Properties(assessment.WikidataItemProperties);
                add = Try("AddMissingCommands", () => WikidataCitation.AddMissingCommands(parts, itemQid, present, taxonItem, model));
            }
            var commands = add is { Count: > 0 } ? add : null;
            return new WikidataCiteView {
                ItemQid = itemQid,
                CiteQ = string.IsNullOrWhiteSpace(citeQ) ? null : new WikitextBox(CiteQBoxId, SiteText.LabelCiteQ, "{{cite Q}}", citeQ, Rows: 2),
                Commands = commands is null ? null : CommandsBox(SiteText.LabelAddStatements, commands),
                QuickStatementsUrl = commands is null ? null : Try("QuickStatementsUrl", () => WikidataCitation.QuickStatementsUrl(commands)),
                AddedStatements = commands is null ? [] : StatementsAdded(commands),
            };
        }

        var view = new WikidataCiteView { SearchUrl = parts is null ? null : SearchUrl(parts) };
        if (parts is null || model is null) {
            return view;
        }
        var create = Try("CreateItemCommands", () => WikidataCitation.CreateItemCommands(parts, taxonItem, model));
        if (create is not { Count: > 0 }) {
            return view;
        }
        return view with {
            Commands = CommandsBox(SiteText.LabelCreateItem, create),
            QuickStatementsUrl = Try("QuickStatementsUrl", () => WikidataCitation.QuickStatementsUrl(create)),
        };
    }

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
        foreach (var line in commands) {
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
