using System.Globalization;
using BeastieBot3.Shared.Wikitext;

namespace BeastieBot3.Site.Display;

// Strings of the taxon page's Wikidata part: {{cite Q}}, QuickStatements commands for the
// assessment's item, and the taxon item's IUCN conservation status (P141).

public static partial class SiteText {
    // Taxon page: {{cite Q}} and QuickStatements, the last part of the wikitext section. The site
    // never edits Wikidata; the reader runs the QuickStatements commands with their own account.
    public const string HeadingWikidataCite = "{{cite Q}} citation from Wikidata";
    /// Before a link to the item ("Q123").
    public const string WikidataItemBefore = "Wikidata item for this assessment: ";
    public const string LabelCiteQ = "{{cite Q}} citation";
    /// Link("English Wikipedia guidance") + after: the 2017 deletion discussion of Template:Cite Q
    /// and WP:Citing sources#Wikidata.
    public const string CiteQGuidanceLink = "English Wikipedia guidance";
    public const string CiteQGuidanceUrl = "https://en.wikipedia.org/wiki/Wikipedia:Citing_sources#Wikidata";
    public const string CiteQGuidanceAfter =
        ": check every detail that {{cite Q}} takes from Wikidata, and don't use {{cite Q}} in an article whose citations mostly give authors as “Last, First” or in Vancouver style.";
    /// statements: "publisher (P123), DOI (P356)".
    public static string MissingStatements(string statements) => $"The item is missing these statements: {statements}.";
    /// After MissingStatements, in the same paragraph.
    public const string AddStatementsLine = "To add them, run these commands in QuickStatements with your Wikidata account.";
    public const string LabelAddStatements = "QuickStatements commands to add the missing statements";
    public const string NoWikidataItem = "No Wikidata item found for this assessment.";
    /// Before + link("search Wikidata") + after.
    public const string SearchWikidataBefore = "Before creating an item, ";
    public const string SearchWikidataLink = "search Wikidata";
    public const string SearchWikidataAfter = " in case one was added after this site's data was downloaded.";
    public const string CreateItemLine =
        "To create the item, run these commands in QuickStatements with your Wikidata account. Then cite the new item with {{cite Q|<new item id>}}.";
    public const string LabelCreateItem = "QuickStatements commands to create the item";
    public const string OpenInQuickStatements = "Open in QuickStatements";
    public const string CopyQuickStatements = "Copy QuickStatements commands";

    // The item's title and English label, when they differ from the ones the item model gives
    // (WikidataCitation.FixCommands). Shown as a table: what, on Wikidata now, after the commands.
    /// also: the item is missing statements too, listed just above. title, label: what changes.
    public static string ReplacesLine(bool also, bool title, bool label) {
        var what = (title, label) switch {
            (true, true) => "the item's title and English label",
            (true, false) => "the item's title",
            _ => "the item's English label",
        };
        return also ? $"The commands also replace {what}:" : $"The commands replace {what}:";
    }
    public const string ColChange = "Replaced";
    public const string ColNow = "On Wikidata now";
    public const string ColAfter = "After the commands";
    /// A title value with its language, when the language changes too: "Myrmecophaga tridactyla (la)".
    public static string TitleWithLanguage(string text, string? language) => $"{text} ({language})";
    /// Under the table when the title changes.
    public const string TitleChangeNote = "{{cite Q}} shows the item's title as the title of the work.";
    public const string RunCommandsLine = "Run these commands in QuickStatements with your Wikidata account.";
    public const string LabelUpdateItem = "QuickStatements commands to update the item";

    // The name in the item's title and label (WikidataCitation.TitleNameFor), shown when it
    // differs from IUCN's citation name, or when the commands use IUCN's citation name because
    // neither the item's title nor Crossref's title gives a name. The site says only which title a
    // name was read from: neither title proves the name an assessment first appeared under.
    public static string NameFromItemTitle(string name, string cited) =>
        $"The name in the item's title (P1476) is {name}. {CitedName(cited)}";
    public static string NameRegistered(string name, string cited) =>
        $"The name in the title registered with Crossref for this assessment's DOI is {name}. {CitedName(cited)}";
    private static string CitedName(string cited) => WikidataCitation.IsIucnInternalName(cited)
        ? $"IUCN's citation gives a different name, {cited}, which IUCN uses as an internal name for a taxon it has replaced."
        : $"IUCN's citation gives a different name, {cited}.";
    /// After NameFromItemTitle or NameRegistered, when the commands set the title or label.
    public static string NameCommandsUse(string name) => $"The commands use {name}.";
    public static string NameFromIucnCitation(string cited) =>
        $"The commands use the name in IUCN's citation, {cited}. IUCN's citation gives the taxon's current name, even for an older assessment.";
    /// No item, and no usable name for a new one.
    public static string NoUsableName(string cited) =>
        $"No commands: the only name this site has for this assessment is IUCN's citation name, {cited}, which IUCN uses as an internal name for a taxon it has replaced.";

    // The commands leave out main subject (P921) when the taxon's item may not be the taxon's
    // (TaxonItemDoubt). One line beside the commands box, shown only when the commands would
    // otherwise write it.
    public static string MainSubjectSeveralItems(int itemCount, long taxonId) =>
        $"The commands do not include main subject (P921), because {itemCount} Wikidata items state IUCN taxon ID (P627) {taxonId}. First check which one is the item for this taxon.";
    public static string MainSubjectDeprecated(string taxonItem, long taxonId) =>
        $"The commands do not set main subject (P921) to {taxonItem}, because {taxonItem} states IUCN taxon ID (P627) {taxonId} only at deprecated rank. First check that {taxonItem} is the item for this taxon.";

    // An errata version that shares the Wikidata item of the assessment it corrects (same DOI).
    /// Before + link(BorrowedItemLink) + ".".
    public const string BorrowedItemBefore =
        "This errata version has the same DOI as the assessment it corrects, so it shares that assessment's Wikidata item. Commands to update the item are on ";
    public const string BorrowedItemLink = "that assessment's page";
    public static string BorrowedItemNoPage(long assessmentId) =>
        $"This errata version has the same DOI as the assessment it corrects (assessment {assessmentId}), so it shares that assessment's Wikidata item. This site has no page for that assessment, so it offers no commands to update the item.";

    // Taxon page: the taxon item's IUCN conservation status (P141) beside the latest global
    // assessment, after the {{cite Q}} part. Wikidata's own English labels name the status values
    // ("critically endangered (Q219127)").
    public const string HeadingWikidataStatus = "IUCN conservation status on Wikidata";
    public const string StatusColSource = "Source";
    public const string StatusColValue = "IUCN conservation status (P141)";
    public static string StatusRowIucn(string? release) => release is null ? "IUCN Red List" : $"IUCN Red List {release}";
    /// Before a link to the taxon item, then StatusRowWikidataAfter.
    public const string StatusRowWikidataBefore = "Wikidata item ";
    public static string StatusRowWikidataAfter(string? downloaded) => downloaded is null ? string.Empty : $", downloaded {downloaded}";
    /// A P141 value: Wikidata's English label and the item id, or the id alone for a value not in
    /// the P141 list.
    public static string StatusValue(string? qid) {
        if (qid is null) {
            return "no value";
        }
        return WikidataStatusValues.Describe(qid) is { } value ? $"{value.LabelEn} ({value.Qid})" : qid;
    }
    public static string StatusValueWithRank(string? qid, string rank) => $"{StatusValue(qid)}, {rank} rank";
    /// A statement in the comparison table: its value, its rank when the item has several, and a
    /// note when no reference cites IUCN.
    public static string StatusStatement(WikidataStatusStatement statement, bool showRank) {
        var text = showRank ? StatusValueWithRank(statement.Value, statement.Rank) : StatusValue(statement.Value);
        return statement.CitesIucn ? text : $"{text}, {(statement.References == 0 ? "no reference" : "no reference to IUCN")}";
    }
    public const string StatusNone = "none";
    public const string StatusNotDownloaded = "not downloaded";
    /// After a link to an item in the comparison table.
    public const string StatusRowTaxonIdDeprecated = ", IUCN taxon ID at deprecated rank";
    /// Under the table, when some statements have no reference to IUCN.
    public const string StatusOthersNote =
        "Only statements with a reference to IUCN are compared with the assessment, and the commands never remove the other statements.";
    /// The IUCN row when the category has no P141 value: "no value for LR/cd".
    public static string StatusNoValueFor(string code) => $"no value for {code}";

    /// Under the table: the IUCN category has no value of its own on Wikidata.
    public static string StatusMappedPossiblyExtinct(string iucnLabel, string value) =>
        $"{iucnLabel} is {value} on Wikidata, which has no value for possibly extinct.";
    public static string StatusMappedPossiblyExtinctInTheWild(string iucnLabel, string value) =>
        $"{iucnLabel} is {value} on Wikidata, which has no value for possibly extinct in the wild.";
    public static string StatusMappedLowerRisk(string iucnLabel, string value) =>
        $"{iucnLabel} is {value} on Wikidata, which has no Lower Risk values.";

    public const string StatusAgrees = "Wikidata gives the same status.";
    public const string StatusAgreesCited = "Wikidata gives the same status, with a reference to this assessment's Wikidata item. No commands needed.";
    public static string StatusAgreesCitedTaxonId(long taxonId) =>
        $"Wikidata gives the same status, with a reference that has IUCN taxon ID (P627) {taxonId}. No commands needed.";
    public const string StatusDiffers = "Wikidata gives a different status.";
    public const string StatusMissing = "Wikidata gives no IUCN conservation status for this taxon.";
    public const string StatusMissingNoIucnReference = "None of the item's IUCN conservation status statements has a reference to IUCN.";
    public static string StatusNoValue(string iucnLabel, string code) =>
        $"No commands: Wikidata has no IUCN conservation status value for {iucnLabel} ({code}).";
    public static string StatusBlockedSeveral(int count, string value) =>
        $"No commands: {count} statements on the item have {value}, and QuickStatements cannot be told which one gets the reference.";
    public static string StatusBlockedDeprecated(string value) =>
        $"No commands: a deprecated statement on the item has {value}, and QuickStatements could add the reference to that statement.";
    public const string StatusNotSpecies = "No commands: this site offers them for species and subspecies only.";
    public static string StatusNoItem(long taxonId) => $"No commands: no Wikidata item found that states IUCN taxon ID (P627) {taxonId}.";
    /// Before + link(item) + StatusMatchedByNameAfter.
    public const string StatusMatchedByNameBefore = "No commands: the Wikidata item ";
    public static string StatusMatchedByNameAfter(long taxonId) =>
        $" was matched to this taxon by name and does not state IUCN taxon ID (P627) {taxonId}.";
    /// Before + link(item) + ".".
    public const string StatusNotDownloadedBefore = "No commands: this site has not downloaded the Wikidata item ";
    public static string StatusSeveralItems(int count, long taxonId) =>
        $"No commands: {count} Wikidata items state IUCN taxon ID (P627) {taxonId}, so first check which one is the item for this taxon.";
    /// Before + link(item) + StatusTaxonIdDeprecatedAfter.
    public const string StatusTaxonIdDeprecatedBefore = "No commands: the Wikidata item ";
    public static string StatusTaxonIdDeprecatedAfter(long taxonId) =>
        $" states IUCN taxon ID (P627) {taxonId} only at deprecated rank, so it may not be the item for this taxon.";

    public const string StatusCommandsDo = "The commands:";
    /// same: the other choice, shown after the reference.
    public static string StatusAddValue(string value, bool same = false) =>
        same ? $"add {value} with the same reference" : $"add {value} with the reference below";
    public static string StatusAddReference(string value, bool same = false) =>
        same ? $"add the same reference to the {value} statement" : $"add the reference below to the {value} statement";
    public static string StatusRemove(string value) => $"remove {value}";
    /// Why the commands shown first remove or keep the old status (WikidataStatusEdit.RecommendedChoice).
    public const string StatusReplaceReason = "None of the item's statements has preferred rank, so these commands remove the old status.";
    public const string StatusKeepReason =
        "The item has a statement at preferred rank. Items like this keep earlier statuses at normal rank, so these commands keep the old status.";
    /// The same, when keeping the old status needs no commands, only ranks.
    public const string StatusKeepReasonNoCommands =
        "The item has a statement at preferred rank. Items like this keep earlier statuses at normal rank.";

    public const string ReferenceLabel = "The reference:";
    /// Before + link(item) + RefStatedInAfter.
    public const string RefStatedInBefore = "stated in (P248): ";
    public const string RefStatedInAfter = ", this assessment's Wikidata item";
    public static string RefTaxonId(long taxonId) => $"IUCN taxon ID (P627): {taxonId}";
    /// Before + link(url).
    public const string RefUrlBefore = "reference URL (P854): ";
    public static string RefRetrieved(string date) => $"retrieved (P813): {date}, when this site downloaded the assessment";
    public const string LeftOutLabel = "Left out of the reference:";
    public static string LeftOutRelease(string? release) =>
        $"stated in (P248) for IUCN Red List {release ?? "release"}: this site has no Wikidata item for the release";
    public const string LeftOutAssessmentItem = "stated in (P248) for this assessment's own Wikidata item: this site's data has none";
    public const string LabelStatusCommands = "QuickStatements commands to update the IUCN conservation status";

    /// The summary of the details element with the other choice.
    public static string StatusAltSummary(StatusEditChoice choice) => choice == StatusEditChoice.Keep
        ? "Keep the old status on the item instead"
        : "Remove the old status from the item instead";
    public static string LabelStatusChoiceCommands(StatusEditChoice choice) => choice == StatusEditChoice.Keep
        ? "QuickStatements commands that keep the old status"
        : "QuickStatements commands that remove the old status";
    /// steps: "set endangered (Q96377276) to preferred rank and set near threatened (Q719675) to normal rank".
    public static string RankStepsAfterCommands(string steps) =>
        $"QuickStatements cannot set ranks. After the commands run, on the item's page {steps}.";
    /// The same, when the choice needs no commands, only ranks.
    public static string RankStepsOnly(string steps) => $"No QuickStatements commands are needed. On the item's page, {steps}.";
    public static string RankStep(string value, string rank) => $"set {value} to {rank} rank";

    /// The English labels of the properties the assessment item model uses, as Wikidata gives them.
    public static string WikidataPropertyLabel(string property) {
        var label = property switch {
            "P31" => "instance of",
            "P50" => "author",
            "P123" => "publisher",
            "P356" => "DOI",
            "P407" => "language of work or name",
            "P478" => "volume",
            "P577" => "publication date",
            "P921" => "main subject",
            "P953" => "full work available at URL",
            "P1433" => "published in",
            "P1476" => "title",
            "P2093" => "author name string",
            "P2322" => "article ID",
            _ => null,
        };
        return label is null ? property : $"{label} ({property})";
    }

    /// QuickStatements terms: 'L' label, 'D' description, 'A' alias, 'S' sitelink, with the language
    /// code or site: "label (en)".
    public static string WikidataTermLabel(char kind, string language) => kind switch {
        'L' => $"label ({language})",
        'D' => $"description ({language})",
        'A' => $"alias ({language})",
        _ => $"sitelink ({language})",
    };

}
