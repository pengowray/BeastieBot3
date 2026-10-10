using BeastieBot3.Shared.SiteData;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Display;
using BeastieBot3.Site.Web;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.OutputCaching;
using Microsoft.Extensions.Options;

namespace BeastieBot3.Site.Pages;

/// One wikitext box: its label, the template name used in the copy button's accessible name, and
/// the text. CopyName replaces that accessible name, for a box that holds something other than
/// wikitext.
public sealed record WikitextBox(string Id, string Label, string Template, string Text, int Rows, string? CopyName = null) {
    public string CopyAccessibleName => CopyName ?? SiteText.CopyAccessible(Template);
}

/// How many of a citation's authors have full given names (CitationAuthor.GivenNames), out of the
/// authors who are people or names kept as published, with the first of them as an example.
public sealed record GivenNamesCoverage(int WithGivenNames, int People, CitationAuthor Example) {
    /// Null when no author has full given names, so the option is not shown.
    public static GivenNamesCoverage? Of(IucnCitationParts parts) {
        var named = parts.Authors.Where(a => !string.IsNullOrWhiteSpace(a.GivenNames)).ToList();
        if (named.Count == 0) {
            return null;
        }
        var people = parts.Authors.Count(a => a.Kind != CitationAuthorKind.Organisation || !string.IsNullOrWhiteSpace(a.GivenNames));
        return new GivenNamesCoverage(named.Count, people, named[0]);
    }
}

/// The wikitext page of a taxon (/species/{id}/wikitext): {{cite iucn}}, {{IUCN status}} and taxobox
/// status lines for one of its assessments with the citation options, the {{cite iucn}} of its Green
/// Status assessment, {{cite Q}} and QuickStatements commands for the assessment's Wikidata item,
/// and its IUCN status on Wikidata. The assessment tables pick the assessment (?assessment=).
[OutputCache(PolicyName = SiteCachePolicies.SpeciesWikitext)]
[ResponseCache(Duration = 600, Location = ResponseCacheLocation.Any)]
public sealed class SpeciesWikitextModel : AssessmentToolModel {
    public SpeciesWikitextModel(SiteDatabase db, SiteQueries queries, IOptions<SiteOptions> options, ILogger<SpeciesWikitextModel> logger)
        : base(db, queries, options, logger) {
    }

    public WikitextOptions Options { get; private set; } = WikitextOptions.Default;
    public IReadOnlyList<WikitextBox> Boxes { get; private set; } = [];
    public string? DoiNote { get; private set; }
    public IReadOnlyList<string> UnsplitAuthors { get; private set; } = [];
    public string? DownloadDateText { get; private set; }
    public string TodayText { get; private set; } = string.Empty;

    /// The people and organisations IUCN credits for the selected assessment; null when it has none.
    public CreditsView? Credits { get; private set; }

    /// For the full given names option, which is shown only when this is not null.
    public GivenNamesCoverage? GivenNames { get; private set; }

    /// The name the selected assessment was published under, or the name in its DOI's title, when it
    /// differs from the taxon's current name (PublishedName.For); |title= gives it unless the reader
    /// chose the current name, and the option for the name in |title= is shown only then.
    public PublishedName? TitleName { get; private set; }

    /// The year the selected assessment's DOI was created (IucnCitationParts.DoiCreated); null when not known.
    public int? DoiCreatedYear => Parts?.DoiCreated is { Length: >= 4 } created
        && int.TryParse(created[..4], System.Globalization.NumberStyles.None, System.Globalization.CultureInfo.InvariantCulture, out var year) ? year : null;

    /// The selected assessment's Wikidata item and its {{cite Q}}, shown when the reader chose {{cite Q}}
    /// (Options.Template); null when no assessment is selected.
    public WikidataCiteView? Wikidata { get; private set; }

    /// True when the reader chose {{cite Q}} and the assessment has a Wikidata item with a {{cite Q}}.
    public bool ShowsCiteQ => Edition.IsEnglish && Options.Template == ReferenceTemplate.CiteQ && Wikidata?.CiteQ is not null;

    /// True when the reader chose {{cite Q}} with the item's commands: the Wikidata item section is
    /// shown below the options.
    public bool ShowsWikidataItem => Edition.IsEnglish && Options.Template == ReferenceTemplate.CiteQ && Options.CreateItem && Wikidata is not null;

    /// This page with the third citation template choice, for the link in the note shown when the
    /// assessment has no Wikidata item.
    public string CreateItemUrl {
        get {
            var targetDefault = Selected is null ? DefaultRefNames.LatestGlobal : DefaultRefNameFor(Selected);
            return PathFor(Taxon?.TaxonId ?? RequestedTaxonId,
                (Options with { Template = ReferenceTemplate.CiteQ, CreateItem = true }).ToQuery(SelectedIdForLinks, targetDefault)) + "#wikidata-cite";
        }
    }

    /// This page with the current options, as a link to it would give them.
    public string CurrentOptionsUrl => OptionsUrl(SelectedIsDefault || Selected is null ? null : Selected.AssessmentId);

    /// The taxobox an article about this taxon most likely uses, which names the status parameters box.
    public TaxoboxTemplate Taxobox { get; private set; } = TaxoboxTemplate.Speciesbox;

    /// The wikitext page of a taxon: "/species/22732/wikitext" with the query (which starts with "?" or is empty).
    public static string PathFor(long taxonId, string query = "") => WikipediaPath(taxonId, query);

    public IActionResult OnGet(long taxonId, long? assessment, string? authors, string? access, string? opts,
        [FromQuery(Name = "ref")] string? wrapRef, string? refname, string? amp, string? fullnames,
        [FromQuery(Name = IucnReference.QueryKey)] string? cite = null,
        [FromQuery(Name = WikitextOptions.GreenStatusYearKey)] string? gsyear = null,
        [FromQuery(Name = WikitextOptions.WikiKey)] string? wiki = null,
        [FromQuery(Name = WikitextOptions.TitleNameKey)] string? titlename = null) {
        if (!LoadTaxon(taxonId)) {
            return Page();
        }
        SelectAssessment(assessment);
        Options = WikitextOptions.FromQuery(authors, access, opts, wrapRef, refname, amp,
            Selected is null ? DefaultRefNames.LatestGlobal : DefaultRefNameFor(Selected), fullnames) with {
            Template = WikitextOptions.ReadTemplate(cite).Template,
            CreateItem = WikitextOptions.ReadTemplate(cite).CreateItem,
            GreenStatusYear = WikitextOptions.ReadGreenStatusYear(gsyear),
            Wiki = WikitextOptions.ReadWiki(wiki),
            CurrentNameInTitle = WikitextOptions.ReadCurrentNameInTitle(titlename),
        };
        Taxobox = TaxoboxTemplate.For(Taxon!.Kind, Taxon.Kingdom);
        BuildWikitext();
        // Every set of options has its own address; search engines get the page without them.
        ViewData["Canonical"] = SiteUrls.Absolute(_options.BaseUrl, Request, PathFor(Taxon.TaxonId));
        return Page();
    }

    /// The URL of this page with the current options and the given assessment (null: the default
    /// one). The ref name goes along only when the visitor chose it; otherwise that assessment's
    /// own default applies.
    public override string OptionsUrl(long? assessmentId) {
        var target = (assessmentId is { } id ? Assessments.FirstOrDefault(a => a.AssessmentId == id) : null) ?? StatusAssessment;
        var targetDefault = target is null ? DefaultRefNames.LatestGlobal : DefaultRefNameFor(target);
        return PathFor(Taxon?.TaxonId ?? RequestedTaxonId, Options.ToQuery(assessmentId, targetDefault));
    }

    private string DefaultRefNameFor(AssessmentRow assessment) =>
        DefaultRefNames.For(assessment, LatestGlobal?.AssessmentId, GlobalHistory);

    /// The "Show wikitext" link of a combined history row under another IUCN id: that id's wikitext
    /// page with the assessment and the current options. The ref name goes along only when the
    /// visitor chose it, as on this page; otherwise the default that page gives the assessment applies.
    public override string OtherTaxonOptionsUrl(TaxonRow other, AssessmentRow assessment, IReadOnlyList<AssessmentRow> otherGlobal) {
        var targetDefault = DefaultRefNames.For(assessment, other.InRelease ? other.LatestGlobalAssessmentId : null, otherGlobal);
        return PathFor(other.TaxonId, Options.ToQuery(assessment.AssessmentId, targetDefault));
    }

    public override string ToolColumnHeading => SiteText.ColWikitext;
    public override string ShowLinkText => SiteText.ShowWikitext;
    public override string ShowLinkAccessible(string? region, int? year, string? versionNote) => SiteText.ShowWikitextAccessible(region, year, versionNote);
    public override string ShowOtherIdText(long taxonId) => SiteText.ShowWikitextOtherId(taxonId);
    public override string ShowOtherIdAccessible(long taxonId, int? year, string? versionNote) => SiteText.ShowWikitextOtherIdAccessible(taxonId, year, versionNote);

    private void BuildWikitext() {
        var today = DateOnly.FromDateTime(DateTime.UtcNow);
        TodayText = SiteFormat.Date(today);
        if (Selected is null) {
            return;
        }
        Parts = ReadParts(Selected.CitationJson);
        Credits = CreditsView.Build(_queries.GetCredits(Selected.AssessmentId));
        var boxes = new List<WikitextBox>();
        CiteIucnOptions? citeOptions = null;
        DateOnly? downloaded = Parts?.DownloadedAtUtc is { } at ? DateOnly.FromDateTime(at) : null;

        if (Parts is not null) {
            DownloadDateText = downloaded is { } d ? SiteFormat.Date(d) : null;
            TitleName = PublishedNameOf(Selected);
            citeOptions = Options.ToCiteIucnOptions(today, downloaded) with { TitleName = Options.CurrentNameInTitle ? null : TitleName?.Name };
            GivenNames = GivenNamesCoverage.Of(Parts);
            var cite = Edition.IsEnglish ? CiteIucnRenderer.Render(Parts, citeOptions) : OtherWikipedias.Citation(Edition, Parts, Facts(), citeOptions);
            var citeTemplate = Edition.CitationTemplate;
            boxes.Add(new WikitextBox("wikitext-cite", Edition.IsEnglish ? SiteText.LabelCite : SiteText.LabelCiteTemplate(citeTemplate), citeTemplate, cite, Rows: 5));

            DoiNote = cite.Contains("|doi=", StringComparison.Ordinal)
                ? Parts.DoiSource switch {
                    DoiSource.Gbif => SiteText.DoiGbif,
                    DoiSource.Wikidata => SiteText.DoiWikidata,
                    DoiSource.Resolved => SiteText.DoiResolved,
                    _ => null,
                }
                // The other Wikipedias' citations that take no DOI (de, es, fr) get no note about one.
                : Edition.IsEnglish ? SiteText.NoDoi : null;
            UnsplitAuthors = Parts.Authors
                .Where(a => a.Kind == CitationAuthorKind.Verbatim && !string.IsNullOrWhiteSpace(a.Display))
                .Select(a => a.Display.Trim())
                .ToList();
        }

        if (!Edition.IsEnglish) {
            BuildOtherWiki(boxes, citeOptions);
            Boxes = boxes;
            return;
        }

        if (IucnCategories.HasStatusTemplateCode(Selected)) {
            var status = IucnStatusTemplate.Render(Selected.Category, Selected.PossiblyExtinct, Selected.PossiblyExtinctInTheWild,
                Selected.TaxonId, Selected.AssessmentId, Selected.YearPublished?.ToString(System.Globalization.CultureInfo.InvariantCulture));
            boxes.Add(new WikitextBox("wikitext-status", SiteText.LabelStatus, "{{IUCN status}}", status, Rows: 2));
        }

        if (Parts is not null && citeOptions is not null && IucnCategories.HasTaxoboxCode(Selected)) {
            // status_ref is always a <ref>, whatever the citation box shows. It holds {{cite Q}} when
            // the reader chose it and the assessment has a Wikidata item.
            var statusRef = IucnReference.Render(Options.Template, Parts, Selected.WikidataItemQid, Selected.WikidataItemProperties,
                citeOptions with { WrapInRef = true }, Options.ToCiteQOptions(today, downloaded) with { WrapInRef = true })!;
            var lines = SpeciesboxStatus.Render(Selected.Category, Selected.PossiblyExtinct, Selected.PossiblyExtinctInTheWild,
                Selected.CriteriaVersion, statusRef);
            boxes.Add(new WikitextBox("wikitext-speciesbox", Taxobox.Label, Taxobox.Name, lines, Rows: 5));
        }
        if (GreenStatus is { } green && IucnCitationParts.FromJson(green.CitationJson) is { } greenParts) {
            var greenDownloaded = SiteFormat.TryParseDate(_db.Snapshot?.Get(SiteDbSchema.MetaKeys.GreenStatusFetched), out var g) ? g : (DateOnly?)null;
            var year = BeastieBot3.Shared.Wikitext.GreenStatusYear.For(Options.GreenStatusYear, green.AssessedYear, green.PublishedYear, green.RedListYear);
            var greenOptions = Options.ToCiteIucnOptions(today, greenDownloaded) with { RefName = GreenStatusRefName };
            boxes.Add(new WikitextBox("wikitext-green", SiteText.GreenStatusCiteLabel, "{{cite iucn}}",
                CiteIucnRenderer.RenderGreenStatus(greenParts with { Year = year }, green.Url, greenOptions), Rows: 4));
        }
        Boxes = boxes;

        Wikidata = BuildWikidataCite(Options.ToCiteQOptions(today, downloaded));
    }

    /// The Wikipedia the wikitext is for (Options.Wiki).
    public WikipediaEdition Edition => OtherWikipedias.Find(Options.Wiki) ?? OtherWikipedias.English;

    /// The heading and the row of choices: Wikidata, then each Wikipedia with the current options.
    public CiteForChooser Chooser => BuildChooser(Edition, WikiUrl, wikipediaLinksKeepOptions: true);

    /// The links to the page for each Wikipedia, with the current options.
    public string WikiUrl(WikipediaEdition edition) {
        var target = SelectedIsDefault || Selected is null ? (long?)null : Selected.AssessmentId;
        var targetDefault = Selected is null ? DefaultRefNames.LatestGlobal : DefaultRefNameFor(Selected);
        return PathFor(Taxon?.TaxonId ?? RequestedTaxonId, (Options with { Wiki = edition.Code }).ToQuery(target, targetDefault));
    }

    /// Notes about the chosen Wikipedia's templates, shown under its boxes.
    public IReadOnlyList<string> WikiNotes { get; private set; } = [];

    /// The options that apply to the chosen Wikipedia's citation: the author format and |name-list-style=
    /// for the wikis whose citation is a copy of {{cite iucn}}, full given names for those and zh,
    /// {{cite Q}} and the Green Status year for English only.
    public bool ShowAuthorStyleOption => Edition.CiteIucnCopy is not null;
    public bool ShowFullGivenNamesOption => Edition.TakesFullGivenNames && GivenNames is not null;
    public bool ShowAmpOption => Edition.CiteIucnCopy is not null;
    public bool ShowCiteTemplateOption => Edition.IsEnglish;
    public bool ShowGreenStatusYearOption => Edition.IsEnglish && GreenStatus is not null;

    private AssessmentFacts Facts() => new(Selected!.Category, Selected.PossiblyExtinct, Selected.PossiblyExtinctInTheWild,
        Selected.CriteriaVersion, Selected.Criteria, SiteFormat.TryParseDate(Selected.AssessmentDate, out var assessed) ? assessed.Year : null,
        Taxon?.Authority, Taxon?.Kind ?? TaxonKinds.Species);

    // The taxobox lines and notes of a Wikipedia other than English.
    private void BuildOtherWiki(List<WikitextBox> boxes, CiteIucnOptions? citeOptions) {
        var notes = new List<string>();
        var facts = Facts();
        if (Edition.TaxoboxTemplate is not { } taxobox) {
            notes.Add(SiteText.WikiNoTaxoboxStatus(Edition.Name));
        } else if (Parts is not null && citeOptions is not null && IucnCategories.HasTaxoboxCode(Selected!)) {
            var statusRef = OtherWikipedias.Citation(Edition, Parts, facts, citeOptions with { WrapInRef = true });
            if (OtherWikipedias.TaxoboxLines(Edition, facts, Selected!.TaxonId, statusRef) is { } lines) {
                boxes.Add(new WikitextBox("wikitext-speciesbox", SiteText.TaxoboxLabel(taxobox), taxobox, lines, Rows: 4));
            }
            if (Edition.TaxoboxMainCategoriesOnly) {
                var taxoboxCode = SpeciesboxStatus.ToStatusCode(facts.Category, facts.PossiblyExtinct, facts.PossiblyExtinctInTheWild);
                var shownAs = OtherWikipedias.MainCategoryCode(taxoboxCode);
                if (shownAs is null) {
                    notes.Add(SiteText.WikiCategoryNotShown(Edition.Name, taxoboxCode));
                } else if (shownAs != taxoboxCode) {
                    notes.Add(SiteText.WikiCategoryShownAs(Edition.Name, taxoboxCode switch { "PE" => "CR(PE)", "PEW" => "CR(PEW)", _ => taxoboxCode }, shownAs));
                }
            }
        }
        if (Edition.CitationLinksCurrentAssessment) {
            notes.Add(SiteText.WikiLinksCurrentAssessment(Edition.CitationTemplate));
        }
        if (Edition.CitationLinksFromArticleItem) {
            notes.Add(SiteText.WikiLinksFromArticleItem(Edition.Name, Edition.CitationTemplate));
        }
        if (Edition.TaxoboxFootnoteRefName is { } refName) {
            notes.Add(SiteText.WikiFootnoteRefName(refName));
        }
        WikiNotes = notes;
    }

    /// The ref name of the Green Status citation, beside the Red List assessment's "iucn".
    public const string GreenStatusRefName = "iucn-green";
}
