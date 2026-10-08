using System.Text;
using BeastieBot3.Shared.SiteData;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Display;
using BeastieBot3.Site.Update;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;

namespace BeastieBot3.Site.Pages;

/// The status update page (/update): the visitor pastes wikitext and gets it back with the IUCN
/// statuses brought up to date (StatusUpdater), with a report. The page stores nothing and returns
/// only the visitor's own text and the report, so it is the one page that answers POST
/// (SiteMiddleware.UseGetAndHeadOnly). It sets no cookies, so it needs no antiforgery token, and
/// its answers are never cached.
[IgnoreAntiforgeryToken]
[ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
public sealed class UpdateModel : PageModel {
    public const string Path = "/update";

    /// The most text accepted, in UTF-8 bytes.
    public const int MaxTextBytes = 2 * 1024 * 1024;

    /// The largest request body: the text in a multipart/form-data form, with room for the form's
    /// boundaries and headers.
    public const long MaxBodyBytes = MaxTextBytes + 64 * 1024;

    /// The form field of the text.
    public const string TextField = "text";

    private readonly SiteDatabase _db;
    private readonly SiteQueries _queries;
    private readonly IWikipediaPageSource _wikipedia;

    public UpdateModel(SiteDatabase db, SiteQueries queries, IWikipediaPageSource wikipedia) {
        _db = db;
        _queries = queries;
        _wikipedia = wikipedia;
    }

    /// The query field that names a page to load from English Wikipedia (/update?page=Title, which the
    /// search box sends for a Wikipedia URL or a wikilink), and the revision to load (oldid).
    public const string PageField = "page";
    public const string RevisionField = "oldid";
    /// Sent with the form when the text was loaded from Wikipedia: TextKey of the loaded text, so the
    /// "loaded from" line stays only while the text is the page's own.
    public const string PageKeyField = "pagekey";
    public const string PageTimeField = "pagetime";

    /// The Wikipedia page the text was loaded from (its Text is empty after a POST), or null.
    public WikipediaPageText? LoadedPage { get; private set; }

    public string? Version { get; private set; }

    /// The text as the visitor sent it, shown again in the form.
    public string Input { get; private set; } = string.Empty;

    public StatusUpdateResult? Result { get; private set; }

    /// The taxa the text lists compared with the group they are in; null when the text lists too
    /// few taxa (ListScope).
    public ListScopeResult? Scope { get; private set; }

    /// The missing taxa put into the wikitext (AddMissingField), or null when not asked for.
    public ListPlacementResult? Placement { get; private set; }

    /// The reader asked for the missing taxa to be put into the wikitext.
    public bool AddMissing { get; private set; }

    /// The reader asked to compare with the species of the Catalogue of Life and Wikidata too.
    public bool ExtraSpecies { get; private set; }

    // Every scientific name the text writes, folded: a species of the Catalogue of Life or Wikidata
    // that the text names is not missing from it.
    private static HashSet<string> WrittenNames(string text) {
        var scanner = new WikitextScanner(text);
        var names = new HashSet<string>();
        for (var start = 0; start < text.Length;) {
            var end = text.IndexOf('\n', start);
            end = end < 0 ? text.Length : end;
            names.UnionWith(StatusUpdater.NameOccurrencesIn(scanner, new TextSpan(start, end)).Select(o => SiteNameKey.Fold(o.Name)));
            start = end + 1;
        }
        // Every page the text links: a species may be listed under another genus by the list
        // ("A. dhofarensis") than by the Catalogue of Life (Pipistrellus dhofarensis), with a link to
        // its article ("[[Dhofar pipistrelle]]").
        foreach (System.Text.RegularExpressions.Match m in System.Text.RegularExpressions.Regex.Matches(scanner.Masked, @"\[\[([^|\]\n#]+)")) {
            names.Add(SiteNameKey.Fold(m.Groups[1].Value.Replace('_', ' ')));
        }
        // The binomials of species table rows, which abbreviate the genus of their table ("L. braccatus").
        string? genus = null;
        foreach (var template in scanner.Templates) {
            if (template.Name == "species table") {
                genus = template.Named("genus") is { } g
                    ? StatusUpdater.LinkText(System.Text.RegularExpressions.Regex.Replace(scanner.CoreText(g.Value), @"\{\{[^{}]*\}\}", string.Empty))
                    : null;
            } else if (template.Name == "species table/row" && template.Named("binomial") is { } b) {
                var binomial = StatusUpdater.LinkText(scanner.CoreText(b.Value));
                var parts = binomial.Split(' ', 2);
                names.Add(SiteNameKey.Fold(parts.Length == 2 && parts[0].EndsWith('.') && genus is { Length: > 0 } ? $"{genus.Split(' ')[0]} {parts[1]}" : binomial));
            }
        }
        return names;
    }

    /// The options the form sent ("1" in the fields below); all off on a first visit.
    public StatusUpdateOptions Options { get; private set; } = new();

    public const string PossiblyExtinctField = "pe";
    public const string IdsField = "ids";
    public const string YearField = "year";
    public const string CitationsField = "cites";
    public const string CommonNamesField = "common";
    public const string CiteQField = "citeq";
    public const string AddToListLinesField = "addlines";
    public const string AddStatusColumnsField = "addcols";
    public const string AtLineEndField = "addend";
    public const string AddReferencesField = "addrefs";
    public const string AddSummaryField = "addsummary";
    public const string ColumnHeaderField = "colhead";
    public const string ColumnTablesField = "cols";
    /// Sent with the table checkboxes, so that a form with none of them ticked means no tables. Its
    /// value is TextKey of the text they were shown for: pasting another text drops the choice.
    public const string ColumnTablesShownField = "colsshown";

    /// A short key of a text: the start of its SHA-256 in hex.
    public static string TextKey(string text) =>
        Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(Encoding.UTF8.GetBytes(text)))[..16];

    /// The headings a new status column can have; the first is the default.
    public static readonly string[] ColumnHeaders = [StatusUpdater.StatusColumnHeader, "Conservation status", "Red List status", "Status"];

    public const string ScopeField = "scope";
    /// The categories to compare (ListCategories key); empty: from the codes in the wikitext.
    public const string CategoriesField = "cats";
    /// Show each taxon's EPBC Act listing beside its IUCN status (ticked for a page loaded from
    /// Wikipedia whose title names Australia).
    public const string EpbcField = "epbc";
    /// The country or area to compare with (an area code); empty: the whole group.
    public const string AreaField = "area";
    /// Which of the area's records count (AreaMode, by name).
    public const string AreaModeField = "areamode";

    /// The IUCN region whose latest assessments the text is compared with instead of the global ones.
    public const string RegionField = "region";

    /// The region chosen ("Europe"), or null for global assessments.
    public string? Region { get; private set; }

    /// Every IUCN region with assessments, with how many taxa have one, for the choice.
    public IReadOnlyList<(string Region, int Taxa)> Regions => _queries.Regions();

    // A region the site has, or null.
    private string? KnownRegion(string? value) =>
        string.IsNullOrWhiteSpace(value) ? null : Regions.Select(r => r.Region).FirstOrDefault(r => r == value.Trim());

    /// The area chosen, or found in the title of a page loaded from Wikipedia.
    public AreaName? Area { get; private set; }
    public AreaMode AreaMode { get; private set; } = AreaMode.Native;

    /// Every area, for the choice.
    public AreaNames Areas => _queries.AreaNames();

    public bool ShowEpbc { get; private set; }

    /// The EPBC Act listings of the taxa in the report and the comparison, when ShowEpbc.
    public IReadOnlyDictionary<long, IReadOnlyList<EpbcListingRow>> Epbc { get; private set; } = new Dictionary<long, IReadOnlyList<EpbcListingRow>>();

    /// "List of reptiles of Australia", "List of Australian birds", "Fauna of Australia".
    public static bool IsAustralianTitle(string title) =>
        System.Text.RegularExpressions.Regex.IsMatch(title, @"\bAustral(ia|ian|asia|asian)\b", System.Text.RegularExpressions.RegexOptions.IgnoreCase);

    /// The categories chosen, or read from the title of a page loaded from Wikipedia; null: from the codes.
    public ListCategoryChoice? Categories { get; private set; }

    /// Categories was set from the title of the page loaded from Wikipedia, and not changed since.
    public bool CategoriesFromTitle => LoadedPage is { } page && Categories is { } c && ListCategories.FromTitle(page.Title)?.Key == c.Key;
    public const string ListAnywayField = "anyway";
    public const string ExtraSpeciesField = "extra";
    public const string AddMissingField = "addmissing";
    /// The taxa now in another category to take out (taxon ids, one checkbox each), and TextKey of
    /// the text the checkboxes were shown for: with it, a form with none ticked means none.
    public const string RemoveField = "rm";
    public const string RemoveShownField = "rmshown";
    /// The button that takes out every taxon now in another category.
    public const string RemoveAllField = "rmall";
    /// Rebuild the list (ListRebuild) instead of putting the missing taxa in.
    public const string RebuildField = "rebuild";

    /// The rebuilt list, when the reader asked for one.
    public RebuildResult? Rebuild { get; private set; }

    /// The taxa now in another category the reader chose to take out; null when the reader has not
    /// asked to take any out.
    public IReadOnlySet<long>? Removing { get; private set; }

    /// What the text lists its taxa in, for the help text of the missing species.
    public ListForms Forms { get; private set; } = new(false, false, false, false);

    public string? Error { get; private set; }

    public async Task<IActionResult> OnGetAsync() {
        Version = _db.Snapshot?.IucnRelease;
        var page = Request.Query[PageField].FirstOrDefault()?.Trim();
        if (string.IsNullOrEmpty(page)) {
            return Page();
        }
        // The search box sends a title; a URL or a wikilink typed into the address bar works too.
        var input = WikipediaPageInput.Parse(page) ?? new WikipediaPageInput(page, null, true, "en");
        if (!input.English) {
            return Failed(StatusCodes.Status400BadRequest, UpdateText.ErrorNotEnglishWikipedia(input.Language));
        }
        var revision = input.RevisionId
            ?? (long.TryParse(Request.Query[RevisionField].FirstOrDefault(), out var oldid) && oldid > 0 ? oldid : null);
        var loaded = await _wikipedia.GetAsync(input.Title, revision, HttpContext.RequestAborted);
        if (loaded.Page is not { } wikiPage) {
            return loaded.Error switch {
                WikipediaPageError.NotFound => Failed(StatusCodes.Status404NotFound, UpdateText.ErrorPageNotFound(input.Title, revision)),
                WikipediaPageError.TooLarge => Failed(StatusCodes.Status413PayloadTooLarge, UpdateText.ErrorPageTooLarge(input.Title)),
                WikipediaPageError.NotConfigured => Failed(StatusCodes.Status503ServiceUnavailable, UpdateText.ErrorLoadingNotSetUp),
                WikipediaPageError.Busy => Failed(StatusCodes.Status429TooManyRequests, UpdateText.ErrorLoadingBusy),
                _ => Failed(StatusCodes.Status502BadGateway, UpdateText.ErrorPageNotLoaded(input.Title)),
            };
        }
        LoadedPage = wikiPage;
        Input = wikiPage.Text;
        Categories = ListCategories.FromTitle(wikiPage.Title);
        Area = Areas.FromTitle(wikiPage.Title);
        AreaMode = AreaNames.ModeFromTitle(wikiPage.Title);
        ShowEpbc = IsAustralianTitle(wikiPage.Title);
        Region = KnownRegion(Request.Query[RegionField].FirstOrDefault());
        Run(wikiPage.Text, scope: null, listAnyway: false, extraSpecies: false, addMissing: false);
        return Page();
    }

    public async Task<IActionResult> OnPostAsync() {
        Version = _db.Snapshot?.IucnRelease;
        if (Request.ContentLength > MaxBodyBytes) {
            return Failed(StatusCodes.Status413PayloadTooLarge, UpdateText.ErrorTooLarge);
        }
        IFormCollection form;
        try {
            form = await Request.ReadFormAsync(HttpContext.RequestAborted);
        } catch (BadHttpRequestException e) when (e.StatusCode == StatusCodes.Status413PayloadTooLarge) {
            return Failed(StatusCodes.Status413PayloadTooLarge, UpdateText.ErrorTooLarge);
        } catch (Exception e) when (e is BadHttpRequestException or InvalidDataException or InvalidOperationException) {
            // A body that is not a form, or a form over the form reader's limits.
            return Failed(StatusCodes.Status400BadRequest, UpdateText.ErrorUnreadable);
        }

        var text = form[TextField].ToString();
        if (Encoding.UTF8.GetByteCount(text) > MaxTextBytes) {
            return Failed(StatusCodes.Status413PayloadTooLarge, UpdateText.ErrorTooLarge);
        }
        Input = text;
        // The last value counts: an offer's button sends "1" after the form's own checkbox.
        bool On(string field) => form[field].LastOrDefault() == "1";
        Options = new StatusUpdateOptions {
            PossiblyExtinctCodes = On(PossiblyExtinctField),
            AddIds = On(IdsField),
            AddYear = On(YearField),
            UpdateCitations = On(CitationsField),
            MatchCommonNames = On(CommonNamesField),
            CiteQ = On(CiteQField),
            AddToListLines = On(AddToListLinesField),
            AddStatusColumns = On(AddStatusColumnsField),
            StatusAtLineEnd = On(AtLineEndField),
            AddReferences = On(AddReferencesField),
            AddStatusSummary = On(AddSummaryField),
            ColumnHeader = form[ColumnHeaderField].LastOrDefault() is { } header && ColumnHeaders.Contains(header) ? header : null,
            ColumnTables = form[ColumnTablesShownField].LastOrDefault() == TextKey(text)
                ? form[ColumnTablesField].Select(v => int.TryParse(v, out var line) ? line : -1).Where(l => l > 0).ToHashSet()
                : null,
        };
        if (string.IsNullOrWhiteSpace(text)) {
            Error = UpdateText.ErrorEmpty;
            return Page();
        }
        if (form[PageField].LastOrDefault() is { Length: > 0 } pageTitle && form[PageKeyField].LastOrDefault() == TextKey(text)
            && long.TryParse(form[RevisionField].LastOrDefault(), out var pageRevision)) {
            LoadedPage = new WikipediaPageText(pageTitle, string.Empty, pageRevision,
                DateTimeOffset.TryParse(form[PageTimeField].LastOrDefault(), System.Globalization.CultureInfo.InvariantCulture,
                    System.Globalization.DateTimeStyles.AssumeUniversal, out var time) ? time : null);
        }
        Categories = ListCategories.Find(form[CategoriesField].LastOrDefault());
        Area = Areas.ByCode(form[AreaField].LastOrDefault());
        AreaMode = Enum.TryParse<AreaMode>(form[AreaModeField].LastOrDefault(), out var mode) && Enum.IsDefined(mode) ? mode : AreaMode.Native;
        ShowEpbc = On(EpbcField);
        Region = KnownRegion(form[RegionField].LastOrDefault());
        // A rebuild first leaves out every taxon now in another category, as the button does.
        IReadOnlySet<long>? removing = On(RemoveAllField) || (On(RebuildField) && form[RemoveShownField].LastOrDefault() != TextKey(text)) ? null
            : form[RemoveShownField].LastOrDefault() == TextKey(text)
            ? form[RemoveField].Select(v => long.TryParse(v, out var id) ? id : 0).Where(id => id > 0).ToHashSet()
            : new HashSet<long>();
        var rebuild = On(RebuildField);
        Run(text, form[ScopeField].LastOrDefault(), On(ListAnywayField), On(ExtraSpeciesField), On(AddMissingField), removing,
            removeAsked: On(RemoveAllField) || form[RemoveShownField].LastOrDefault() == TextKey(text)
                || (rebuild && form[RemoveShownField].LastOrDefault() != TextKey(text)),
            rebuild: rebuild);
        return Page();
    }

    // Updates the text with Options and compares it with its group (ListScope). removing: the taxa
    // now in another category to take out, null for all of them; removeAsked: the reader has asked to
    // take some out (the button, or the checkboxes shown for this text).
    private void Run(string text, string? scope, bool listAnyway, bool extraSpecies, bool addMissing,
        IReadOnlySet<long>? removing = null, bool removeAsked = false, bool rebuild = false) {
        using var lookup = _queries.OpenStatusLookup(Region);
        var updater = new StatusUpdater(lookup, DateOnly.FromDateTime(DateTime.UtcNow), options: Options);
        Result = updater.Update(text);
        var scopeLookup = new SiteListScopeLookup(_queries, new SpeciesTableQueries(_db), lookup);
        Scope = ListScope.Check(Result.Members ?? [], scopeLookup,
            new ListScopeOptions(string.IsNullOrWhiteSpace(scope) ? null : scope, listAnyway, extraSpecies,
                extraSpecies ? WrittenNames(text) : null) { Categories = Categories, Area = Area?.Code, AreaMode = AreaMode, Region = Region });
        ExtraSpecies = extraSpecies;
        AddMissing = addMissing;
        Forms = ListForms.Of(text, Result.Members ?? []);
        if (removeAsked && Scope is not null) {
            Removing = removing ?? Scope.OtherCategory.Select(m => m.Taxon.TaxonId).ToHashSet();
        }
        var placementOptions = new ListPlacementOptions(Options.AddIds, Options.AddYear, Options.CiteQ, Options.AddToListLines) {
            Remove = Removing ?? new HashSet<long>(),
        };
        if (rebuild && Scope is not null) {
            // The rebuilt list has the missing taxa; the taxa now in another category are left out
            // unless the reader unticks them.
            Rebuild = ListRebuild.Rebuild(text, updater, Result.Members ?? [], Scope, placementOptions, scopeLookup);
            if (Rebuild.Refusal == RebuildRefusal.None) {
                Result = Result with {
                    Text = Rebuild.Text,
                    MissingAdded = Rebuild.Taxa.Count(t => t.Change == RebuildChange.Added),
                    TaxaRemoved = Rebuild.Removed.Count,
                };
            }
        }
        var placing = Rebuild is null && AddMissing && Scope is { Partial: false } && (Scope.Missing?.Count ?? 0) + (Scope.MissingExtra?.Count ?? 0) > 0;
        if (Scope is not null && Rebuild is null && (placing || Removing is { Count: > 0 })) {
            Placement = ListPlacement.Place(text, Result.Members ?? [], Scope,
                new ListPlacementOptions(Options.AddIds, Options.AddYear, Options.CiteQ, Options.AddToListLines) {
                    TablesWithNewColumn = Result.Findings
                        .Where(f => f.Kind == StatusItemKind.TableColumnAdded && f.Outcome == StatusOutcome.Updated)
                        .ToDictionary(f => f.Line, f => (int)(f.Notes.First(n => n.Kind == StatusNoteKind.ColumnAdded).Id ?? 0)),
                    AddMissing = placing,
                    Remove = Removing ?? new HashSet<long>(),
                }, scopeLookup);
            if (Placement.Placed.Count > 0 || Placement.Removals.Count > 0) {
                Result = Result with {
                    Text = updater.TextWith(ListPlacement.Insertions(text, Placement), Placement.Removals),
                    MissingAdded = Placement.Placed.Count,
                    TaxaRemoved = Placement.Removed.Count,
                };
            }
        }
        // Last, so the counts include the statuses changed and the taxa added above.
        Result = IucnStatusesSummary.Apply(text, Result, Options.AddStatusSummary);
        if (ShowEpbc) {
            var ids = Result.Findings.Select(f => f.Taxon?.TaxonId).OfType<long>()
                .Concat(Scope?.OtherCategory.Concat(Scope.Outside).Select(m => m.Taxon.TaxonId) ?? [])
                .Concat(Scope?.Missing?.Select(t => t.TaxonId).Where(id => id > 0) ?? [])
                .ToHashSet();
            Epbc = _queries.GetEpbcListings(ids);
        }
    }

    private PageResult Failed(int status, string message) {
        Response.StatusCode = status;
        Error = message;
        return Page();
    }
}
