using System.Text;
using BeastieBot3.Shared.SiteData;
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

    public UpdateModel(SiteDatabase db, SiteQueries queries) {
        _db = db;
        _queries = queries;
    }

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
    public const string ListAnywayField = "anyway";
    public const string ExtraSpeciesField = "extra";
    public const string AddMissingField = "addmissing";

    public string? Error { get; private set; }

    public void OnGet() {
        Version = _db.Snapshot?.IucnRelease;
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
            ColumnHeader = form[ColumnHeaderField].LastOrDefault() is { } header && ColumnHeaders.Contains(header) ? header : null,
            ColumnTables = form[ColumnTablesShownField].LastOrDefault() == TextKey(text)
                ? form[ColumnTablesField].Select(v => int.TryParse(v, out var line) ? line : -1).Where(l => l > 0).ToHashSet()
                : null,
        };
        if (string.IsNullOrWhiteSpace(text)) {
            Error = UpdateText.ErrorEmpty;
            return Page();
        }
        using var lookup = _queries.OpenStatusLookup();
        var updater = new StatusUpdater(lookup, DateOnly.FromDateTime(DateTime.UtcNow), options: Options);
        Result = updater.Update(text);
        var scope = form[ScopeField].LastOrDefault();
        var scopeLookup = new SiteListScopeLookup(_queries, new SpeciesTableQueries(_db), lookup);
        Scope = ListScope.Check(Result.Members ?? [], scopeLookup,
            new ListScopeOptions(string.IsNullOrWhiteSpace(scope) ? null : scope, On(ListAnywayField), On(ExtraSpeciesField),
                On(ExtraSpeciesField) ? WrittenNames(text) : null));
        ExtraSpecies = On(ExtraSpeciesField);
        AddMissing = On(AddMissingField);
        if (AddMissing && Scope is { Partial: false, Missing.Count: > 0 }) {
            Placement = ListPlacement.Place(text, Result.Members ?? [], Scope,
                new ListPlacementOptions(Options.AddIds, Options.AddYear, Options.CiteQ, Options.AddToListLines), scopeLookup);
            if (Placement.Placed.Count > 0) {
                Result = Result with {
                    Text = updater.TextWith(ListPlacement.Insertions(text, Placement)),
                    MissingAdded = Placement.Placed.Count,
                };
            }
        }
        return Page();
    }

    private PageResult Failed(int status, string message) {
        Response.StatusCode = status;
        Error = message;
        return Page();
    }
}
