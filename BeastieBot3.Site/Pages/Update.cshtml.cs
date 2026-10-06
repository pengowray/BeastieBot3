using System.Text;
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

    /// The options the form sent ("1" in the fields below); all off on a first visit.
    public StatusUpdateOptions Options { get; private set; } = new();

    public const string PossiblyExtinctField = "pe";
    public const string IdsField = "ids";
    public const string YearField = "year";
    public const string CitationsField = "cites";
    public const string CommonNamesField = "common";
    public const string CiteQField = "citeq";

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
        };
        if (string.IsNullOrWhiteSpace(text)) {
            Error = UpdateText.ErrorEmpty;
            return Page();
        }
        using var lookup = _queries.OpenStatusLookup();
        Result = new StatusUpdater(lookup, DateOnly.FromDateTime(DateTime.UtcNow), options: Options).Update(text);
        return Page();
    }

    private PageResult Failed(int status, string message) {
        Response.StatusCode = status;
        Error = message;
        return Page();
    }
}
