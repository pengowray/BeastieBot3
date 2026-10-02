using BeastieBot3.Site.Display;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.Extensions.Options;

namespace BeastieBot3.Site.Pages;

/// The status code pages (UseStatusCodePagesWithReExecute and UseExceptionHandler). They never
/// query the database, which may be the thing that failed.
[ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
public sealed class ErrorModel : PageModel {
    private readonly SiteOptions _options;

    public ErrorModel(IOptions<SiteOptions> options) {
        _options = options.Value;
    }

    public int Code { get; private set; }
    public string Heading { get; private set; } = string.Empty;
    public string Line { get; private set; } = string.Empty;
    public bool ShowSearch { get; private set; }
    public string? ContactText { get; private set; }
    public string? ContactUrl { get; private set; }

    public void OnGet(int code) {
        Code = code is >= 400 and <= 599 ? code : 404;
        Response.StatusCode = Code;
        ContactText = string.IsNullOrWhiteSpace(_options.ContactText) ? null : _options.ContactText.Trim();
        ContactUrl = string.IsNullOrWhiteSpace(_options.ContactUrl) ? null : _options.ContactUrl.Trim();
        switch (Code) {
            case StatusCodes.Status404NotFound:
                Heading = SiteText.NotFoundHeading;
                Line = SiteText.NotFoundLine;
                ShowSearch = true;
                break;
            case StatusCodes.Status429TooManyRequests:
                Heading = SiteText.TooManyHeading;
                Line = SiteText.TooManyLine;
                break;
            case StatusCodes.Status503ServiceUnavailable:
                Heading = SiteText.UnavailableHeading;
                Line = SiteText.UnavailableLine;
                break;
            case >= 500:
                Heading = SiteText.ServerErrorHeading;
                break;
            default:
                Heading = SiteText.OtherErrorHeading(Code);
                Line = SiteText.NotFoundLine;
                ShowSearch = true;
                break;
        }
    }

    public bool IsServerError => Code >= 500 && Code != StatusCodes.Status503ServiceUnavailable;
}
