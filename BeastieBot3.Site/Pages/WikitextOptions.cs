using System.Globalization;
using BeastieBot3.Shared.Wikitext;

namespace BeastieBot3.Site.Pages;

/// The citation options form on a taxon page, read from and written to the query string:
///   authors=author|lastfirst   access=download|today|none   ref=1   refname=...   amp=1   opts=1
/// Unticked checkboxes are not sent by the browser, so the form also sends opts=1: with it, a
/// missing ref or amp means "off" and a missing or empty refname means a plain <ref>; without it (a
/// plain link) the defaults apply. An empty value and a missing one always mean the same thing,
/// because the output cache cannot tell them apart.
public sealed record WikitextOptions(CiteAuthorStyle AuthorStyle, string Access, bool WrapInRef, string RefName, bool Amp) {
    public const string AccessDownload = "download";
    public const string AccessToday = "today";
    public const string AccessNone = "none";
    public const string DefaultRefName = "iucn";
    public const int MaxRefNameLength = 40;

    public static readonly WikitextOptions Default = new(CiteAuthorStyle.AuthorN, AccessDownload, WrapInRef: true, DefaultRefName, Amp: false);

    public static WikitextOptions FromQuery(string? authors, string? access, string? opts, string? wrapRef, string? refName, string? amp) {
        var formSent = opts == "1";
        var style = string.Equals(authors, "lastfirst", StringComparison.OrdinalIgnoreCase) ? CiteAuthorStyle.LastFirst : CiteAuthorStyle.AuthorN;
        var accessValue = access?.Trim().ToLowerInvariant() switch {
            AccessToday => AccessToday,
            AccessNone => AccessNone,
            _ => AccessDownload,
        };
        var name = string.IsNullOrWhiteSpace(refName) ? (formSent ? string.Empty : DefaultRefName) : refName.Trim();
        if (name.Length > MaxRefNameLength) {
            name = name[..MaxRefNameLength];
        }
        return new WikitextOptions(
            style,
            accessValue,
            formSent ? wrapRef == "1" : Default.WrapInRef,
            name,
            formSent ? amp == "1" : Default.Amp);
    }

    /// The query string that reproduces these options (only what differs from the defaults), plus
    /// the assessment to show, starting with "?", or empty.
    public string ToQuery(long? assessmentId) {
        var parts = new List<string>();
        if (assessmentId is { } id) {
            parts.Add("assessment=" + id.ToString(CultureInfo.InvariantCulture));
        }
        if (AuthorStyle == CiteAuthorStyle.LastFirst) {
            parts.Add("authors=lastfirst");
        }
        if (Access != AccessDownload) {
            parts.Add("access=" + Access);
        }
        if (WrapInRef != Default.WrapInRef || Amp != Default.Amp || RefName != DefaultRefName) {
            parts.Add("opts=1");
            if (WrapInRef) {
                parts.Add("ref=1");
            }
            if (Amp) {
                parts.Add("amp=1");
            }
            if (RefName.Length > 0) {
                parts.Add("refname=" + Uri.EscapeDataString(RefName));
            }
        }
        return parts.Count == 0 ? string.Empty : "?" + string.Join('&', parts);
    }
}
