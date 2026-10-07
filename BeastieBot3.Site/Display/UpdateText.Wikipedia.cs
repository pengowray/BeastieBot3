using System.Globalization;

namespace BeastieBot3.Site.Display;

// The status update page with a page loaded from English Wikipedia (/update?page=Title): the line
// that says which page and revision the text is, and the errors of loading it.

public static partial class UpdateText {
    public const string LoadedFromPrefix = "Loaded from English Wikipedia: ";

    public static string LoadedRevision(long revisionId, DateTimeOffset? timestamp) =>
        timestamp is { } t
            ? $"revision {revisionId.ToString(CultureInfo.InvariantCulture)} of {t.UtcDateTime.ToString("d MMMM yyyy, HH:mm", CultureInfo.InvariantCulture)} UTC"
            : $"revision {revisionId.ToString(CultureInfo.InvariantCulture)}";

    public const string EditOnWikipedia = "Edit the page on Wikipedia";

    public static string ErrorNotEnglishWikipedia(string language) =>
        $"Not English Wikipedia: this site checks only English Wikipedia pages, and the link is to the {language} Wikipedia.";

    public static string ErrorPageNotFound(string title, long? revisionId) => revisionId is { } r
        ? $"Page not found: English Wikipedia has no revision {r.ToString(CultureInfo.InvariantCulture)}."
        : $"Page not found: English Wikipedia has no page called \"{title}\".";

    public static string ErrorPageTooLarge(string title) =>
        $"Page too large: the wikitext of \"{title}\" is over 2 MB, the most this page takes. Paste part of it instead.";

    public const string ErrorLoadingNotSetUp = "Loading pages from Wikipedia is not set up on this site. Paste the page's wikitext instead.";

    public static string ErrorPageNotLoaded(string title) =>
        $"Wikipedia did not answer: \"{title}\" could not be loaded. Try again in a moment, or paste the page's wikitext.";
}
