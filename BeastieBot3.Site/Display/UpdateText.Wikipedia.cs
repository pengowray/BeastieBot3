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

    public const string EditOnWikipedia = "Edit on Wikipedia";

    public static string ErrorNotEnglishWikipedia(string language) =>
        $"Not English Wikipedia: the link is to {language}.wikipedia.org. This site loads pages from en.wikipedia.org only.";

    public static string ErrorPageNotFound(string title, long? revisionId) => revisionId is { } r
        ? $"Revision not found: English Wikipedia has no revision {r.ToString(CultureInfo.InvariantCulture)}."
        : $"Page not found: English Wikipedia has no page called \"{title}\". Check the spelling of the title.";

    public static string ErrorPageTooLarge(string title) =>
        $"Page too large: the wikitext of \"{title}\" is over the 2 MB limit. Copy the wikitext from Wikipedia in parts and update each part separately.";

    public const string ErrorLoadingNotSetUp = "Loading not available: this site is not set up to load pages from Wikipedia. Paste the page's wikitext instead.";

    public static string ErrorPageNotLoaded(string title) =>
        $"Could not load page: Wikipedia did not answer or returned an error for \"{title}\". Try again in a moment, or paste the page's wikitext.";
}
