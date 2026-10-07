namespace BeastieBot3.Site;

/// The "Site" configuration section (appsettings.json, or environment variables such as
/// Site__DatabasePath).
public sealed class SiteOptions {
    public const string Section = "Site";

    /// Path of the site database built by `site build-db`. A leading "~" is the user's home folder.
    public string? DatabasePath { get; set; }

    /// How to reach the person who runs the site, shown on the About and error pages.
    public string? ContactText { get; set; }

    /// Optional link for ContactText (a URL or a mailto: address).
    public string? ContactUrl { get; set; }

    /// Optional link to the site's source code, shown in the footer.
    public string? SourceUrl { get; set; }

    /// The site's public address ("https://species.example.org"), used for canonical links. When it
    /// is not set, the request's scheme and host are used, with the host in lower case.
    public string? BaseUrl { get; set; }

    /// The User-Agent sent when the status update page loads a page from English Wikipedia, with a
    /// way to contact the site's owner, as Wikimedia's User-Agent policy asks
    /// ("BeastieBotSpeciesStatus/1.0 (https://species.example.org; someone@example.org)"). When it is
    /// not set, the site does not load pages from Wikipedia, and says so.
    public string? WikipediaUserAgent { get; set; }

    public RateLimitOptions RateLimits { get; set; } = new();
}

/// Requests allowed per client IP address per minute. IPv6 clients are counted per /64 network.
public sealed class RateLimitOptions {
    public int PagesPerMinute { get; set; } = 60;
    public int SearchPerMinute { get; set; } = 30;
    public int SuggestPerMinute { get; set; } = 30;

    /// Texts sent to the status update page (POST /update), and pages it loads from Wikipedia
    /// (GET /update?page=...), counted together.
    public int UpdatesPerMinute { get; set; } = 10;

    /// Search pages, suggestion requests and status updates handled at the same time, counting every client.
    /// More wait in a queue of SearchQueueLength; when the queue is full they get status 429.
    public int ConcurrentSearches { get; set; } = 4;
    public int SearchQueueLength { get; set; } = 8;

    /// Taxon, group and name pages (/species, /taxa, /name) per client per hour and per day, on top
    /// of PagesPerMinute: a person checking a list's taxa opens far fewer, and a scraper that stays
    /// under the per-minute limit could otherwise copy the whole site in a few days. 0: no limit.
    public int TaxonPagesPerHour { get; set; } = 600;
    public int TaxonPagesPerDay { get; set; } = 3000;
}
