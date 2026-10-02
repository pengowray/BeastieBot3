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

    public RateLimitOptions RateLimits { get; set; } = new();
}

/// Requests allowed per client IP address per minute. IPv6 clients are counted per /64 network.
public sealed class RateLimitOptions {
    public int PagesPerMinute { get; set; } = 60;
    public int SearchPerMinute { get; set; } = 30;
    public int SuggestPerMinute { get; set; } = 30;
}
