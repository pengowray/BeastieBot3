using System.Net;
using System.Text.RegularExpressions;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;

namespace BeastieBot3.Site.Tests;

/// The site over a fixture database, in the Production environment so error pages, the exception
/// handler and the status code pages are the real ones. Rate limits are set high so tests do not
/// trip them, except in RateLimitedSiteFactory.
public class SiteFactory : WebApplicationFactory<Program> {
    protected virtual string DatabasePath => FixtureDb.Path;
    protected virtual int PagesPerMinute => 100_000;
    protected virtual int SearchPerMinute => 100_000;

    protected override void ConfigureWebHost(IWebHostBuilder builder) {
        builder.UseEnvironment("Production");
        builder.UseSetting("Site:DatabasePath", DatabasePath);
        builder.UseSetting("Site:ContactText", "User talk:Example on English Wikipedia");
        builder.UseSetting("Site:RateLimits:PagesPerMinute", PagesPerMinute.ToString());
        builder.UseSetting("Site:RateLimits:SearchPerMinute", SearchPerMinute.ToString());
        builder.UseSetting("Site:RateLimits:SuggestPerMinute", SearchPerMinute.ToString());
        builder.UseSetting("Site:RateLimits:UpdatesPerMinute", SearchPerMinute.ToString());
    }

    public HttpClient Client() => CreateClient(new WebApplicationFactoryClientOptions { AllowAutoRedirect = false });
}

/// A database whose schema_version is not the one the site expects.
public sealed class SchemaMismatchSiteFactory : SiteFactory {
    private static readonly Lazy<string> Db = new(() => FixtureDb.Create("mismatch", schemaVersion: "999"));
    protected override string DatabasePath => Db.Value;
}

/// A database with the right schema_version but no assessment table, so a taxon page fails.
public sealed class BrokenSiteFactory : SiteFactory {
    private static readonly Lazy<string> Db = new(() => FixtureDb.Create("broken", dropTable: "assessment"));
    protected override string DatabasePath => Db.Value;
}

/// Site:BaseUrl set, as in production.
public sealed class BaseUrlSiteFactory : SiteFactory {
    public const string BaseUrl = "https://species.example.org/";

    protected override void ConfigureWebHost(IWebHostBuilder builder) {
        base.ConfigureWebHost(builder);
        builder.UseSetting("Site:BaseUrl", BaseUrl);
    }
}

public sealed class RateLimitedSiteFactory : SiteFactory {
    public const int Limit = 3;
    protected override int PagesPerMinute => Limit;
    protected override int SearchPerMinute => Limit;
}

public static class Html {
    /// The decoded text of the textarea with this id.
    public static string? Textarea(string html, string id) {
        var match = Regex.Match(html, $"<textarea id=\"{Regex.Escape(id)}\"[^>]*>(.*?)</textarea>", RegexOptions.Singleline);
        return match.Success ? WebUtility.HtmlDecode(match.Groups[1].Value) : null;
    }

    /// The page with tags removed and entities decoded, whitespace collapsed, for checking sentences.
    public static string Text(string html) {
        var noScript = Regex.Replace(html, "<(script|style|textarea)[^>]*>.*?</\\1>", " ", RegexOptions.Singleline);
        // Block elements separate words; inline ones (a, i, span, abbr, code) do not.
        var blocks = Regex.Replace(noScript,
            "</?(p|div|li|dt|dd|tr|td|th|h[1-6]|br|ul|ol|dl|table|thead|tbody|section|header|footer|nav|main|label|summary|details|fieldset|legend|form|button)\\b[^>]*>",
            " ", RegexOptions.IgnoreCase);
        var noTags = Regex.Replace(blocks, "<[^>]+>", string.Empty);
        var decoded = WebUtility.HtmlDecode(noTags);
        return Regex.Replace(decoded, "\\s+", " ").Trim();
    }

    /// Position of the first match, or -1.
    public static int IndexOf(string html, string text) => html.IndexOf(text, StringComparison.Ordinal);
}
