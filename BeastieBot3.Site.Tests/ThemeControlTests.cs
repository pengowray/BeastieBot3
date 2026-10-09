using System.Net;
using System.Text.RegularExpressions;
using BeastieBot3.Site.Web;

namespace BeastieBot3.Site.Tests;

/// The theme control in the header (System, Light, Dark). The choice is made and stored in the
/// browser by theme.js, so the server sends the same page whatever the visitor chose.
public sealed class ThemeControlTests(SiteFactory factory) : IClassFixture<SiteFactory> {
    private readonly HttpClient _client = factory.Client();

    public static TheoryData<string> LayoutPages => new() {
        "/",
        $"/species/{FixtureDb.PolarBear}",
        "/search?q=Fillerus",
        "/about",
        "/no/such/page",
    };

    [Theory]
    [MemberData(nameof(LayoutPages))]
    public async Task EveryPageHasTheThemeControlInTheHeader(string url) {
        var html = await (await _client.GetAsync(url)).Content.ReadAsStringAsync();

        var header = Regex.Match(html, "<header class=\"site-header\">(.*?)</header>", RegexOptions.Singleline);
        Assert.True(header.Success, "page has no site header");
        var control = header.Groups[1].Value;
        // An icon button whose hidden name gives the current choice, and a menu of three choices.
        Assert.Contains("<details class=\"nav-menu theme-menu\">", control);
        Assert.Contains("data-system=\"Theme: System\" data-light=\"Theme: Light\" data-dark=\"Theme: Dark\">Theme: System</span>", control);
        Assert.Contains("<legend class=\"visually-hidden\">Theme</legend>", control);
        Assert.Contains("<label><input type=\"radio\" name=\"theme\" value=\"system\" checked autocomplete=\"off\"> System</label>", control);
        Assert.Contains("<label><input type=\"radio\" name=\"theme\" value=\"light\" autocomplete=\"off\"> Light</label>", control);
        Assert.Contains("<label><input type=\"radio\" name=\"theme\" value=\"dark\" autocomplete=\"off\"> Dark</label>", control);
        Assert.Equal(3, Regex.Matches(html, "name=\"theme\"").Count);
    }

    [Theory]
    [MemberData(nameof(LayoutPages))]
    public async Task ThemeScriptRunsInTheHeadBeforeThePageIsDrawn(string url) {
        var html = await (await _client.GetAsync(url)).Content.ReadAsStringAsync();
        var headEnd = html.IndexOf("</head>", StringComparison.Ordinal);
        var script = Regex.Match(html, "<script src=\"/theme.js\\?v=[^\"]+\"></script>");

        Assert.True(script.Success, "theme.js is not referenced, or has defer or async");
        Assert.True(script.Index < headEnd, "theme.js is not in <head>");
        Assert.True(script.Index < html.IndexOf("/site.css?v=", StringComparison.Ordinal), "theme.js comes after the stylesheet");
        // Every script is a file: the CSP has no 'unsafe-inline'.
        Assert.DoesNotMatch("<script(?![^>]*\\bsrc=)", html);
        Assert.DoesNotContain(" style=\"", html);
    }

    [Theory]
    [MemberData(nameof(LayoutPages))]
    public async Task ServerNeverChoosesATheme(string url) {
        var html = await (await _client.GetAsync(url)).Content.ReadAsStringAsync();
        Assert.Contains("<html lang=\"en\">", html);
        Assert.DoesNotContain("data-theme", html);
        Assert.Contains("<meta name=\"color-scheme\" content=\"light dark\">", html);
    }

    [Fact]
    public async Task CachedPageIsTheSameWhateverTheVisitorsThemeHints() {
        var url = $"/species/{FixtureDb.PolarBear}?q=theme-cache-test";
        var plain = await _client.GetAsync(url);

        using var hinted = new HttpRequestMessage(HttpMethod.Get, url);
        hinted.Headers.Add("Cookie", "theme=dark");
        hinted.Headers.Add("Sec-CH-Prefers-Color-Scheme", "dark");
        var dark = await _client.SendAsync(hinted);

        Assert.Equal(HttpStatusCode.OK, dark.StatusCode);
        Assert.Equal(await plain.Content.ReadAsStringAsync(), await dark.Content.ReadAsStringAsync());
        Assert.Empty(dark.Headers.Vary);
        Assert.False(dark.Headers.Contains("Set-Cookie"));
    }

    [Fact]
    public async Task ThemeScriptIsServedWithTheSecurityHeaders() {
        var response = await _client.GetAsync("/theme.js");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Contains("javascript", response.Content.Headers.ContentType?.MediaType);
        Assert.Equal(SiteMiddleware.ContentSecurityPolicy, string.Join(";", response.Headers.GetValues("Content-Security-Policy")));
        Assert.Equal("nosniff", response.Headers.GetValues("X-Content-Type-Options").Single());

        var html = await _client.GetStringAsync("/");
        var versioned = Regex.Match(html, "/theme.js\\?v=[^\"]+").Value;
        var cached = await _client.GetAsync(versioned);
        Assert.Contains("immutable", cached.Headers.CacheControl?.ToString());
    }

    [Fact]
    public void ContentSecurityPolicyIsUnchanged() =>
        Assert.Equal(
            "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; " +
            "object-src 'none'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'",
            SiteMiddleware.ContentSecurityPolicy);

    [Fact]
    public async Task ControlIsHiddenUntilThemeScriptHasRun() {
        // site.css shows the theme menu only once theme.js has set data-theme on <html>, and makes it
        // visible once theme.js has ticked the saved choice. Without JavaScript it stays hidden.
        var css = Regex.Replace(await _client.GetStringAsync("/site.css"), "\\s+", " ");
        Assert.Contains(".theme-menu { display: none;", css);
        Assert.Contains(":root[data-theme] .theme-menu { display: block; visibility: hidden; }", css);
        Assert.Contains(":root[data-theme-ready] .theme-menu { visibility: visible; }", css);
        // The icon of the current choice is the one shown.
        Assert.Contains(":root[data-theme=\"dark\"] .theme-icon-dark { display: block; }", css);
    }

    [Fact]
    public async Task ThemeScriptChangesOnlyAttributes() {
        // The CSP has no 'unsafe-inline' for styles, so the script must not write style attributes
        // or markup.
        var js = await _client.GetStringAsync("/theme.js");
        Assert.Contains("root.setAttribute(\"data-theme\"", js);
        Assert.Contains("root.setAttribute(\"data-theme-ready\"", js);
        Assert.DoesNotContain("innerHTML", js);
        Assert.DoesNotContain(".style", js);
        Assert.DoesNotContain("\"style\"", js);
    }
}
