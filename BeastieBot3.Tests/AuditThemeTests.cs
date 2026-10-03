using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using BeastieBot3.Audit;
using BeastieBot3.Audit.Commentary;
using BeastieBot3.Audit.Model;
using BeastieBot3.Audit.Rendering;

namespace BeastieBot3.Tests;

// The redlist audit-site's light and dark themes: the Theme setting in every page's header,
// theme.js in the head before the stylesheet, the colour tokens in audit.css (dark values twice,
// identical, screen only), and 4.5:1 text contrast in both themes, status badges included.
public class AuditThemeTests {
    private static AuditDocument Doc() => new() {
        Release = "2026-1",
        GeneratedAt = "2026-10-03",
        Reports = Array.Empty<AuditReport>(),
        ReleaseCounts = AuditReleaseCounts.Empty,
    };

    // ---- page head and header ----

    [Fact]
    public void Page_LoadsThemeScriptBeforeTheStylesheet_WithoutDefer() {
        var html = AuditPageLayout.Page(Doc(), "Demo", null, "<p>body</p>");
        var head = html[..html.IndexOf("</head>", StringComparison.Ordinal)];
        Assert.Contains("<meta name=\"color-scheme\" content=\"light dark\">", head);
        Assert.Contains(AuditPageLayout.ThemeScript, head);
        Assert.True(head.IndexOf(AuditPageLayout.ThemeScript, StringComparison.Ordinal)
            < head.IndexOf(AuditPageLayout.StylesheetLink, StringComparison.Ordinal));
        Assert.DoesNotContain("defer", AuditPageLayout.ThemeScript);
        Assert.DoesNotContain("async", AuditPageLayout.ThemeScript);
        // Earlier runs' pages are recognised by the stylesheet link in their first 4 KB.
        Assert.True(html.IndexOf(AuditPageLayout.StylesheetLink, StringComparison.Ordinal) < 4096);
    }

    [Fact]
    public void Page_HeaderHasTheThemeSetting_WithSystemSelected() {
        var html = AuditPageLayout.Page(Doc(), "Demo", null, "<p>body</p>");
        var header = html[html.IndexOf("<header", StringComparison.Ordinal)..html.IndexOf("</header>", StringComparison.Ordinal)];
        Assert.Contains("<label for=\"theme-select\">Theme</label>", header);
        Assert.Contains("<select id=\"theme-select\" autocomplete=\"off\">", header);
        Assert.Contains("<option value=\"system\" selected>System</option>", header);
        Assert.Contains("<option value=\"light\">Light</option>", header);
        Assert.Contains("<option value=\"dark\">Dark</option>", header);
        // The server-side HTML never picks a theme; theme.js does, in the browser.
        Assert.DoesNotContain("data-theme", html);
    }

    [Fact]
    public void Write_SavesThemeScriptAndListsItInTheManifest() {
        var dir = Path.Combine(Path.GetTempPath(), "audit-theme-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try {
            AuditSiteRenderer.Write(Doc(), dir);
            Assert.Equal(AuditAssets.ThemeJs, File.ReadAllText(Path.Combine(dir, "assets", "theme.js")));
            Assert.Contains("assets/theme.js", File.ReadAllLines(Path.Combine(dir, AuditSiteRenderer.ManifestFileName)));
            Assert.Contains(AuditPageLayout.ThemeControl, File.ReadAllText(Path.Combine(dir, "index.html")));
        } finally {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void ThemeScript_OnlySetsAttributesAndStorage() {
        var js = AuditAssets.ThemeJs;
        Assert.Contains("localStorage", js);
        Assert.Contains("setAttribute(\"data-theme\"", js);
        Assert.Contains("setAttribute(\"data-theme-ready\"", js);
        Assert.DoesNotContain(".style", js);
        Assert.DoesNotContain("innerHTML", js);
        Assert.DoesNotContain("document.write", js);
    }

    // ---- stylesheet ----

    private static string Block(string css, string selectorPattern) {
        var match = Regex.Match(css, selectorPattern + @"\s*\{(?<body>[^{}]*)\}");
        Assert.True(match.Success, "No block for " + selectorPattern);
        return match.Groups["body"].Value;
    }

    private static string LightBlock => Block(AuditAssets.Css, @"(?m)^:root");
    private static string DarkMediaBlock => Block(AuditAssets.Css,
        @"@media screen and \(prefers-color-scheme: dark\)\s*\{\s*:root:not\(\[data-theme=""light""\]\)");
    private static string DarkAttributeBlock => Block(AuditAssets.Css, @"@media screen\s*\{\s*:root\[data-theme=""dark""\]");

    private static Dictionary<string, string> Colours(string block) =>
        Regex.Matches(block, @"--(?<name>[\w-]+):\s*(?<value>#[0-9a-fA-F]{6});")
            .ToDictionary(m => m.Groups["name"].Value, m => m.Groups["value"].Value);

    private static string Normalise(string block) => Regex.Replace(block, @"\s+", " ").Trim();

    [Fact]
    public void Css_DarkBlocksAreIdentical_AndDefineEveryLightColour() {
        Assert.Equal(Normalise(DarkMediaBlock), Normalise(DarkAttributeBlock));
        Assert.Contains("color-scheme: light;", LightBlock);
        Assert.Contains("color-scheme: dark;", DarkMediaBlock);
        var light = Regex.Matches(LightBlock, @"--(?<name>[\w-]+):").Select(m => m.Groups["name"].Value)
            .Where(name => name is not ("max" or "max-wide")).ToHashSet();
        var dark = Regex.Matches(DarkMediaBlock, @"--(?<name>[\w-]+):").Select(m => m.Groups["name"].Value).ToHashSet();
        Assert.Equal(light.OrderBy(n => n), dark.OrderBy(n => n));
    }

    [Fact]
    public void Css_ColoursAreOnlyInTheTokenBlocks() {
        var css = AuditAssets.Css;
        var outside = css.Replace(LightBlock, "").Replace(DarkMediaBlock, "").Replace(DarkAttributeBlock, "");
        Assert.DoesNotMatch(@"#[0-9a-fA-F]{3,6}\b", outside);
        Assert.DoesNotMatch(@"rgba?\(", outside);
    }

    [Fact]
    public void Css_HidesTheThemeSettingWithoutJavaScriptAndInPrint() {
        var css = AuditAssets.Css;
        Assert.Contains(".theme-control { display: none;", css);
        Assert.Contains(":root[data-theme] .theme-control { display: flex; visibility: hidden; }", css);
        Assert.Contains(":root[data-theme-ready] .theme-control { visibility: visible; }", css);
        var print = css[css.IndexOf("@media print", StringComparison.Ordinal)..];
        Assert.Contains(":root[data-theme] .theme-control { display: none; }", print);
    }

    // ---- contrast ----

    private static double Luminance(string hex) {
        double Channel(int offset) {
            var c = int.Parse(hex.AsSpan(1 + offset, 2), NumberStyles.HexNumber) / 255.0;
            return c <= 0.04045 ? c / 12.92 : Math.Pow((c + 0.055) / 1.055, 2.4);
        }
        return 0.2126 * Channel(0) + 0.7152 * Channel(2) + 0.0722 * Channel(4);
    }

    private static double Contrast(string a, string b) {
        double la = Luminance(a), lb = Luminance(b);
        return (Math.Max(la, lb) + 0.05) / (Math.Min(la, lb) + 0.05);
    }

    // Text colour, background, as the stylesheet uses them.
    private static readonly (string Text, string Background)[] TextPairs = {
        ("ink", "bg"), ("ink", "bg-soft"), ("ink", "accent-soft"),
        ("ink-soft", "bg"), ("ink-soft", "bg-soft"), ("ink-soft", "accent-soft"),
        ("accent", "bg"), ("accent", "bg-soft"), ("accent", "accent-soft"),
        ("breaking", "bg"), ("breaking", "bg-soft"), ("clear", "bg"),
        ("breaking", "badge-by-hand-bg"), ("clear", "badge-mechanical-bg"),
        ("badge-policy-ink", "badge-policy-bg"), ("ink-soft", "badge-info-bg"),
        ("notice-ink", "notice-bg"), ("notice-strong", "notice-bg"),
        ("mark-ink", "mark-bg"),
    };

    [Theory]
    [InlineData("light")]
    [InlineData("dark")]
    public void Css_TextContrastIsAtLeast4_5(string theme) {
        var colours = Colours(theme == "light" ? LightBlock : DarkMediaBlock);
        foreach (var (text, background) in TextPairs) {
            var ratio = Contrast(colours[text], colours[background]);
            Assert.True(ratio >= 4.5, $"{theme}: --{text} on --{background} is {ratio:0.00}:1");
        }
    }

    [Fact]
    public void Css_DarkFilterBoxBorderIsAtLeast3To1() {
        var dark = Colours(DarkMediaBlock);
        Assert.True(Contrast(dark["control-border"], dark["bg"]) >= 3.0);
    }

    [Theory]
    [InlineData("EX")]
    [InlineData("EW")]
    [InlineData("RE")]
    [InlineData("CR")]
    [InlineData("CR(PE)")]
    [InlineData("CR(PEW)")]
    [InlineData("EN")]
    [InlineData("VU")]
    [InlineData("NT")]
    [InlineData("LR/cd")]
    [InlineData("LC")]
    [InlineData("DD")]
    [InlineData("NA")]
    [InlineData(null)]
    public void StatusBadge_TextContrastIsAtLeast4_5(string? code) {
        var visual = IucnStatusVisuals.For(code);
        var ratio = Contrast(visual.Text, visual.Background);
        Assert.True(ratio >= 4.5, $"{code}: {visual.Text} on {visual.Background} is {ratio:0.00}:1");
    }
}
