using System;
using System.Linq;
using BeastieBot3.Iucn;
using Xunit;

namespace BeastieBot3.Tests;

// Pins IucnHtmlUtilities.CleanRedundantMarkup, the "suggested cleaned-up HTML" shown in the audit
// site's compare modal. The contract that matters most: it only removes markup that adds no rendered
// text, so the plain text extracted from the cleaned HTML must always equal the plain text extracted
// from the original. Each case also checks the specific redundancy it targets.
public class IucnHtmlUtilitiesTests {
    private static string Clean(string html) => IucnHtmlUtilities.CleanRedundantMarkup(html) ?? "";

    private static void AssertSameReadableText(string html) {
        var before = IucnHtmlUtilities.ConvertHtmlToExactPlainText(html);
        var after = IucnHtmlUtilities.ConvertHtmlToExactPlainText(Clean(html));
        Assert.Equal(before, after);
    }

    [Fact]
    public void NullAndEmptyArePassedThrough() {
        Assert.Null(IucnHtmlUtilities.CleanRedundantMarkup(null));
        Assert.Equal("", IucnHtmlUtilities.CleanRedundantMarkup(""));
    }

    [Fact]
    public void DeeplyNestedIdenticalSpansCollapseToOne() {
        // The Euonymus corymbosus pattern: hundreds of identical wrappers around the text.
        var html = string.Concat(System.Linq.Enumerable.Repeat("<span class=\"st\">", 200)) +
                   "Hello" +
                   string.Concat(System.Linq.Enumerable.Repeat("</span>", 200));
        var cleaned = Clean(html);
        // Exactly one opening and one closing span survive; the inner 199 add no styling.
        Assert.Equal("<span class=\"st\">Hello</span>", cleaned);
        AssertSameReadableText(html);
    }

    [Fact]
    public void StylingFreeSpansAreUnwrapped() {
        // tabindex/lang are editor noise; a bare span carries nothing either. All should disappear.
        var html = "<span tabindex=\"0\" lang=\"en\"><span>The <span lang=\"en\">range</span> is small.</span></span>";
        Assert.Equal("The range is small.", Clean(html));
        AssertSameReadableText(html);
    }

    [Fact]
    public void PasteArtefactSpansAreUnwrapped() {
        // Google Docs wraps a paste in a guid span; Google Sheets adds data-* and a run of empty-value
        // attributes. None of it renders, so all of it goes.
        var html = "<span id=\"docs-internal-guid-1\">A <span data-sheets-value=\"{&quot;1&quot;:2}\" climate=\"\" change=\"\">B</span></span>";
        Assert.Equal("A B", Clean(html));
        AssertSameReadableText(html);
    }

    [Fact]
    public void BrokenOpenTagIsDroppedByCleanerAndPlainText() {
        // The Bolitoglossa synoria threats pattern: an open tag whose ">" never arrives, so its
        // attributes run straight into the closing tags. The tag is dropped and the leading text kept.
        var html = "Text.<span><span data-sheets-value=\"{&quot;1&quot;:2}\" lost=\"\" words=\"\"</span></span>";
        Assert.Equal("Text.", Clean(html));
        Assert.Equal("Text.", IucnHtmlUtilities.ConvertHtmlToExactPlainText(html));
        Assert.Equal("Text.", Clean("Text.<br/><br/>")); // trailing breaks render nothing
        Assert.Equal("a<br/>b", Clean("a<br/>b"));
    }

    [Fact]
    public void TrappedTextIsReportedNotRecovered() {
        var attrs = string.Concat(System.Linq.Enumerable.Range(1, 14).Select(i => $" w{i}=\"\""));
        var html = $"<span data-sheets-value=\"x\"{attrs}>hi</span>";
        Assert.Equal("w1 w2 w3 w4 w5 w6 w7 w8 w9 w10 w11 w12 w13 w14", IucnHtmlUtilities.TextTrappedInAttributes(html));
        Assert.Null(IucnHtmlUtilities.TextTrappedInAttributes("<span a=\"\" b=\"\">x</span>"));
    }

    [Fact]
    public void StyledSpansAndSemanticInlineTagsArePreserved() {
        var html = "<span style=\"color:red\">red</span> and <b>bold</b> and <em>italic</em>";
        Assert.Equal(html, Clean(html));
    }

    [Fact]
    public void RepeatedSemanticTagsKeepOneLevel() {
        Assert.Equal("<b>x</b>", Clean("<b><b><b>x</b></b></b>"));
    }

    [Fact]
    public void EmptyInlineTagsAreRemoved() {
        Assert.Equal("ab", Clean("a<span></span>b"));
        Assert.Equal("a<i></i>".Replace("<i></i>", ""), Clean("a<i></i>")); // "a"
    }

    [Fact]
    public void EmptyTagWrappingNonBreakingSpaceKeepsThatExactSpace() {
        // The empty wrapper goes, but the narrow no-break space it held must not become an ASCII space.
        var html = "Argentina<span lang=\"en\"> </span>from";
        var cleaned = Clean(html);
        Assert.Contains(" ", cleaned);
        Assert.Equal("Argentina from", cleaned);
        AssertSameReadableText(html);
    }

    [Fact]
    public void SoftHyphenAndZeroWidthCharactersAreDeleted() {
        // The Pagrus pagrus case: a soft hyphen inside a word, plus a zero-width space.
        var html = "<span class=\"st\">con­comitant​ decline</span>";
        var cleaned = Clean(html);
        Assert.DoesNotContain('­', cleaned);
        Assert.DoesNotContain('​', cleaned);
        Assert.Contains("concomitant", cleaned);
        AssertSameReadableText(html);
    }

    [Fact]
    public void StructuralTagsAreLeftIntact() {
        var html = "<p>First.</p><p>Second.<br/>line</p><ul><li>one</li></ul>";
        Assert.Equal(html, Clean(html));
    }

    [Fact]
    public void HtmlCommentsAreRemoved() {
        Assert.Equal("ab", Clean("a<!-- a comment -->b"));
    }

    [Fact]
    public void MixedNestingOfNoiseAndStylingResolvesToTheStyling() {
        // A styling-free wrapper around a kept one (and vice versa) reduces to just the kept tag.
        AssertSameReadableText("<span tabindex=\"0\"><span class=\"st\">text</span></span>");
        Assert.Equal("<span class=\"st\">text</span>", Clean("<span tabindex=\"0\"><span class=\"st\">text</span></span>"));
        Assert.Equal("<span class=\"st\">text</span>", Clean("<span class=\"st\"><span tabindex=\"0\">text</span></span>"));
    }
}
