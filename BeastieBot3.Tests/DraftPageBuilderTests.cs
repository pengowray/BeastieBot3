using System;
using System.Linq;
using BeastieBot3.Wikipedia;
using Xunit;

namespace BeastieBot3.Tests;

// `wikipedia post-drafts` posts generated lists to user space. A user page must not be in article
// categories, and an unchanged page is skipped by comparing the sha1 of the text with the sha1
// MediaWiki reports, so the text must be built the same way every time.
public class DraftPageBuilderTests {
    private const string Index = "User:Beastie Bot/Draft 2026";

    private const string List =
        "{{Short description|none}}\r\n" +
        "Intro text.\r\n" +
        "\r\n" +
        "== References ==\r\n" +
        "{{Reflist}}\r\n" +
        "\r\n" +
        "[[Category:IUCN Red List critically endangered species|*Mammals]]\r\n" +
        "[[Category:Mammal conservation]]\r\n";

    [Fact]
    public void Categories_move_into_draft_categories_block() {
        var draft = DraftPageBuilder.BuildDraft(List, "List of critically endangered mammals", Index, new DateTime(2026, 10, 1));

        Assert.EndsWith(
            "{{Reflist}}\n\n{{Draft categories|\n[[Category:IUCN Red List critically endangered species|*Mammals]]\n[[Category:Mammal conservation]]\n}}",
            draft);
    }

    [Fact]
    public void Category_links_inside_draft_categories_are_the_only_category_links() {
        var draft = DraftPageBuilder.BuildDraft(List, "List of critically endangered mammals", Index, new DateTime(2026, 10, 1));
        var beforeBlock = draft[..draft.IndexOf("{{Draft categories|", StringComparison.Ordinal)];
        Assert.DoesNotContain("[[Category:", beforeBlock);
    }

    [Fact]
    public void Colon_category_links_are_ordinary_links_and_stay() {
        var (body, categories) = DraftPageBuilder.ExtractCategories("See [[:Category:Mammals]] here.\n[[Category:A]]");
        Assert.Equal("See [[:Category:Mammals]] here.", body);
        Assert.Equal(new[] { "[[Category:A]]" }, categories);
    }

    [Fact]
    public void Draft_starts_with_noindex_and_banner_linking_article_and_index() {
        var draft = DraftPageBuilder.BuildDraft(List, "List of critically endangered mammals", Index, new DateTime(2026, 10, 1));
        var lines = draft.Split('\n');
        Assert.Equal("__NOINDEX__", lines[0]);
        Assert.StartsWith("{{ombox", lines[1]);
        Assert.Contains("[[List of critically endangered mammals]]", lines[1]);
        Assert.Contains("[[User:Beastie Bot/Draft 2026|", lines[1]);
        Assert.Contains("1 October 2026", lines[1]);
        Assert.Equal("{{Short description|none}}", lines[2]);
    }

    [Fact]
    public void Same_file_builds_the_same_text() {
        var a = DraftPageBuilder.BuildDraft(List, "List of X", Index, new DateTime(2026, 10, 1, 10, 3, 0));
        var b = DraftPageBuilder.BuildDraft(List.Replace("\r\n", "\n"), "List of X", Index, new DateTime(2026, 10, 1, 23, 0, 0));
        Assert.Equal(DraftPageBuilder.Sha1Hex(a), DraftPageBuilder.Sha1Hex(b));
    }

    [Fact]
    public void Normalize_matches_mediawiki_storage() {
        // Line endings become LF, trailing whitespace goes, decomposed accents are composed (NFC).
        Assert.Equal("a\nb\n\ncafé", DraftPageBuilder.Normalize("a\r\nb\r\n\r\ncafé \n\n"));
    }

    [Fact]
    public void Sha1_is_lowercase_hex_of_utf8() {
        Assert.Equal("f7ff9e8b7bb2e09b70935a5d785e0cc5d9d0abf0", DraftPageBuilder.Sha1Hex("Hello"));
    }

    [Fact]
    public void Index_lists_drafts_by_group_and_the_oversize_lists() {
        var posted = new[] {
            new DraftIndexEntry(DraftGroup.Iucn, "List of critically endangered mammals", Index + "/List of critically endangered mammals", 34_457),
            new DraftIndexEntry(DraftGroup.Australia, "List of rare and threatened mammals of Australia", Index + "/List of rare and threatened mammals of Australia", 37_736),
        };
        var tooLarge = new[] {
            new DraftIndexEntry(DraftGroup.Iucn, "List of least concern dicotyledons", Index + "/List of least concern dicotyledons", 2_715_549),
        };

        var index = DraftPageBuilder.BuildIndex(Index, posted, tooLarge, new DateTime(2026, 10, 1), "2026-1");

        Assert.StartsWith("__NOINDEX__\n", index);
        Assert.Contains("[[User:Beastie Bot/Draft 2026/List of critically endangered mammals|List of critically endangered mammals]] || [[List of critically endangered mammals]] || style=\"text-align:right\" | 34", index);
        Assert.Contains("2026-1", index);
        Assert.Contains("* [[List of least concern dicotyledons]] (2.7 MB)", index);
        Assert.True(index.IndexOf("mammals of Australia", StringComparison.Ordinal) > index.IndexOf("critically endangered mammals", StringComparison.Ordinal));
        Assert.DoesNotContain("[[Category:", index);
    }

    [Fact]
    public void Iucn_version_is_read_from_the_citation() {
        const string cite = "{{cite web |title=IUCN Red List of Threatened Species. Version 2026-1 |url=https://www.iucnredlist.org/}}";
        Assert.Equal("2026-1", DraftPageBuilder.FindIucnVersion(cite));
        Assert.Null(DraftPageBuilder.FindIucnVersion("no citation"));
    }

    [Fact]
    public void Size_limit_is_english_wikipedias_2048_kib() {
        Assert.Equal(2_097_152, DraftPageBuilder.MaxPageBytes);
    }
}
