using BeastieBot3.Shared.SiteData;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Display;

namespace BeastieBot3.Site.Tests;

// The species page's "Seafood guide ratings" section: SeafoodGuideSection.Build (labels, order,
// columns, the count line of a long table, the notes) and the section on the page.
public sealed class SeafoodGuideTests : IClassFixture<SiteFactory> {
    private readonly HttpClient _client;

    public SeafoodGuideTests(SiteFactory factory) => _client = factory.CreateClient();

    private static SeafoodRatingRow GoodFish(string id, string ratings, string? region = "WA", string? method = "Wild Caught",
        string? fishery = null, string? note = null, string? modified = null, string? listedName = null) =>
        new(SeafoodGuides.GoodFish, id, ratings, null, "Gummy Shark", listedName, region, null, method, "wild", fishery, note,
            $"https://goodfish.org.au/species/{id}/", modified);

    private static SeafoodRatingRow Watch(string id, string ratings, string? color, string? region = null, string? waterBody = null,
        string? method = "Drifting longlines", string? production = "wild", string? fishery = null) =>
        new(SeafoodGuides.SeafoodWatch, id, ratings, color, "Yellowfin tuna", null, region, waterBody, method, production, fishery, null,
            $"https://www.seafoodwatch.org/recommendation/{id}", null);

    private static readonly Dictionary<string, string> Downloaded = new() {
        [SeafoodGuides.GoodFish] = "28 October 2026",
        [SeafoodGuides.SeafoodWatch] = "27 October 2026",
    };

    private static SeafoodGuideSection Build(params SeafoodRatingRow[] rows) =>
        SeafoodGuideSection.Build(rows, guide => Downloaded.GetValueOrDefault(guide))!;

    private static string Text(IReadOnlyList<NoteSegment> note) => string.Concat(note.Select(s => s.Text));

    // ------------------------------------------------------------ builder

    [Fact]
    public void No_rows_no_section() =>
        Assert.Null(SeafoodGuideSection.Build([], _ => null));

    [Theory]
    [InlineData("Better Choice", null, SeafoodRatingColour.Green)]
    [InlineData("Eat Less", null, SeafoodRatingColour.Amber)]
    [InlineData("Say No", null, SeafoodRatingColour.Red)]
    [InlineData("Under Review", null, SeafoodRatingColour.Grey)]
    [InlineData("Best Choice", "Green", SeafoodRatingColour.Green)]
    [InlineData("Good Alternative", "Yellow", SeafoodRatingColour.Amber)]
    [InlineData("Avoid", "Red", SeafoodRatingColour.Red)]
    [InlineData("Certified", "Blue", SeafoodRatingColour.Blue)]
    // Seafood Watch's colour wins over the words; words the site does not know are grey.
    [InlineData("Something new", "Green", SeafoodRatingColour.Green)]
    [InlineData("Something new", null, SeafoodRatingColour.Grey)]
    public void Label_takes_the_guides_colour(string text, string? color, SeafoodRatingColour expected) {
        var label = SeafoodGuideSection.Label(text, color);
        Assert.Equal(expected, label.Colour);
        Assert.Equal(text, label.Text);
        Assert.Equal("seafood-rating seafood-rating-" + expected.ToString().ToLowerInvariant(), label.CssClass);
    }

    [Fact]
    public void GoodFish_first_then_Seafood_Watch() {
        var section = Build(Watch("1", "Avoid", "Red", waterBody: "Indian Ocean"), GoodFish("2", "Say No"));
        Assert.Equal(["GoodFish (Australia)", "Seafood Watch (United States)"], section.Tables.Select(t => t.Heading));
        Assert.Equal(["Fishery or farm", "Fishery or stock"], section.Tables.Select(t => t.FisheryHeading));
    }

    [Fact]
    public void Rows_go_from_green_to_red_then_certified_then_by_place() {
        var table = Build(
            Watch("1", "Certified", "Blue", region: "Fiji"),
            Watch("2", "Avoid", "Red", waterBody: "Indian Ocean"),
            Watch("3", "Good Alternative", "Yellow", waterBody: "Southeast Atlantic Ocean"),
            Watch("4", "Best Choice", "Green", region: "Maldives", waterBody: "Western Indian Ocean"),
            Watch("5", "Good Alternative", "Yellow", region: "United States", waterBody: "Gulf of Mexico")).Tables[0];
        Assert.Equal(["Best Choice", "Good Alternative", "Good Alternative", "Avoid", "Certified"],
            table.Rows.Select(r => r.Ratings[0].Text));
        Assert.Equal(["Southeast Atlantic Ocean"], table.Rows[1].Where.Lines);
        Assert.Equal(["United States", "Gulf of Mexico"], table.Rows[2].Where.Lines);
    }

    [Fact]
    public void Two_GoodFish_ratings_are_two_labels_and_sort_by_the_better() {
        var table = Build(GoodFish("1", "Say No", region: "NSW"), GoodFish("2", "Better Choice|Say No", note: "Choose line-caught.")).Tables[0];
        Assert.Equal(["Better Choice", "Say No"], table.Rows[0].Ratings.Select(l => l.Text));
        Assert.Equal("Choose line-caught.", table.Rows[0].Note);
        // Each label counts, so the total is the sum of the counts.
        Assert.Equal("3 ratings:", table.TotalText);
        Assert.Equal([("Better Choice", 1), ("Say No", 2)], table.Counts.Select(c => (c.Label.Text, c.Count)));
    }

    [Fact]
    public void Certified_rating_shows_its_certification_under_the_label() {
        var row = Build(Watch("1", "Certified", "Blue", region: "Australia",
            fishery: "Marine Stewardship Council (MSC) Certified|Australian Eastern Tuna and Billfish Fishery")).Tables[0].Rows[0];
        Assert.Equal("Marine Stewardship Council (MSC)", row.Certification);
        Assert.Equal(["Australian Eastern Tuna and Billfish Fishery"], row.Fishery.Lines);
    }

    [Fact]
    public void Certification_alone_leaves_the_fishery_not_given() {
        var table = Build(Watch("1", "Certified", "Blue", region: "Ireland", fishery: "Aquaculture Stewardship Council (ASC) Certified")).Tables[0];
        Assert.Equal("Aquaculture Stewardship Council (ASC)", table.Rows[0].Certification);
        Assert.True(table.Rows[0].Fishery.NoValue);
        Assert.False(table.ShowFishery);
    }

    [Fact]
    public void Certified_alone_names_no_certifier() {
        var row = Build(Watch("1", "Certified", "Blue", region: "Ireland", fishery: "Certified|Ireland Bottom Grown Mussel Fishery")).Tables[0].Rows[0];
        Assert.Null(row.Certification);
        Assert.Equal(["Ireland Bottom Grown Mussel Fishery"], row.Fishery.Lines);
    }

    [Fact]
    public void A_stock_line_is_not_a_certification() {
        var row = Build(Watch("1", "Avoid", "Red", waterBody: "Indian Ocean", fishery: "Small purse seine Fishery|Indian Ocean stock")).Tables[0].Rows[0];
        Assert.Null(row.Certification);
        Assert.Equal(["Small purse seine Fishery", "Indian Ocean stock"], row.Fishery.Lines);
    }

    [Theory]
    [InlineData("Drifting longlines", "wild", "Drifting longlines (wild)")]
    [InlineData("Otter trawls|Danish seines", "wild", "Otter trawls, Danish seines (wild)")]
    [InlineData("Marine net pen", "farmed", "Marine net pen (farmed)")]
    [InlineData("Boat dredges|Ropes", "wild|farmed", "Boat dredges, Ropes (wild and farmed)")]
    [InlineData(null, "farmed", "Farmed")]
    [InlineData("Handlines", null, "Handlines")]
    [InlineData(null, null, null)]
    public void Seafood_Watch_method_says_wild_or_farmed(string? method, string? production, string? expected) =>
        Assert.Equal(expected, SeafoodGuideSection.MethodText(Watch("1", "Avoid", "Red", method: method, production: production)));

    [Fact]
    public void GoodFish_method_is_as_written() =>
        Assert.Equal("Wild Caught", SeafoodGuideSection.MethodText(GoodFish("1", "Say No")));

    [Fact]
    public void No_place_is_not_given() {
        var table = Build(Watch("1", "Avoid", "Red")).Tables[0];
        Assert.True(table.Rows[0].Where.NoValue);
        Assert.Equal(["not given"], table.Rows[0].Where.Lines);
    }

    [Fact]
    public void Columns_show_only_with_a_value() {
        var table = Build(GoodFish("1", "Say No", method: null)).Tables[0];
        Assert.False(table.ShowMethod);
        Assert.False(table.ShowFishery);
        Assert.Equal(3, table.ColumnCount);
        Assert.False(table.ShowPageChanged);
    }

    [Fact]
    public void GoodFish_page_date_and_listed_name() {
        var row = Build(GoodFish("1", "Eat Less", modified: "2024-07-18", listedName: "Mustelus lenticulatus")).Tables[0].Rows[0];
        Assert.Equal("18 July 2024", row.PageChanged);
        Assert.Equal("<i>Mustelus lenticulatus</i>", row.ListedNameHtml);
    }

    [Fact]
    public void Ten_rows_are_open_eleven_are_closed_under_the_counts() {
        var ten = Enumerable.Range(1, SeafoodGuideSection.OpenRows).Select(i => Watch(i.ToString(), "Avoid", "Red", waterBody: "Ocean " + i)).ToArray();
        Assert.False(Build(ten).Tables[0].Collapsed);

        var eleven = ten.Append(Watch("11", "Best Choice", "Green", waterBody: "Ocean 11")).ToArray();
        var table = Build(eleven).Tables[0];
        Assert.True(table.Collapsed);
        Assert.Equal("11 ratings:", table.TotalText);
        Assert.Equal([("Best Choice", 1), ("Avoid", 10)], table.Counts.Select(c => (c.Label.Text, c.Count)));
    }

    [Fact]
    public void One_rating_is_singular() =>
        Assert.Equal("1 rating:", Build(GoodFish("1", "Say No")).Tables[0].TotalText);

    [Fact]
    public void Notes_name_the_guide_publisher_and_dates() {
        var section = Build(GoodFish("1", "Say No", modified: "2024-07-18"), Watch("2", "Avoid", "Red"));
        Assert.Equal("GoodFish ratings were downloaded on 28 October 2026 from GoodFish, Australia's Sustainable Seafood Guide, "
            + "published by the Australian Marine Conservation Society. GoodFish does not give the date of each rating; "
            + "the dates in the table are when GoodFish last changed each rating's web page.", Text(section.Tables[0].Note));
        Assert.Equal(("GoodFish, Australia's Sustainable Seafood Guide", "https://goodfish.org.au/"),
            section.Tables[0].Note.Where(s => s.Href is not null).Select(s => (s.Text, s.Href!)).Single());
        Assert.Equal("Seafood Watch ratings were downloaded on 27 October 2026 from Seafood Watch, published by the Monterey Bay Aquarium. "
            + "The date of each rating is in the rating's full report on the Seafood Watch website.", Text(section.Tables[1].Note));
    }

    [Fact]
    public void Notes_without_a_download_date_or_page_dates() {
        var section = SeafoodGuideSection.Build([GoodFish("1", "Say No"), Watch("2", "Avoid", "Red")], _ => null)!;
        Assert.Equal("GoodFish ratings are from GoodFish, Australia's Sustainable Seafood Guide, published by the Australian Marine "
            + "Conservation Society. GoodFish does not give the date of each rating.", Text(section.Tables[0].Note));
        Assert.StartsWith("Seafood Watch ratings are from Seafood Watch, published by", Text(section.Tables[1].Note));
    }

    // ------------------------------------------------------------ the page

    [Fact]
    public async Task Page_shows_each_guides_table_after_the_other_statuses() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.RegionalOnly}");
        var section = Html.Section(html, "seafood-guides");
        Assert.Contains("<h2 id=\"seafood-guides-heading\">Seafood guide ratings</h2>", section);
        Assert.Contains("not by the species' risk of extinction.", Html.Text(section).Replace("&#x27;", "'"));

        var rows = Html.TableRows(section);
        Assert.Equal(["Rating", "Where caught or farmed", "Method", "Fishery or farm", "Name in guide", "Page last changed"], rows[0]);
        Assert.Equal(["Better Choice Say No", "WA", "Wild Caught", "WA Northern Demersal Scalefish Fishery (284t in 2021) Pilbara Trawl Fishery",
            "Kovachev Gudgeon", "18 July 2024"], rows[1]);
        // The note: a row of its own under its rating, across the table's 6 columns.
        Assert.Equal(["GoodFish note: Choose line and trap caught gudgeon if available."], rows[2]);
        Assert.Contains("<td colspan=\"6\">", section);
        Assert.Equal(["Rating", "Where caught or farmed", "Method", "Fishery or stock", "Name in guide"], rows[3]);
        Assert.Equal(["Best Choice", "not given", "Handlines (wild)", "not given", "Bulgarian gudgeon Gobio bulgaricus"], rows[4]);
        Assert.Equal(["Good Alternative", "United States Gulf of Mexico", "Drifting longlines (wild)", "Western Atlantic stock", "Gudgeon"], rows[5]);
        Assert.Equal(["Avoid", "China", "Ponds (farmed)", "not given", "Gudgeon"], rows[6]);
        Assert.Equal(["Certified Marine Stewardship Council (MSC)", "Australia", "Otter trawls, Danish seines (wild)",
            "Great Australian Bight trawl fishery", "Gudgeon"], rows[7]);

        Assert.Contains("<span class=\"seafood-rating seafood-rating-green\">Better Choice</span> <span class=\"seafood-rating seafood-rating-red\">Say No</span>", section);
        Assert.Contains("<a href=\"https://example.org/goodfish/1658\">Kovachev Gudgeon</a>", section);
        Assert.Contains("<h3>GoodFish (Australia)</h3>", section);
        Assert.Contains("<h3>Seafood Watch (United States)</h3>", section);
        Assert.Contains("downloaded on 28 October 2026 from <a href=\"https://goodfish.org.au/\">", section);
        Assert.Contains("downloaded on 27 October 2026 from <a href=\"https://www.seafoodwatch.org/\">Seafood Watch</a>", section);
        // Each guide's note is under its own table.
        Assert.True(Html.IndexOf(section, "GoodFish ratings were downloaded") < Html.IndexOf(section, "<h3>Seafood Watch"));
        // Four rows: the table is open, with no count line.
        Assert.DoesNotContain("<details", section);

        // After the other conservation statuses' place and before the names.
        Assert.True(Html.IndexOf(html, "id=\"seafood-guides\"") < Html.IndexOf(html, "id=\"names-heading\""));
    }

    [Fact]
    public async Task Page_without_ratings_has_no_section() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.Tiger}");
        Assert.DoesNotContain("seafood-guides", html);
        Assert.DoesNotContain("Seafood guide ratings", html);
    }
}
