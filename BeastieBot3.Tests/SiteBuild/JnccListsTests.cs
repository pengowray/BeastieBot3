using BeastieBot3.SiteBuild;

namespace BeastieBot3.Tests.SiteBuild;

// Pins JnccLists.Classify: the list each JNCC designation code goes to on the species page and the
// status shown, for the codes of the 9 June 2026 workbook.
public class JnccListsTests {
    [Theory]
    [InlineData("RedList_GB_post2001-VU", "Vulnerable", "VU", "Great Britain", null, "jncc-redlist-gb", "Vulnerable")]
    [InlineData("RedList_GB_post2001-LC", "Least concern", "LC", "Great Britain", null, "jncc-redlist-gb", "Least Concern")]
    [InlineData("RedList_GB_post2001-EX", "Extinct", "EX", "England", null, "jncc-redlist-england", "Extinct")]
    [InlineData("Bird_RedList_GB_post2001-EN_Breeding", "Endangered", "EN", "Great Britain", "Breeding", "jncc-redlist-gb", "Endangered (breeding)")]
    [InlineData("RedList_GB_Pre94-Insu", "IUCN (pre 1994) - Insufficiently known", "Insu", "Great Britain", null, "jncc-redlist-gb-pre1994", "Insufficiently Known")]
    [InlineData("Bird-Red", "Bird Population Status - red", "Red", "United Kingdom", null, "jncc-bocc", "Red")]
    [InlineData("NS-excludes", "Nationally Scarce. Excludes Red Listed taxa", null, "Great Britain", null, "jncc-rarity", "Nationally Scarce")]
    [InlineData("WACA-Sch5_sect9.4b", "Schedule 5 Section 9.4b", null, "Great Britain", null, "jncc-waca", "Schedule 5")]
    [InlineData("WACA-Sch1_part1", "Schedule 1 - Part 1", null, "Great Britain", null, "jncc-waca", "Schedule 1, Part 1")]
    [InlineData("England_NERC_S.41", "England NERC S.41", null, "England", null, "jncc-nerc-s41", "Species of principal importance")]
    public void DesignationsGoToTheirList(string code, string designation, string? statusCode, string? area, string? population,
        string listKey, string status) {
        var classified = JnccLists.Classify(code, designation, statusCode, area, population);
        Assert.NotNull(classified);
        Assert.Equal(listKey, classified.List.Key);
        Assert.Equal(status, classified.Status);
    }

    [Theory]
    [InlineData("Schedule 5 Section 9.4b", "Schedule 5", "9(4)(b)")]
    [InlineData("Schedule 5 Section 9.4.a", "Schedule 5", "9(4)(a)")]
    [InlineData("Schedule 5 Section 9.4A", "Schedule 5", "9(4A)")]
    [InlineData("Schedule 5 Section 9.2", "Schedule 5", "9(2)")]
    [InlineData("Schedule 5 Section 9.1 (killing/injuring)", "Schedule 5", "9(1) (killing or injuring)")]
    [InlineData("Schedule 8 - Part 1", "Schedule 8, Part 1", null)]
    [InlineData("Schedule 1 Part 2", "Schedule 1, Part 2", null)]
    public void LawDesignationsAreAScheduleAndASection(string designation, string schedule, string? section) =>
        Assert.Equal((schedule, section), JnccLists.Schedule(designation));

    [Fact]
    public void SectionsAreJoinedAsUkLawWritesThem() {
        Assert.Equal("section 9(2)", JnccLists.Sections(["9(2)"]));
        Assert.Equal("sections 9(4)(b) and 9(5)(a)", JnccLists.Sections(["9(4)(b)", "9(5)(a)"]));
        Assert.Equal("sections 9(1) (taking), 9(2) and 9(4)(b)", JnccLists.Sections(["9(1) (taking)", "9(2)", "9(4)(b)"]));
    }

    [Fact]
    public void ACodeTheSiteDoesNotKnowIsLeftOut() =>
        Assert.Null(JnccLists.Classify("Bern-A2", "Bern Convention Appendix 2", null, "Great Britain", null));
}
