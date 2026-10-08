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
    [InlineData("WACA-Sch5_sect9.4b", "Schedule 5 Section 9.4b", null, "Great Britain", null, "jncc-waca", "Schedule 5, section 9.4b")]
    [InlineData("WACA-Sch1_part1", "Schedule 1 - Part 1", null, "Great Britain", null, "jncc-waca", "Schedule 1, Part 1")]
    [InlineData("England_NERC_S.41", "England NERC S.41", null, "England", null, "jncc-nerc-s41", "Species of principal importance")]
    public void DesignationsGoToTheirList(string code, string designation, string? statusCode, string? area, string? population,
        string listKey, string status) {
        var classified = JnccLists.Classify(code, designation, statusCode, area, population);
        Assert.NotNull(classified);
        Assert.Equal(listKey, classified.Value.List.Key);
        Assert.Equal(status, classified.Value.Status);
    }

    [Fact]
    public void ACodeTheSiteDoesNotKnowIsLeftOut() =>
        Assert.Null(JnccLists.Classify("Bern-A2", "Bern Convention Appendix 2", null, "Great Britain", null));
}
