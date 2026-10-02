using BeastieBot3.Shared.SiteData;

namespace BeastieBot3.Site.Tests;

public class SkeletonTests {
    [Fact]
    public void FoldLowercasesAndStripsDiacritics() =>
        Assert.Equal("ours polaire etre", SiteNameKey.Fold("  Ours  Polaire\tÊtre "));
}
