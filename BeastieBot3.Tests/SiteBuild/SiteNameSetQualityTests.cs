using System.Linq;
using BeastieBot3.SiteBuild;

namespace BeastieBot3.Tests.SiteBuild;

// SiteNameSet passes common names through CommonNameQuality: junk is left out of the site's name
// table and a fixable name is stored repaired. Scientific names and synonyms are not checked.
public class SiteNameSetQualityTests {
    [Fact]
    public void JunkCommonNames_AreLeftOut() {
        var names = new SiteNameSet();

        Assert.False(names.Add("Weil 3 bauch-Graslandmaus", "common", "en", "col"));
        Assert.False(names.Add("meaning large bear cat", "common", "en", "col"));
        Assert.False(names.Add("| image = FMIB 51989 Crystal Darter.jpeg", "common", "en", "wikipedia"));
        Assert.False(names.Add("Pholidoscelis polops (Cope", "common", "en", "col"));

        Assert.Empty(names.Names);
    }

    [Fact]
    public void RepairableCommonNames_AreStoredRepaired() {
        var names = new SiteNameSet();

        Assert.True(names.Add("Sunda slow loris{sfn|Groves|2005|p=122}", "common", "en", "wikipedia"));
        Assert.True(names.Add("Da Xiong Mao (meaning large bear cat)", "common", "en", "col"));
        Assert.True(names.Add("\\\\ Woolly Akodont", "common", "en", "col"));
        Assert.True(names.Add("chabo-chidori (チャボチドリ, 矮鶏千鳥", "common", "ja", "iucn"));

        Assert.Equal(new[] { "Sunda slow loris", "Da Xiong Mao", "Woolly Akodont", "chabo-chidori (チャボチドリ, 矮鶏千鳥)" },
            names.Names.Select(n => n.Name));
    }

    [Fact]
    public void RepairedName_IsTheSameNameAsItsCleanCopy() {
        var names = new SiteNameSet();

        Assert.True(names.Add("Sunda slow loris", "common", "en", "wikipedia"));
        Assert.False(names.Add("Sunda slow loris{sfn|Groves|2005|p=122}", "common", "en", "wikipedia"));
    }

    // The build summary's counts. The store repeats IUCN's names, so the same junk name can be
    // offered twice from one source; it is one name left out.
    [Fact]
    public void JunkCommonNames_AreCountedOncePerNameLanguageAndSource() {
        var names = new SiteNameSet();

        names.Add("Calvert, 1902", "common", "en", "iucn");
        names.Add("Calvert, 1902", "common", "en", "iucn");
        names.Add("Calvert, 1902", "common", "en", "col");
        names.Add("Weil 3 bauch-Graslandmaus", "common", "en", "col");

        Assert.Equal(3, names.JunkCommonNames);
        Assert.Equal(0, names.RepairedCommonNames);
    }

    [Fact]
    public void RepairedCommonNames_AreCountedOnlyWhenAdded() {
        var names = new SiteNameSet();

        names.Add("Sunda slow loris", "common", "en", "iucn");
        // The same name as the clean copy from this source: not added.
        names.Add("Sunda slow loris{sfn|Groves|2005|p=122}", "common", "en", "iucn");
        names.Add("Sunda slow loris{sfn|Groves|2005|p=122}", "common", "en", "wikipedia");
        names.Add("Sunda slow loris{sfn|Groves|2005|p=122}", "common", "en", "wikipedia");
        names.Add("Da Xiong Mao (meaning large bear cat)", "common", "en", "col");

        Assert.Equal(2, names.RepairedCommonNames);
        Assert.Equal(0, names.JunkCommonNames);
    }

    [Fact]
    public void DigitRule_OnlyAppliesToEnglishNames() {
        var names = new SiteNameSet();

        Assert.True(names.Add("Libélula de 4 manchas", "common", "es", "iucn"));
        Assert.True(names.Add("Tortuga B2", "common", "es", "iucn"));
    }

    [Fact]
    public void ScientificNames_AreNotChecked() {
        var names = new SiteNameSet();

        Assert.True(names.Add("Pholidoscelis polops (Cope", "synonym", null, "col"));
        Assert.Equal(0, names.JunkCommonNames);
    }
}
