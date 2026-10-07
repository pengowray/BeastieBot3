using System.Collections.Generic;
using BeastieBot3.SiteBuild;

namespace BeastieBot3.Tests;

public class SiteTaxoboxStatusTests {
    private static Dictionary<string, string> F(params (string Key, string Value)[] fields) {
        var d = new Dictionary<string, string>();
        foreach (var (k, v) in fields) {
            d[k] = v;
        }
        return d;
    }

    [Fact]
    public void TheSubjectIsTheGenusAndSpeciesOrTheTaxon() {
        Assert.Equal("Panthera leo", SiteTaxoboxStatusReader.Subject(F(("genus", "Panthera"), ("species", "leo<ref>MSW3</ref>"))));
        Assert.Equal("Panthera tigris sumatrae", SiteTaxoboxStatusReader.Subject(F(("genus", "Panthera"), ("species", "tigris"), ("subspecies", "sumatrae"))));
        Assert.Equal("Quercus robur", SiteTaxoboxStatusReader.Subject(F(("taxon", "''Quercus robur''"))));
        Assert.Null(SiteTaxoboxStatusReader.Subject(F(("name", "Lion"))));
    }

    [Theory]
    [InlineData("Panthera tigris sumatrae", "Panthera tigris ssp. sumatrae", true)]
    [InlineData("Impatiens engleri pubescens", "Impatiens engleri subsp. pubescens", true)]
    [InlineData("Panthera", "Panthera leo", false)]
    public void TheSubjectIsComparedWithoutRankMarkers(string subject, string name, bool same) =>
        Assert.Equal(same, SiteTaxoboxStatusReader.SameTaxon(subject, name));

    [Fact]
    public void OnlyAnIucnStatusIsRead() {
        Assert.Equal(("VU", "IUCN3.1"), SiteTaxoboxStatusReader.Status(F(("status", "VU"), ("status_system", "iucn3.1"))));
        Assert.Equal(((string?)null, (string?)null), SiteTaxoboxStatusReader.Status(F(("status", "G5"), ("status_system", "TNC"))));
        Assert.Equal(((string?)null, (string?)null), SiteTaxoboxStatusReader.Status(F(("status_system", "IUCN3.1"))));
    }

    [Theory]
    [InlineData("<ref>{{cite iucn |article-number=e.T15951A259030422 |doi=10.2305/IUCN.UK.2024-1.RLTS.T15951A259030422.en}}</ref>", "", 259030422L)]
    [InlineData("<ref>[https://www.iucnredlist.org/species/15951/259030422 Lion]</ref>", "", 259030422L)]
    [InlineData("<ref name=\"IUCN\" />", "Text.<ref name=\"IUCN\">{{cite iucn |article-number=e.T15951A115130419}}</ref>", 115130419L)]
    [InlineData("<ref name=IUCN/>", "No such reference.", null)]
    [InlineData("<ref>Some book.</ref>", "", null)]
    public void TheReferenceGivesTheAssessmentItCites(string statusRef, string wikitext, long? id) =>
        Assert.Equal(id, SiteTaxoboxStatusReader.RefAssessmentId(statusRef, wikitext));
}
