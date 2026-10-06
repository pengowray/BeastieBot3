namespace BeastieBot3.Site.Tests;

// The group page's species tables over the fixture: the polar bear in genus Ursus, family Ursidae.
public sealed class SpeciesTablePageTests(SiteFactory factory) : IClassFixture<SiteFactory> {
    private readonly HttpClient _client = factory.Client();

    [Fact]
    public async Task TableTypeWritesASpeciesTableWithAListDefinedReference() {
        var html = await _client.GetStringAsync("/taxa/family/ursidae?type=table");
        var wikitext = Html.Textarea(html, "list-wikitext");

        Assert.Equal("""
            {{IUCN statuses|ex=0|ew=0|cr=0|en=0|vu=1|nt=0|lc=0|dd=0|ne=0}}

            {{Species table |no-note=y |genus=[[Ursus]] |authority-name= |authority-year= |species-count=one}}
            {{Species table/row
            |name=[[Polar bear]] |binomial=U. maritimus
            |image= |image-alt=
            |authority-name=Phipps |authority-year=1774
            |range= |range-image=
            |size= |habitat= |diet=
            |iucn-status=VU |population=Unknown
            |direction={{population change unknown}}<ref name="IUCNPolarbear"/>
            }}
            {{Species table/end}}
            """.ReplaceLineEndings("\n"), wikitext[..wikitext.IndexOf("\n\n{{reflist", StringComparison.Ordinal)]);
        Assert.Contains("{{reflist|refs=\n<ref name=\"IUCNPolarbear\">{{cite iucn |author=Wiig, Ø.", wikitext);
        Assert.Contains("|article-number=e.T22823A14871490 |doi=10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en", wikitext);
        Assert.Contains("1 species in 1 table", html);
        Assert.Contains("<caption>Genus <i>Ursus</i> – one species</caption>", html);
        // The bullet-list options are hidden, the table options shown.
        Assert.Contains("data-list-type=\"bullets\" hidden", html);
        Assert.Contains("data-options-url=\"/taxa/family/ursidae?type=table\"", html);
    }

    [Fact]
    public async Task CiteQWithNoWikidataItemFallsBackToCiteIucnInTheRow() {
        var html = await _client.GetStringAsync("/taxa/family/ursidae?type=table&refs=inline&refnames=id&cite=q&cols=noecology&summary=0");
        var wikitext = Html.Textarea(html, "list-wikitext");

        Assert.StartsWith("{{Species table |no-note=y |no-ecology=yes |genus=[[Ursus]]", wikitext);
        Assert.Contains("{{population change unknown}}<ref name=\"iucn-22823\">{{cite iucn |author=Wiig, Ø.", wikitext);
        Assert.DoesNotContain("IUCN statuses", wikitext);
        Assert.DoesNotContain("reflist", wikitext);
    }

    [Fact]
    public async Task BulletListIsUnchangedByTheTableOptions() {
        var html = await _client.GetStringAsync("/taxa/family/ursidae?refs=none&cite=q");

        Assert.Equal("* [[Polar bear]] {{IUCN status|VU|22823/14871490|1|year=2015}}", Html.Textarea(html, "list-wikitext"));
        Assert.Contains("data-list-type=\"tables\" hidden", html);
    }
}
