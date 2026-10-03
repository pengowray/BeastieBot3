using System.Collections.Generic;
using System.Text.Json;
using BeastieBot3.Taxonomy;

namespace BeastieBot3.Tests;

// TaxoboxParser on taxoboxes copied from English Wikipedia articles in the Wikipedia cache. Each one
// was parsed wrongly before: a nested template kept one brace ("{sfn|...}"), and a parameter was
// only found at the start of a line, so "| name = Downy oak| image = ..." and a line starting
// " | status = LC" ran into the parameter before them.
public class TaxoboxParserTests {
    private static Dictionary<string, string> Fields(string wikitext) {
        var data = TaxoboxParser.TryParse(1, wikitext);
        Assert.NotNull(data);
        return JsonSerializer.Deserialize<Dictionary<string, string>>(data!.DataJson!)!;
    }

    [Fact]
    public void ParameterOnALineStartingWithASpaceAndAPipe_IsItsOwnParameter() {
        // Grey's mudsnake
        var fields = Fields("""
            {{speciesbox
            | name = Grey's mudsnake
             | status = LC
             | status_system = IUCN3.1
             | status_ref = <ref name="iucn status 19 November 2021">{{cite iucn |author=Lukoschek, V. |date=2010 |title=''Ephalophis greyae'' |access-date=19 November 2021}}</ref>
            | genus = Ephalophis
            | species = greyae
            }}
            """);

        Assert.Equal("Grey's mudsnake", fields["name"]);
        Assert.Equal("LC", fields["status"]);
        Assert.Equal("IUCN3.1", fields["status_system"]);
        Assert.Equal("<ref name=\"iucn status 19 November 2021\">{{cite iucn |author=Lukoschek, V. |date=2010 |title=''Ephalophis greyae'' |access-date=19 November 2021}}</ref>",
            fields["status_ref"]);
        Assert.Equal("greyae", fields["species"]);
    }

    [Fact]
    public void TwoParametersOnOneLine_AreSplit() {
        // Quercus pubescens
        var fields = Fields("""
            {{Speciesbox
            | name = Downy oak| image = Quercus pubescens Tuscany.jpg
            | image_caption = A mature tree
            | genus = Quercus
            | species = pubescens
            | synonyms = {{collapsible list|bullets = true
              |''Eriodrys lanata'' <small>Raf.</small>
              |''Quercus adjecta'' <small>Gand.</small>
            }}
            }}
            """);

        Assert.Equal("Downy oak", fields["name"]);
        Assert.Equal("Quercus pubescens Tuscany.jpg", fields["image"]);
        Assert.Equal("A mature tree", fields["image_caption"]);
        // The pipes and "=" inside the nested list belong to it, not to the taxobox.
        Assert.Equal("{{collapsible list|bullets = true |''Eriodrys lanata'' <small>Raf.</small> |''Quercus adjecta'' <small>Gand.</small> }}",
            fields["synonyms"]);
        Assert.False(fields.ContainsKey("bullets"));
    }

    [Fact]
    public void EmptyParameterAfterTheNameOnTheSameLine_IsSplit() {
        // Dillwynia juniperina
        var fields = Fields("""
            {{speciesbox
            |name = Prickly parrotpea|image =
            |image_caption =
            |genus = Dillwynia
            |species = juniperina
            }}
            """);

        Assert.Equal("Prickly parrotpea", fields["name"]);
        Assert.Equal("", fields["image"]);
        Assert.Equal("juniperina", fields["species"]);
    }

    [Fact]
    public void NestedTemplate_KeepsBothBraces() {
        // Sunda slow loris
        var fields = Fields("""
            {{Speciesbox
            | name = Sunda slow loris{{sfn|Groves|2005|p=122}}
            | image = Nycticebus coucang 004.jpg
            | status2_ref = {{r|CITES}}
            | genus = Nycticebus
            | species = coucang
            }}
            """);

        Assert.Equal("Sunda slow loris{{sfn|Groves|2005|p=122}}", fields["name"]);
        Assert.Equal("{{r|CITES}}", fields["status2_ref"]);
        Assert.Equal("Nycticebus coucang 004.jpg", fields["image"]);
    }

    [Fact]
    public void TemplatesNestedTwoDeep_KeepTheirBraces() {
        // Humpback whale
        var fields = Fields("""
            {{Speciesbox
            | fossil_range = {{fossil range|7.5|0|ref={{r|Arnason_etal_2018}}{{r|fossil}}}} [[Late Miocene]] – [[Recent]]
            | name = Humpback whale{{r|MSW3}}
            | status = LC
            | status_ref = {{R|iucn}}
             | status2 = CITES_A1
             | status2_system = CITES
            | genus = Megaptera
            | species = novaeangliae
            }}
            """);

        Assert.Equal("{{fossil range|7.5|0|ref={{r|Arnason_etal_2018}}{{r|fossil}}}} [[Late Miocene]] – [[Recent]]", fields["fossil_range"]);
        Assert.Equal("Humpback whale{{r|MSW3}}", fields["name"]);
        Assert.Equal("{{R|iucn}}", fields["status_ref"]);
        Assert.Equal("CITES_A1", fields["status2"]);
        Assert.Equal("CITES", fields["status2_system"]);
        Assert.False(fields.ContainsKey("ref"));
    }

    [Fact]
    public void ImageOnTheLineAfterTheName_IsNotPartOfTheName() {
        // Chapala chub
        var data = TaxoboxParser.TryParse(1, """
            {{Speciesbox
            | name = Chapala chub
             | image = FMIB 40490 Falcula chapalae Jordan & Snyder, new genus and species Type.jpeg
            | status = EN
            | taxon = Yuriria chapalae
            | authority = ([[David Starr Jordan|Jordan]] & [[John Otterbein Snyder|Snyder]], 1899)
            | synonyms =
            }}
            """);

        Assert.NotNull(data);
        var fields = JsonSerializer.Deserialize<Dictionary<string, string>>(data!.DataJson!)!;
        Assert.Equal("Chapala chub", fields["name"]);
        Assert.Equal("FMIB 40490 Falcula chapalae Jordan & Snyder, new genus and species Type.jpeg", fields["image"]);
        Assert.Equal("([[David Starr Jordan|Jordan]] & [[John Otterbein Snyder|Snyder]], 1899)", fields["authority"]);
        Assert.Equal("Yuriria chapalae", data.ScientificName);
        Assert.Equal("species", data.Rank);
    }

    [Fact]
    public void CitationParametersOnTheirOwnLines_StayInsideTheCitation() {
        // Pereskia aculeata: the {{cite web}} parameters start lines with "|", and used to be read
        // as taxobox parameters "url", "title" and "access-date".
        var fields = Fields("""
            {{Speciesbox
            |image = Pereskia aculeata4 cropped.jpg
            |genus = Pereskia
            |species = aculeata
            |synonyms_ref = <ref>{{cite web
            |url=http://www.theplantlist.org/tpl1.1/record/kew-2414656
            |title=The Plant List: A Working List of All Plant Species
            |access-date=16 May 2014}}</ref>
            |}}
            """);

        Assert.Equal("<ref>{{cite web |url=http://www.theplantlist.org/tpl1.1/record/kew-2414656 |title=The Plant List: A Working List of All Plant Species |access-date=16 May 2014}}</ref>",
            fields["synonyms_ref"]);
        Assert.False(fields.ContainsKey("url"));
        Assert.False(fields.ContainsKey("title"));
        Assert.Equal("aculeata", fields["species"]);
    }

    [Fact]
    public void PipesInsideCommentsAndReferences_DoNotSplitParameters() {
        var fields = Fields("""
            {{Speciesbox
            | name = Red mullet <!-- not | image = x -->
            | status_ref = <ref>Smith | 2005</ref>
            | genus = Mullus
            | species = barbatus
            }}
            """);

        Assert.Equal("Red mullet <!-- not | image = x -->", fields["name"]);
        Assert.Equal("<ref>Smith | 2005</ref>", fields["status_ref"]);
        Assert.False(fields.ContainsKey("image"));
    }

    [Fact]
    public void ParametersOnSeparateLines_AreReadAsBefore() {
        var data = TaxoboxParser.TryParse(1, """
            {{Taxobox
            | name = Lion
            | regnum = [[Animal]]ia
            | familia = [[Felidae]]
            | genus = ''[[Panthera]]''
            | species = '''''P. leo'''''
            | binomial = ''Panthera leo''
            | synonyms =
            * ''Felis leo''
            * ''Leo leo''
            }}
            """);

        Assert.NotNull(data);
        Assert.Equal("''Panthera leo''", JsonSerializer.Deserialize<Dictionary<string, string>>(data!.DataJson!)!["binomial"]);
        Assert.Equal("[[Animal]]ia", data.Kingdom);
        Assert.Equal("[[Felidae]]", data.Family);
        Assert.Equal("* ''Felis leo'' * ''Leo leo''", JsonSerializer.Deserialize<Dictionary<string, string>>(data.DataJson!)!["synonyms"]);
    }
}
