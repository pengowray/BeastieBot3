using System.Collections.Generic;
using System.Linq;
using BeastieBot3.Taxonomy;

namespace BeastieBot3.Tests;

// TaxoboxSynonymsParser on "synonyms" values copied from English Wikipedia taxoboxes in the
// Wikipedia cache, one test per way of writing the list.
public class TaxoboxSynonymsParserTests {
    private static List<(string Name, string? Authority)> Parse(string wikitext) =>
        TaxoboxSynonymsParser.Parse(wikitext).Select(s => (s.Name, s.Authority)).ToList();

    [Fact]
    public void ItalicNamesWithSmallAuthorities_SeparatedByBreaks() {
        var result = Parse("''Hylorana tytleri'' <small>Theobald, 1868</small><br/> ''Rana tytleri'' <small>(Theobald, 1868)</small>");
        Assert.Equal(new List<(string, string?)> {
            ("Hylorana tytleri", "Theobald, 1868"),
            ("Rana tytleri", "(Theobald, 1868)"),
        }, result);
    }

    [Fact]
    public void BulletsOnOneLine_WithoutAuthorities() {
        var result = Parse("*''Thanaos marloyi'' *''Thanaos sericea''");
        Assert.Equal(new List<(string, string?)> {
            ("Thanaos marloyi", null),
            ("Thanaos sericea", null),
        }, result);
    }

    [Fact]
    public void SpeciesListTemplate_PairsNamesWithAuthorities() {
        var result = Parse("{{Species list | Amoria hybrida | (L.) C.Presl (1831) | Trifolium elegans subsp. hybridum | (L.) Bonnier & Layens (1894) }}");
        Assert.Equal(new List<(string, string?)> {
            ("Amoria hybrida", "(L.) C.Presl (1831)"),
            ("Trifolium elegans subsp. hybridum", "(L.) Bonnier & Layens (1894)"),
        }, result);
    }

    [Fact]
    public void SpeciesListTemplate_NamedParametersAreNotNames() {
        var result = Parse("{{Specieslist |header=17 synonyms |hidden=yes | Doryichthys sculptus|Günther, 1870|Doryrhamphus macgregori|[[David Starr Jordan|Jordan]] & [[Robert Earl Richardson|Richardson]], 1908}}");
        Assert.Equal(new List<(string, string?)> {
            ("Doryichthys sculptus", "Günther, 1870"),
            ("Doryrhamphus macgregori", "Jordan & Richardson, 1908"),
        }, result);
    }

    [Fact]
    public void CollapsibleList_WithSpeciesListInside() {
        var result = Parse("{{Collapsible list |title={{nowrap|''T. kurabayashii''}} |{{Species list |Trillium kurabayashii f.&nbsp;luteum |V.G.Soukup }}}}");
        Assert.Equal(new List<(string, string?)> { ("Trillium kurabayashii f. luteum", "V.G.Soukup") }, result);
    }

    [Fact]
    public void CollapsibleList_ItemsAreEntries() {
        var result = Parse("{{collapsible list|bullets = true |''Acer dissectum'' var. ''tenuifolium'' <small>(Koidz.) Koidz.</small> |''Acer japonicum'' f. ''aureum'' <small>(Siesmayer) Schwer.</small>}}");
        Assert.Equal(new List<(string, string?)> {
            ("Acer dissectum var. tenuifolium", "(Koidz.) Koidz."),
            ("Acer japonicum f. aureum", "(Siesmayer) Schwer."),
        }, result);
    }

    [Fact]
    public void Plainlist_WithStyleParameter() {
        var result = Parse("{{Plainlist | style = font-size:90% | * ''Ficus aggregata'' <small>Miq.</small> * ''Urostigma reflexum'' <small>(Thunb.) Miq.</small>}}");
        Assert.Equal(new List<(string, string?)> {
            ("Ficus aggregata", "Miq."),
            ("Urostigma reflexum", "(Thunb.) Miq."),
        }, result);
    }

    [Fact]
    public void AuTemplateAndItalicEx() {
        var result = Parse("*''Althaea kragujevacensis'' {{Au|Pančić ''ex'' Diklić & Stevan.}} *''Malva althaea'' {{Au|E.H.L.Krause}}");
        Assert.Equal(new List<(string, string?)> {
            ("Althaea kragujevacensis", "Pančić ex Diklić & Stevan."),
            ("Malva althaea", "E.H.L.Krause"),
        }, result);
    }

    [Fact]
    public void PlainAuthorityAfterTheName_LosesItsNomenclaturalNote() {
        var result = Parse("''Cairina sylvestris'' Stephens, 1824, ''nom. superfl''.");
        Assert.Equal(new List<(string, string?)> { ("Cairina sylvestris", "Stephens, 1824") }, result);
    }

    [Fact]
    public void AuthorityOnTheNextLine_AfterADash_BelongsToTheName() {
        var result = Parse("*''Distira annandalei'' <br/>{{small|Laidlaw, 1901}} *''Kolpophis annandalei'' <br/>{{small|— M.A. Smith, 1926}}");
        Assert.Equal(new List<(string, string?)> {
            ("Distira annandalei", "Laidlaw, 1901"),
            ("Kolpophis annandalei", "M.A. Smith, 1926"),
        }, result);
    }

    [Fact]
    public void ReferencesCommentsAndLinksAreRemoved() {
        var result = Parse("''Profundulus hildebrandi'' <small>Miller, 1950</small><ref name = CofF/> <!-- old --> *''[[Testudo (genus)|Testudo]] flava'' <small>[[Bernard Germain de Lacépède|Lacépède]], 1788</small><ref>{{cite book |title=X}}</ref>");
        Assert.Equal(new List<(string, string?)> {
            ("Profundulus hildebrandi", "Miller, 1950"),
            ("Testudo flava", "Lacépède, 1788"),
        }, result);
    }

    [Fact]
    public void AmbiguousSynonymMarkerIsDropped() {
        var result = Parse("''Carcharias amblyrhynchos'' <small>Bleeker, 1856</small><br /> ''Carcharias menisorrah''* <small>Müller & Henle, 1839</small><br /> <small>*ambiguous synonym</small>");
        Assert.Equal(new List<(string, string?)> {
            ("Carcharias amblyrhynchos", "Bleeker, 1856"),
            ("Carcharias menisorrah", "Müller & Henle, 1839"),
        }, result);
    }

    [Fact]
    public void NotesInTheAuthorityAreRemoved() {
        var result = Parse("*''Cassia chinensis'' <small>Jacq., nom. illeg.</small> *''Montastrea annularis'' <small>(Ellis & Solander, 1786)</small> [lapsus] *''Ixora densa'' <small>R.Br. ex Wall. [Invalid]</small> *''Posidonia caulini'' {{small|St.-Lag., [[orthographic variant]]}} *''Pouteria campechiana'' var. ''typica'' {{small|Baehni, not validly publ.}} *''Testudo meleagris'' <small>Shaw, 1793</small> <br />''(nomen suppressum)''");
        Assert.Equal(new List<(string, string?)> {
            ("Cassia chinensis", "Jacq."),
            ("Montastrea annularis", "(Ellis & Solander, 1786)"),
            ("Ixora densa", "R.Br. ex Wall."),
            ("Posidonia caulini", "St.-Lag."),
            ("Pouteria campechiana var. typica", "Baehni"),
            ("Testudo meleagris", "Shaw, 1793"),
        }, result);
    }

    [Fact]
    public void QuotedYearStaysInTheAuthority() {
        var result = Parse("''Fejervarya granosa'' <small>Kuramoto, Joshy, Kurabayashi, and Sumida, 2008 \"2007\"</small>");
        Assert.Equal(new List<(string, string?)> { ("Fejervarya granosa", "Kuramoto, Joshy, Kurabayashi, and Sumida, 2008 \"2007\"") }, result);
    }

    [Fact]
    public void BracketedAuthorsAndYearsStay() {
        var result = Parse("*''Sphinx spheciformis'' <small>[Denis & Schiffermüller], 1775</small> *''Polyommatus asteris'' <small>Godart, [1824]</small>");
        Assert.Equal(new List<(string, string?)> {
            ("Sphinx spheciformis", "[Denis & Schiffermüller], 1775"),
            ("Polyommatus asteris", "Godart, [1824]"),
        }, result);
    }

    [Fact]
    public void SubgenusAndRankMarkersStayInTheName() {
        var result = Parse("''Rana (Rana) bilineata'' <small>Smith</small><br/>''Terminalia catappa'' var. ''pubescens'' <small>Miq.</small><br/>''Leuciscus (Telestes) souffia'' ssp. ''keadicus'' <small>Stephanidis, 1971</small><br/>*''Iolaus'' (''Iolaphilus'') ''carolinae''");
        Assert.Equal(new List<string> {
            "Rana (Rana) bilineata", "Terminalia catappa var. pubescens", "Leuciscus (Telestes) souffia ssp. keadicus", "Iolaus (Iolaphilus) carolinae",
        }, result.Select(r => r.Name).ToList());
    }

    [Fact]
    public void HybridMarkerStaysInTheName() {
        var result = Parse("* ''Eucalyptus × cordieri'' var. ''nortoni'' <small>Blakely</small>");
        Assert.Equal(new List<(string, string?)> { ("Eucalyptus × cordieri var. nortoni", "Blakely") }, result);
    }

    [Fact]
    public void NamesWithoutItalics_AndNoGapBetweenEntries() {
        var result = Parse("*''Juncus hylanderi'' (Hämet-Ahti) Tzvelev & Glazkova *Juncus lamprocarpus Ehrh. ''Zakerana keralensis'' <small>(Dubois, 1981)</small>");
        Assert.Equal(new List<(string, string?)> {
            ("Juncus hylanderi", "(Hämet-Ahti) Tzvelev & Glazkova"),
            ("Juncus lamprocarpus", "Ehrh."),
            ("Zakerana keralensis", "(Dubois, 1981)"),
        }, result);
    }

    [Fact]
    public void WholeEntryInSmallType() {
        var result = Parse("* <small>''Rottlera discolor''</small> <small>[[F.Muell.]]</small> *<small>''Orania nicobarica'' Kurz</small>");
        Assert.Equal(new List<(string, string?)> {
            ("Rottlera discolor", "F.Muell."),
            ("Orania nicobarica", "Kurz"),
        }, result);
    }

    [Fact]
    public void FullStopAfterTheName() {
        var result = Parse("* ''Callichthys paleatus''. Jenyns, 1842. * ''Corydoras maculatus''. [[Franz Steindachner|Steindachner]], 1879.");
        Assert.Equal(new List<(string, string?)> {
            ("Callichthys paleatus", "Jenyns, 1842"),
            ("Corydoras maculatus", "Steindachner, 1879"),
        }, result);
    }

    [Fact]
    public void GenusOnlyFamilyNamesAndFreeTextAreDropped() {
        Assert.Empty(Parse("Around 80, including:"));
        Assert.Empty(Parse("See text"));
        Assert.Empty(Parse("Red-browed parrot"));
        Assert.Empty(Parse("Many, see [[#Synonyms|text]]"));
        Assert.Empty(Parse("Colymbiformes <small>[[Richard Bowdler Sharpe|Sharpe]], 1891</small>"));
        Assert.Empty(Parse("* ''Callula'' {{small|Günther, 1877}} (partial) * ''Mantipus'' {{small|Peters, 1883}}"));
        Assert.Empty(Parse("''T. poliocephalus leucocephalus''"));
        Assert.Empty(Parse("*''Parinari'' sect. ''Neocarya'' <small>DC.</small> *''Aster'' subg. ''Symphyotrichum''"));
    }

    [Fact]
    public void FreeTextBeforeAList_IsNotAName() {
        var result = Parse("Around 80, including: *''Ficus aggregata'' Vahl *''Ficus punctata'' Lam.");
        Assert.Equal(new List<(string, string?)> {
            ("Ficus aggregata", "Vahl"),
            ("Ficus punctata", "Lam."),
        }, result);
    }

    [Fact]
    public void DuplicateName_KeepsTheFirstButTakesALaterAuthority() {
        var result = Parse("*''Testudo flava'' *''Testudo flava'' <small>Bonnaterre, 1789</small> *''Testudo flava'' <small>Someone, 1800</small>");
        Assert.Equal(new List<(string, string?)> { ("Testudo flava", "Bonnaterre, 1789") }, result);
    }

    [Fact]
    public void NbspAndDaggerAndLeadingQuestionMark() {
        var result = Parse("* ''Strix stridula'' {{small|Linnaeus,&nbsp;1758}} * †''Strix glaux'' {{small|Linnaeus,{{nbsp}}1758}} * ?''Strix aluco'' <small>L.</small>");
        Assert.Equal(new List<(string, string?)> {
            ("Strix stridula", "Linnaeus, 1758"),
            ("Strix glaux", "Linnaeus, 1758"),
            ("Strix aluco", "L."),
        }, result);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("{{Species list|")]
    [InlineData("''unclosed <small>{{small|")]
    [InlineData("}}]]''<ref>")]
    public void UnparseableInput_GivesAnEmptyList(string? wikitext) {
        Assert.Empty(TaxoboxSynonymsParser.Parse(wikitext));
    }

    [Theory]
    [InlineData("([[Oldfield Thomas|Thomas]], 1904)", "(Thomas, 1904)")]
    [InlineData("{{small|[[Carl Linnaeus|Linnaeus]],&nbsp;1758}}", "Linnaeus, 1758")]
    [InlineData("<small>(L.) [[Kuntze]]</small><ref name=x>{{cite web|title=y}}</ref>", "(L.) Kuntze")]
    [InlineData("{{au|Pančić ''ex'' Diklić}} <!-- check -->", "Pančić ex Diklić")]
    [InlineData("  ", null)]
    [InlineData("<ref>only a reference</ref>", null)]
    public void CleanAuthority_RemovesMarkup(string? wikitext, string? expected) {
        Assert.Equal(expected, TaxoboxSynonymsParser.CleanAuthority(wikitext));
    }

    [Fact]
    public void KeepsABracketedYearAfterAnAuthorOutsideBrackets() {
        var result = TaxoboxSynonymsParser.Parse("''Papilio ibiris'' C. & R. Felder, [1867]");
        Assert.Equal("C. & R. Felder, [1867]", Assert.Single(result).Authority);
    }
}
