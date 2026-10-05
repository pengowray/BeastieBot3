using BeastieBot3.Shared.Wikitext;

namespace BeastieBot3.Tests.SharedWikitext;

// Pins which parts of an IUCN name are italic. The cases are real names from the 2026-1 CSV.
public class ScientificNameMarkupTests {
    [Theory]
    [InlineData("Ursus maritimus", null, "''Ursus maritimus''")]
    [InlineData("Panthera pardus ssp. orientalis", null, "''Panthera pardus'' ssp. ''orientalis''")]
    [InlineData("Delphinium fissum subsp. caseyi", null, "''Delphinium fissum'' subsp. ''caseyi''")]
    [InlineData("Cupressus arizonica var. glabra", null, "''Cupressus arizonica'' var. ''glabra''")]
    [InlineData("Panthera leo West Africa subpopulation", "West Africa", "''Panthera leo'' West Africa subpopulation")]
    [InlineData("Panthera leo West Africa subpopulation", "West Africa subpopulation", "''Panthera leo'' West Africa subpopulation")]
    [InlineData("Panthera leo West Africa subpopulation", null, "''Panthera leo'' West Africa subpopulation")]
    [InlineData("Zea mays subsp. mexicana Durango subpopulation", "Durango", "''Zea mays'' subsp. ''mexicana'' Durango subpopulation")]
    [InlineData("Zea mays subsp. mexicana Durango subpopulation", null, "''Zea mays'' subsp. ''mexicana'' Durango subpopulation")]
    [InlineData("Eschrichtius robustus western subpopulation", "western subpopulation", "''Eschrichtius robustus'' western subpopulation")]
    [InlineData("Eschrichtius robustus western subpopulation", null, "''Eschrichtius robustus'' western subpopulation")]
    [InlineData("Oncorhynchus nerka FRASER RIVER, LOWER: Cultus Lk (late)", "FRASER RIVER, LOWER: Cultus Lk (late)",
        "''Oncorhynchus nerka'' FRASER RIVER, LOWER: Cultus Lk (late)")]
    [InlineData("Crataegus x canescens", null, "''Crataegus'' x ''canescens''")]
    [InlineData("Salix × pendulina", null, "''Salix'' × ''pendulina''")]
    [InlineData("×Agropogon littoralis", null, "×''Agropogon littoralis''")]
    [InlineData("Acmella sp. nov. 'Ba Tai'", null, "''Acmella'' sp. nov. 'Ba Tai'")]
    [InlineData("Enteromius cf. eutaenia ‘Southern Africa’", null, "''Enteromius'' cf. ''eutaenia'' ‘Southern Africa’")]
    [InlineData("Mercuria cf zopissa", null, "''Mercuria'' cf ''zopissa''")]
    [InlineData("Ferrissia sp. indet.", null, "''Ferrissia'' sp. indet.")]
    [InlineData("Memecylon sp. nov. 1", null, "''Memecylon'' sp. nov. 1")]
    [InlineData("Kosciuscola sp. nov. 1 'K. tristis Bogong Clade'", null, "''Kosciuscola'' sp. nov. 1 'K. tristis Bogong Clade'")]
    [InlineData("Tricholoma borgsjoeënse", null, "''Tricholoma borgsjoeënse''")]
    [InlineData("Rosa canina f. dumalis", null, "''Rosa canina'' f. ''dumalis''")]
    [InlineData("  Ursus \n maritimus ", null, "''Ursus maritimus''")]
    public void ToWikitext(string name, string? subpopulation, string expected) {
        Assert.Equal(expected, ScientificNameMarkup.ToWikitext(name, subpopulation));
    }

    [Theory]
    [InlineData("Panthera pardus ssp. orientalis", null, "<i>Panthera pardus</i> ssp. <i>orientalis</i>")]
    [InlineData("Panthera leo West Africa subpopulation", "West Africa", "<i>Panthera leo</i> West Africa subpopulation")]
    [InlineData("Acmella sp. nov. 'Ba Tai'", null, "<i>Acmella</i> sp. nov. &#39;Ba Tai&#39;")]
    [InlineData("Acipenser brevirostrum Delaware/Chesapeake Bay & <x> subpopulation", null,
        "<i>Acipenser brevirostrum</i> Delaware/Chesapeake Bay &amp; &lt;x&gt; subpopulation")]
    [InlineData("Tricholoma borgsjoeënse", null, "<i>Tricholoma borgsjoeënse</i>")]
    public void ToHtml(string name, string? subpopulation, string expected) {
        Assert.Equal(expected, ScientificNameMarkup.ToHtml(name, subpopulation));
    }

    [Theory]
    [InlineData("Ursus maritimus marinus", "<i>Ursus maritimus marinus</i>")]
    [InlineData("Rana (Hylarana) albolineata", "<i>Rana (Hylarana) albolineata</i>")]
    [InlineData("Terminalia catappa var. pubescens", "<i>Terminalia catappa</i> var. <i>pubescens</i>")]
    [InlineData("Abies alba subsp. alba var. pyramidalis", "<i>Abies alba</i> subsp. <i>alba</i> var. <i>pyramidalis</i>")]
    [InlineData("Felis tigris", "<i>Felis tigris</i>")]
    public void ToSynonymHtml(string name, string expected) {
        Assert.Equal(expected, ScientificNameMarkup.ToSynonymHtml(name));
    }

    [Fact]
    public void ToWikitext_EscapesApostrophesThatWouldBecomeMarkup() {
        // Two apostrophes in a row in upright text would start italics; they are written as entities.
        Assert.Equal("''Barbus'' sp. nov. &#39;&#39;Chimanimani&#39;&#39;",
            ScientificNameMarkup.ToWikitext("Barbus sp. nov. ''Chimanimani''"));
    }

    [Fact]
    public void ToWikitext_EmptyNameGivesEmptyText() {
        Assert.Equal(string.Empty, ScientificNameMarkup.ToWikitext("  "));
    }
}
