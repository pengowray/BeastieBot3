using BeastieBot3.CommonNames;

namespace BeastieBot3.Tests;

// Pins CommonNameQuality: which names from the sources are junk, which are repaired to a good name,
// and which odd-looking names are kept. The examples are real names from the common names store
// and the site database (October 2026), with their source.
public class CommonNameQualityTests {
    [Theory]
    // Wikipedia taxobox: only the next infobox parameter, no name.
    [InlineData("| image = FMIB 51989 Crystal Darter, Crystallaria asprella (Jordan) Wabash River.jpeg", nameof(CommonNameFlaw.Empty))]
    // Wikipedia taxobox: a template in the middle of the name.
    [InlineData("jimbuLadakh onion{lang|zh|青甘韭} qing gan jiu", nameof(CommonNameFlaw.WikiMarkup))]
    // Wikipedia taxobox: the scientific name in a Latin lang template.
    [InlineData("{lang|la|Gymnadenia runei}", nameof(CommonNameFlaw.WikiMarkup))]
    // Wikipedia taxobox: the scientific name in italics.
    [InlineData("''{okina}Ō{okina}ū''", nameof(CommonNameFlaw.WikiMarkup))]
    [InlineData("''Phelsuma sundbergi'' <!-- |image = Green gecko 1979 stamp of Seychelles.jpg -->", nameof(CommonNameFlaw.WikiMarkup))]
    // Catalogue of Life: a gloss with no name.
    [InlineData("meaning large bear cat", nameof(CommonNameFlaw.GlossOnly))]
    // Catalogue of Life: OCR of German names (ß read as "l 3", "f 3"), French and Spanish (à, ó read as 4, 6).
    [InlineData("Weil 3 bauch-Graslandmaus", nameof(CommonNameFlaw.OcrDigit))]
    [InlineData("Grol 3 e Hamsterratte", nameof(CommonNameFlaw.OcrDigit))]
    [InlineData("Weilful 3 - Hochlandratte", nameof(CommonNameFlaw.OcrDigit))]
    [InlineData("Gib 6 n de Hainan", nameof(CommonNameFlaw.OcrDigit))]
    [InlineData("Rat 4 queue blanche", nameof(CommonNameFlaw.OcrDigit))]
    [InlineData("Goeld 1 ’ s Spiny-rat", nameof(CommonNameFlaw.OcrDigit))]
    [InlineData("Mount Pirr 1 Deermouse", nameof(CommonNameFlaw.OcrDigit))]
    [InlineData("Sr 1 Lankan Highland Shrew", nameof(CommonNameFlaw.OcrDigit))]
    // Catalogue of Life: OCR that ran words together.
    [InlineData("AtrcanTidenr Bat AfrcanTrdent nosed Bat asrAtrcanTrdent Bat Por iva s Short-cared", nameof(CommonNameFlaw.MixedCaseWords))]
    // Catalogue of Life: OCR backslashes and slashes inside the name.
    [InlineData("Hill- \\\\ WeiRzahnspitzmaus", nameof(CommonNameFlaw.OcrArtefact))]
    [InlineData("Two-striped forest-pitviper \\\\[smaragdinus]", nameof(CommonNameFlaw.OcrArtefact))]
    [InlineData("Lasia Long-tailed Macaque (/ asiae)", nameof(CommonNameFlaw.OcrArtefact))]
    [InlineData("Pale Pygmy Balkhash Jerboa (s / udskii)", nameof(CommonNameFlaw.OcrArtefact))]
    // Catalogue of Life and Wikidata: cut off at a bracket.
    [InlineData("Pholidoscelis polops (Cope", nameof(CommonNameFlaw.UnbalancedBrackets))]
    [InlineData("Gansu Birch Mouse (concolor", nameof(CommonNameFlaw.UnbalancedBrackets))]
    [InlineData("Rooiberg girdled lizard)", nameof(CommonNameFlaw.UnbalancedBrackets))]
    [InlineData("and grant)", nameof(CommonNameFlaw.UnbalancedBrackets))]
    [InlineData("Sharpe’s / Southern Highlands Angolan Colobus (sharpe ))", nameof(CommonNameFlaw.UnbalancedBrackets))]
    [InlineData("tu sun [兔狲", nameof(CommonNameFlaw.UnbalancedBrackets))]
    // Wikidata: author citations.
    [InlineData("Calvert, 1902", nameof(CommonNameFlaw.AuthorCitation))]
    [InlineData("Grewia crenata (J.R.Forst. & G.Forst.) Schinz & Guillaumin (1921), non (Unger) Heer (1857)", nameof(CommonNameFlaw.AuthorCitation))]
    [InlineData("Paradoxa confirmata Fernandes & Rolán, 1990", nameof(CommonNameFlaw.AuthorCitation))]
    [InlineData("Phragmites vallatoria [(L.) Veldkamp (1992)]", nameof(CommonNameFlaw.AuthorCitation))]
    // Wikidata: IUCN's placeholder.
    [InlineData("Species code: Rc", nameof(CommonNameFlaw.SpeciesCode))]
    [InlineData("Species Code: Ha", nameof(CommonNameFlaw.SpeciesCode))]
    // A stray equals sign.
    [InlineData("basket=grass", nameof(CommonNameFlaw.WikiMarkup))]
    // Nothing left.
    [InlineData("   ", nameof(CommonNameFlaw.Empty))]
    public void Junk(string name, string flaw) {
        var result = CommonNameQuality.Assess(name, "en");

        Assert.Equal(CommonNameVerdict.Junk, result.Verdict);
        Assert.Equal(flaw, result.Flaw.ToString());
    }

    [Theory]
    // Wikipedia taxobox: a citation template after the name.
    [InlineData("Sunda slow loris{sfn|Groves|2005|p=122}", "Sunda slow loris", nameof(CommonNameFlaw.TrailingTemplate))]
    [InlineData("Humpback whale{r|MSW3}", "Humpback whale", nameof(CommonNameFlaw.TrailingTemplate))]
    // Wikipedia taxobox: the next infobox parameters after the name.
    [InlineData("Grey's mudsnake | status = LC | status_system = IUCN3.1 | status_ref =", "Grey's mudsnake", nameof(CommonNameFlaw.ParameterTail))]
    [InlineData("Chapala chub | image = FMIB 40490 Falcula chapalae Jordan & Snyder, new genus and species Type.jpeg", "Chapala chub", nameof(CommonNameFlaw.ParameterTail))]
    [InlineData("Western rock sengi | image = Elephantulus rupestris Smith 1839.tif", "Western rock sengi", nameof(CommonNameFlaw.ParameterTail))]
    [InlineData("Downy oak| image = Quercus pubescens Tuscany.jpg", "Downy oak", nameof(CommonNameFlaw.ParameterTail))]
    [InlineData("Prickly parrotpea|image =", "Prickly parrotpea", nameof(CommonNameFlaw.ParameterTail))]
    // Wikipedia taxobox: templates for the ʻokina and for the language.
    [InlineData("Kāwa{okina}u", "Kāwaʻu", nameof(CommonNameFlaw.Okina))]
    [InlineData("Hawai{okina}i {okina}ō{okina}ō", "Hawaiʻi ʻōʻō", nameof(CommonNameFlaw.Okina))]
    [InlineData("{Okina}I{Okina}iwi", "ʻIʻiwi", nameof(CommonNameFlaw.Okina))]
    [InlineData("{Lang|haw|Reef triggerfish|italic=no}", "Reef triggerfish", nameof(CommonNameFlaw.LangTemplate))]
    // Wikipedia taxobox: links.
    [InlineData("[[Mount Tohiea|Tohiea]] tree snail", "Tohiea tree snail", nameof(CommonNameFlaw.WikiLink))]
    [InlineData("Durango shiner [[File:Durango Shiner.jpg|thumb]]", "Durango shiner", nameof(CommonNameFlaw.WikiLink))]
    // Catalogue of Life: a gloss after the name.
    [InlineData("Da Xiong Mao (meaning large bear cat)", "Da Xiong Mao", nameof(CommonNameFlaw.MeaningGloss))]
    // Wikidata and Catalogue of Life: an author and year after the name.
    [InlineData("Mountain Ground Skink (Walters, 2008)", "Mountain Ground Skink", nameof(CommonNameFlaw.AuthorYearNote))]
    [InlineData("DR. STANGER's EUPREPES (Gray 1845)", "DR. STANGER's EUPREPES", nameof(CommonNameFlaw.AuthorYearNote))]
    [InlineData("Angolan adder; Bocage's Horned Adder (after FRANK & RAMUS 1995).", "Angolan adder; Bocage's Horned Adder", nameof(CommonNameFlaw.AuthorYearNote))]
    // Wikidata: a remark after the name.
    [InlineData("Kitti’s Hog-nosed Bat (Smallest Mammal!)", "Kitti’s Hog-nosed Bat", nameof(CommonNameFlaw.ExclamationNote))]
    // Catalogue of Life: a footnote marker, an HTML comment, URL encoding, underscores, OCR.
    [InlineData("Alexander River mallee[2] or milkshake mallee", "Alexander River mallee or milkshake mallee", nameof(CommonNameFlaw.FootnoteMarker))]
    [InlineData("Cascading Bean<!--", "Cascading Bean", nameof(CommonNameFlaw.HtmlMarkup))]
    [InlineData("Flinders Ranges%2C Barcoo%2C Or Bulloo Mogurnda", "Flinders Ranges, Barcoo, Or Bulloo Mogurnda", nameof(CommonNameFlaw.PercentEncoding))]
    [InlineData("Mountain_gorilla", "Mountain gorilla", nameof(CommonNameFlaw.Underscore))]
    [InlineData("\\\\ Woolly Akodont", "Woolly Akodont", nameof(CommonNameFlaw.LeadingBackslash))]
    [InlineData("Merriam' ’ s Wapiti (merriami)", "Merriam's Wapiti (merriami)", nameof(CommonNameFlaw.SplitApostrophe))]
    public void Repaired(string name, string repaired, string flaw) {
        var result = CommonNameQuality.Assess(name, "en");

        Assert.Equal(CommonNameVerdict.Repaired, result.Verdict);
        Assert.Equal(repaired, result.Name);
        Assert.Equal(flaw, result.Flaw.ToString());
    }

    [Theory]
    [InlineData("Cassin's 17-year Cicada")]
    [InlineData("Southern 2-lined Salamander")]
    [InlineData("Type 3 Evening Grosbeak")]
    [InlineData("Spring 4-Flasher")]
    [InlineData("4 Eye Butterflyfish")]
    [InlineData("45 Khz Pipistrelle")]
    [InlineData("40-mile Per Hour Lichen")]
    [InlineData("Super VC-10 Hap")]
    [InlineData("Loopy 5")]
    [InlineData("L172")]
    [InlineData("Taw Nwar (aka) Sai")]
    [InlineData("European pilchard (=sardine)")]
    [InlineData("Grey (Red) Phalarope")]
    [InlineData("(Caspian) Kutum")]
    [InlineData("Barbary Red Deer (barbarus)")]
    [InlineData("Grayling [fish]")]
    [InlineData("Turks & Caicos boa")]
    [InlineData("KwaZulu-Natal Hinged Tortoise")]
    [InlineData("FitzSimons' Dwarf Gecko")]
    [InlineData("McDowell's Bevelnosed Boa [mcdowelli]")]
    [InlineData("MacArthur's Shrew")]
    [InlineData("uMlalazi Dwarf Chameleon")]
    [InlineData("Peters' Writhing Skink")]
    [InlineData("!Nara Cricket")]
    [InlineData("Chinese ephedra(Cao Ma Huang—草麻黄)")]
    [InlineData("Moluccan/Ambon scrub python")]
    [InlineData("Alticola: Montane Toad-headed Agama")]
    public void Kept_EnglishNamesThatLookOddButAreReal(string name) {
        var result = CommonNameQuality.Assess(name, "en");

        Assert.Equal(CommonNameVerdict.Good, result.Verdict);
        Assert.Equal(name, result.Name);
    }

    [Theory]
    [InlineData("Tortuga B2", "es")]
    [InlineData("Libélula de 4 manchas", "es")]
    [InlineData("Cá Bướm 7 sọc", "vi")]
    [InlineData("Giunchina a 5 Fiori", "it")]
    [InlineData("كروي الرأس الشائع  [Kouraoui Arras Achaii]", "ar")]
    [InlineData("Манул [manul]", "ru")]
    [InlineData("Gorbe-ye-Palas [گربه پالاس]", "fa")]
    [InlineData("Jinmao/金猫", "zh")]
    [InlineData("Manarimboraka (=Manariboraka)", "mg")]
    [InlineData("Kawara (hausa [dalziel] (ghana))", "und")]
    [InlineData("!Xobo", "khi")]
    [InlineData("Coral D''água (falsa)", "pt")]
    [InlineData("Mexclapique (erronously: Mexcalpique) de Tamazula", "es")]
    [InlineData("òjíjí ọrọ́ta = to wake on a stone", "yo")]
    [InlineData("Τσιμούχα = Tsimoucha", "el")]
    // Arabic with its brackets stored mirrored.
    [InlineData("(قرش ماكو قصير الزعانف (الذيبة", "ar")]
    public void Kept_NamesInOtherLanguages(string name, string language) {
        var result = CommonNameQuality.Assess(name, language);

        Assert.Equal(CommonNameVerdict.Good, result.Verdict);
        Assert.Equal(name, result.Name);
    }

    [Theory]
    [InlineData("Espino herrero (TROPICOS, 2021)", "es", "Espino herrero")]
    [InlineData("Vara blanca (Honduras) (Pruski, 2018)", "es", "Vara blanca (Honduras)")]
    [InlineData("chabo-chidori (チャボチドリ, 矮鶏千鳥", "ja", "chabo-chidori (チャボチドリ, 矮鶏千鳥)")]
    [InlineData("tu sun [兔狲", "zh", "tu sun [兔狲]")]
    public void Repaired_NamesInOtherLanguages(string name, string language, string repaired) {
        var result = CommonNameQuality.Assess(name, language);

        Assert.Equal(CommonNameVerdict.Repaired, result.Verdict);
        Assert.Equal(repaired, result.Name);
    }

    [Theory]
    [InlineData("Weil 3 bauch-Graslandmaus", "de", null)]
    [InlineData("MalyiI AmudarinskiiI Lzhelopatonos", "ru", null)]
    [InlineData("Planta (com nota)) estranha", "pt", nameof(CommonNameFlaw.UnbalancedBrackets))]
    [InlineData("Species code: Rc", "fr", nameof(CommonNameFlaw.SpeciesCode))]
    public void OtherLanguages_UseEveryRuleButTheTwoOcrRules(string name, string language, string? flaw) {
        var result = CommonNameQuality.Assess(name, language);

        if (flaw is null) {
            Assert.Equal(CommonNameVerdict.Good, result.Verdict);
        } else {
            Assert.Equal(CommonNameVerdict.Junk, result.Verdict);
            Assert.Equal(flaw, result.Flaw.ToString());
        }
    }

    [Fact]
    public void GoodName_IsReturnedUnchanged() {
        var result = CommonNameQuality.Assess(" Polar bear ", "en");

        Assert.Equal(CommonNameVerdict.Good, result.Verdict);
        Assert.Equal(" Polar bear ", result.Name);
    }
}
