using BeastieBot3.CommonNames;

namespace BeastieBot3.Tests;

// Pins TaxoboxCommonName: the English common name `common-names aggregate --source wikipedia` takes
// from a taxobox's name field. The inputs are real name fields from the Wikipedia cache (October
// 2026), as the taxobox parser stores them (one brace of a nested template kept).
public class TaxoboxCommonNameTests {
    [Theory]
    [InlineData("Sunda slow loris{sfn|Groves|2005|p=122}", "Sunda slow loris")]
    [InlineData("Humpback whale{r|MSW3}", "Humpback whale")]
    [InlineData("Chapala chub | image = FMIB 40490 Falcula chapalae Jordan & Snyder, new genus and species Type.jpeg", "Chapala chub")]
    [InlineData("Grey's mudsnake | status = LC | status_system = IUCN3.1 | status_ref =", "Grey's mudsnake")]
    [InlineData("Ford's boa | image         = PelophilusFordiiFord.jpg | image_caption = illustration by [[George Henry Ford|G.H. Ford]], <br>for whom the species is named | gen", "Ford's boa")]
    [InlineData("Kāwa{okina}u", "Kāwaʻu")]
    [InlineData("{okina}Akiapōlā{okina}au", "ʻAkiapōlāʻau")]
    [InlineData("{Lang|haw|Reef triggerfish|italic=no}", "Reef triggerfish")]
    [InlineData("Golden bandicoot&nbsp;", "Golden bandicoot")]
    [InlineData("Silver birch<br />''Betula pendula''", "Silver birch")]
    [InlineData("''Abies grandis''<br/>Grand fir", "Grand fir")]
    [InlineData("''Agave vilmoriniana''<br/>(Octopus agave)", "Octopus agave")]
    [InlineData("Purple onion<br>Granat-Kugellauch", "Purple onion")]
    [InlineData("Piedmont garlic<br />Narcissus onion", "Piedmont garlic")]
    [InlineData("Titberry,<br>Indian allophylus", "Titberry")]
    [InlineData("Frog orchid or<br/>long-bracted orchid", "Frog orchid")]
    [InlineData("Hoogstraal's striped<br/>grass mouse", "Hoogstraal's striped grass mouse")]
    [InlineData("''Amanita ocreata''<br /><small>Western North American destroying angel</small>", "Western North American destroying angel")]
    [InlineData("''Techmarscincus'' (genus)<br />Bartle Frere skink", "Bartle Frere skink")]
    [InlineData("Salamandridae<br/>True salamanders and newts", "True salamanders and newts")]
    [InlineData("Aceramarca gracile opossum<ref name=MSW3>{MSW3 Gardner | pages = 6}</ref>", "Aceramarca gracile opossum")]
    [InlineData("Agricola's gracile opossum<ref>{cite journal | doi = 10.1206/0003-0082 | last = Voss", "Agricola's gracile opossum")]
    [InlineData("O'Shaughnessy's [[chameleon]]", "O'Shaughnessy's chameleon")]
    [InlineData("Durango shiner [[File:Durango Shiner.jpg|thumb]]", "Durango shiner")]
    [InlineData("Acute tree frog<br/ > (''Scinax sugillatus'')", "Acute tree frog")]
    [InlineData("Chinese ephedra<br>(Cao Ma Huang—草麻黄)", "Chinese ephedra")]
    // The scientific name is returned; the aggregator's own scientific-name test drops it.
    [InlineData("Abies sibirica", "Abies sibirica")]
    public void TakesTheFirstUsableName(string field, string expected) {
        Assert.Equal(expected, TaxoboxCommonName.FromNameField(field));
    }

    [Theory]
    [InlineData("Orca<br />Killer whale", "Orca", "Killer whale")]
    [InlineData("Moluccan goshawk<br />Halmaheran goshawk", "Moluccan goshawk", "Halmaheran goshawk")]
    [InlineData("Essex skipper or<br />European skipper", "Essex skipper", "European skipper")]
    [InlineData("Grayling<br />Grayling butterfly", "Grayling (butterfly)", "Grayling butterfly")]
    public void SkipsTheLineThatRepeatsThePageTitle(string field, string pageTitle, string expected) {
        Assert.Equal(expected, TaxoboxCommonName.FromNameField(field, pageTitle));
    }

    [Theory]
    [InlineData("Humpback whale{r|MSW3}", "Humpback whale")]
    [InlineData("O{okina}ahu nukupu{okina}u", "Oʻahu nukupuʻu")]
    public void NothingElse_IsNull_WhenTheOnlyNameIsThePageTitle(string field, string pageTitle) {
        Assert.Null(TaxoboxCommonName.FromNameField(field, pageTitle));
    }

    [Theory]
    [InlineData("| image = FMIB 51989 Crystal Darter, Crystallaria asprella (Jordan) Wabash River.jpeg")]
    [InlineData("{lang|la|Gymnadenia runei}")]
    [InlineData("''Nicrophorus americanus'' {Italic title}")]
    [InlineData("''Phelsuma sundbergi'' <!-- |image = Green gecko 1979 stamp of Seychelles.jpg -->")]
    [InlineData("<big>Лук Вешнякова</big><br><big>坛丝韭</big> tan si jiu")]
    [InlineData("<big>粗根韭</big> cu gen jiu")]
    [InlineData("Manchurian hare ~{fossil range|0.7|0}-->")]
    [InlineData("")]
    public void NoUsableName_IsNull(string field) {
        Assert.Null(TaxoboxCommonName.FromNameField(field));
    }
}
