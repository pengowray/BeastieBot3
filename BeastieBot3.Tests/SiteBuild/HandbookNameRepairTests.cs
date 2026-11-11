using BeastieBot3.SiteBuild;

namespace BeastieBot3.Tests.SiteBuild;

// HandbookNameRepair: the damage that text extraction did to the Handbook of the Mammals of the
// World's French and German names, from real Catalogue of Life names.
public class HandbookNameRepairTests {
    [Theory]
    // A column heading after "I".
    [InlineData("Rhinolophe du Cap I German", "fra", "Rhinolophe du Cap")]
    [InlineData("Tanga-Hufeisennase I Spanish", "deu", "Tanga-Hufeisennase")]
    [InlineData("Asellia dArabie I German", "fra", "Asellia d'Arabie")]
    // Without the "I", the name is left for OtherLanguageNameRules to leave out.
    [InlineData("Grand drer-Dre zahnblannase Spanish", "deu", "Grand drer-Dre zahnblannase Spanish")]
    // French.
    [InlineData("Souris a queue fragile", "fra", "Souris à queue fragile")]
    [InlineData("Baleine-a-bec dArnoux", "fra", "Baleine-à-bec d'Arnoux")]
    [InlineData("Pélomys dAngola", "fra", "Pélomys d'Angola")]
    [InlineData("Minioptére a oreillons courts", "fra", "Minioptère à oreillons courts")]
    [InlineData("Atéle a téte brune", "fra", "Atéle à tête brune")]
    // German.
    [InlineData("WeiRRbauch-Nacktsohlenrennmaus", "deu", "Weißbauch-Nacktsohlenrennmaus")]
    [InlineData("GroRe Philippinenwaldmaus", "deu", "Große Philippinenwaldmaus")]
    [InlineData("SchwarzfulR-Zwergreisratte", "deu", "Schwarzfuß-Zwergreisratte")]
    [InlineData("SchwarzfuR-Baumratte", "deu", "Schwarzfuß-Baumratte")]
    [InlineData("WeilRkopflangur", "deu", "Weißkopflangur")]
    // Correct names stay as they are.
    [InlineData("Rhinolophe de Darling", "fra", "Rhinolophe de Darling")]
    [InlineData("Murin à oreilles échancrées", "fra", "Murin à oreilles échancrées")]
    [InlineData("Souris-musaraigne de German", "fra", "Souris-musaraigne de German")]
    [InlineData("Kap-Hufeisennase", "deu", "Kap-Hufeisennase")]
    [InlineData("Ringbeutler", "deu", "Ringbeutler")]
    [InlineData("Langhaar-Ringbeutler", "deu", "Langhaar-Ringbeutler")]
    [InlineData("KwaZulu-Lamellenzahnratte", "deu", "KwaZulu-Lamellenzahnratte")]
    [InlineData("Darfur-Rennmaus", "deu", "Darfur-Rennmaus")]
    // The French and German rules apply only to their language.
    [InlineData("WeiRbauch a dAngola", "spa", "WeiRbauch a dAngola")]
    public void Repair(string name, string language, string expected) =>
        Assert.Equal(expected, HandbookNameRepair.Repair(name, language));
}
