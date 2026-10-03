using BeastieBot3.Iucn.Citations;

namespace BeastieBot3.Tests.Citations;

// Pins how one credits[].full string splits into names, which rule of the ladder did it, and how a
// credit's value[] entries are counted to confirm a doubtful split.
public class CreditNameSplitterTests {
    [Theory]
    // Plain citation-style lists, including particles, hyphens and non-ASCII letters.
    [InlineData("Wood, T.J., Devalez, J., Kierat, J., Mudri-Stojnić, S. & Álvarez Fidalgo, P.",
        "Wood, T.J.|Devalez, J.|Kierat, J.|Mudri-Stojnić, S.|Álvarez Fidalgo, P.")]
    [InlineData("Evrard, D., Scheuchl, E., de Meulemeester, T., Le Divelec, R. & De Manincor, N.",
        "Evrard, D.|Scheuchl, E.|de Meulemeester, T.|Le Divelec, R.|De Manincor, N.")]
    // Short capitalised surnames that look like particles.
    [InlineData("Das, I., Lakim, M., Van Damme, D. & Do, V.", "Das, I.|Lakim, M.|Van Damme, D.|Do, V.")]
    // Suffixes: inside the surname token, and as a token of their own. "Neto" is a surname here.
    [InlineData("Kaewmuan, A., Tran, V.T., Lowry II, P.P. & Middleton, D.", "Kaewmuan, A.|Tran, V.T.|Lowry II, P.P.|Middleton, D.")]
    [InlineData("Evangelista, V., Malabrigo Jr., P.L. & Umali, A.", "Evangelista, V.|Malabrigo Jr., P.L.|Umali, A.")]
    [InlineData("Agoo, E.M.G., Cootes, J., Golamco, A., Jr., de Vogel, E.F. & Tiu, D.", "Agoo, E.M.G.|Cootes, J.|Golamco, A., Jr.|de Vogel, E.F.|Tiu, D.")]
    // The suffix between surname and initials (aid 3121523).
    [InlineData("Driggers, III, W.B. & Carlson, J.", "Driggers, III, W.B.|Carlson, J.")]
    // Spanish "los" and "las" are particles, so this six-word surname still pairs.
    [InlineData("Vacas, O., Baldeón, S., de los Ángeles La Torre Cuadros, M. & Reynel, C.",
        "Vacas, O.|Baldeón, S.|de los Ángeles La Torre Cuadros, M.|Reynel, C.")]
    [InlineData("Fernandez, E., Negrão, R., Guimarães, A. & Neto, L.N.", "Fernandez, E.|Negrão, R.|Guimarães, A.|Neto, L.N.")]
    // Initials without dots, hyphenated, or with a particle inside.
    [InlineData("Ahissa, L, Decher, J. & Gazzard, A.", "Ahissa, L|Decher, J.|Gazzard, A.")]
    [InlineData("Al Fotooh, A.A., Al-Doeis, MA, Davidson, ZD & Luo, S.-J.", "Al Fotooh, A.A.|Al-Doeis, MA|Davidson, ZD|Luo, S.-J.")]
    [InlineData("Chung, H.-Y., Lee, Y-W, Lim, C.-S. & Oh, H.-K", "Chung, H.-Y.|Lee, Y-W|Lim, C.-S.|Oh, H.-K")]
    [InlineData("Silveira, A.L., Prudente, A.L. da C., Nogueira, C. de C. & Zaher, H. el D.", "Silveira, A.L.|Prudente, A.L. da C.|Nogueira, C. de C.|Zaher, H. el D.")]
    // "and", semicolons, stray punctuation and line breaks.
    [InlineData("De Silva, R., Milligan, H., Smith, J. and Livingston, F.", "De Silva, R.|Milligan, H.|Smith, J.|Livingston, F.")]
    [InlineData("Ramandimbisoa, B.; Razafindrahaja, V.; Rajaovelona, L.", "Ramandimbisoa, B.|Razafindrahaja, V.|Rajaovelona, L.")]
    [InlineData("Ouedraogo, L., Diop, F.N. & Hilton-Taylor, C.; Luke W.R.Q.", "Ouedraogo, L.|Diop, F.N.|Hilton-Taylor, C.|Luke W.R.Q.")]
    [InlineData("Mora Vicente, S,. Urdiales Perales, N. & Tapia, F.", "Mora Vicente, S|Urdiales Perales, N.|Tapia, F.")]
    [InlineData("Tykarski, P., Putchkov, A. &\n Mannerkoski, I. \n", "Tykarski, P.|Putchkov, A.|Mannerkoski, I.")]
    // Parenthetical affiliation notes are dropped from the name; so is a note standing alone.
    [InlineData("Dijkstra, K.-D.B. & Suhling, F. (SSC Odonata Specialist Group), & Allen, D. (IUCN Freshwater Biodiversity Unit)",
        "Dijkstra, K.-D.B.|Suhling, F.|Allen, D.")]
    [InlineData("Schneider, W., Boudot, J.-P., (Freshwater Biodiversity Assessment Workshop, Oct. 2007), Pollock, C.M. (IUCN Red List Unit) & Allen, D.",
        "Schneider, W.|Boudot, J.-P.|Pollock, C.M.|Allen, D.")]
    // A missing comma between two people.
    [InlineData("Ng, P. Yeo, D. and McIvor, A.", "Ng, P.|Yeo, D.|McIvor, A.")]
    // Names already in one token, surname or initials first.
    [InlineData("Rhazi L., de Bélair G. & Cuttelod, A. (IUCN Centre for Mediterranean Cooperation)", "Rhazi L.|de Bélair G.|Cuttelod, A.")]
    [InlineData("N.H. Rakotoarivelo & L. Faranirina", "N.H. Rakotoarivelo|L. Faranirina")]
    // The same person listed twice.
    [InlineData("Seddon, M. & Seddon, M. & Allen, D. (IUCN Freshwater Biodiversity Unit)", "Seddon, M.|Allen, D.")]
    // People written given name first, with no organisation words.
    [InlineData("Neil Cox and Helen Temple", "Neil Cox|Helen Temple")]
    // People and organisations: every name that pairs with nothing has an organisation word.
    [InlineData("Eudey, A. & Members of the Primate Specialist Group", "Eudey, A.|Members of the Primate Specialist Group")]
    [InlineData("Carter, R.L., Hayes, W.K. & West Indian Iguana Specialist Group", "Carter, R.L.|Hayes, W.K.|West Indian Iguana Specialist Group")]
    [InlineData("Hero, J.-M. & IUCN SSC Australian Amphibian Assessment Workshop participants",
        "Hero, J.-M.|IUCN SSC Australian Amphibian Assessment Workshop participants")]
    [InlineData("Linzey, A.V. & NatureServe (Hammerson, G.)", "Linzey, A.V.|NatureServe (Hammerson, G.)")]
    // A suffix with a note after it stays with its person; the note is dropped, as after initials.
    [InlineData("Kirkland, G.L., Jr. (Rodent Specialist Group)", "Kirkland, G.L., Jr.")]
    [InlineData("Thomas K. Kristensen & Anne-Sofie Stensgaard", "Thomas K. Kristensen|Anne-Sofie Stensgaard")]
    // Kept whole.
    [InlineData("IUCN SSC Antelope Specialist Group", "IUCN SSC Antelope Specialist Group")]
    [InlineData("Tortoise & Freshwater Turtle Specialist Group", "Tortoise & Freshwater Turtle Specialist Group")]
    [InlineData("Eastern Arc Mountains & Coastal Forests CEPF Plant Assessment Project", "Eastern Arc Mountains & Coastal Forests CEPF Plant Assessment Project")]
    [InlineData("IUCN SSC Anteater, Sloth and Armadillo Specialist Group", "IUCN SSC Anteater, Sloth and Armadillo Specialist Group")]
    [InlineData("Royal Botanic Gardens, Kew", "Royal Botanic Gardens, Kew")]
    [InlineData("Botanic Gardens Conservation International (BGCI) & IUCN SSC Global Tree Specialist Group",
        "Botanic Gardens Conservation International (BGCI) & IUCN SSC Global Tree Specialist Group")]
    [InlineData("Jaffré, T. <i>et al.</i>", "Jaffré, T. et al.")]
    [InlineData("Schnell, D., Catling, P., Gardner, R., <i>et al.</i>", "Schnell, D., Catling, P., Gardner, R., et al.")]
    [InlineData("Qin, Hai-Ning & Kohorn, L. (China Plants Red List Authority)", "Qin, Hai-Ning & Kohorn, L. (China Plants Red List Authority)")]
    [InlineData("Weber, O. & Sebsebe Demissew", "Weber, O. & Sebsebe Demissew")]
    [InlineData("Marshall, B.E & Tweddle, D., R Bills", "Marshall, B.E & Tweddle, D., R Bills")]
    // A stand-alone name with no organisation word ("Mantasoa", "Eastern Arc Mountains") keeps the list whole.
    [InlineData("Loiselle, P. & participants of the CBSG/ANGAP CAMP \"Faune de Madagascar\" workshop, Mantasoa, Madagascar 2001",
        "Loiselle, P. & participants of the CBSG/ANGAP CAMP \"Faune de Madagascar\" workshop, Mantasoa, Madagascar 2001")]
    [InlineData("Eastern Arc Mountains & Coastal Forests CEPF Plant Assessment Project & Bösenberg, J.D.",
        "Eastern Arc Mountains & Coastal Forests CEPF Plant Assessment Project & Bösenberg, J.D.")]
    // "of" and "the" alone don't make an organisation.
    [InlineData("Smith, J. & Friends of the Forest", "Smith, J. & Friends of the Forest")]
    // Nor does an affiliation in parentheses after a person's name (aid 12463382).
    [InlineData("Shuk Man, C. & Ng Wai Chuen (Grouper & Wrasse Specialist Group)", "Shuk Man, C. & Ng Wai Chuen (Grouper & Wrasse Specialist Group)")]
    [InlineData("GTA Singapore Southeast Asia Trees Workshop 2023, P.", "GTA Singapore Southeast Asia Trees Workshop 2023, P.")]
    public void SplitCreditNames_WithoutCount(string full, string expected) {
        Assert.Equal(expected.Split('|'), CreditNameSplitter.Split(full));
    }

    [Theory]
    // Organisations split only when value[] confirms how many there are.
    [InlineData("Botanic Gardens Conservation International (BGCI) & IUCN SSC Global Tree Specialist Group", 2,
        "Botanic Gardens Conservation International (BGCI)|IUCN SSC Global Tree Specialist Group")]
    [InlineData("Centro Nacional de Conservação da Flora (CNCFlora), IUCN SSC Brazil Plant Red List Authority & Botanic Gardens Conservation International", 3,
        "Centro Nacional de Conservação da Flora (CNCFlora)|IUCN SSC Brazil Plant Red List Authority|Botanic Gardens Conservation International")]
    [InlineData("Neam, K., Hobin, L. & NatureServe", 3, "Neam, K.|Hobin, L.|NatureServe")]
    [InlineData("IUCN SSC Southern African Plant Specialist Group & Royal Botanic Gardens, Kew", 2,
        "IUCN SSC Southern African Plant Specialist Group|Royal Botanic Gardens, Kew")]
    // A full given name after the comma, confirmed by count.
    [InlineData("Reid, A., Rogers, Alex & Bohm, M.", 3, "Reid, A.|Rogers, Alex|Bohm, M.")]
    [InlineData("Damit, A., Mohd Yusof, Nur Adillah & Sugau, J.", 3, "Damit, A.|Mohd Yusof, Nur Adillah|Sugau, J.")]
    // Counts that don't agree leave the string whole.
    [InlineData("IUCN SSC Anteater, Sloth and Armadillo Specialist Group", 1, "IUCN SSC Anteater, Sloth and Armadillo Specialist Group")]
    [InlineData("Chobanov, D.P., Hochkirch, A., Iorgu, I.S., Ivkovic, S., Kristin, A., Lemonnier-Darcemont, M., Pushkar, T., Sirin, D., Skejo, J. Skejo, Szovenyi, G., Vedenina, V. & Willemse, L.P.M.", 12,
        "Chobanov, D.P., Hochkirch, A., Iorgu, I.S., Ivkovic, S., Kristin, A., Lemonnier-Darcemont, M., Pushkar, T., Sirin, D., Skejo, J. Skejo, Szovenyi, G., Vedenina, V. & Willemse, L.P.M.")]
    // A clean pair list doesn't need the count to agree. Here value[] lists Suzanne Livingstone twice
    // word for word, so it has 2 distinct entries for 3 names, and the repeat is one person.
    [InlineData("Livingstone, S., Livingstone, S. & Neubert, E.", 2, "Livingstone, S.|Neubert, E.")]
    // A name listed twice is kept twice when the count of distinct value[] entries confirms it.
    // Anthony and Antony Harold (aid 141564386).
    [InlineData("Harold, A. & Harold, A.", 2, "Harold, A.|Harold, A.")]
    // Sung-Hwan and Si-Hyung Park (aid 113555367).
    [InlineData("Chung, H.-Y., Lee, Y-W, Park, S.-H., Lim, C.-S., Park, S.-H., Hong, M.-H., Lee, Y.-S., Lee, D.-H., Shin, H.-S., Lee, S.-J., Oh, H.-K & Gwon, S.-A", 12,
        "Chung, H.-Y.|Lee, Y-W|Park, S.-H.|Lim, C.-S.|Park, S.-H.|Hong, M.-H.|Lee, Y.-S.|Lee, D.-H.|Shin, H.-S.|Lee, S.-J.|Oh, H.-K|Gwon, S.-A")]
    // Shambel and Sisay Alemu, in a list that only splits with the count (aid 223078443).
    [InlineData("Alemu, S., Alemu, S., Atnafu, H., Awas, T., Birhanu Belay, Sebsebe Demissew, Luke, W.R.Q., Musili, P., Nemomissa, S., Bahdon, J. & Efrata Mekbib", 11,
        "Alemu, S.|Alemu, S.|Atnafu, H.|Awas, T.|Birhanu Belay|Sebsebe Demissew|Luke, W.R.Q.|Musili, P.|Nemomissa, S.|Bahdon, J.|Efrata Mekbib")]
    // Without a confirming count the repeat is one person.
    [InlineData("Harold, A. & Harold, A.", 3, "Harold, A.")]
    public void SplitCreditNames_WithCount(string full, int count, string expected) {
        Assert.Equal(expected.Split('|'), CreditNameSplitter.Split(full, count));
    }

    [Theory]
    [InlineData("Seddon, M. & Seddon, M.", null, "Strict")]
    [InlineData("BirdLife International", null, "Single")]
    [InlineData("Jaffré, T. <i>et al.</i>", null, "EtAl")]
    [InlineData("Neam, K., Hobin, L. & NatureServe", 3, "CountStandalone")]
    [InlineData("Reid, A., Rogers, Alex & Bohm, M.", 3, "CountGiven")]
    [InlineData("Neil Cox and Helen Temple", null, "GivenFirst")]
    [InlineData("Eudey, A. & Members of the Primate Specialist Group", null, "PairsAndOrganisations")]
    [InlineData("Kirkland, G.L., Jr. (Rodent Specialist Group)", null, "Strict")]
    [InlineData("Weber, O. & Sebsebe Demissew", null, "Whole")]
    [InlineData("IUCN SSC Anteater, Sloth and Armadillo Specialist Group", 1, "WholeCountMismatch")]
    [InlineData(" ", null, "Empty")]
    public void SplitCreditNamesWithRule_NamesTheRuleThatApplied(string full, int? count, string rule) {
        Assert.Equal(Enum.Parse<CreditSplitRule>(rule),
            CreditNameSplitter.SplitWithRule(full, count).Rule);
    }

    [Fact]
    public void DistinctValueCount_IgnoresNullsBlanksAndExactRepeats() {
        using var document = System.Text.Json.JsonDocument.Parse("""
            [{"value":["Suzanne Livingstone (GMSA)"," Suzanne Livingstone (GMSA) ",null,"","Eike Neubert"]},{"full":"x"},{"value":[]}]
            """);
        var credits = document.RootElement.EnumerateArray().ToList();

        Assert.Equal(2, CreditNameSplitter.DistinctValueCount(credits[0]));
        Assert.Null(CreditNameSplitter.DistinctValueCount(credits[1]));
        Assert.Equal(0, CreditNameSplitter.DistinctValueCount(credits[2]));
    }

    [Fact]
    public void SplitCreditNames_BlankInput_ReturnsNothing() {
        Assert.Empty(CreditNameSplitter.Split("  \n "));
    }
}
