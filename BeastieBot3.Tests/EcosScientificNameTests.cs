using BeastieBot3.StatusLists;

namespace BeastieBot3.Tests;

// The brackets in ECOS scientific names, from real rows of the October 2026 export: earlier names to
// keep as other names, and notes to drop.
public sealed class EcosScientificNameTests {
    private static void Expect(string raw, string name, params string[] names) {
        var parts = EcosScientificName.Parse(raw);
        Assert.Equal(name, parts.Name);
        Assert.Equal(new[] { name }.Concat(names).ToArray(), parts.Names.ToArray());
    }

    [Fact]
    public void A_name_without_brackets_is_its_only_name() =>
        Expect("Cyclura rileyi nuchalis", "Cyclura rileyi nuchalis");

    [Fact]
    public void Earlier_genus() =>
        Expect("Papasula (=Sula) abbotti", "Papasula abbotti", "Sula abbotti");

    [Fact]
    public void Earlier_genus_and_epithet_are_combined() =>
        Expect("Harrisia (=Cereus) aboriginum (=gracilis)", "Harrisia aboriginum",
            "Cereus aboriginum", "Harrisia gracilis", "Cereus gracilis");

    [Fact]
    public void Two_earlier_genera_in_one_bracket() =>
        Expect("Pediocactus (=Echinocactus,=Utahia) sileri", "Pediocactus sileri", "Echinocactus sileri", "Utahia sileri");

    [Fact]
    public void Earlier_species_epithet_before_a_subspecies() =>
        Expect("Damaliscus pygarus (=dorcas) dorcas", "Damaliscus pygarus dorcas", "Damaliscus dorcas dorcas");

    // "(=H. iroquois)" is a whole name; H. is the genus of the main name.
    [Fact]
    public void Whole_earlier_name_with_an_abbreviated_genus() =>
        Expect("Hemileuca maia menyanthevora (=H. iroquois)", "Hemileuca maia menyanthevora", "Hemileuca iroquois");

    [Fact]
    public void Whole_earlier_name_with_genus_and_species_abbreviated() {
        Expect("Euphydryas editha quino (=E. e. wrighti)", "Euphydryas editha quino", "Euphydryas editha wrighti");
        Expect("Lessingia germanorum (=L.g. var. germanorum)", "Lessingia germanorum", "Lessingia germanorum var. germanorum");
        Expect("Navarretia leucocephala ssp. pauciflora (=N. pauciflora)", "Navarretia leucocephala ssp. pauciflora", "Navarretia pauciflora");
    }

    // "c." is not the main name's second word (melanops) but the word before the bracket.
    [Fact]
    public void Whole_earlier_name_whose_abbreviation_is_the_word_before_the_bracket() =>
        Expect("Lichenostomus melanops cassidix (=Meliphaga c.)", "Lichenostomus melanops cassidix", "Meliphaga cassidix");

    // "(=davidianus j.)": the species was a subspecies, Andrias davidianus japonicus.
    [Fact]
    public void Earlier_species_and_abbreviated_subspecies() {
        Expect("Andrias japonicus (=davidianus j.)", "Andrias japonicus", "Andrias davidianus japonicus");
        Expect("Viverra civettina (=megaspila c.)", "Viverra civettina", "Viverra megaspila civettina");
        Expect("Procolobus (=Colobus) preussi (=badius p.)", "Procolobus preussi",
            "Colobus preussi", "Procolobus badius preussi", "Colobus badius preussi");
    }

    [Fact]
    public void A_capitalised_word_after_the_genus_is_another_genus() =>
        Expect("Icaricia (Plebejus) shasta charlestonensis", "Icaricia shasta charlestonensis", "Plebejus shasta charlestonensis");

    [Theory]
    [InlineData("Avahi laniger (entire genus)", "Avahi laniger", "entire genus")]
    [InlineData("Eriogonum apricum (incl. var. prostratum)", "Eriogonum apricum", "incl. var. prostratum")]
    [InlineData("Orthalicus reses (not incl. nesodryas)", "Orthalicus reses", "not incl. nesodryas")]
    [InlineData("Lasiorhinus krefftii (formerly L. barnardi and L. gillespiei)", "Lasiorhinus krefftii", "formerly L. barnardi and L. gillespiei")]
    [InlineData("Hylobates spp. (including Nomascus)", "Hylobates spp.", "including Nomascus")]
    [InlineData("Lemuridae (incl. genera Lemur, Phaner, Hapalemur)", "Lemuridae", "incl. genera Lemur, Phaner, Hapalemur")]
    public void A_note_gives_no_name(string raw, string name, string note) {
        var parts = EcosScientificName.Parse(raw);
        Assert.Equal(name, parts.Name);
        Assert.Equal(new[] { name }, parts.Names.ToArray());
        Assert.Equal(note, parts.Note);
    }

    [Fact]
    public void A_note_and_an_earlier_name_in_one_name() {
        var parts = EcosScientificName.Parse("Puma (=Felis) concolor (all subsp. except coryi)");
        Assert.Equal(new[] { "Puma concolor", "Felis concolor" }, parts.Names.ToArray());
        Assert.Equal("all subsp. except coryi", parts.Note);
    }

    [Theory]
    [InlineData("Gila bicolor ssp.")]
    [InlineData("Cyprogenia sp. cf. aberti")]
    [InlineData("Cyanea st.-johnii")]
    [InlineData("Echinomastus erectocentrus var. acunensis")]
    public void Rank_words_and_dots_are_kept_as_written(string raw) =>
        Expect(raw, raw);

    [Theory]
    [InlineData("")]
    [InlineData("(=Sula)")]
    [InlineData("Papasula (=Sula abbotti")]
    [InlineData("Dasyornis longirostris (=brachypterus I.)")]
    public void Odd_text_does_not_throw(string raw) {
        var parts = EcosScientificName.Parse(raw);
        Assert.NotNull(parts.Names);
    }
}
