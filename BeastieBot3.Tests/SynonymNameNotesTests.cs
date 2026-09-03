using System.Linq;
using BeastieBot3.Audit.Producers;
using BeastieBot3.Taxonomy;

namespace BeastieBot3.Tests;

using SynonymNoteKind = BeastieBot3.Audit.Producers.SynonymNoteKind;

// Pins the two halves of the "notes inside synonym names" work: what the audit reports, and what
// the Wikipedia/Wikidata matcher does with the same strings. Real values from IUCN 2026-1.
public class SynonymNameNotesTests {
    [Theory]
    [InlineData("Eumeces schneideri (Daudin, 1802) [orth. error]", "orth. error")]
    [InlineData("Malaclemys tuberculifera Gray, 1844 [<i>nomen oblitum</i>]", "nomen oblitum")]
    [InlineData("Ocadia glyphistoma McCord & Iverson, 1994 [<i>partim</i>, hybrid]", "partim, hybrid")]
    public void A_bracketed_note_is_found_with_its_markup_removed(string name, string expected) {
        var notes = SynonymNameNotes.FindWordNotes(name);
        Assert.Equal(expected, Assert.Single(notes).Text);
    }

    // "[1803]" is how an inferred publication date is recorded. Flagging it would be wrong.
    [Theory]
    [InlineData("Vespertilio murinus Schreber [1803]")]
    [InlineData("Helix modesta A. Ferussac [1821]")]
    public void A_bracketed_publication_year_is_not_a_note(string name) {
        Assert.Empty(SynonymNameNotes.FindWordNotes(name));
        Assert.Equal(SynonymNoteKind.Year, Assert.Single(SynonymNameNotes.Find(name)).Kind);
    }

    // Bracketed author attributions and in-name expansions are standard nomenclature, not notes.
    [Theory]
    [InlineData("Scyllium marmoratum Anonymous [Bennett], 1830", "Author")]
    [InlineData("Papilio manto [Denis & Schiffermüller], 1775", "Author")]
    [InlineData("Myliobatis cornuta Günther [A.] 1870", "Author")]
    [InlineData("Leptoconus mappa ([Lightfoot], 1786)", "Author")]
    [InlineData("S[imia] erythropyga G. Cuvier, 1829", "Expansion")]
    public void A_bracketed_author_or_expansion_is_not_a_note(string name, string expected) {
        Assert.Empty(SynonymNameNotes.FindWordNotes(name));
        Assert.Equal(expected, Assert.Single(SynonymNameNotes.Find(name)).Kind.ToString());
    }

    // A capitalised note is still a note, and odd bracket content stays visible as one.
    [Theory]
    [InlineData("Cervus hippelaphus G.Q. Cuvier, 1825 [preoccupied)]", "preoccupied)")]
    [InlineData("Pronolagus lebomboensis A. Roberts, 1936 [(apsus]", "(apsus")]
    [InlineData("Anas oustaleti Salvadori, 1894 [Illegitimate]", "Illegitimate")]
    public void Odd_or_capitalised_bracket_content_is_still_a_note(string name, string expected) {
        Assert.Equal(expected, Assert.Single(SynonymNameNotes.FindWordNotes(name)).Text);
    }

    // Only the note comes out of the suggested name; standard brackets stay.
    [Theory]
    [InlineData("Chaetodon rafflesi [Bennett], 1830 [orth. error]", "Chaetodon rafflesi [Bennett], 1830")]
    [InlineData("Vespertilio murinus Schreber [1803]", "Vespertilio murinus Schreber [1803]")]
    public void Stripping_removes_only_worded_notes(string name, string expected) {
        Assert.Equal(expected, SynonymNameNotes.StripNotes(name).Trim());
    }

    [Fact]
    public void A_name_with_no_brackets_has_no_notes() {
        Assert.Empty(SynonymNameNotes.Find("Ursus maritimus Phipps, 1774"));
    }

    // The variants table exists to put these on one row: five spellings of one note in 2026-1.
    [Fact]
    public void Spellings_of_one_note_share_a_key() {
        var keys = new[] { "orth. error", "orth.error", "orth error", "orth. Error", "orth. error." }
            .Select(SynonymNameNotes.VariantKey)
            .Distinct()
            .ToList();
        Assert.Single(keys);
    }

    [Fact]
    public void A_different_note_gets_a_different_key() {
        Assert.NotEqual(SynonymNameNotes.VariantKey("orth. error"), SynonymNameNotes.VariantKey("orth. var."));
        // A typo is a different note, not a spelling of the same one; it shows on its own row.
        Assert.NotEqual(SynonymNameNotes.VariantKey("orth. error"), SynonymNameNotes.VariantKey("orth. eror"));
    }

    // --- the matcher's side ------------------------------------------------------------------

    [Theory]
    [InlineData("Eumeces schneideri (Daudin, 1802) [orth. error]", "Eumeces schneideri")]
    [InlineData("Hexanchus griseus ssp. australis de Buen, 1960", "Hexanchus griseus ssp. australis")]
    [InlineData("Scyllium marmoratum Anonymous [Bennett], 1830", "Scyllium marmoratum")]
    [InlineData("Molossus ater É. Geoffroy Saint-Hilaire, 1805", "Molossus ater")]
    [InlineData("Ursus arctos horribilis Ord, 1815", "Ursus arctos horribilis")]
    [InlineData("Aralia franchetii J.Wen [illegit]", "Aralia franchetii")]
    public void An_authority_and_note_are_dropped_from_a_candidate_title(string stored, string expected) {
        Assert.Equal(expected, BareScientificName.Strip(stored));
    }

    // Titles that are already bare, and the shapes that must survive intact.
    [Theory]
    [InlineData("Ursus maritimus")]
    [InlineData("Felidae")]
    [InlineData("Gyraulus (Gyraulus) laevis")]      // subgenus, and a real article title
    [InlineData("Balanites maughamii subsp. acuta")]
    public void A_name_with_nothing_to_drop_is_unchanged(string name) {
        Assert.Equal(name, BareScientificName.Strip(name));
        Assert.False(BareScientificName.CarriesAuthorityOrNote(name));
    }

    [Fact]
    public void Stripping_is_idempotent() {
        var once = BareScientificName.Strip("Eumeces schneideri (Daudin, 1802) [orth. error]");
        Assert.Equal(once, BareScientificName.Strip(once));
    }

    [Fact]
    public void An_authority_bearing_name_is_reported_as_such() {
        Assert.True(BareScientificName.CarriesAuthorityOrNote("Ursus maritimus Phipps, 1774"));
        Assert.True(BareScientificName.CarriesAuthorityOrNote("Aralia franchetii J.Wen [illegit]"));
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void Blank_input_gives_blank_output(string value) {
        Assert.Equal("", BareScientificName.Strip(value));
    }

    // The queue also holds common-name titles from Wikidata sitelinks. Strip cuts those too (a
    // third lowercase word reads as leftovers after a binomial), so the prune test has to be
    // stricter than "Strip changed it" or it deletes real article titles.
    [Theory]
    [InlineData("Gila spotted whiptail")]
    [InlineData("Large-spined bell toad")]
    [InlineData("Black-and-white ruffed lemur")]
    [InlineData("Black-faced Impala")]
    [InlineData("Cape Town frog")]
    [InlineData("Telchinia encedon subspecies encedon")]   // not a title, but harmless: one 404
    public void A_common_name_title_is_not_read_as_an_authority(string title) {
        Assert.False(BareScientificName.CarriesAuthorityOrNote(title));
    }

    [Theory]
    [InlineData("Marlierea insignis McVaugh")]
    [InlineData("Melanoselinum edule (Lowe) Baillon")]
    [InlineData("Squalus  vacca\t Bloch & Schneider, 1801")]
    [InlineData("Hexanchus griseus ssp. australis de Buen, 1960")]
    [InlineData("Vespertilio murinus Schreber [1803]")]
    public void An_authority_after_a_binomial_is_reported(string title) {
        Assert.True(BareScientificName.CarriesAuthorityOrNote(title));
    }
}
