using System.Linq;
using BeastieBot3.Audit.Producers;
using BeastieBot3.Taxonomy;

namespace BeastieBot3.Tests;

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
        Assert.True(Assert.Single(SynonymNameNotes.Find(name)).IsDate);
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
}
