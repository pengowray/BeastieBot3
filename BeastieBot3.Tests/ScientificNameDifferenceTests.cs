using System.Text;
using BeastieBot3.Taxonomy;

namespace BeastieBot3.Tests;

// Pins the scientific-name difference classifier used by the CoL crosscheck audit to explain why a
// name has no exact match: spacing, punctuation, Unicode encoding, diacritics, case, combinations,
// and the Levenshtein fallback for genuine spelling variants. Non-ASCII cases use \u escapes so the
// byte-level distinction (precomposed vs. combining) is explicit and survives editing.
public class ScientificNameDifferenceTests {
    // "Daphne mulleri" with a precomposed u-umlaut (U+00FC) in the epithet.
    private const string Precomposed = "Daphne mülleri";
    // Same name with a plain ASCII "u" (no diacritic).
    private const string Plain = "Daphne mulleri";

    [Fact]
    public void Identical_IsExact() {
        Assert.Equal(ScientificNameDifference.Kind.Exact, ScientificNameDifference.Classify("Panthera leo", "Panthera leo").Kind);
    }

    [Fact]
    public void SpacingOnly_IsWhitespace() {
        var r = ScientificNameDifference.Classify("Panthera  leo", "Panthera leo");
        Assert.Equal(ScientificNameDifference.Kind.Whitespace, r.Kind);
        Assert.Contains("spacing", r.Description);
    }

    [Theory]
    [InlineData("Cyclamen novae-zelandiae", "Cyclamen novaezelandiae")]
    [InlineData("Abies alba 'Pendula'", "Abies alba Pendula")]
    public void PunctuationOnly_IsPunctuation(string a, string b) {
        var r = ScientificNameDifference.Classify(a, b);
        Assert.Equal(ScientificNameDifference.Kind.Punctuation, r.Kind);
        Assert.Contains("punctuation", r.Description);
    }

    [Fact]
    public void DiacriticOnly_IsDiacritic() {
        var r = ScientificNameDifference.Classify(Precomposed, Plain);
        Assert.Equal(ScientificNameDifference.Kind.Diacritic, r.Kind);
        Assert.Contains("diacritics", r.Description);
    }

    [Fact]
    public void SameTextDifferentNormalization_IsUnicode() {
        // Precomposed u-umlaut (U+00FC) vs. "u" + combining diaeresis (U+0308): identical text, different bytes.
        var composed = Precomposed.Normalize(NormalizationForm.FormC);
        var combining = Precomposed.Normalize(NormalizationForm.FormD);
        Assert.NotEqual(composed, combining); // sanity: the two forms really do differ byte-for-byte
        var r = ScientificNameDifference.Classify(composed, combining);
        Assert.Equal(ScientificNameDifference.Kind.Unicode, r.Kind);
        Assert.Contains("encoding", r.Description);
    }

    [Fact]
    public void CaseOnly_IsCase() {
        var r = ScientificNameDifference.Classify("Panthera Leo", "Panthera leo");
        Assert.Equal(ScientificNameDifference.Kind.Case, r.Kind);
    }

    [Fact]
    public void MultipleFormattingDimensions_IsFormatting() {
        // Letter case (capital M) and diacritics (umlaut) both differ.
        var r = ScientificNameDifference.Classify("Daphne Mülleri", "daphne mulleri");
        Assert.Equal(ScientificNameDifference.Kind.Formatting, r.Kind);
        Assert.Contains("and", r.Description);
    }

    [Theory]
    [InlineData("Panthera leo", "Panthera leoo", 1)]
    [InlineData("Loxodonta africana", "Loxodonta afrikana", 1)]
    public void SmallTypo_IsFuzzy(string a, string b, int distance) {
        var r = ScientificNameDifference.Classify(a, b);
        Assert.Equal(ScientificNameDifference.Kind.Fuzzy, r.Kind);
        Assert.Equal(distance, r.Distance);
    }

    [Fact]
    public void DifferentName_IsUnrelated() {
        Assert.Equal(ScientificNameDifference.Kind.Unrelated, ScientificNameDifference.Classify("Panthera leo", "Canis lupus").Kind);
    }

    [Fact]
    public void Levenshtein_CountsEdits() {
        Assert.Equal(0, ScientificNameDifference.Levenshtein("abc", "abc"));
        Assert.Equal(1, ScientificNameDifference.Levenshtein("abc", "abd"));
        Assert.Equal(3, ScientificNameDifference.Levenshtein("abc", ""));
    }
}
