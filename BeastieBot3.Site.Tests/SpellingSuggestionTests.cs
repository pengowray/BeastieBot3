using BeastieBot3.Site.Data;

namespace BeastieBot3.Site.Tests;

public sealed class SpellingSuggestionTests {
    private static readonly NameWordIndex Index = new([
        ("panthera", 12), ("leo", 9), ("pardus", 7), ("lion", 30), ("koala", 4), ("abditomys", 1), ("latidens", 2), ("lea", 1),
    ]);

    [Theory]
    [InlineData("panthera", "pantera", 1)]
    [InlineData("panthera", "pnathera", 1)]
    [InlineData("panthera", "panthreaa", 2)]
    [InlineData("koala", "kaola", 1)]
    [InlineData("koala", "kowalski", 3)]
    public void DistanceCountsChangedLettersAndSwappedNeighbours(string a, string b, int distance) =>
        Assert.Equal(distance, NameWordIndex.Distance(a, b, 2));

    [Fact]
    public void MisspelledWordsAreReplaced() =>
        Assert.Equal(["panthera leo"], SpellingSuggestions.Candidates("Pantera leoo", Index, 8));

    [Fact]
    public void AmongWordsAsCloseTheMostUsedComesFirst() =>
        // "lex" is one letter from both "leo" (9 names) and "lea" (1 name).
        Assert.Equal(["panthera leo", "panthera lea"], SpellingSuggestions.Candidates("Panthera lex", Index, 8));

    [Fact]
    public void ACorrectlySpelledTextGivesNoCandidates() =>
        Assert.Empty(SpellingSuggestions.Candidates("Panthera leo", Index, 8));

    [Fact]
    public void AWordWithNothingSimilarGivesNoCandidates() =>
        Assert.Empty(SpellingSuggestions.Candidates("Pantera qwxzvbnm", Index, 8));

    [Fact]
    public void ShortWordsAllowOneChangedLetter() {
        Assert.Equal(["lion"], SpellingSuggestions.Candidates("lino", Index, 8));
        Assert.Empty(SpellingSuggestions.Candidates("lnoi", Index, 8));
    }
}
