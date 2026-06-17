namespace BeastieBot3.WikipediaLists;

// Small number-to-prose formatting helpers shared by the intro paragraphs and the parent
// summary-table sentences. Kept separate so both builders can reach them without coupling.
internal static class ProseFormat {
    // Spell out small counts ("three species") but render anything above ten as digits with a
    // thousands separator ("6,445", "12,345") so all multi-digit counts group consistently.
    public static string NewspaperNumber(int number) {
        var words = new[] { "zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten" };
        if (number >= 0 && number <= 10) return words[number];
        return number.ToString("N0");
    }

    // Percentage with adaptive precision: one decimal across the common 1%–100% range (so neighbouring
    // lists read "9.9%" / "10.0%" / "11.0%" consistently), then two decimals for tiny sub-1% shares.
    public static string FormatPercentage(int count, int total) {
        var ratio = (double)count / total;
        if (ratio > 0.01) return ratio.ToString("P1");
        return ratio.ToString("P2");
    }

    // Converts a taxonomic name to title case (e.g. "ARTIODACTYLA" → "Artiodactyla").
    public static string ToTitleCase(string value) {
        if (string.IsNullOrWhiteSpace(value)) {
            return value;
        }

        return char.ToUpperInvariant(value[0]) + value[1..].ToLowerInvariant();
    }

    // Uppercases only the first character, leaving the rest untouched (e.g. "bald eagle" → "Bald eagle").
    public static string? Uppercase(string? value) {
        if (string.IsNullOrWhiteSpace(value)) {
            return value;
        }

        return value.Length == 1
            ? value.ToUpperInvariant()
            : char.ToUpperInvariant(value[0]) + value[1..];
    }
}
