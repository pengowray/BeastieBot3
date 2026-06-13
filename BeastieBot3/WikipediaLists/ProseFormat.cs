namespace BeastieBot3.WikipediaLists;

// Small number-to-prose formatting helpers shared by the intro paragraphs and the parent
// summary-table sentences. Kept separate so both builders can reach them without coupling.
internal static class ProseFormat {
    // Spell out small counts ("three species") but render large ones as digits, with
    // thousands separators above 10,000 ("12,345").
    public static string NewspaperNumber(int number) {
        var words = new[] { "zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten" };
        if (number >= 0 && number <= 10) return words[number];
        if (number >= 10000) return number.ToString("N0");
        return number.ToString();
    }

    // Percentage with adaptive precision: whole percent above 10%, then 1 and 2 decimals as the
    // ratio shrinks, so tiny shares ("0.03%") stay legible.
    public static string FormatPercentage(int count, int total) {
        var ratio = (double)count / total;
        if (ratio > 0.1) return ratio.ToString("P0");
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
