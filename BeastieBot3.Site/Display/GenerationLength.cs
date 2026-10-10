using System.Text.RegularExpressions;

namespace BeastieBot3.Site.Display;

/// Generation length as the page shows it, from the value IUCN publishes in years
/// (assessment.generation_length): "6.5" is "6.5 years", "10-15" is "10–15 years", "10-200,60"
/// (a range and a best estimate, as IUCN writes population sizes) is "10–200 years (best estimate
/// 60)". Any other text ("<10years", "10 years") is shown as published, and a blank or "unknown"
/// value is not shown.
public static partial class GenerationLength {
    public static string? Display(string? value) {
        var text = value?.Trim() ?? string.Empty;
        if (text.Length == 0 || text.Equals("unknown", StringComparison.OrdinalIgnoreCase)) {
            return null;
        }
        if (WithBest().Match(text) is { Success: true } withBest) {
            return SiteText.GenerationLengthBest(Range(withBest.Groups["range"].Value), Range(withBest.Groups["best"].Value));
        }
        if (Value().IsMatch(text)) {
            return SiteText.GenerationLengthYears(Range(text), text == "1");
        }
        return text;
    }

    // "10-15" as "10–15" (an en dash between the ends of a range).
    private static string Range(string text) => Dash().Replace(text.Trim(), "–");

    [GeneratedRegex(@"^\d+(\.\d+)?(\s*-\s*\d+(\.\d+)?)?$")]
    private static partial Regex Value();

    [GeneratedRegex(@"^(?<range>\d+(\.\d+)?\s*-\s*\d+(\.\d+)?)\s*,\s*(?<best>\d+(\.\d+)?(\s*-\s*\d+(\.\d+)?)?)$")]
    private static partial Regex WithBest();

    [GeneratedRegex(@"\s*-\s*")]
    private static partial Regex Dash();
}
