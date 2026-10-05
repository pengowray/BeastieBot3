using System.Globalization;
using System.Text.RegularExpressions;

namespace BeastieBot3.Site.Update;

/// A {{Species table/row}} whose population differs from the number of mature individuals in the
/// taxon's latest global assessment. The text is not changed: the reader decides. Current: the
/// row's population without its references; IucnValue: the value as IUCN publishes it; Suggested:
/// the value written the way the species tables write numbers.
public sealed record PopulationSuggestion(int Line, StatusTaxon Taxon, string Current, string IucnValue, int? Year, string Suggested);

/// Compares a species table row's population with IUCN's number of mature individuals
/// (supplementary_info.population_size). IUCN writes a range ("1000-1200"), a number ("2177"), a range
/// and a best estimate ("500000-999999,800000"), two ranges ("2500-9999,2500-5000") or "U" (unknown).
/// The species tables write "Unknown", "2,500" and "8,000–10,000".
public static partial class PopulationValues {
    private readonly record struct Range(long Low, long High);

    /// The value to suggest for the row, or null when the row agrees with IUCN or IUCN gives nothing
    /// usable. The row agrees when its number or range matches any part of IUCN's value, end by end.
    /// An end matches when it is the same number, IUCN's number rounded the way the row writes it
    /// (2,200 for 2177, 9,000 for 8932) and no more than 5% away, or one more than IUCN's (the
    /// band "2500-9999" for "2,500–10,000").
    public static string? Suggest(string current, string? iucn) {
        if (iucn is null) {
            return null;
        }
        var row = ReadRow(current);
        if (iucn.Trim() is "U" or "u") {
            return row is { IsUnknown: true } ? null : "Unknown";
        }
        var parts = ReadIucn(iucn);
        if (parts is null) {
            return null;
        }
        if (row is { Range: { } r } && parts.Any(p => Matches(r.Low, p.Low) && Matches(r.High, p.High))) {
            return null;
        }
        return Format(parts[0]);
    }

    private static bool Matches(long row, long iucn) {
        if (row == iucn || row == iucn + 1) {
            return true;
        }
        long unit = 1;
        while (row != 0 && row % (unit * 10) == 0) {
            unit *= 10;
        }
        var difference = Math.Abs(row - iucn);
        return difference * 2 <= unit && difference * 20 <= iucn;
    }

    /// The row's population as text, with references, notes ({{efn}}) and comments removed.
    public static string Clean(string value) => Whitespace().Replace(Ref().Replace(value, " "), " ").Trim();

    private sealed record RowValue(bool IsUnknown, Range? Range);

    private static RowValue? ReadRow(string current) {
        var text = Clean(current);
        if (text.Equals("unknown", StringComparison.OrdinalIgnoreCase) || text == "?") {
            return new RowValue(true, null);
        }
        var compact = Dash().Replace(text.Replace("&nbsp;", string.Empty).Replace(",", string.Empty), "-");
        compact = Whitespace().Replace(compact, string.Empty);
        return RangeOf(compact) is { } r ? new RowValue(false, r) : null;
    }

    private static List<Range>? ReadIucn(string iucn) {
        var parts = new List<Range>();
        foreach (var part in iucn.Split(',')) {
            if (RangeOf(Whitespace().Replace(part, string.Empty)) is not { } r) {
                return null;
            }
            parts.Add(r);
        }
        return parts.Count == 0 ? null : parts;
    }

    private static Range? RangeOf(string text) {
        var m = NumberRange().Match(text);
        if (!m.Success || !long.TryParse(m.Groups["low"].Value, NumberStyles.None, CultureInfo.InvariantCulture, out var low)) {
            return null;
        }
        var high = low;
        if (m.Groups["high"].Success && !long.TryParse(m.Groups["high"].Value, NumberStyles.None, CultureInfo.InvariantCulture, out high)) {
            return null;
        }
        return new Range(low, high);
    }

    private static string Format(Range r) => r.Low == r.High
        ? N(r.Low)
        : $"{N(r.Low)}–{N(r.High)}";

    private static string N(long n) => n.ToString("N0", CultureInfo.InvariantCulture);

    [GeneratedRegex(@"^(?<low>\d+)(?:-(?<high>\d+))?$")]
    private static partial Regex NumberRange();

    // A hyphen, en dash, em dash, minus sign or "to" between two numbers.
    [GeneratedRegex(@"\s*(?:-|–|—|−|&ndash;|&mdash;|\bto\b)\s*")]
    private static partial Regex Dash();

    [GeneratedRegex(@"<ref\b[^>]*/>|<ref\b[^>]*>.*?</ref\s*>|<!--.*?-->|\{\{\s*(?:efn|refn|sfn|r|cn|citation needed)\s*(?:\|[^{}]*)?\}\}",
        RegexOptions.IgnoreCase | RegexOptions.Singleline)]
    private static partial Regex Ref();

    [GeneratedRegex(@"\s+")]
    private static partial Regex Whitespace();
}
